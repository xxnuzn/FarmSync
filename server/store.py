"""저장소 — 사용자(역할별 프로필), 친구 관계, 1:1 대화, 메시지 이력.

이 파일이 보장하는 것
  · **전역 순번**      : messages.seq AUTOINCREMENT — 대화 순서를 서버가 확정한다 (L5 · L7)
  · **멱등성**         : UNIQUE(conv, cid) — 재전송이 중복 주문이 되지 않는다 (L7)
  · **수신 오프셋**    : since(conv, after_seq) — 못 받은 구간만 정확히 돌려준다 (L5)
  · **역할별 프로필**  : 농장주와 기업의 회원가입 항목이 다르다 (L7 응용 계층)
"""

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    login_id   TEXT NOT NULL UNIQUE,
    pw_hash    TEXT NOT NULL,
    role       TEXT NOT NULL,          -- 'farmer' | 'buyer'
    name       TEXT NOT NULL,
    phone      TEXT,
    -- 농장주 전용
    farm_name  TEXT,
    region     TEXT,
    crops      TEXT,
    area       TEXT,
    -- 기업 전용
    company    TEXT,
    dept       TEXT,
    position   TEXT,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    token      TEXT PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);

CREATE TABLE IF NOT EXISTS friendships (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    requester  INTEGER NOT NULL,
    addressee  INTEGER NOT NULL,
    status     TEXT NOT NULL,          -- 'pending' | 'accepted'
    created_at REAL NOT NULL,
    UNIQUE(requester, addressee)
);
CREATE INDEX IF NOT EXISTS idx_friend_req ON friendships(requester);
CREATE INDEX IF NOT EXISTS idx_friend_add ON friendships(addressee);

CREATE TABLE IF NOT EXISTS conversations (
    conv_key   TEXT PRIMARY KEY,       -- "작은id-큰id"
    user_a     INTEGER NOT NULL,
    user_b     INTEGER NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    conv   TEXT NOT NULL,
    cid    TEXT NOT NULL,
    sender INTEGER NOT NULL,
    text   TEXT NOT NULL,
    ts     REAL NOT NULL,
    -- 'chat' = 사람이 쓴 글, 'event_invite' = 공동 일정 초대 카드 (ref = 초대 번호)
    kind   TEXT NOT NULL DEFAULT 'chat',
    ref    INTEGER,
    UNIQUE(conv, cid)
);
CREATE INDEX IF NOT EXISTS idx_msg_conv_seq ON messages(conv, seq);

CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    owner     INTEGER NOT NULL,          -- 만든 사람
    conv      TEXT,                      -- 공유 일정이면 대화방 키, 개인 일정이면 NULL
    scope     TEXT NOT NULL,             -- 'personal' | 'shared'
    title     TEXT NOT NULL,
    note      TEXT DEFAULT '',
    start_ts  INTEGER NOT NULL,          -- 밀리초
    end_ts    INTEGER,
    created_at INTEGER NOT NULL,
    -- 'pending'   = 초대를 보내 상대의 수락을 기다리는 공동 일정
    -- 'confirmed' = 확정 (개인 일정, 또는 상대가 수락한 공동 일정)
    status    TEXT NOT NULL DEFAULT 'confirmed'
);
CREATE INDEX IF NOT EXISTS idx_ev_owner ON events(owner, start_ts);

-- 공동 일정 초대 (L7 응용 계층의 '전달 확인' 규약).
-- 만든 사람이 초대를 남기고, 받는 사람이 수락해야 그 사람 캘린더에 들어간다.
CREATE TABLE IF NOT EXISTS event_invites (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    INTEGER NOT NULL,
    conv        TEXT NOT NULL,
    from_user   INTEGER NOT NULL,
    to_user     INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',   -- 'pending' | 'accepted'
    created_at  INTEGER NOT NULL,
    accepted_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_inv_to ON event_invites(to_user, status);

CREATE TABLE IF NOT EXISTS receipts (
    seq    INTEGER NOT NULL,
    reader INTEGER NOT NULL,
    ts     REAL NOT NULL,
    PRIMARY KEY(seq, reader)
);
"""

PROFILE_FIELDS_FARMER = ("farm_name", "region", "crops", "area")
PROFILE_FIELDS_BUYER = ("company", "dept", "position")


def conv_key(a: int, b: int) -> str:
    lo, hi = (a, b) if a < b else (b, a)
    return f"{lo}-{hi}"


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        if path != ":memory:":
            Path(path).resolve().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            if path != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(SCHEMA)
            self._migrate()
            self._conn.commit()
        self.counters: Dict[str, int] = {
            "signups": 0,
            "logins": 0,
            "appended": 0,
            "duplicates_ignored": 0,
            "resyncs": 0,
            "resync_messages": 0,
            "friend_requests": 0,
            "friends_accepted": 0,
        }
        self._last_signup_error: str = ""

    # ---------------------------------------------------------------- 마이그레이션
    def _migrate(self) -> None:
        """이미 쓰던 DB 에도 새 열/표를 더한다 (기존 대화·일정은 그대로 보존)."""
        def cols(table: str) -> set:
            return {r["name"] for r in self._conn.execute(f"PRAGMA table_info({table})").fetchall()}

        for table, col, ddl in (
            ("messages", "kind", "kind TEXT NOT NULL DEFAULT 'chat'"),
            ("messages", "ref", "ref INTEGER"),
            ("events", "status", "status TEXT NOT NULL DEFAULT 'confirmed'"),
        ):
            try:
                if col not in cols(table):
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
            except sqlite3.OperationalError:
                pass        # 표가 없는 아주 옛 DB — SCHEMA 가 새로 만든다

        # 열을 더한 뒤에야 걸 수 있는 인덱스 (status 는 나중에 생긴 열이다)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ev_scope ON events(scope, status, start_ts)"
        )

    # ================================================================ 사용자
    def create_user(self, data: Dict[str, Any]) -> Optional[int]:
        now = time.time()
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO users(login_id, pw_hash, role, name, phone, farm_name, region, crops, area,"
                    " company, dept, position, created_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        data["login_id"].strip(),
                        data["pw_hash"],
                        data["role"],
                        data.get("name", "").strip(),
                        (data.get("phone") or "").strip() or None,
                        (data.get("farm_name") or "").strip() or None,
                        (data.get("region") or "").strip() or None,
                        (data.get("crops") or "").strip() or None,
                        (data.get("area") or "").strip() or None,
                        (data.get("company") or "").strip() or None,
                        (data.get("dept") or "").strip() or None,
                        (data.get("position") or "").strip() or None,
                        now,
                    ),
                )
                self._conn.commit()
                self.counters["signups"] += 1
                self._last_signup_error = ""
                return int(cur.lastrowid)
            except sqlite3.IntegrityError:
                self._last_signup_error = "duplicate_login_id"
                return None

    def last_signup_error(self) -> str:
        return self._last_signup_error

    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        return self._public(row) if row else None

    def get_user_raw(self, login_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM users WHERE login_id=?", (login_id.strip(),)).fetchone()
        return dict(row) if row else None

    def list_users(self, role: Optional[str] = None, q: str = "", exclude: int = 0, limit: int = 200) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM users WHERE id != ?"
        args: List[Any] = [exclude]
        if role in ("farmer", "buyer"):
            sql += " AND role = ?"
            args.append(role)
        if q:
            like = f"%{q.strip()}%"
            sql += (" AND (name LIKE ? OR login_id LIKE ? OR IFNULL(farm_name,'') LIKE ?"
                    " OR IFNULL(company,'') LIKE ? OR IFNULL(region,'') LIKE ? OR IFNULL(crops,'') LIKE ?)")
            args += [like] * 6
        sql += " ORDER BY role, name LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [self._public(r) for r in rows]

    @staticmethod
    def _public(row: sqlite3.Row) -> Dict[str, Any]:
        role = row["role"]
        out: Dict[str, Any] = {
            "id": int(row["id"]),
            "login_id": row["login_id"],
            "role": role,
            "role_label": "농장주" if role == "farmer" else "기업",
            "name": row["name"],
            "phone": row["phone"],
        }
        if role == "farmer":
            out["farm_name"] = row["farm_name"]
            out["region"] = row["region"]
            out["crops"] = row["crops"]
            out["area"] = row["area"]
            out["subtitle"] = " · ".join(
                [x for x in (row["farm_name"], row["region"], row["crops"]) if x]
            )
        else:
            out["company"] = row["company"]
            out["dept"] = row["dept"]
            out["position"] = row["position"]
            out["subtitle"] = " · ".join(
                [x for x in (row["company"], row["dept"], row["position"]) if x]
            )
        return out

    # ================================================================ 세션
    def open_session(self, token: str, user_id: int, expires_at: float) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO sessions(token, user_id, created_at, expires_at) VALUES(?,?,?,?)",
                (token, user_id, time.time(), expires_at),
            )
            self._conn.commit()

    def user_for_token(self, token: str) -> Optional[int]:
        if not token:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT user_id, expires_at FROM sessions WHERE token=?", (token,)
            ).fetchone()
        if not row:
            return None
        if row["expires_at"] < time.time():
            self.close_session(token)
            return None
        return int(row["user_id"])

    def close_session(self, token: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM sessions WHERE token=?", (token,))
            self._conn.commit()

    # ================================================================ 친구
    def friend_status(self, a: int, b: int) -> str:
        """'none' | 'requested'(내가 보냄) | 'incoming'(받음) | 'friends'"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT requester, addressee, status FROM friendships "
                "WHERE (requester=? AND addressee=?) OR (requester=? AND addressee=?)",
                (a, b, b, a),
            ).fetchall()
        for r in rows:
            if r["status"] == "accepted":
                return "friends"
        for r in rows:
            if int(r["requester"]) == a:
                return "requested"
            return "incoming"
        return "none"

    def request_friend(self, requester: int, addressee: int) -> str:
        """친구 요청. 상대가 이미 요청했다면 바로 수락된다."""
        if requester == addressee:
            return "self"
        status = self.friend_status(requester, addressee)
        if status == "friends":
            return "already"
        with self._lock:
            if status == "incoming":
                self._conn.execute(
                    "UPDATE friendships SET status='accepted' WHERE requester=? AND addressee=?",
                    (addressee, requester),
                )
                self._conn.commit()
                self.counters["friends_accepted"] += 1
                return "accepted"
            if status == "requested":
                return "already"
            self._conn.execute(
                "INSERT INTO friendships(requester, addressee, status, created_at) VALUES(?,?,'pending',?)",
                (requester, addressee, time.time()),
            )
            self._conn.commit()
            self.counters["friend_requests"] += 1
            return "pending"

    def accept_friend(self, addressee: int, requester: int) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE friendships SET status='accepted' WHERE requester=? AND addressee=? AND status='pending'",
                (requester, addressee),
            )
            self._conn.commit()
            if cur.rowcount:
                self.counters["friends_accepted"] += 1
                return True
        return False

    def reject_friend(self, addressee: int, requester: int) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM friendships WHERE requester=? AND addressee=? AND status='pending'",
                (requester, addressee),
            )
            self._conn.commit()
        return bool(cur.rowcount)

    def friends(self, user_id: int) -> List[Dict[str, Any]]:
        """수락된 친구 목록 (역할·프로필 포함)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT u.* FROM friendships f "
                "JOIN users u ON u.id = CASE WHEN f.requester=? THEN f.addressee ELSE f.requester END "
                "WHERE f.status='accepted' AND (f.requester=? OR f.addressee=?) "
                "ORDER BY u.role, u.name",
                (user_id, user_id, user_id),
            ).fetchall()
        return [self._public(r) for r in rows]

    def friend_ids(self, user_id: int) -> List[int]:
        return [f["id"] for f in self.friends(user_id)]

    def incoming_requests(self, user_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT u.*, f.created_at AS asked_at FROM friendships f "
                "JOIN users u ON u.id = f.requester "
                "WHERE f.addressee=? AND f.status='pending' ORDER BY f.created_at DESC",
                (user_id,),
            ).fetchall()
        out = []
        for r in rows:
            p = self._public(r)
            p["asked_at"] = r["asked_at"]
            out.append(p)
        return out

    def outgoing_requests(self, user_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT u.* FROM friendships f JOIN users u ON u.id = f.addressee "
                "WHERE f.requester=? AND f.status='pending' ORDER BY u.name",
                (user_id,),
            ).fetchall()
        return [self._public(r) for r in rows]

    # ================================================================ 대화
    def ensure_conversation(self, a: int, b: int) -> str:
        key = conv_key(a, b)
        lo, hi = (a, b) if a < b else (b, a)
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO conversations(conv_key, user_a, user_b, created_at) VALUES(?,?,?,?)",
                (key, lo, hi, time.time()),
            )
            self._conn.commit()
        return key

    def is_participant(self, key: str, user_id: int) -> bool:
        lo, hi = key.split("-")
        return user_id in (int(lo), int(hi))

    def other_party(self, key: str, user_id: int) -> int:
        lo, hi = (int(x) for x in key.split("-"))
        return hi if user_id == lo else lo

    @staticmethod
    def normalize_ts(ts: Any = None) -> int:
        """타임스탬프를 **밀리초**로 통일한다 (초 단위로 들어오면 환산)."""
        try:
            v = float(ts)
        except (TypeError, ValueError):
            v = 0.0
        if v <= 0:
            return int(time.time() * 1000)
        return int(v * 1000) if v < 1e11 else int(v)

    def append(self, conv: str, cid: str, sender: int, text: str, ts: Any = None,
               kind: str = "chat", ref: Optional[int] = None) -> Tuple[int, bool]:
        """(seq, duplicate) 반환. 같은 cid 는 새 행을 만들지 않는다.

        kind='event_invite' 로 넣으면 대화창에서 공동 일정 초대 카드로 그려진다
        (ref = event_invites.id).
        """
        now = self.normalize_ts(ts)
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO messages(conv, cid, sender, text, ts, kind, ref) VALUES(?,?,?,?,?,?,?)",
                    (conv, cid, sender, text, now, kind or "chat", ref),
                )
                self._conn.commit()
                self.counters["appended"] += 1
                return int(cur.lastrowid), False
            except sqlite3.IntegrityError:
                row = self._conn.execute(
                    "SELECT seq FROM messages WHERE conv=? AND cid=?", (conv, cid)
                ).fetchone()
                self.counters["duplicates_ignored"] += 1
                return (int(row["seq"]) if row else 0), True

    def since(self, conv: str, after_seq: int, limit: int = 1000) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, cid, sender, text, ts, kind, ref FROM messages "
                "WHERE conv=? AND seq>? ORDER BY seq ASC LIMIT ?",
                (conv, after_seq, limit),
            ).fetchall()
            receipts = self._receipt_map(conv)
            rt = max(0, len(self.participants(conv)) - 1)
            return [self._row(r, receipts, rt) for r in rows]

    def history(self, conv: str, limit: int = 300) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM (SELECT seq, cid, sender, text, ts, kind, ref FROM messages "
                "WHERE conv=? ORDER BY seq DESC LIMIT ?) ORDER BY seq ASC",
                (conv, limit),
            ).fetchall()
            receipts = self._receipt_map(conv)
            rt = max(0, len(self.participants(conv)) - 1)
            return [self._row(r, receipts, rt) for r in rows]

    def latest_seq(self, conv: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(seq) AS m FROM messages WHERE conv=?", (conv,)
            ).fetchone()
        return int(row["m"]) if row and row["m"] is not None else 0

    def _receipt_map(self, conv: str) -> Dict[int, List[int]]:
        rows = self._conn.execute(
            "SELECT r.seq, r.reader FROM receipts r JOIN messages m ON m.seq=r.seq WHERE m.conv=?",
            (conv,),
        ).fetchall()
        out: Dict[int, List[int]] = {}
        for r in rows:
            out.setdefault(int(r["seq"]), []).append(int(r["reader"]))
        return out

    def _row(self, r: sqlite3.Row, receipts: Dict[int, List[int]], reader_total: int = 0) -> Dict[str, Any]:
        seq = int(r["seq"])
        keys = set(r.keys())
        kind = (r["kind"] if "kind" in keys else None) or "chat"
        ref = r["ref"] if "ref" in keys else None
        out: Dict[str, Any] = {
            "seq": seq,
            "cid": r["cid"],
            "sender": int(r["sender"]),
            "text": r["text"],
            "ts": Store.normalize_ts(r["ts"]),
            "read_by": receipts.get(seq, []),
            "read_total": reader_total,
            "kind": kind,
            "ref": int(ref) if ref else None,
        }
        # 초대 카드에는 화면에 그릴 내용(제목·시간·수락 여부)을 함께 실어 보낸다
        if kind == "event_invite" and out["ref"]:
            inv = self.invite_payload(out["ref"])
            if inv:
                out["invite"] = inv
        return out

    def add_receipt(self, seq: int, reader: int) -> bool:
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO receipts(seq, reader, ts) VALUES(?,?,?)", (seq, reader, time.time())
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def participants(self, key: str) -> List[int]:
        """대화방 참여자 목록. 지금은 1:1 이라 두 명이다 (단체방으로 확장되는 자리)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT user_a, user_b FROM conversations WHERE conv_key=?", (key,)
            ).fetchone()
        return [row["user_a"], row["user_b"]] if row else []

    def reader_total(self, key: str, sender: int) -> int:
        """이 메시지를 읽어야 할 사람 수 (보낸 사람 제외). 1:1 이면 1, 3인방이면 2."""
        return max(0, len([p for p in self.participants(key) if p != sender]))

    def last_message(self, conv: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM messages WHERE conv=? ORDER BY seq DESC LIMIT 1", (conv,)
            ).fetchone()
            if not row:
                return None
            receipts = self._receipt_map(conv)
        return self._row(row, receipts, 0)

    def unread_count(self, conv: str, user_id: int) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) c FROM messages m WHERE m.conv=? AND m.sender!=? "
                "AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.seq=m.seq AND r.reader=?)",
                (conv, user_id, user_id),
            ).fetchone()
        return int(row["c"])

    def conversation_summary(self, user_id: int) -> List[Dict[str, Any]]:
        """친구별 마지막 메시지 시각 — 대화 목록 정렬용."""
        out: List[Dict[str, Any]] = []
        for f in self.friends(user_id):
            key = conv_key(user_id, f["id"])
            hist = self.history(key, limit=1)
            last = hist[-1] if hist else None
            out.append({
                "friend": f,
                "conv": key,
                "latest_seq": self.latest_seq(key),
                "last_message": last,
            })
        return out

    # ---------------------------------------------------------------- 일정
    def add_event(self, owner: int, scope: str, title: str, note: str,
                  start_ts: Any, end_ts: Any = None, conv: Optional[str] = None,
                  status: str = "confirmed") -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO events(owner, conv, scope, title, note, start_ts, end_ts, created_at, status) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (owner, conv if scope == "shared" else None, scope, title, note or "",
                 self.normalize_ts(start_ts), self.normalize_ts(end_ts) if end_ts else None,
                 int(time.time() * 1000), status),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def events_between(self, user_id: int, start_ts: int, end_ts: int) -> List[Dict[str, Any]]:
        """내 개인 일정 + 내가 참여한 대화방의 공유 일정 (남의 개인 일정은 절대 포함되지 않는다)."""
        with self._lock:
            # 개인 일정만 여기서 가져온다. 공유 일정은 아래 대화방 조회에서 한 번만 가져온다
            # (scope 조건이 없으면 내가 만든 공유 일정이 두 번 잡혀 2개로 보인다).
            mine = self._conn.execute(
                "SELECT * FROM events WHERE owner=? AND scope='personal' "
                "AND start_ts>=? AND start_ts<? ORDER BY start_ts",
                (user_id, start_ts, end_ts),
            ).fetchall()
            convs = [r["conv_key"] for r in self._conn.execute(
                "SELECT conv_key FROM conversations WHERE user_a=? OR user_b=?", (user_id, user_id)
            ).fetchall()]
            shared = []
            if convs:
                q = ",".join("?" * len(convs))
                # 공동 일정이라도 내가 만든 것이 아니면 '수락한 것'만 보인다.
                # 초대만 받고 아직 수락하지 않은 일정은 내 캘린더에 들어오지 않는다.
                shared = self._conn.execute(
                    f"SELECT * FROM events WHERE scope='shared' AND conv IN ({q}) "
                    "AND (owner=? OR status='confirmed') "
                    "AND start_ts>=? AND start_ts<? ORDER BY start_ts",
                    (*convs, user_id, start_ts, end_ts),
                ).fetchall()
            owner_names = {r["id"]: r["name"] for r in self._conn.execute("SELECT id, name FROM users")}
            out = []
            for r in list(mine) + list(shared):
                out.append({
                    "id": int(r["id"]), "owner": int(r["owner"]),
                    "owner_name": owner_names.get(r["owner"], ""),
                    "mine": int(r["owner"]) == user_id,
                    "scope": r["scope"], "conv": r["conv"], "title": r["title"],
                    "note": r["note"] or "", "start_ts": self.normalize_ts(r["start_ts"]),
                    "end_ts": self.normalize_ts(r["end_ts"]) if r["end_ts"] else None,
                    "status": (r["status"] if "status" in set(r.keys()) else None) or "confirmed",
                })
            out.sort(key=lambda x: x["start_ts"])
            return out

    def event_owner(self, event_id: int) -> Optional[int]:
        with self._lock:
            row = self._conn.execute("SELECT owner FROM events WHERE id=?", (event_id,)).fetchone()
        return int(row["owner"]) if row else None

    def delete_event(self, event_id: int, user_id: int) -> bool:
        """자기 일정만 지울 수 있다 (공유 일정도 만든 사람만)."""
        with self._lock:
            cur = self._conn.execute("DELETE FROM events WHERE id=? AND owner=?", (event_id, user_id))
            self._conn.commit()
        return cur.rowcount > 0

    # ------------------------------------------------------------ 공동 일정 초대
    def add_invite(self, event_id: int, conv: str, from_user: int, to_user: int) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO event_invites(event_id, conv, from_user, to_user, status, created_at) "
                "VALUES(?,?,?,?, 'pending', ?)",
                (event_id, conv, from_user, to_user, int(time.time() * 1000)),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def invite_payload(self, invite_id: int) -> Optional[Dict[str, Any]]:
        """초대 하나를 화면에 그릴 수 있는 형태로 돌려준다 (대화창 카드용)."""
        if not invite_id:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM event_invites WHERE id=?", (int(invite_id),)
            ).fetchone()
            if not row:
                return None
            ev = self._conn.execute(
                "SELECT * FROM events WHERE id=?", (int(row["event_id"]),)
            ).fetchone()
            names = {r["id"]: r["name"] for r in self._conn.execute("SELECT id, name FROM users")}
        if not ev:
            return None
        return {
            "invite_id": int(row["id"]),
            "event_id": int(row["event_id"]),
            "conv": row["conv"],
            "from_user": int(row["from_user"]),
            "from_name": names.get(int(row["from_user"]), ""),
            "to_user": int(row["to_user"]),
            "to_name": names.get(int(row["to_user"]), ""),
            "status": row["status"],
            "accepted_at": int(row["accepted_at"]) if row["accepted_at"] else None,
            "title": ev["title"],
            "note": ev["note"] or "",
            "start_ts": self.normalize_ts(ev["start_ts"]),
            "end_ts": self.normalize_ts(ev["end_ts"]) if ev["end_ts"] else None,
        }

    def invite_accept(self, invite_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        """받는 사람이 수락 → 그 일정을 확정해 수락자의 캘린더에도 나타나게 한다."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM event_invites WHERE id=?", (int(invite_id),)
            ).fetchone()
            if not row or int(row["to_user"]) != int(user_id):
                return None
            if row["status"] != "accepted":
                self._conn.execute(
                    "UPDATE event_invites SET status='accepted', accepted_at=? WHERE id=?",
                    (int(time.time() * 1000), int(invite_id)),
                )
                self._conn.execute(
                    "UPDATE events SET status='confirmed' WHERE id=?", (int(row["event_id"]),)
                )
                self._conn.commit()
        return self.invite_payload(invite_id)

    def pending_invites(self, user_id: int) -> List[Dict[str, Any]]:
        """아직 수락하지 않은 내 초대 목록."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM event_invites WHERE to_user=? AND status='pending' ORDER BY id",
                (int(user_id),),
            ).fetchall()
        return [x for x in (self.invite_payload(int(r["id"])) for r in rows) if x]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
