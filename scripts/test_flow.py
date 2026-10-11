"""종단 간 검증 — 회원가입 → 로그인 → 친구 요청/수락 → 1:1 대화 → 오프라인 재동기화.

실제 서버를 띄우고 HTTP + WebSocket 으로 전체 흐름을 통과시킨 뒤 판정을 출력한다.

실행
    python -u scripts/test_flow.py
"""

import asyncio
import http.cookiejar
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import websockets

ROOT = Path(__file__).resolve().parents[1]
PORT = os.environ.get("FLOW_PORT", "8877")
BASE = f"http://127.0.0.1:{PORT}"


class Session:
    """쿠키를 유지하는 HTTP 클라이언트 (로그인 세션 대역)."""

    def __init__(self) -> None:
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar))
        self.token = None

    def call(self, method: str, path: str, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(BASE + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with self.opener.open(req, timeout=15) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read())
            except Exception:
                return e.code, {}

    def cookie_header(self) -> str:
        return "; ".join(f"{c.name}={c.value}" for c in self.jar)


class WsClient:
    def __init__(self, sess: Session, name: str):
        self.sess = sess
        self.name = name
        self.ws = None
        self.inbox = []
        self.last_seq = 0
        self.dups = 0
        self.resync = []
        self.outbox = []

    async def connect(self, conv: str):
        self.ws = await websockets.connect(
            f"ws://127.0.0.1:{PORT}/ws",
            additional_headers={"Cookie": self.sess.cookie_header()},
            max_size=4 * 1024 * 1024,
        )
        await self.ws.send(json.dumps({"type": "HELLO", "conv": conv, "last_seq": self.last_seq}))
        while True:
            m = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=15))
            self._dispatch(m)
            if m.get("type") == "WELCOME":
                if m.get("latest_seq", 0) > self.last_seq:
                    m2 = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=15))
                    self._dispatch(m2)
                break
        return self

    def _dispatch(self, m):
        t = m.get("type")
        if t == "SYNC":
            for r in m["messages"]:
                self._merge(r)
            self.resync.append((m["from_seq"], m["to_seq"], m["count"]))
        elif t == "MSG":
            self._merge(m)
        elif t == "DELIVERED" and m.get("duplicate"):
            self.dups += 1

    def _merge(self, r):
        if any(x["seq"] == r["seq"] for x in self.inbox):
            return
        self.inbox.append({"seq": r["seq"], "cid": r.get("cid"), "sender": r.get("sender"),
                           "text": r["text"]})
        self.inbox.sort(key=lambda x: x["seq"])
        self.last_seq = max(self.last_seq, r["seq"])

    async def send(self, text):
        cid = str(uuid.uuid4())
        await self.ws.send(json.dumps({"type": "SEND", "cid": cid, "text": text, "ts": time.time()}))
        return cid

    def queue(self, text):
        cid = str(uuid.uuid4())
        self.outbox.append({"cid": cid, "text": text, "sent": False})
        return cid

    async def flush(self, force=False):
        n = 0
        for it in self.outbox:
            if it["sent"] and not force:
                continue
            await self.ws.send(json.dumps({"type": "SEND", "cid": it["cid"], "text": it["text"], "ts": time.time()}))
            it["sent"] = True
            n += 1
        return n

    async def drain(self, sec=1.0):
        end = time.time() + sec
        while True:
            remain = end - time.time()
            if remain <= 0:
                return
            try:
                m = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=remain))
            except asyncio.TimeoutError:
                return
            self._dispatch(m)

    async def cut(self):
        await self.ws.close()
        self.ws = None


async def main() -> int:
    db = ROOT / "data" / "flow.db"
    for f in (db, Path(str(db) + "-wal"), Path(str(db) + "-shm")):
        if f.exists():
            f.unlink()
    env = dict(os.environ, FARMSYNC_DB=str(db))
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "server.app:app", "--host", "127.0.0.1", "--port", PORT, "--log-level", "warning"],
        cwd=str(ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(60):
        try:
            with urllib.request.urlopen(f"{BASE}/healthz", timeout=1):
                break
        except Exception:
            time.sleep(0.3)
    else:
        proc.terminate()
        print("서버 기동 실패")
        return 1

    checks = {}
    print("=" * 74)
    print(" FarmSync 종단 간 검증 — 가입 · 친구 · 1:1 대화 · 오프라인 재동기화")
    print("=" * 74)

    sfx = str(int(time.time()))[-6:]
    farmer = Session()
    buyer = Session()

    # ---------------- 1) 회원가입 (농장주 / 기업 분리 폼)
    st, j = farmer.call("POST", "/api/signup", {
        "login_id": "farm" + sfx, "password": "secret123", "password2": "secret123",
        "role": "farmer", "name": "김농부", "farm_name": "초록들녘 농장",
        "region": "강원 평창", "crops": "상추, 토마토", "area": "1,200㎡", "phone": "010-1111-2222",
    })
    checks["농장주 회원가입"] = st == 200 and j.get("ok")
    farmer_id = (j.get("user") or {}).get("id")
    print(f"\n[1] 농장주 회원가입 → {st} {'성공' if checks['농장주 회원가입'] else j}")

    st, j = buyer.call("POST", "/api/signup", {
        "login_id": "buy" + sfx, "password": "secret123", "password2": "secret123",
        "role": "buyer", "name": "김서연", "company": "한빛유통", "dept": "구매팀",
        "position": "과장", "phone": "010-3333-4444",
    })
    checks["기업 회원가입"] = st == 200 and j.get("ok")
    buyer_id = (j.get("user") or {}).get("id")
    print(f"[1] 기업 회원가입   → {st} {'성공' if checks['기업 회원가입'] else j}")

    # 검증: 필수 항목 누락은 거부되어야 한다
    st, j = Session().call("POST", "/api/signup", {
        "login_id": "x" + sfx, "password": "secret123", "password2": "secret123",
        "role": "farmer", "name": "이름만", "farm_name": "", "region": "", "crops": "",
    })
    checks["필수 항목 누락 거부"] = st == 400 and "region" in (j.get("errors") or {}) and "crops" in (j.get("errors") or {})
    print(f"[1] 필수 항목 누락   → {st} {list((j.get('errors') or {}).keys())}")

    # 검증: 아이디 중복 거부
    st, j = Session().call("POST", "/api/signup", {
        "login_id": "farm" + sfx, "password": "secret123", "password2": "secret123",
        "role": "farmer", "name": "중복", "farm_name": "F", "region": "R", "crops": "C",
    })
    checks["아이디 중복 거부"] = st == 409
    print(f"[1] 아이디 중복      → {st} {j.get('detail')}")

    # ---------------- 2) 로그인
    fresh = Session()
    st, j = fresh.call("POST", "/api/login", {"login_id": "farm" + sfx, "password": "secret123"})
    checks["로그인"] = st == 200 and j.get("ok")
    print(f"\n[2] 로그인 → {st} {'성공' if checks['로그인'] else j}")
    st, j = Session().call("POST", "/api/login", {"login_id": "farm" + sfx, "password": "wrongpw"})
    checks["잘못된 비밀번호 거부"] = st == 401
    print(f"[2] 잘못된 비밀번호 → {st} {j.get('detail')}")

    # 로그인 안 한 상태는 차단
    st, j = Session().call("GET", "/api/friends")
    checks["미로그인 차단"] = st == 401
    print(f"[2] 미로그인 접근   → {st} {j.get('detail')}")

    # ---------------- 3) 상대 검색 · 친구 요청/수락
    st, j = farmer.call("GET", "/api/users?role=buyer&q=" + urllib.parse.quote("한빛"))
    found = [u for u in j.get("users", []) if u["id"] == buyer_id]
    checks["기업 목록 검색"] = st == 200 and len(found) == 1
    print(f"\n[3] 기업 검색 '한빛' → {len(j.get('users', []))}건 {found[0]['subtitle'] if found else ''}")

    st, j = farmer.call("POST", "/api/friends/request", {"user_id": buyer_id})
    checks["친구 요청"] = st == 200 and j.get("result") == "pending"
    print(f"[3] 친구 요청 → {j.get('detail')}")

    st, j = buyer.call("GET", "/api/friends")
    checks["받은 요청 표시"] = any(u["id"] == farmer_id for u in j.get("incoming", []))
    print(f"[3] 받은 요청 목록 → {len(j.get('incoming', []))}건")

    st, j = buyer.call("POST", "/api/friends/accept", {"user_id": farmer_id})
    checks["친구 수락"] = st == 200 and j.get("ok")
    conv = j.get("conv")
    print(f"[3] 친구 수락 → {j.get('detail')} · 대화방 {conv}")

    # 친구가 아니면 대화 차단 (제3자)
    third = Session()
    third.call("POST", "/api/signup", {
        "login_id": "third" + sfx, "password": "secret123", "password2": "secret123",
        "role": "farmer", "name": "제3자", "farm_name": "F2", "region": "R2", "crops": "C2",
    })
    st, j = third.call("GET", f"/api/conversation?with={buyer_id}")
    checks["비친구 대화 차단"] = st == 403
    print(f"[3] 비친구 대화 시도 → {st} {j.get('detail')}")

    # 친구 목록에 역할이 구분되어 나오는지
    st, j = farmer.call("GET", "/api/friends")
    fr = j.get("friends", [])
    checks["친구 목록 역할 구분"] = len(fr) == 1 and fr[0]["role"] == "buyer" and fr[0]["conv"] == conv
    print(f"[3] 친구 목록 → {[(f['name'], f['role_label']) for f in fr]}")

    # ---------------- 4) 접속 상태 (온라인/오프라인)
    f_ws = WsClient(farmer, "김농부")
    b_ws = WsClient(buyer, "김서연")
    await f_ws.connect(conv)

    # 로비에 붙어 있는 농장주가 온라인으로 잡히는지
    st, j = buyer.call("GET", "/api/conversation?with=" + str(farmer_id))
    checks["온라인 상태 반영"] = j["partner"]["online"] is True
    print(f"\n[4] 농장주 접속 중 → partner.online = {j['partner']['online']}")

    await b_ws.connect(conv)

    # ---------------- 5) 실제 대화
    await f_ws.send("이번 주 상추 300kg 가능합니다")
    await asyncio.sleep(0.5)
    await f_ws.drain(0.5)
    await b_ws.drain(0.8)
    checks["메시지 전달"] = any("상추 300kg" in m["text"] for m in b_ws.inbox)
    print(f"[5] 메시지 전달 → 기업 수신 {len(b_ws.inbox)}건")

    await b_ws.send("확정하겠습니다. 화요일 오전 출하로 부탁드려요")
    await asyncio.sleep(0.5)
    await f_ws.drain(0.8)
    checks["양방향 대화"] = any("확정하겠습니다" in m["text"] for m in f_ws.inbox)
    print(f"[5] 양방향 대화 → 농장주 수신 {len(f_ws.inbox)}건")

    # ---------------- 6) 오프라인 구간 + 재동기화 + 멱등성
    print("\n[6] 농장주 회선 강제 단절")
    await f_ws.cut()
    queued = [f_ws.queue("300kg 확정입니다"), f_ws.queue("출하 당일 아침에 연락드릴게요")]
    await b_ws.send("검수는 창고에서 진행합니다")
    await asyncio.sleep(0.4)
    await b_ws.drain(0.8)
    print(f"     로컬 큐 {len(queued)}건 적재 · 기업은 정상 발신")

    await f_ws.connect(conv)
    print(f"     재동기화 {f_ws.resync} · 복구 후 수신 오프셋 seq {f_ws.last_seq}")
    await f_ws.flush()
    await asyncio.sleep(0.5)
    await f_ws.drain(0.8)
    await f_ws.flush(force=True)
    await asyncio.sleep(0.5)
    await f_ws.drain(0.8)
    await b_ws.drain(0.8)
    checks["오프라인 큐 보존"] = f_ws.dups >= len(queued)
    print(f"     같은 cid 재전송 → 서버 중복 차단 {f_ws.dups}건 (기대 {len(queued)}건)")

    # 서버 이력 대조
    st, stats = farmer.call("GET", "/api/stats")
    st2, hist = farmer.call("GET", f"/api/history?conv={conv}") if False else (0, None)
    # 이력은 DB 로 직접 확인
    import sqlite3
    conn = sqlite3.connect(str(db))
    rows = conn.execute("SELECT seq, cid FROM messages WHERE conv=? ORDER BY seq", (conv,)).fetchall()
    conn.close()
    seqs = [r[0] for r in rows]
    cids = len({r[1] for r in rows})
    checks["중복 저장 0"] = cids == len(rows)
    checks["순서 오류 0"] = seqs == sorted(seqs)
    checks["중복 차단 집계"] = stats["counters"]["duplicates_ignored"] >= len(queued)
    # 발신자 자신의 메시지는 서버가 브로드캐스트에서 제외한다(앱은 자기 메시지를 로컬 outbox 로 그린다).
    # 따라서 자기 메시지를 뺀 나머지가 유실 없이 도착했는지 확인한다.
    conn = sqlite3.connect(str(db))
    own = {r[0] for r in conn.execute(
        "SELECT seq FROM messages WHERE conv=? AND sender=?", (conv, farmer_id)).fetchall()}
    conn.close()
    expected = [s for s in seqs if s not in own]
    f_seqs = [m["seq"] for m in f_ws.inbox]
    checks["농장주 유실 0"] = all(s in f_seqs for s in expected)
    print(f"     농장주가 받아야 할 순번 {expected} · 실제 보유 {f_seqs}")
    print(f"     서버 이력 {len(rows)}건 · 고유 cid {cids}건 · 순번 증가 {'OK' if seqs == sorted(seqs) else 'FAIL'}")
    print(f"     중복 차단 {stats['counters']['duplicates_ignored']}건 · 재동기화 {stats['counters']['resyncs']}회")

    # ---------------- 판정
    print("\n" + "=" * 74)
    for k, v in checks.items():
        print(f"  [{'통과' if v else '실패'}] {k}")
    verdict = all(checks.values())
    print("-" * 74)
    print(" 판정:", "통과 — 가입·로그인·친구·대화·복구 전 구간 정상" if verdict else "실패 — 위 항목 확인")
    print("=" * 74)

    await f_ws.cut()
    await b_ws.cut()
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
    return 0 if verdict else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
