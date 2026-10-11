"""FarmSync 중앙 서버 — 실제 서비스용 (로그인 · 친구 · 1:1 대화 · 접속 상태).

역할
    ① 인증       : 회원가입 · 로그인 · 세션 쿠키                       (L7 · L5)
    ② 사용자 목록 : 역할별(농장주 / 기업) 검색                          (L7)
    ③ 친구 관계   : 요청 · 수락 · 거절                                  (L7)
    ④ 대화        : 1:1 대화방, 전역 순번, 멱등성, 수신 오프셋 재동기화  (L5 · L7)
    ⑤ 접속 상태   : 온라인 / 오프라인 실시간 브로드캐스트                (L5)
    ⑥ 이력 보관   : 모든 메시지를 DB 에 보관 (상대가 꺼져 있어도 유지)    (L7)

실행
    uvicorn server.app:app --host 0.0.0.0 --port 8000
"""

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import Body, FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from . import auth
from .protocol import P, decode, encode_text, event, msg, validate_hello, validate_send
from .store import Store, conv_key

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "web"
DB_PATH = os.environ.get("FARMSYNC_DB", str(ROOT / "data" / "farmsync.db"))
SYNC_BATCH = int(os.environ.get("FARMSYNC_SYNC_BATCH", "1000"))
LOBBY = "__lobby__"   # 접속 상태 전용 채널 (대화방 아님)

store: Store = None  # type: ignore[assignment]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global store
    store = Store(DB_PATH)
    yield
    store.close()


app = FastAPI(title="FarmSync", version="2.0.0", lifespan=lifespan)

# --- 브라우저가 옛 화면을 캐시하지 않도록 HTML 응답에 항상 no-store 를 붙인다 ---
@app.middleware("http")
async def _no_store(request, call_next):
    resp = await call_next(request)
    if resp.headers.get("content-type", "").startswith("text/html"):
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp


# ================================================================ 접속 상태 (온라인/오프라인)
class Conn:
    def __init__(self, ws: WebSocket, user: Dict[str, Any], conv: str):
        self.ws = ws
        self.user = user
        self.conv = conv
        self.lock = asyncio.Lock()
        self.closed = False

    async def send(self, payload: Dict[str, Any]) -> None:
        if self.closed:
            return
        try:
            async with self.lock:
                await self.ws.send_text(encode_text(payload))
        except Exception:
            self.closed = True


class Hub:
    """사용자별 접속 관리. 온라인 판정과 친구 대상 브로드캐스트를 담당한다."""

    def __init__(self) -> None:
        self.by_user: Dict[int, Set[Conn]] = {}
        self.by_conv: Dict[str, Set[Conn]] = {}

    # ---------- 등록
    def join(self, conn: Conn) -> bool:
        uid = conn.user["id"]
        was_offline = uid not in self.by_user or not self.by_user[uid]
        self.by_user.setdefault(uid, set()).add(conn)
        self.by_conv.setdefault(conn.conv, set()).add(conn)
        return was_offline

    def leave(self, conn: Conn) -> bool:
        uid = conn.user["id"]
        conn.closed = True
        self.by_user.get(uid, set()).discard(conn)
        self.by_conv.get(conn.conv, set()).discard(conn)
        return not self.by_user.get(uid)

    def is_online(self, user_id: int) -> bool:
        return bool(self.by_user.get(user_id))

    def online_ids(self) -> List[int]:
        return [uid for uid, conns in self.by_user.items() if conns]

    # ---------- 전송
    async def to_user(self, user_id: int, payload: Dict[str, Any]) -> int:
        conns = list(self.by_user.get(user_id, set()))
        if conns:
            await asyncio.gather(*(c.send(payload) for c in conns), return_exceptions=True)
        return len(conns)

    async def to_conversation(self, conv: str, payload: Dict[str, Any], exclude: Conn = None) -> int:
        targets = [c for c in self.by_conv.get(conv, set()) if c is not exclude]
        if targets:
            await asyncio.gather(*(c.send(payload) for c in targets), return_exceptions=True)
        return len(targets)

    async def presence_to_friends(self, user_id: int, online: bool) -> None:
        payload = msg(P.PRESENCE, user_id=user_id, online=online, online_ids=self.online_ids())
        for fid in store.friend_ids(user_id):
            await self.to_user(fid, payload)


hub = Hub()


def current_user(request: Request) -> Optional[Dict[str, Any]]:
    uid = store.user_for_token(request.cookies.get(auth.COOKIE_NAME, ""))
    return store.get_user(uid) if uid else None


def require_user(request: Request) -> Optional[Dict[str, Any]]:
    return current_user(request)


def err(code: str, detail: str, status: int = 400) -> JSONResponse:
    return JSONResponse({"ok": False, "code": code, "detail": detail}, status_code=status)


# ================================================================ 인증 API
@app.post("/api/signup")
async def api_signup(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    errors = auth.validate_signup(payload)
    if errors:
        return JSONResponse({"ok": False, "errors": errors, "detail": "입력값을 확인하세요"}, status_code=400)

    data = dict(payload)
    data["pw_hash"] = auth.hash_password(payload["password"])
    uid = store.create_user(data)
    if uid is None:
        return JSONResponse(
            {"ok": False, "errors": {"login_id": "이미 사용 중인 아이디입니다"}, "detail": "아이디 중복"},
            status_code=409,
        )

    token = auth.new_token()
    store.open_session(token, uid, auth.issued_at_expiry())
    resp = JSONResponse({"ok": True, "user": store.get_user(uid), "next": "/app"})
    resp.set_cookie(auth.COOKIE_NAME, token, httponly=True, samesite="lax", max_age=auth.SESSION_TTL)
    return resp


@app.post("/api/login")
async def api_login(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    row = store.get_user_raw(payload.get("login_id") or "")
    if not row or not auth.verify_password(payload.get("password") or "", row["pw_hash"]):
        return err("BAD_CREDENTIALS", "아이디 또는 비밀번호가 올바르지 않습니다", 401)
    token = auth.new_token()
    store.open_session(token, int(row["id"]), auth.issued_at_expiry())
    store.counters["logins"] += 1
    resp = JSONResponse({"ok": True, "user": store.get_user(int(row["id"])), "next": "/app"})
    resp.set_cookie(auth.COOKIE_NAME, token, httponly=True, samesite="lax", max_age=auth.SESSION_TTL)
    return resp


@app.post("/api/logout")
async def api_logout(request: Request) -> JSONResponse:
    store.close_session(request.cookies.get(auth.COOKIE_NAME, ""))
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(auth.COOKIE_NAME)
    return resp


@app.get("/api/me")
async def api_me(request: Request) -> JSONResponse:
    user = current_user(request)
    if not user:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    return JSONResponse({"ok": True, "user": user, "online_ids": hub.online_ids()})


# ================================================================ 사용자 · 친구 API
@app.get("/api/users")
async def api_users(
    request: Request,
    role: str = Query("", description="farmer | buyer | (빈값=전체)"),
    q: str = Query("", description="이름·농장명·기업명·지역 검색"),
) -> JSONResponse:
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    users = store.list_users(role if role in ("farmer", "buyer") else None, q, exclude=me["id"])
    online = set(hub.online_ids())
    out = []
    for u in users:
        u["online"] = u["id"] in online
        u["friend_status"] = store.friend_status(me["id"], u["id"])
        out.append(u)
    return JSONResponse({"ok": True, "users": out, "online_ids": list(online)})


@app.get("/api/friends")
async def api_friends(request: Request) -> JSONResponse:
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    online = set(hub.online_ids())
    friends = store.friends(me["id"])
    for f in friends:
        f["online"] = f["id"] in online
        f["conv"] = conv_key(me["id"], f["id"])
    incoming = store.incoming_requests(me["id"])
    outgoing = store.outgoing_requests(me["id"])
    for f in incoming + outgoing:
        f["online"] = f["id"] in online
    convs = store.conversation_summary(me["id"])
    last_by_friend = {c["friend"]["id"]: c for c in convs}
    return JSONResponse({
        "ok": True,
        "me": me,
        "friends": friends,
        "incoming": incoming,
        "outgoing": outgoing,
        "summaries": last_by_friend,
        "counts": {
            "farmer": len([f for f in friends if f["role"] == "farmer"]),
            "buyer": len([f for f in friends if f["role"] == "buyer"]),
            "online": len([f for f in friends if f["online"]]),
        },
        "online_ids": list(online),
    })


@app.post("/api/friends/request")
async def api_friend_request(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    try:
        target_id = int(payload.get("user_id"))
    except (TypeError, ValueError):
        return err("BAD_TARGET", "대상 사용자가 올바르지 않습니다")

    target = store.get_user(target_id)
    if not target:
        return err("NO_USER", "해당 사용자를 찾을 수 없습니다", 404)

    result = store.request_friend(me["id"], target_id)
    if result == "self":
        return err("SELF", "자기 자신에게는 요청할 수 없습니다")

    # 상대에게 실시간 알림
    await hub.to_user(target_id, msg(P.FRIEND, kind="request" if result == "pending" else "accepted",
                                     from_user=me, conv=conv_key(me["id"], target_id)))
    if result == "accepted":
        await hub.to_user(me["id"], msg(P.FRIEND, kind="accepted", from_user=target,
                                        conv=conv_key(me["id"], target_id)))

    labels = {
        "pending": "친구 요청을 보냈습니다",
        "accepted": "상대의 요청을 수락해 친구가 되었습니다",
        "already": "이미 요청했거나 친구입니다",
    }
    return JSONResponse({"ok": True, "result": result, "detail": labels.get(result, "처리했습니다")})


@app.post("/api/friends/accept")
async def api_friend_accept(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    try:
        requester_id = int(payload.get("user_id"))
    except (TypeError, ValueError):
        return err("BAD_TARGET", "대상 사용자가 올바르지 않습니다")
    if not store.accept_friend(me["id"], requester_id):
        return err("NO_REQUEST", "대기 중인 요청이 없습니다", 404)
    requester = store.get_user(requester_id)
    conv = store.ensure_conversation(me["id"], requester_id)
    await hub.to_user(requester_id, msg(P.FRIEND, kind="accepted", from_user=me, conv=conv))
    return JSONResponse({"ok": True, "detail": f"{requester['name']} 님과 친구가 되었습니다", "conv": conv})


@app.post("/api/friends/reject")
async def api_friend_reject(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    try:
        requester_id = int(payload.get("user_id"))
    except (TypeError, ValueError):
        return err("BAD_TARGET", "대상 사용자가 올바르지 않습니다")
    store.reject_friend(me["id"], requester_id)
    return JSONResponse({"ok": True, "detail": "요청을 거절했습니다"})


@app.get("/api/conversation")
async def api_conversation(request: Request, with_user: int = Query(..., alias="with")) -> JSONResponse:
    """대화방 정보 + 최근 이력. 친구가 아니면 열리지 않는다."""
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    partner = store.get_user(with_user)
    if not partner:
        return err("NO_USER", "상대를 찾을 수 없습니다", 404)
    if store.friend_status(me["id"], with_user) != "friends":
        return err("NOT_FRIEND", "친구가 아니면 대화할 수 없습니다", 403)
    conv = store.ensure_conversation(me["id"], with_user)
    return JSONResponse({
        "ok": True,
        "me": me,
        "partner": {**partner, "online": hub.is_online(with_user)},
        "conv": conv,
        "latest_seq": store.latest_seq(conv),
        "read_total": store.reader_total(conv, me["id"]),
        "participants": len(store.participants(conv)),
        "messages": store.history(conv, limit=300),
    })


@app.get("/api/threads")
async def api_threads(request: Request) -> JSONResponse:
    """좌측 사이드바 '대화' 목록 — 상대 이름 + 가장 마지막 대화 1줄 + 안 읽은 수."""
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    out = []
    for f in store.friends(me["id"]):
        conv = store.ensure_conversation(me["id"], f["id"])
        last = store.last_message(conv)
        out.append({
            **f,
            "online": hub.is_online(f["id"]),
            "conv": conv,
            "last_text": (last or {}).get("text", ""),
            "last_ts": (last or {}).get("ts"),
            "last_mine": bool(last) and last.get("sender") == me["id"],
            "unread": store.unread_count(conv, me["id"]),
        })
    out.sort(key=lambda x: (x.get("last_ts") or 0), reverse=True)
    return JSONResponse({"ok": True, "threads": out, "online_ids": hub.online_ids()})


# ================================================================ 캘린더
@app.get("/api/events")
async def api_events(request: Request, start: int = Query(...), end: int = Query(...)) -> JSONResponse:
    """내 개인 일정 + 내 대화방의 공유 일정. 남의 개인 일정은 내려가지 않는다."""
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    return JSONResponse({"ok": True, "events": store.events_between(me["id"], int(start), int(end))})


@app.post("/api/events")
async def api_event_add(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    title = str(payload.get("title") or "").strip()
    if not title:
        return err("NO_TITLE", "일정 제목을 입력하세요")
    scope = "shared" if payload.get("scope") == "shared" else "personal"
    conv = payload.get("conv")
    with_user = payload.get("with")
    if scope == "shared":
        # 상대를 지정하면 그 대화방으로, 아니면 conv 키로
        if with_user:
            try:
                conv = store.ensure_conversation(me["id"], int(with_user))
            except (TypeError, ValueError):
                return err("BAD_TARGET", "상대가 올바르지 않습니다")
        if not conv or not store.is_participant(str(conv), me["id"]):
            return err("NO_CONV", "공유할 대화방을 찾을 수 없습니다")
    start_ts = payload.get("start_ts")
    if not start_ts:
        return err("NO_START", "일정 날짜를 선택하세요")
    # 공동 일정은 만든 순간 확정되지 않는다. 대화방에 초대 카드가 도착하고,
    # 상대가 수락해야 그 사람 캘린더에 들어간다 (status='pending' → 'confirmed').
    status = "pending" if scope == "shared" else "confirmed"
    eid = store.add_event(me["id"], scope, title[:120], str(payload.get("note") or "")[:400],
                          start_ts, payload.get("end_ts"), str(conv) if conv else None,
                          status=status)

    if scope != "shared":
        return JSONResponse({"ok": True, "id": eid, "scope": scope, "conv": None,
                             "status": status, "sent": False,
                             "detail": "내 캘린더에 저장했습니다 (나만 확인)"})

    # 대화방에 초대 카드를 남긴다 — 상대는 대화 목록 배지로 바로 알게 된다.
    other = store.other_party(str(conv), me["id"])
    invite_id = store.add_invite(eid, str(conv), me["id"], other)
    text = "📅 공동 일정 초대 · " + title[:120]
    seq, _dup = store.append(str(conv), f"invite-{invite_id}", me["id"], text, None,
                             kind="event_invite", ref=invite_id)
    record = {"seq": seq, "cid": f"invite-{invite_id}", "conv": str(conv),
              "sender": me["id"], "sender_name": me["name"], "text": text,
              "ts": store.normalize_ts(), "read_by": [], "kind": "event_invite",
              "ref": invite_id, "invite": store.invite_payload(invite_id)}
    # 대화창을 열고 있으면 즉시, 닫아 둔 상대에게는 목록 배지용으로 한 번 더
    delivered = await hub.to_conversation(str(conv), msg(P.MSG, **record))
    if delivered == 0:
        await hub.to_user(other, msg(P.MSG, **record, background=True))
    await hub.to_user(other, msg(P.EVENT, kind="calendar_invite", invite_id=invite_id,
                                 event_id=eid, title=title, from_user=me,
                                 start_ts=store.normalize_ts(start_ts)))
    return JSONResponse({"ok": True, "id": eid, "scope": scope, "conv": str(conv),
                         "invite_id": invite_id, "status": status, "sent": True,
                         "detail": "공동 일정 초대를 대화방으로 보냈습니다"})


@app.post("/api/events/accept")
async def api_event_accept(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    """공동 일정 초대 수락 — 수락하는 순간 내 캘린더에 저장된다.

    보낸 사람에게는 수락 여부를 따로 알리지 않는다(요구사항). 받는 사람 화면만 갱신한다.
    """
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    try:
        invite_id = int(payload.get("invite_id") or payload.get("id"))
    except (TypeError, ValueError):
        return err("BAD_ID", "초대 번호가 올바르지 않습니다")
    info = store.invite_accept(invite_id, me["id"])
    if not info:
        return err("NO_INVITE", "내가 받은 초대만 수락할 수 있습니다", 404)
    await hub.to_user(me["id"], msg(P.EVENT, kind="invite_accepted", invite_id=invite_id,
                                    event_id=info["event_id"], title=info["title"]))
    return JSONResponse({"ok": True, "invite": info,
                         "detail": "수락했습니다 · 내 캘린더에 저장되었습니다"})


@app.get("/api/events/invites")
async def api_event_invites(request: Request) -> JSONResponse:
    """아직 수락하지 않은 내 초대 — 캘린더 화면의 '받은 초대' 표시용."""
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    return JSONResponse({"ok": True, "invites": store.pending_invites(me["id"])})


@app.post("/api/events/delete")
async def api_event_delete(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    me = current_user(request)
    if not me:
        return err("NOT_LOGGED_IN", "로그인이 필요합니다", 401)
    try:
        eid = int(payload.get("id"))
    except (TypeError, ValueError):
        return err("BAD_ID", "일정 번호가 올바르지 않습니다")
    if not store.delete_event(eid, me["id"]):
        return err("NO_EVENT", "내가 만든 일정만 삭제할 수 있습니다", 404)
    return JSONResponse({"ok": True, "detail": "일정을 삭제했습니다"})


@app.get("/api/stats")
async def api_stats() -> Dict[str, Any]:
    return {"status": "ok", "online": hub.online_ids(), "counters": store.counters}


@app.get("/healthz")
async def healthz() -> Dict[str, Any]:
    return {"status": "ok", "service": "farmsync", "version": "2.0.0"}


# ================================================================ WebSocket
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    token = ws.cookies.get(auth.COOKIE_NAME, "")
    uid = store.user_for_token(token)
    if not uid:
        await ws.send_text(encode_text(msg(P.ERROR, code="NOT_LOGGED_IN", detail="로그인이 필요합니다")))
        await ws.close(code=1008)
        return
    me = store.get_user(uid)

    conn: Optional[Conn] = None
    try:
        first = await ws.receive()
        if first.get("bytes") is not None:
            hello = decode(first["bytes"])
        elif first.get("text") is not None:
            hello = decode(first["text"].encode("utf-8"))
        else:
            return

        errors = validate_hello(hello)
        if hello.get("type") != P.HELLO or errors:
            await ws.send_text(encode_text(msg(P.ERROR, code="BAD_HELLO", detail="; ".join(errors) or "HELLO 필요")))
            await ws.close(code=1008)
            return

        conv = hello["conv"]
        # __lobby__ 는 대화가 아니라 접속 상태(온라인/오프라인) 전용 채널이다.
        if conv != LOBBY:
            if not store.is_participant(conv, uid):
                await ws.send_text(encode_text(msg(P.ERROR, code="NOT_PARTICIPANT", detail="이 대화방의 참여자가 아닙니다")))
                await ws.close(code=1008)
                return
            if store.friend_status(uid, store.other_party(conv, uid)) != "friends":
                await ws.send_text(encode_text(msg(P.ERROR, code="NOT_FRIEND", detail="친구가 아니면 대화할 수 없습니다")))
                await ws.close(code=1008)
                return

        last_seq = int(hello.get("last_seq") or 0)
        conn = Conn(ws, me, conv)
        was_offline = hub.join(conn)

        latest = store.latest_seq(conv)
        await conn.send(msg(P.WELCOME, session_user=me, conv=conv, latest_seq=latest,
                            online_ids=hub.online_ids()))
        await conn.send(event(5, f"세션 시작 · {me['name']} · 대화방 {conv}"))
        await conn.send(event(4, "WebSocket 핸드셰이크 완료 (하부 TCP 연결 위에서 동작)"))

        # 밀린 구간만 전송 — 수신 오프셋 기반 재동기화
        if last_seq < latest:
            missing = store.since(conv, last_seq, limit=SYNC_BATCH)
            store.counters["resyncs"] += 1
            store.counters["resync_messages"] += len(missing)
            await conn.send(msg(P.SYNC, conv=conv, from_seq=last_seq, to_seq=latest,
                                count=len(missing), messages=missing))
            await conn.send(event(5, f"재동기화: 수신 오프셋 {last_seq} → {latest} · 밀린 {len(missing)}건 전송"))
        else:
            await conn.send(event(5, f"재동기화 불필요 (수신 오프셋 {last_seq} = 서버 최신)"))

        if was_offline:
            await hub.presence_to_friends(uid, True)

        while True:
            raw = await ws.receive_text()
            data = decode(raw.encode("utf-8"))
            t = data.get("type")

            if t == P.SEND:
                await handle_send(conn, data)
            elif t == P.READ:
                seq = int(data.get("seq") or 0)
                if seq and store.add_receipt(seq, uid):
                    await hub.to_conversation(conv, msg(P.READ, seq=seq, reader=uid, reader_name=me["name"]))
            elif t == P.TYPING:
                await hub.to_conversation(conv, msg(P.TYPING, user_id=uid, name=me["name"]), exclude=conn)
            elif t == P.PING:
                await conn.send(msg(P.PONG, ts=data.get("ts")))
            else:
                await conn.send(msg(P.ERROR, code="UNKNOWN_TYPE", detail=f"알 수 없는 타입: {t}"))

    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001
        if conn:
            await conn.send(msg(P.ERROR, code="SERVER_ERROR", detail=str(exc)))
    finally:
        if conn:
            now_last = hub.leave(conn)
            if now_last:
                await hub.presence_to_friends(conn.user["id"], False)


async def handle_send(conn: Conn, data: Dict[str, Any]) -> None:
    errors = validate_send(data)
    if errors:
        await conn.send(msg(P.ERROR, code="BAD_SEND", cid=data.get("cid"), detail="; ".join(errors)))
        return

    cid, text = data["cid"], data["text"]
    uid, conv = conn.user["id"], conn.conv

    await conn.send(event(7, f"메시지 수신 cid={cid[:10]}… '{text[:40]}'"))
    seq, duplicate = store.append(conv, cid, uid, text, data.get("ts"))
    await conn.send(event(6, f"JSON 직렬화 + UTF-8 · 본문 {len(text.encode('utf-8'))}바이트 저장"))

    if duplicate:
        await conn.send(msg(P.DELIVERED, cid=cid, seq=seq, duplicate=True,
                                    read_total=store.reader_total(conv, uid)))
        await conn.send(event(7, f"중복 감지 · cid 는 이미 seq {seq} 로 저장됨 — 새로 저장하지 않음"))
        return

    await conn.send(msg(P.DELIVERED, cid=cid, seq=seq, duplicate=False,
                                read_total=store.reader_total(conv, uid)))
    other = store.other_party(conv, uid)
    record = {"seq": seq, "cid": cid, "conv": conv, "sender": uid, "sender_name": conn.user["name"],
              "text": text, "ts": store.normalize_ts(data.get("ts")), "read_by": []}
    delivered = await hub.to_conversation(conv, msg(P.MSG, **record), exclude=conn)
    # 대화방을 열지 않은 상대에게도 알림 (목록 배지용)
    if delivered == 0:
        await hub.to_user(other, msg(P.MSG, **record, background=True))
    await conn.send(event(4, f"브로드캐스트 → {delivered}명 · 전역 순번 seq {seq} 확정"))


# ================================================================ 페이지
# 누구나 볼 수 있는 페이지
PAGES = {
    "/": "index.html",
    "/login": "login.html",
    "/signup": "signup.html",
}

# 로그인해야 볼 수 있는 페이지.
# 예전에는 세션이 없어도 껍데기 HTML 이 200 으로 내려가서, 화면이 "연결 중"
# 상태로 영구히 멈췄다. 이제는 세션이 없으면 로그인 화면으로 보낸다.
AUTHED_PAGES = {
    "/app": "app.html",
    "/friends": "friends.html",
    "/chat": "chat.html",
    "/calendar": "calendar.html",
}

EXPIRED_HTML = """<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>로그인이 필요합니다</title></head>
<body style="margin:0;display:grid;place-items:center;height:100vh;background:#f4f6f5;
font:15px/1.6 system-ui,'Apple SD Gothic Neo','Malgun Gothic',sans-serif;color:#14261d">
<div style="text-align:center;padding:24px">
<p style="font-weight:700;font-size:17px;margin:0 0 6px">로그인이 필요합니다</p>
<p style="color:#5b6f66;margin:0 0 16px">세션이 만료되었습니다. 다시 로그인해 주세요.</p>
<a href="/login" target="_top" style="display:inline-block;background:#17916a;color:#fff;
text-decoration:none;font-weight:700;padding:10px 18px;border-radius:10px">로그인 화면으로</a>
</div></body></html>"""


def _page_loader(fn: str):
    async def page() -> FileResponse:
        return FileResponse(WEB / fn)
    return page


def _authed_page_loader(fn: str, route: str):
    async def page(request: Request):
        if current_user(request) is None:
            if request.query_params.get("embed") == "1":
                return HTMLResponse(EXPIRED_HTML, status_code=401)
            return RedirectResponse("/login?next=" + route, status_code=303)
        return FileResponse(WEB / fn)
    return page


for route, filename in PAGES.items():
    app.get(route)(_page_loader(filename))

for route, filename in AUTHED_PAGES.items():
    app.get(route)(_authed_page_loader(filename, route))

assets = WEB / "assets"
if assets.exists():
    app.mount("/assets", StaticFiles(directory=assets), name="assets")
