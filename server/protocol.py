"""FarmSync 프로토콜 — 7계층 응용 계층에 해당하는 메시지 규약.

L7 응용       : HELLO / SEND / SYNC / MSG / DELIVERED / READ / PRESENCE / FRIEND / EVENT
L6 표현       : encode()/decode() — JSON 직렬화 + UTF-8 (묶음은 zlib 압축)
L5 세션       : 세션 토큰(로그인) · 대화별 수신 오프셋(last_seq)으로 연속성 유지
L4 전송       : WebSocket over TCP (순서 보장·재전송, 연결이 끊기면 그 지점까지)
L3~L1         : 서버 IP 라우팅 · 무선 프레임 · 물리 신호 (코드 밖, OS·하드웨어 담당)
"""

import json
import zlib
from typing import Any, Dict, List


class P:
    # 클라이언트 → 서버
    HELLO = "HELLO"          # 대화방 구독 + 내가 어디까지 받았는지
    SEND = "SEND"            # 메시지 전송 (cid = 멱등성 키)
    READ = "READ"            # 읽음 표시
    TYPING = "TYPING"
    PING = "PING"

    # 서버 → 클라이언트
    WELCOME = "WELCOME"      # 구독 승인 + 서버 최신 순번
    SYNC = "SYNC"            # 밀린 구간 일괄 전송
    MSG = "MSG"              # 새 메시지 (전역 순번 포함)
    DELIVERED = "DELIVERED"  # 저장 완료 통보 (duplicate 플래그)
    PRESENCE = "PRESENCE"    # 접속/접속 종료
    FRIEND = "FRIEND"        # 친구 요청·수락 알림
    EVENT = "EVENT"          # 계층 로그용 서버 이벤트
    ERROR = "ERROR"
    PONG = "PONG"


LAYER = {7: "응용", 6: "표현", 5: "세션", 4: "전송", 3: "네트워크", 2: "데이터링크", 1: "물리"}


def event(layer: int, text: str, **extra: Any) -> Dict[str, Any]:
    payload = {"type": P.EVENT, "layer": layer, "layer_name": LAYER.get(layer, "?"), "text": text}
    payload.update(extra)
    return payload


def msg(type_: str, **fields: Any) -> Dict[str, Any]:
    payload = {"type": type_}
    payload.update(fields)
    return payload


def encode(payload: Dict[str, Any], compress: bool = False) -> bytes:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return zlib.compress(body, 6) if compress else body


def encode_text(payload: Dict[str, Any], compress: bool = False) -> str:
    return encode(payload, compress=compress).decode("utf-8")


def decode(raw: bytes) -> Dict[str, Any]:
    if raw[:1] == b"\x78":
        try:
            raw = zlib.decompress(raw)
        except zlib.error:
            pass
    return json.loads(raw.decode("utf-8"))


def validate_hello(payload: Dict[str, Any]) -> List[str]:
    errors = []
    if not isinstance(payload.get("conv"), str) or not payload.get("conv"):
        errors.append("conv(대화방 키) 누락")
    if payload.get("last_seq") is not None and not isinstance(payload["last_seq"], int):
        errors.append("last_seq 는 정수여야 함")
    return errors


def validate_send(payload: Dict[str, Any]) -> List[str]:
    errors = []
    if not isinstance(payload.get("cid"), str) or not payload.get("cid"):
        errors.append("cid 누락 (멱등성 키)")
    text = payload.get("text")
    if not isinstance(text, str) or not text.strip():
        errors.append("text 누락")
    return errors
