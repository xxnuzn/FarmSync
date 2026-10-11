"""인증 — 비밀번호 해시, 세션 토큰 (7계층 응용 계층의 보안 부분).

비밀번호는 pbkdf2-HMAC-SHA256 으로 해시해 저장한다. 평문은 어디에도 남기지 않는다.
세션은 랜덤 토큰을 HttpOnly 쿠키로 내려보내고, 서버가 토큰→사용자 매핑을 DB 에 보관한다.
"""

import hashlib
import hmac
import os
import secrets
import time
from typing import Any, Dict, Optional

ITERATIONS = 120_000
SESSION_TTL = 60 * 60 * 24 * 30  # 30일
COOKIE_NAME = "fs_token"


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, ITERATIONS)
    return f"pbkdf2_sha256${ITERATIONS}${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iters, salt_hex, hash_hex = stored.split("$")
        if algo != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), int(iters)
        )
        return hmac.compare_digest(dk.hex(), hash_hex)
    except Exception:
        return False


def new_token() -> str:
    return secrets.token_urlsafe(32)


# ---------------------------------------------------------------- 검증 규칙
LOGIN_ID_RE = "^[a-zA-Z0-9_]{4,20}$"

FARMER_FIELDS = [
    ("name", "이름", True),
    ("farm_name", "농장명", True),
    ("region", "지역", True),
    ("crops", "주요 품목", True),
    ("area", "재배 면적", False),
    ("phone", "연락처", False),
]
BUYER_FIELDS = [
    ("name", "담당자 성명", True),
    ("company", "기업명", True),
    ("dept", "부서", False),
    ("position", "직책", False),
    ("phone", "연락처", False),
]


def validate_signup(payload: Dict[str, Any]) -> Dict[str, str]:
    """회원가입 입력을 검증한다. 문제가 있으면 {필드: 메시지} 를 돌려준다."""
    errors: Dict[str, str] = {}

    login_id = (payload.get("login_id") or "").strip()
    if not login_id:
        errors["login_id"] = "아이디를 입력하세요"
    elif not (4 <= len(login_id) <= 20):
        errors["login_id"] = "아이디는 4~20자여야 합니다"
    elif not all(c.isalnum() or c == "_" for c in login_id):
        errors["login_id"] = "아이디는 영문·숫자·밑줄만 쓸 수 있습니다"

    pw = payload.get("password") or ""
    if len(pw) < 6:
        errors["password"] = "비밀번호는 6자 이상이어야 합니다"
    elif pw != payload.get("password2"):
        errors["password2"] = "비밀번호가 서로 다릅니다"

    role = payload.get("role")
    if role not in ("farmer", "buyer"):
        errors["role"] = "농장주 또는 기업을 선택하세요"
        return errors

    fields = FARMER_FIELDS if role == "farmer" else BUYER_FIELDS
    for key, label, required in fields:
        if required and not (payload.get(key) or "").strip():
            errors[key] = f"{label}을(를) 입력하세요"

    return errors


def issued_at_expiry() -> float:
    return time.time() + SESSION_TTL
