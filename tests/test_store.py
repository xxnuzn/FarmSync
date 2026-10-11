"""저장소 단위 테스트 — 멱등성 · 전역 순번 · 수신 오프셋 · 친구 관계 · 역할별 프로필."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.store import Store, conv_key  # noqa: E402


@pytest.fixture()
def store():
    s = Store(":memory:")
    yield s
    s.close()


def make_user(store, login_id, role):
    return store.create_user({
        "login_id": login_id, "pw_hash": "x", "role": role, "name": login_id,
        "farm_name": "농장" if role == "farmer" else None,
        "region": "지역" if role == "farmer" else None,
        "crops": "품목" if role == "farmer" else None,
        "company": "기업" if role == "buyer" else None,
        "dept": "부서" if role == "buyer" else None,
        "position": "직책" if role == "buyer" else None,
    })


def test_signup_fields_differ_by_role(store):
    fid = make_user(store, "farm1", "farmer")
    bid = make_user(store, "buy1", "buyer")
    f = store.get_user(fid)
    b = store.get_user(bid)
    assert f["farm_name"] == "농장" and f.get("company") is None
    assert b["company"] == "기업" and b.get("farm_name") is None
    assert f["role_label"] == "농장주" and b["role_label"] == "기업"


def test_duplicate_login_id_rejected(store):
    assert make_user(store, "same", "farmer") is not None
    assert make_user(store, "same", "buyer") is None
    assert store.last_signup_error() == "duplicate_login_id"


def test_append_assigns_increasing_global_seq(store):
    seq1, d1 = store.append("1-2", "cid-1", 1, "첫 메시지")
    seq2, d2 = store.append("1-2", "cid-2", 2, "두 번째")
    assert (d1, d2) == (False, False)
    assert seq2 == seq1 + 1


def test_duplicate_cid_not_stored_twice(store):
    first, d1 = store.append("1-2", "cid-a", 1, "확정")
    second, d2 = store.append("1-2", "cid-a", 1, "확정")
    assert d1 is False and d2 is True
    assert first == second
    assert len(store.history("1-2")) == 1
    assert store.counters["duplicates_ignored"] == 1


def test_sequences_are_per_conversation(store):
    store.append("1-2", "c1", 1, "A-B")
    store.append("1-3", "c2", 1, "A-C")
    assert store.latest_seq("1-2") == 1
    assert store.latest_seq("1-3") == 2
    assert len(store.history("1-2")) == 1


def test_since_returns_only_missing_range(store):
    for i in range(1, 6):
        store.append("1-2", f"cid-{i}", 1, f"m{i}")
    missing = store.since("1-2", 2)
    assert [m["text"] for m in missing] == ["m3", "m4", "m5"]
    assert store.since("1-2", 5) == []
    assert store.latest_seq("1-2") == 5


def test_receipts_are_deduplicated(store):
    seq, _ = store.append("1-2", "cid-1", 1, "확인")
    assert store.add_receipt(seq, 2) is True
    assert store.add_receipt(seq, 2) is False
    assert store.history("1-2")[0]["read_by"] == [2]


def test_conversation_key_is_order_independent(store):
    assert conv_key(7, 3) == conv_key(3, 7) == "3-7"
    key = store.ensure_conversation(7, 3)
    assert key == "3-7"
    assert store.is_participant(key, 7) and store.is_participant(key, 3)
    assert not store.is_participant(key, 9)
    assert store.other_party(key, 7) == 3


def test_friend_request_accept_flow(store):
    a = make_user(store, "farmerA", "farmer")
    b = make_user(store, "buyerB", "buyer")
    assert store.friend_status(a, b) == "none"
    assert store.request_friend(a, b) == "pending"
    assert store.friend_status(a, b) == "requested"
    assert store.friend_status(b, a) == "incoming"
    assert store.friends(a) == []
    assert [u["id"] for u in store.incoming_requests(b)] == [a]
    assert store.accept_friend(b, a) is True
    assert store.friend_status(a, b) == "friends"
    assert [u["id"] for u in store.friends(a)] == [b]
    assert store.friends(b)[0]["role"] == "farmer"


def test_mutual_request_becomes_friendship(store):
    a = make_user(store, "f1", "farmer")
    b = make_user(store, "b1", "buyer")
    store.request_friend(a, b)
    assert store.request_friend(b, a) == "accepted"
    assert store.friend_status(a, b) == "friends"


def test_reject_removes_request(store):
    a = make_user(store, "f2", "farmer")
    b = make_user(store, "b2", "buyer")
    store.request_friend(a, b)
    assert store.reject_friend(b, a) is True
    assert store.friend_status(a, b) == "none"


def test_list_users_filters_by_role_and_query(store):
    make_user(store, "farmX", "farmer")
    make_user(store, "buyX", "buyer")
    farmers = store.list_users("farmer")
    buyers = store.list_users("buyer")
    assert [u["login_id"] for u in farmers] == ["farmX"]
    assert [u["login_id"] for u in buyers] == ["buyX"]
    assert len(store.list_users(None, q="farm")) == 1
    assert len(store.list_users(None, q="없는이름")) == 0


def test_messages_survive_process_restart(tmp_path):
    db = str(tmp_path / "f.db")
    s1 = Store(db)
    s1.append("1-2", "cid-1", 1, "재시작 전 메시지")
    s1.close()
    s2 = Store(db)
    assert s2.latest_seq("1-2") == 1
    assert s2.history("1-2")[0]["text"] == "재시작 전 메시지"
    s2.close()
