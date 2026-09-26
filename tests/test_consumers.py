"""命名消费者拉取游标测试。

覆盖：
- 领取：事务内从已确认游标后返回连续批次（含缺口标记），签发带过期时间的
  lease_token；未过期租约存续期间同一消费者重复领取返回 409 lease_active；
- 确认：lease_token 与 expected_cursor 双重校验，只允许推进到批次内序号；
  过期、旧令牌、越界、并发落后、无租约一律 409 且不改写状态；
- 租约过期后可重新领取同一未确认批次（新令牌），确认后下一批从新游标继续；
- 状态查询接口（游标 / 水位线 / 滞后量 / 租约及过期标记）；
- 并发：多线程领取与确认各自只有一个成功。
"""

import time
from concurrent.futures import ThreadPoolExecutor

from app import service


def submit(client, channel, seq, revision, text="t"):
    resp = client.post(
        f"/channels/{channel}/fragments",
        json={"seq": seq, "revision": revision, "text": text},
    )
    assert resp.status_code == 200
    return resp.json()


def decide_gap(client, channel, start_seq, end_seq):
    resp = client.post(
        f"/channels/{channel}/gaps",
        json={"start_seq": start_seq, "end_seq": end_seq},
    )
    assert resp.status_code == 200
    return resp.json()


def claim(client, channel, consumer, limit=100, lease_seconds=30.0):
    return client.post(
        f"/channels/{channel}/consumers/{consumer}/claims",
        json={"limit": limit, "lease_seconds": lease_seconds},
    )


def ack(client, channel, consumer, lease_token, expected_cursor, new_cursor):
    return client.post(
        f"/channels/{channel}/consumers/{consumer}/acks",
        json={
            "lease_token": lease_token,
            "expected_cursor": expected_cursor,
            "new_cursor": new_cursor,
        },
    )


# ---------- 领取 ----------


def test_claim_returns_contiguous_batch_with_gap_markers(client):
    submit(client, "news", 1, 1, "一")
    submit(client, "news", 4, 1, "四")
    submit(client, "news", 5, 1, "五")
    decide_gap(client, "news", 2, 3)

    resp = claim(client, "news", "w1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["channel"] == "news"
    assert body["consumer"] == "w1"
    assert body["cursor"] == 0
    assert body["watermark"] == 5
    assert body["lease_token"]
    assert body["lease_expires_at"]
    # 连续批次：字幕与缺口标记按序混合
    assert [(i["kind"], i["seq"]) for i in body["items"]] == [
        ("subtitle", 1),
        ("gap", 2),
        ("gap", 3),
        ("subtitle", 4),
        ("subtitle", 5),
    ]
    assert body["items"][1]["start_seq"] == 2
    assert body["items"][1]["end_seq"] == 3
    assert body["items"][1]["text"] is None


def test_claim_defaults_apply_without_body(client):
    submit(client, "news", 1, 1, "one")
    resp = client.post("/channels/news/consumers/w1/claims")
    assert resp.status_code == 200
    assert [i["seq"] for i in resp.json()["items"]] == [1]


def test_claim_only_covers_finalized_prefix(client):
    submit(client, "news", 1, 1, "one")
    submit(client, "news", 3, 1, "three")  # 乱序缓冲，未定稿

    body = claim(client, "news", "w1").json()
    assert body["watermark"] == 1
    assert [i["seq"] for i in body["items"]] == [1]

    # 确认后缺口补齐，下一批可见 2、3
    assert ack(client, "news", "w1", body["lease_token"], 0, 1).status_code == 200
    submit(client, "news", 2, 1, "two")
    nxt = claim(client, "news", "w1").json()
    assert nxt["cursor"] == 1
    assert [i["seq"] for i in nxt["items"]] == [2, 3]


def test_claim_empty_when_no_new_items(client):
    body = claim(client, "news", "w1").json()
    assert body["items"] == []
    assert body["lease_token"] is None
    assert body["lease_expires_at"] is None
    assert body["watermark"] == 0
    # 空领取也会注册消费者，状态可查
    assert client.get("/channels/news/consumers/w1").status_code == 200

    submit(client, "news", 1, 1, "one")
    lease = claim(client, "news", "w1").json()
    assert ack(client, "news", "w1", lease["lease_token"], 0, 1).status_code == 200
    again = claim(client, "news", "w1").json()
    assert again["items"] == []
    assert again["lease_token"] is None
    assert again["cursor"] == 1


def test_unexpired_lease_blocks_same_consumer_reclaim(client, conn):
    submit(client, "news", 1, 1)
    first = claim(client, "news", "w1").json()

    resp = claim(client, "news", "w1")
    assert resp.status_code == 409
    assert resp.json()["error"] == "lease_active"
    # 状态未被改写：租约仍是第一次领取的
    row = conn.execute(
        "SELECT lease_token, cursor FROM consumers "
        "WHERE channel = 'news' AND consumer = 'w1'"
    ).fetchone()
    assert row["lease_token"] == first["lease_token"]
    assert row["cursor"] == 0
    # 不同消费者不受影响
    assert claim(client, "news", "w2").status_code == 200


def test_consumers_have_independent_cursors(client):
    for seq in range(1, 4):
        submit(client, "news", seq, 1)
    alice = claim(client, "news", "alice").json()
    bob = claim(client, "news", "bob").json()
    assert alice["lease_token"] != bob["lease_token"]
    assert [i["seq"] for i in alice["items"]] == [1, 2, 3]
    assert [i["seq"] for i in bob["items"]] == [1, 2, 3]

    assert ack(client, "news", "alice", alice["lease_token"], 0, 3).status_code == 200
    # alice 已确认完毕，bob 的租约与游标互不影响
    assert claim(client, "news", "alice").json()["items"] == []
    body = client.get("/channels/news/consumers/bob").json()
    assert body["cursor"] == 0
    assert body["lease"]["to_seq"] == 3


def test_claim_validation_rejects_bad_params(client):
    submit(client, "news", 1, 1)
    assert claim(client, "news", "w1", limit=0).status_code == 422
    assert claim(client, "news", "w1", limit=10001).status_code == 422
    assert claim(client, "news", "w1", lease_seconds=0).status_code == 422
    assert claim(client, "news", "w1", lease_seconds=-5).status_code == 422
    assert claim(client, "news", "w1", lease_seconds=3601).status_code == 422


# ---------- 确认 ----------


def test_ack_advances_cursor_and_next_claim_continues(client):
    for seq in range(1, 6):
        submit(client, "news", seq, 1, f"t{seq}")

    first = claim(client, "news", "w1", limit=2).json()
    assert [i["seq"] for i in first["items"]] == [1, 2]

    resp = ack(client, "news", "w1", first["lease_token"], 0, 2)
    assert resp.status_code == 200
    body = resp.json()
    assert body["previous_cursor"] == 0
    assert body["cursor"] == 2
    assert body["watermark"] == 5

    second = claim(client, "news", "w1", limit=2).json()
    assert second["cursor"] == 2
    assert [i["seq"] for i in second["items"]] == [3, 4]
    assert second["lease_token"] != first["lease_token"]


def test_ack_may_advance_to_any_seq_within_batch(client):
    for seq in range(1, 6):
        submit(client, "news", seq, 1)
    lease = claim(client, "news", "w1", limit=5).json()

    # 部分确认：推进到批次内的 3 而不是批次末尾 5
    resp = ack(client, "news", "w1", lease["lease_token"], 0, 3)
    assert resp.status_code == 200
    assert resp.json()["cursor"] == 3

    nxt = claim(client, "news", "w1").json()
    assert [i["seq"] for i in nxt["items"]] == [4, 5]


def test_ack_rejects_stale_token_wrong_expected_and_out_of_range(client, conn):
    for seq in range(1, 7):
        submit(client, "news", seq, 1)
    lease = claim(client, "news", "w1", limit=5).json()  # 批次 1..5

    # 旧 / 错误令牌
    resp = ack(client, "news", "w1", "not-the-token", 0, 5)
    assert resp.status_code == 409
    assert resp.json()["error"] == "lease_token_mismatch"

    # expected_cursor 与当前游标不一致（并发落后）
    resp = ack(client, "news", "w1", lease["lease_token"], 3, 5)
    assert resp.status_code == 409
    assert resp.json()["error"] == "cursor_conflict"

    # new_cursor 超出批次范围（高于批次终点）
    resp = ack(client, "news", "w1", lease["lease_token"], 0, 6)
    assert resp.status_code == 409
    assert resp.json()["error"] == "cursor_out_of_range"

    # 全部失败均不改写状态
    row = conn.execute(
        "SELECT cursor, lease_token FROM consumers "
        "WHERE channel = 'news' AND consumer = 'w1'"
    ).fetchone()
    assert row["cursor"] == 0
    assert row["lease_token"] == lease["lease_token"]

    # 正确确认仍可进行
    assert ack(client, "news", "w1", lease["lease_token"], 0, 5).status_code == 200

    # 推进后再领一批：批次 6..6，new_cursor 低于批次起点同样越界
    lease2 = claim(client, "news", "w1").json()
    assert [i["seq"] for i in lease2["items"]] == [6]
    resp = ack(client, "news", "w1", lease2["lease_token"], 5, 5)
    assert resp.status_code == 409
    assert resp.json()["error"] == "cursor_out_of_range"


def test_ack_without_active_lease_returns_409(client):
    submit(client, "news", 1, 1)
    # 从未领取的消费者
    resp = ack(client, "news", "ghost", "x", 0, 1)
    assert resp.status_code == 409
    assert resp.json()["error"] == "no_active_lease"

    lease = claim(client, "news", "w1").json()
    assert ack(client, "news", "w1", lease["lease_token"], 0, 1).status_code == 200

    # 确认后租约已清空：游标匹配但无租约
    resp = ack(client, "news", "w1", lease["lease_token"], 1, 1)
    assert resp.status_code == 409
    assert resp.json()["error"] == "no_active_lease"
    # 用旧 expected_cursor 重试：并发落后
    resp = ack(client, "news", "w1", lease["lease_token"], 0, 1)
    assert resp.status_code == 409
    assert resp.json()["error"] == "cursor_conflict"


def test_ack_validation_rejects_bad_payload(client):
    submit(client, "news", 1, 1)
    claim(client, "news", "w1")
    resp = client.post(
        "/channels/news/consumers/w1/acks",
        json={"lease_token": "x", "expected_cursor": -1, "new_cursor": 1},
    )
    assert resp.status_code == 422
    resp = client.post(
        "/channels/news/consumers/w1/acks",
        json={"lease_token": "x", "expected_cursor": 0, "new_cursor": 0},
    )
    assert resp.status_code == 422
    resp = client.post("/channels/news/consumers/w1/acks", json={})
    assert resp.status_code == 422


# ---------- 租约过期 ----------


def test_expired_lease_allows_reclaim_of_same_unacked_batch(client, monkeypatch):
    clock = {"t": 1_000.0}
    monkeypatch.setattr(service, "_now", lambda: clock["t"])

    for seq in range(1, 4):
        submit(client, "news", seq, 1, f"t{seq}")
    first = claim(client, "news", "w1", lease_seconds=30).json()
    assert [i["seq"] for i in first["items"]] == [1, 2, 3]

    # 未过期：重复领取被拒
    assert claim(client, "news", "w1").status_code == 409

    # 过期后：确认被拒（lease_expired），游标不变
    clock["t"] = 1_031.0
    resp = ack(client, "news", "w1", first["lease_token"], 0, 3)
    assert resp.status_code == 409
    assert resp.json()["error"] == "lease_expired"
    assert client.get("/channels/news/consumers/w1").json()["cursor"] == 0

    # 过期后允许重新领取同一未确认批次（内容相同、签发新令牌）
    second = claim(client, "news", "w1", lease_seconds=30).json()
    assert second["lease_token"] != first["lease_token"]
    assert [i["seq"] for i in second["items"]] == [1, 2, 3]
    assert second["cursor"] == 0

    # 旧令牌确认被拒
    resp = ack(client, "news", "w1", first["lease_token"], 0, 3)
    assert resp.status_code == 409
    assert resp.json()["error"] == "lease_token_mismatch"

    # 新令牌确认成功，游标推进；下一批从新游标后继续
    assert ack(client, "news", "w1", second["lease_token"], 0, 3).status_code == 200
    submit(client, "news", 4, 1, "t4")
    third = claim(client, "news", "w1").json()
    assert third["cursor"] == 3
    assert [i["seq"] for i in third["items"]] == [4]


def test_lease_expiry_with_real_clock(client):
    submit(client, "news", 1, 1)
    lease = claim(client, "news", "w1", lease_seconds=0.2).json()
    time.sleep(0.4)

    resp = ack(client, "news", "w1", lease["lease_token"], 0, 1)
    assert resp.status_code == 409
    assert resp.json()["error"] == "lease_expired"

    # 过期后可重新领取并在新租约内确认
    again = claim(client, "news", "w1", lease_seconds=30).json()
    assert [i["seq"] for i in again["items"]] == [1]
    assert ack(client, "news", "w1", again["lease_token"], 0, 1).status_code == 200


# ---------- 状态查询 ----------


def test_consumer_status_reports_cursor_lag_and_lease(client, monkeypatch):
    clock = {"t": 5_000.0}
    monkeypatch.setattr(service, "_now", lambda: clock["t"])

    resp = client.get("/channels/news/consumers/w1")
    assert resp.status_code == 404
    assert resp.json()["error"] == "consumer_not_found"

    for seq in range(1, 6):
        submit(client, "news", seq, 1)
    lease = claim(client, "news", "w1", limit=3, lease_seconds=60).json()

    body = client.get("/channels/news/consumers/w1").json()
    assert body["channel"] == "news"
    assert body["consumer"] == "w1"
    assert body["cursor"] == 0
    assert body["watermark"] == 5
    assert body["lag"] == 5
    assert body["lease"]["from_seq"] == 1
    assert body["lease"]["to_seq"] == 3
    assert body["lease"]["expires_at"] == lease["lease_expires_at"]
    assert body["lease"]["expired"] is False

    # 确认后租约清空、游标推进、滞后减少
    assert ack(client, "news", "w1", lease["lease_token"], 0, 3).status_code == 200
    body = client.get("/channels/news/consumers/w1").json()
    assert body["cursor"] == 3
    assert body["lag"] == 2
    assert body["lease"] is None

    # 时钟越过过期时刻后，状态中的租约标记为已过期
    lease2 = claim(client, "news", "w1", limit=2, lease_seconds=60).json()
    assert [i["seq"] for i in lease2["items"]] == [4, 5]
    clock["t"] = 5_061.0
    body = client.get("/channels/news/consumers/w1").json()
    assert body["lease"]["from_seq"] == 4
    assert body["lease"]["to_seq"] == 5
    assert body["lease"]["expired"] is True


# ---------- 并发 ----------


def test_concurrent_claims_exactly_one_wins(client):
    for seq in range(1, 51):
        submit(client, "news", seq, 1)

    def attempt(_):
        try:
            return service.claim_batch("news", "w1", 100, 30.0)
        except service.ConsumerError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(attempt, range(20)))

    ok = [r for r in results if isinstance(r, dict)]
    blocked = [r for r in results if isinstance(r, service.ConsumerError)]
    assert len(ok) == 1
    assert len(ok[0]["items"]) == 50
    assert len(blocked) == 19
    assert all(e.error == "lease_active" for e in blocked)


def test_concurrent_acks_exactly_one_wins(client, conn):
    for seq in range(1, 6):
        submit(client, "news", seq, 1)
    lease = service.claim_batch("news", "w1", 100, 30.0)
    token = lease["lease_token"]

    def attempt(_):
        try:
            return service.ack_batch("news", "w1", token, 0, 5)
        except service.ConsumerError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(attempt, range(20)))

    ok = [r for r in results if isinstance(r, dict)]
    failed = [r for r in results if isinstance(r, service.ConsumerError)]
    assert len(ok) == 1
    assert ok[0]["cursor"] == 5
    assert ok[0]["previous_cursor"] == 0
    # 其余请求在胜者提交后进入事务：expected_cursor 已落后
    assert len(failed) == 19
    assert all(e.error == "cursor_conflict" for e in failed)
    # 游标只被推进一次
    row = conn.execute(
        "SELECT cursor, lease_token FROM consumers "
        "WHERE channel = 'news' AND consumer = 'w1'"
    ).fetchone()
    assert row["cursor"] == 5
    assert row["lease_token"] is None
