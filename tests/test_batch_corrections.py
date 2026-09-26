"""批量更正接口测试。

覆盖：
- 单事务全成或全败：逐项按输入顺序追加，任一项失败整批零写入，
  conflicts 按输入顺序返回全部冲突原因（未定稿/缺口/版本不符/批内重复 seq）；
- 幂等：同频道同键且请求完全相同返回原批次结果（同 batch_id、idempotent=true），
  键相同内容不同返回 409 idempotency_key_conflict；幂等键按频道隔离；
- 批量与单条更正、定稿原文/生效视图、审计接口协同正常；
- 请求体校验：空批 / 超过 100 条 / 字段非法返回 422；
- 并发提交相同批次恰好一次落库；批次表存储层不可变。
"""

from concurrent.futures import ThreadPoolExecutor

import pytest

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


def batch(client, channel, corrections, key="k1"):
    return client.post(
        f"/channels/{channel}/corrections/batch",
        json={"idempotency_key": key, "corrections": corrections},
    )


def item(seq, expected_version=0, text=None, reason="批量更正"):
    return {
        "seq": seq,
        "expected_version": expected_version,
        "text": text if text is not None else f"seq{seq}-改",
        "reason": reason,
    }


# ---------- 成功路径 ----------


def test_batch_appends_all_in_input_order(client, conn):
    for seq in range(1, 4):
        submit(client, "c1", seq, 1, f"原文{seq}")

    resp = batch(
        client,
        "c1",
        [
            item(3, 0, "第三条-改", "r3"),
            item(1, 0, "第一条-改", "r1"),
            item(2, 0, "第二条-改", "r2"),
        ],
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["idempotent"] is False
    assert body["batch_id"]
    assert body["channel"] == "c1"
    assert body["idempotency_key"] == "k1"
    assert body["created_at"]

    # results 严格按输入顺序，且各自版本为 1
    assert [(r["index"], r["seq"], r["correction_version"]) for r in body["results"]] == [
        (0, 3, 1),
        (1, 1, 1),
        (2, 2, 1),
    ]
    assert [r["text"] for r in body["results"]] == [
        "第三条-改",
        "第一条-改",
        "第二条-改",
    ]
    assert all(r["created_at"] for r in body["results"])

    # corrections 实际落库三条
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM corrections WHERE channel = 'c1'"
    ).fetchone()["c"] == 3
    # 原始定稿未被改写
    for seq in range(1, 4):
        assert conn.execute(
            "SELECT text FROM finalized WHERE channel = 'c1' AND seq = ?",
            (seq,),
        ).fetchone()["text"] == f"原文{seq}"


def test_batch_version_progression_uses_current_version(client):
    submit(client, "c1", 1, 1, "原文")
    # 先有一条更正（版本 1），批量里再追加版本 2
    submit_single = client.post(
        "/channels/c1/finalized/1/corrections",
        json={"expected_version": 0, "text": "v1", "reason": "r"},
    )
    assert submit_single.status_code == 200

    resp = batch(client, "c1", [item(1, 1, "v2", "r2")])
    assert resp.status_code == 200
    assert resp.json()["results"][0]["correction_version"] == 2

    audit = client.get("/channels/c1/finalized/1/corrections").json()
    assert [i["version"] for i in audit["items"]] == [1, 2]


def test_single_item_batch_succeeds(client):
    submit(client, "c1", 1, 1, "原文")
    resp = batch(client, "c1", [item(1, 0)])
    assert resp.status_code == 200
    assert len(resp.json()["results"]) == 1


# ---------- 整批拒绝与零写入 ----------


def test_duplicate_seq_within_batch_rejects_entire_batch(client, conn):
    submit(client, "c1", 1, 1)
    submit(client, "c1", 2, 1)

    resp = batch(client, "c1", [item(1), item(2), item(1)])
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"] == "batch_conflict"
    conflicts = body["conflicts"]
    # 仅重复出现的项冲突，首个 seq=1 与 seq=2 本身无冲突；按输入顺序返回
    assert [(c["index"], c["seq"], c["error"]) for c in conflicts] == [
        (2, 1, "duplicate_seq"),
    ]
    assert conn.execute("SELECT COUNT(*) AS c FROM corrections").fetchone()["c"] == 0
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM correction_batches"
    ).fetchone()["c"] == 0


def test_mixed_failures_report_all_in_input_order_and_write_nothing(client, conn):
    submit(client, "c1", 1, 1, "一")
    submit(client, "c1", 2, 1, "二")  # seq 3 未定稿
    decide_gap(client, "c1", 3, 3)
    client.post(
        "/channels/c1/finalized/1/corrections",
        json={"expected_version": 0, "text": "v1", "reason": "r"},
    )

    resp = batch(
        client,
        "c1",
        [
            item(2, 0),           # 通过
            item(99, 0),          # not_finalized
            item(1, 0, "bad"),    # version_conflict（当前版本已是 1）
            item(3, 0),           # seq_is_gap
        ],
    )
    assert resp.status_code == 409
    conflicts = resp.json()["conflicts"]
    assert [(c["index"], c["seq"], c["error"]) for c in conflicts] == [
        (1, 99, "not_finalized"),
        (2, 1, "version_conflict"),
        (3, 3, "seq_is_gap"),
    ]
    # 整批零写入：通过的第 0 项也不得追加
    assert conn.execute("SELECT COUNT(*) AS c FROM corrections").fetchone()["c"] == 1
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM correction_batches"
    ).fetchone()["c"] == 0


def test_unknown_channel_batch_is_zero_write_conflict(client, conn):
    resp = batch(client, "ghost", [item(1)])
    assert resp.status_code == 409
    assert resp.json()["conflicts"][0]["error"] == "not_finalized"
    assert conn.execute("SELECT COUNT(*) AS c FROM corrections").fetchone()["c"] == 0


def test_failed_batch_does_not_consume_idempotency_key(client, conn):
    submit(client, "c1", 1, 1)

    # 首次失败（seq 未定稿）
    resp = batch(client, "c1", [item(1), item(9)], key="k9")
    assert resp.status_code == 409

    # 同一幂等键换成合法内容应正常成功（失败不占键）
    resp = batch(client, "c1", [item(1)], key="k9")
    assert resp.status_code == 200
    assert resp.json()["idempotent"] is False


# ---------- 幂等 ----------


def test_identical_retry_returns_original_batch(client, conn):
    for seq in range(1, 3):
        submit(client, "c1", seq, 1, f"原文{seq}")
    payload = [item(1, 0, "改A", "rA"), item(2, 0, "改B", "rB")]

    first = batch(client, "c1", payload, key="idem-1")
    assert first.status_code == 200
    first_body = first.json()

    # 相同频道、相同幂等键、完全相同请求重试
    second = batch(client, "c1", [dict(x) for x in payload], key="idem-1")
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["idempotent"] is True
    assert second_body["batch_id"] == first_body["batch_id"]
    assert second_body["created_at"] == first_body["created_at"]
    assert second_body["results"] == first_body["results"]

    # 没有重复追加更正版本
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM corrections WHERE channel = 'c1'"
    ).fetchone()["c"] == 2
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM correction_batches WHERE channel = 'c1'"
    ).fetchone()["c"] == 1


def test_same_key_different_content_returns_conflict_and_writes_nothing(client, conn):
    submit(client, "c1", 1, 1)
    submit(client, "c1", 2, 1)

    assert batch(client, "c1", [item(1, 0, "A")], key="k").status_code == 200

    # 同键不同内容（不同 text / seq / expected_version / 顺序 均算不同）
    resp = batch(client, "c1", [item(1, 1, "B")], key="k")
    assert resp.status_code == 409
    assert resp.json()["error"] == "idempotency_key_conflict"

    resp = batch(client, "c1", [item(2, 0, "A")], key="k")
    assert resp.status_code == 409
    assert resp.json()["error"] == "idempotency_key_conflict"

    # 冲突重试不得追加任何更正
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM corrections WHERE channel = 'c1'"
    ).fetchone()["c"] == 1


def test_idempotency_key_is_scoped_per_channel(client, conn):
    submit(client, "a", 1, 1, "a原文")
    submit(client, "b", 1, 1, "b原文")

    ra = batch(client, "a", [item(1, 0, "a改")], key="shared").json()
    rb = batch(client, "b", [item(1, 0, "b改")], key="shared").json()
    assert ra["batch_id"] != rb["batch_id"]
    assert ra["idempotent"] is False and rb["idempotent"] is False


def test_retry_does_not_depend_on_current_version(client, conn):
    """批次成功后版本已推进；原请求（旧 expected_version）重试仍幂等返回，

    而不会因当前版本变化被判冲突——幂等判断先于版本核对。"""
    submit(client, "c1", 1, 1, "原文")
    payload = [item(1, 0, "改1")]

    first = batch(client, "c1", payload, key="k").json()
    # 版本已推进到 1，用原始 expected_version=0 的请求重试
    retry = batch(client, "c1", [dict(x) for x in payload], key="k")
    assert retry.status_code == 200
    assert retry.json()["batch_id"] == first["batch_id"]
    assert retry.json()["idempotent"] is True


# ---------- 与单条更正 / 视图 / 审计协同 ----------


def test_single_correction_still_works_alongside_batch(client):
    submit(client, "c1", 1, 1, "原文1")
    submit(client, "c1", 2, 1, "原文2")

    # 单条更正 -> 批量 -> 单条更正 的版本链连续
    assert client.post(
        "/channels/c1/finalized/1/corrections",
        json={"expected_version": 0, "text": "v1", "reason": "r"},
    ).status_code == 200
    body = batch(client, "c1", [item(1, 1, "v2"), item(2, 0, "seq2-v1")]).json()
    assert [(r["seq"], r["correction_version"]) for r in body["results"]] == [
        (1, 2),
        (2, 1),
    ]
    assert client.post(
        "/channels/c1/finalized/2/corrections",
        json={"expected_version": 1, "text": "seq2-v2", "reason": "r"},
    ).status_code == 200

    audit1 = client.get("/channels/c1/finalized/1/corrections").json()
    audit2 = client.get("/channels/c1/finalized/2/corrections").json()
    assert [i["version"] for i in audit1["items"]] == [1, 2]
    assert [i["version"] for i in audit2["items"]] == [1, 2]

    # original 视图仍是定稿原文，effective 视图合并最新更正
    original = client.get("/channels/c1/finalized", params={"view": "original"}).json()
    assert [i["text"] for i in original["items"]] == ["原文1", "原文2"]
    effective = client.get("/channels/c1/finalized", params={"view": "effective"}).json()
    assert [i["text"] for i in effective["items"]] == ["v2", "seq2-v2"]


# ---------- 请求体校验 ----------


def test_batch_size_limits(client):
    submit(client, "c1", 1, 1)

    assert batch(client, "c1", []).status_code == 422
    assert batch(client, "c1", [item(i % 50 + 1) for i in range(101)]).status_code == 422

    # 恰好 100 条（不同 seq，需先定稿 100 条）通过 schema 校验，
    # seq 未定稿属于业务冲突而非 422
    resp = batch(client, "c1", [item(i) for i in range(1, 101)])
    assert resp.status_code == 409
    assert resp.json()["error"] == "batch_conflict"


def test_batch_item_field_validation(client):
    submit(client, "c1", 1, 1)

    assert batch(client, "c1", [item(1, -1)]).status_code == 422
    assert batch(client, "c1", [{"seq": 0, "expected_version": 0,
                                 "text": "x", "reason": "r"}]).status_code == 422
    assert batch(client, "c1", [item(1, 0, text="")]).status_code == 422
    assert batch(client, "c1", [item(1, 0, text="   ")]).status_code == 422
    assert batch(client, "c1", [item(1, 0, reason="")]).status_code == 422
    assert client.post(
        "/channels/c1/corrections/batch",
        json={"idempotency_key": "  ", "corrections": [item(1)]},
    ).status_code == 422


# ---------- 并发与存储不可变 ----------


def test_concurrent_identical_batch_applies_once(client, conn):
    submit(client, "c1", 1, 1, "原文")
    payload = [item(1, 0, "并发改", "r")]

    def attempt(_):
        return service.submit_correction_batch(
            "c1", "race", [dict(x) for x in payload]
        )

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(attempt, range(12)))

    batch_ids = {r["batch_id"] for r in results}
    assert len(batch_ids) == 1
    # 恰好一个 idempotent=False，其余为重放
    assert sum(1 for r in results if not r["idempotent"]) == 1
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM corrections WHERE channel = 'c1'"
    ).fetchone()["c"] == 1
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM correction_batches WHERE channel = 'c1'"
    ).fetchone()["c"] == 1


def test_batch_tables_reject_update_and_delete_at_storage(client, conn):
    import sqlite3

    from app.db import write_tx

    submit(client, "c1", 1, 1, "原文")
    body = batch(client, "c1", [item(1, 0, "改", "r")]).json()
    bid = body["batch_id"]

    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute("UPDATE correction_batches SET idempotency_key = 'h'")
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute("DELETE FROM correction_batches")
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute("UPDATE correction_batch_items SET text = 'h'")
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute("DELETE FROM correction_batch_items WHERE batch_id = ?", (bid,))

    # 冻结记录仍可原样重放
    replay = batch(client, "c1", [item(1, 0, "改", "r")]).json()
    assert replay["batch_id"] == bid
    assert replay["idempotent"] is True
