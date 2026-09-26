"""批量更正接口测试。

覆盖：
- 1～100 条边界与请求体校验；
- 批内重复 seq 整批拒绝；
- 单事务逐项核对：任一项失败整批零写入，按输入顺序返回各项冲突原因；
- 全部通过按输入顺序追加，返回批次号与逐项结果；
- 幂等：同频道同键同内容重试返回原批次；同键不同内容冲突；键按频道隔离；
  失败批次不占键；
- 与单条更正共用同一版本链；定稿原文不受影响；
- 并发相同批次只有一个真正写入，其余幂等重放。
"""

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


def item(seq, expected_version, text="更正", reason="原因"):
    return {
        "seq": seq,
        "expected_version": expected_version,
        "text": text,
        "reason": reason,
    }


def batch(client, channel, key, items):
    return client.post(
        f"/channels/{channel}/corrections/batch",
        json={"idempotency_key": key, "items": items},
    )


# ---------- 基本追加与响应 ----------


def test_batch_appends_in_input_order_and_returns_batch_id(client):
    for seq in (1, 2, 3):
        submit(client, "c1", seq, 1, f"原文{seq}")

    resp = batch(
        client,
        "c1",
        "k1",
        [item(3, 0, "三改"), item(1, 0, "一改"), item(2, 0, "二改")],
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["channel"] == "c1"
    assert body["batch_id"]
    assert body["idempotency_key"] == "k1"
    assert body["idempotent"] is False
    assert body["created_at"]
    # 逐项结果与请求同序
    assert [i["seq"] for i in body["items"]] == [3, 1, 2]
    assert [i["correction_version"] for i in body["items"]] == [1, 1, 1]
    assert [i["text"] for i in body["items"]] == ["三改", "一改", "二改"]
    assert all(i["created_at"] for i in body["items"])

    # 各序号审计链各自落了一条 version=1
    for seq, text in ((1, "一改"), (2, "二改"), (3, "三改")):
        audit = client.get(f"/channels/c1/finalized/{seq}/corrections").json()
        assert audit["current_version"] == 1
        assert [(i["version"], i["text"]) for i in audit["items"]] == [(1, text)]


def test_batch_size_bounds(client):
    for seq in range(1, 101):
        submit(client, "c1", seq, 1)

    assert batch(client, "c1", "k0", []).status_code == 422
    too_many = batch(client, "c1", "k101", [item(s, 0) for s in range(1, 102)])
    assert too_many.status_code == 422

    resp = batch(client, "c1", "k100", [item(s, 0) for s in range(1, 101)])
    assert resp.status_code == 200
    assert len(resp.json()["items"]) == 100


def test_batch_validation_rejects_bad_fields(client):
    submit(client, "c1", 1, 1)
    assert batch(client, "c1", "", [item(1, 0)]).status_code == 422
    assert batch(client, "c1", "  ", [item(1, 0)]).status_code == 422
    assert batch(client, "c1", "k", [item(0, 0)]).status_code == 422
    assert batch(client, "c1", "k", [item(1, -1)]).status_code == 422
    assert batch(client, "c1", "k", [item(1, 0, text="")]).status_code == 422
    assert batch(client, "c1", "k", [item(1, 0, text="  ")]).status_code == 422
    assert batch(client, "c1", "k", [item(1, 0, reason="")]).status_code == 422


# ---------- 批内重复 seq ----------


def test_duplicate_seq_rejects_whole_batch(client, conn):
    submit(client, "c1", 1, 1)
    submit(client, "c1", 2, 1)

    resp = batch(client, "c1", "k1", [item(1, 0), item(2, 0), item(1, 0)])
    assert resp.status_code == 409
    assert resp.json()["error"] == "duplicate_seq"
    # 整批零写入，幂等键也未被占用
    assert conn.execute("SELECT COUNT(*) AS c FROM corrections").fetchone()["c"] == 0
    assert (
        conn.execute("SELECT COUNT(*) AS c FROM correction_batches").fetchone()["c"]
        == 0
    )
    # 同一键修正内容后仍可使用
    assert batch(client, "c1", "k1", [item(1, 0), item(2, 0)]).status_code == 200


# ---------- 逐项核对与整批零写入 ----------


def test_partial_failure_returns_per_item_reasons_and_writes_nothing(client, conn):
    submit(client, "c1", 1, 1, "一")
    submit(client, "c1", 2, 1, "二")
    submit(client, "c1", 4, 1, "四")
    decide_gap(client, "c1", 3, 3)
    # seq 2 已有一条更正（当前版本 1）
    assert (
        client.post(
            "/channels/c1/finalized/2/corrections",
            json={"expected_version": 0, "text": "二改", "reason": "r"},
        ).status_code
        == 200
    )

    resp = batch(
        client,
        "c1",
        "k1",
        [
            item(1, 0, "一改"),   # 通过
            item(3, 0),           # 缺口
            item(2, 0),           # 版本冲突（当前为 1）
            item(9, 0),           # 未定稿
            item(4, 0, "四改"),   # 通过
        ],
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"] == "batch_conflict"
    # 按输入顺序给出各项结果：通过项 error 为 null，失败项带原因
    assert [(i["seq"], i["error"]) for i in body["items"]] == [
        (1, None),
        (3, "seq_is_gap"),
        (2, "version_conflict"),
        (9, "not_finalized"),
        (4, None),
    ]
    assert "当前更正版本 1" in body["items"][2]["message"]

    # 整批零写入：通过项也没有落库，既有更正在原处
    rows = conn.execute(
        "SELECT seq, version FROM corrections ORDER BY seq"
    ).fetchall()
    assert [(r["seq"], r["version"]) for r in rows] == [(2, 1)]
    assert (
        conn.execute("SELECT COUNT(*) AS c FROM correction_batches").fetchone()["c"]
        == 0
    )


def test_failed_batch_does_not_consume_idempotency_key(client):
    submit(client, "c1", 1, 1)

    bad = batch(client, "c1", "k1", [item(1, 0), item(5, 0)])
    assert bad.status_code == 409
    assert bad.json()["error"] == "batch_conflict"

    # 修正内容后同一键可重新提交
    ok = batch(client, "c1", "k1", [item(1, 0)])
    assert ok.status_code == 200
    assert ok.json()["idempotent"] is False


# ---------- 幂等 ----------


def test_idempotent_retry_returns_original_batch(client, conn):
    submit(client, "c1", 1, 1)
    submit(client, "c1", 2, 1)
    payload = [item(1, 0, "一改"), item(2, 0, "二改")]

    first = batch(client, "c1", "k1", payload).json()
    assert first["idempotent"] is False

    replay = batch(client, "c1", "k1", payload)
    assert replay.status_code == 200
    assert replay.json() == {**first, "idempotent": True}
    assert replay.json()["batch_id"] == first["batch_id"]

    # 没有重复追加
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM corrections WHERE channel = 'c1'"
    ).fetchone()["c"] == 2
    assert (
        conn.execute("SELECT COUNT(*) AS c FROM correction_batches").fetchone()["c"]
        == 1
    )


def test_same_key_different_content_conflicts(client, conn):
    submit(client, "c1", 1, 1)
    submit(client, "c1", 2, 1)

    assert batch(client, "c1", "k1", [item(1, 0)]).status_code == 200

    # 内容不同（条数不同 / 文本不同 / 顺序不同都算不同）
    assert batch(client, "c1", "k1", [item(1, 0), item(2, 0)]).status_code == 409
    changed = batch(client, "c1", "k1", [item(1, 0, "别的文本")])
    assert changed.status_code == 409
    assert changed.json()["error"] == "idempotency_key_conflict"

    # 冲突请求不产生任何写入
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM corrections WHERE channel = 'c1'"
    ).fetchone()["c"] == 1


def test_idempotency_key_scoped_per_channel(client):
    submit(client, "a", 1, 1)
    submit(client, "b", 1, 1)

    ra = batch(client, "a", "shared", [item(1, 0)]).json()
    rb = batch(client, "b", "shared", [item(1, 0)]).json()
    assert ra["idempotent"] is False
    assert rb["idempotent"] is False
    assert ra["batch_id"] != rb["batch_id"]


def test_concurrent_identical_batches_replay_same_batch(client, conn):
    """并发提交完全相同的批次：全部成功且返回同一批次，更正只落一次。"""
    submit(client, "c1", 1, 1)
    payload = [item(1, 0, "一改")]

    def attempt(_):
        try:
            return service.submit_correction_batch("c1", "k1", payload)
        except service.CorrectionError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(attempt, range(8)))

    assert all(not isinstance(r, service.CorrectionError) for r in results)
    assert len({r["batch_id"] for r in results}) == 1
    assert sum(1 for r in results if r["idempotent"] is False) == 1
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM corrections WHERE channel = 'c1'"
    ).fetchone()["c"] == 1


# ---------- 与单条更正 / 定稿原文的兼容 ----------


def test_batch_and_single_corrections_share_version_chain(client):
    submit(client, "c1", 1, 1, "原文")

    # 批量追加 version 1
    body = batch(client, "c1", "k1", [item(1, 0, "批量改")]).json()
    assert body["items"][0]["correction_version"] == 1

    # 单条接口基于批量结果继续追加 version 2
    single = client.post(
        "/channels/c1/finalized/1/corrections",
        json={"expected_version": 1, "text": "单条改", "reason": "r"},
    )
    assert single.status_code == 200
    assert single.json()["correction_version"] == 2

    # 批量再基于单条结果追加 version 3；过期版本依旧冲突
    stale = batch(client, "c1", "k2", [item(1, 1)])
    assert stale.status_code == 409
    assert stale.json()["items"][0]["error"] == "version_conflict"
    ok = batch(client, "c1", "k3", [item(1, 2, "再改")]).json()
    assert ok["items"][0]["correction_version"] == 3

    audit = client.get("/channels/c1/finalized/1/corrections").json()
    assert [i["version"] for i in audit["items"]] == [1, 2, 3]


def test_batch_does_not_rewrite_original_finalized(client, conn):
    submit(client, "c1", 1, 1, "原始定稿")
    batch(client, "c1", "k1", [item(1, 0, "更正后")])

    # 原始定稿不变，original 视图默认兼容
    assert conn.execute(
        "SELECT text FROM finalized WHERE channel = 'c1' AND seq = 1"
    ).fetchone()["text"] == "原始定稿"
    original = client.get("/channels/c1/finalized").json()
    assert original["items"][0]["text"] == "原始定稿"
    # effective 视图合并批量更正
    effective = client.get(
        "/channels/c1/finalized", params={"view": "effective"}
    ).json()
    assert effective["items"][0]["text"] == "更正后"
    assert effective["items"][0]["correction_version"] == 1
