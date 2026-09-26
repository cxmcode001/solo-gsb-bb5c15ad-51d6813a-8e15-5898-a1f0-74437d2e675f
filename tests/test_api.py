import random
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


def test_in_order_submission_finalizes_immediately(client):
    body = submit(client, "c1", 1, 1, "hello")
    assert body["applied"] is True
    assert body["watermark"] == 1
    assert [i["seq"] for i in body["finalized"]] == [1]
    assert body["finalized"][0]["text"] == "hello"


def test_out_of_order_waits_for_gap_then_releases_batch(client):
    body = submit(client, "c1", 2, 1, "second")
    assert body["applied"] is True
    assert body["watermark"] == 0
    assert body["finalized"] == []

    body = submit(client, "c1", 1, 1, "first")
    assert body["watermark"] == 2
    assert [i["seq"] for i in body["finalized"]] == [1, 2]
    assert [i["text"] for i in body["finalized"]] == ["first", "second"]


def test_gap_blocks_prefix_and_fill_releases_all(client):
    assert submit(client, "c1", 1, 1)["watermark"] == 1
    assert submit(client, "c1", 2, 1)["watermark"] == 2
    body = submit(client, "c1", 4, 1)
    assert body["watermark"] == 2  # 缺口 3 未补齐，不能越过
    assert body["finalized"] == []

    body = submit(client, "c1", 3, 1)
    assert body["watermark"] == 4
    assert [i["seq"] for i in body["finalized"]] == [3, 4]


def test_higher_revision_replaces_pending_fragment(client):
    submit(client, "c1", 2, 1, "draft")
    body = submit(client, "c1", 2, 5, "revised")
    assert body["applied"] is True

    body = submit(client, "c1", 1, 1, "first")
    assert body["watermark"] == 2
    assert body["finalized"][1]["revision"] == 5
    assert body["finalized"][1]["text"] == "revised"


def test_stale_or_equal_revision_rejected(client):
    submit(client, "c1", 2, 5, "v5")
    for rev in (5, 3, 0):
        body = submit(client, "c1", 2, rev, "stale")
        assert body["applied"] is False
        assert body["reason"] == "stale_revision"
        assert body["watermark"] == 0


def test_finalized_seq_is_immutable(client):
    submit(client, "c1", 1, 1, "final")
    body = submit(client, "c1", 1, 99, "too late")
    assert body["applied"] is False
    assert body["reason"] == "already_finalized"
    assert body["watermark"] == 1

    resp = client.get("/channels/c1/finalized").json()
    assert resp["items"][0]["text"] == "final"


def test_get_finalized_with_incremental_after(client):
    for seq in (1, 2, 3):
        submit(client, "c1", seq, 1, f"line{seq}")

    resp = client.get("/channels/c1/finalized").json()
    assert resp["watermark"] == 3
    assert [i["seq"] for i in resp["items"]] == [1, 2, 3]

    resp = client.get("/channels/c1/finalized", params={"after": 1}).json()
    assert [i["seq"] for i in resp["items"]] == [2, 3]

    resp = client.get("/channels/unknown/finalized").json()
    assert resp["watermark"] == 0
    assert resp["items"] == []


def test_channels_are_isolated(client):
    submit(client, "a", 2, 1)
    assert submit(client, "b", 1, 1)["watermark"] == 1
    assert submit(client, "a", 2, 1)["watermark"] == 0  # a 的缺口仍在


def test_validation_rejects_bad_input(client):
    resp = client.post("/channels/c1/fragments", json={"seq": 0, "revision": 1, "text": "x"})
    assert resp.status_code == 422
    resp = client.post("/channels/c1/fragments", json={"seq": 1, "revision": -1, "text": "x"})
    assert resp.status_code == 422


def test_concurrent_out_of_order_submissions(conn):
    """多线程乱序提交 + 重复修订竞争：最终定稿必须是 1..N 的连续前缀。"""
    n = 300
    channel = "stress"
    jobs = []
    for seq in range(1, n + 1):
        jobs.append((seq, 1, f"seq{seq}-v1"))
        jobs.append((seq, 2, f"seq{seq}-v2"))  # 更高修订号竞争同一片段
    random.Random(42).shuffle(jobs)

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda j: service.submit_fragment(channel, *j), jobs))

    assert all(r["watermark"] <= n for r in results)
    assert service.get_finalized(channel)["watermark"] == n

    rows = conn.execute(
        "SELECT seq, revision, text FROM finalized WHERE channel = ? ORDER BY seq",
        (channel,),
    ).fetchall()
    assert len(rows) == n  # 无重复定稿
    assert [r["seq"] for r in rows] == list(range(1, n + 1))  # 连续前缀，无缺口
    # 每个序号定稿的是定稿时刻已收到的最高修订号（rev2 若晚于定稿到达则被拒绝）
    assert all(r["revision"] in (1, 2) for r in rows)
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM fragments WHERE channel = ?", (channel,)
    ).fetchone()["c"] == 0  # 缓冲区已清空


# ---------- 永久缺口裁决 ----------


def test_gap_decision_records_skips_and_releases_following_buffer(client):
    # 1、2 已定稿；3、4 缺失；5、6 已缓冲
    submit(client, "c1", 1, 1, "one")
    submit(client, "c1", 2, 1, "two")
    submit(client, "c1", 5, 1, "five")
    submit(client, "c1", 6, 1, "six")

    body = decide_gap(client, "c1", 3, 4)
    assert body["applied"] is True
    assert body["idempotent"] is False
    assert body["reason"] is None
    assert body["watermark"] == 6
    assert body["skipped"] == {"start_seq": 3, "end_seq": 4}
    assert [i["seq"] for i in body["finalized"]] == [5, 6]
    assert body["finalized"][0]["kind"] == "subtitle"
    assert body["finalized"][0]["text"] == "five"


def test_gap_decision_without_following_buffer_stops_at_end(client):
    submit(client, "c1", 1, 1)
    body = decide_gap(client, "c1", 2, 3)
    assert body["applied"] is True
    assert body["watermark"] == 3
    assert body["finalized"] == []

    # 裁决后才到达的片段落在水位线内，不可再修改
    body = submit(client, "c1", 2, 1, "late")
    assert body["applied"] is False
    assert body["reason"] == "already_finalized"


def test_gap_start_must_equal_watermark_plus_one(client):
    submit(client, "c1", 1, 1)
    body = decide_gap(client, "c1", 3, 4)  # 水位线为 1，起点必须是 2
    assert body["applied"] is False
    assert body["idempotent"] is False
    assert body["reason"] == "start_not_at_watermark"
    assert body["watermark"] == 1
    assert body["skipped"] is None
    assert body["finalized"] == []


def test_gap_rejected_when_interval_has_buffered_fragments(client, conn):
    submit(client, "c1", 1, 1)
    submit(client, "c1", 3, 1, "buffered")  # 缺口区间内已有缓冲

    body = decide_gap(client, "c1", 2, 4)
    assert body["applied"] is False
    assert body["reason"] == "buffered_fragments"
    assert body["watermark"] == 1

    # 校验失败不得产生任何写入
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM gap_decisions WHERE channel = 'c1'"
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM gaps WHERE channel = 'c1'"
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT watermark FROM channels WHERE channel = 'c1'"
    ).fetchone()["watermark"] == 1
    # 缓冲片段原样保留，后续仍可正常定稿
    submit(client, "c1", 2, 1, "two")
    assert client.get("/channels/c1/finalized").json()["watermark"] == 3


def test_idempotent_repeat_of_same_decided_interval(client):
    submit(client, "c1", 1, 1, "one")
    submit(client, "c1", 4, 1, "four")
    submit(client, "c1", 5, 1, "five")

    first = decide_gap(client, "c1", 2, 3)
    assert first["watermark"] == 5
    assert [i["seq"] for i in first["finalized"]] == [4, 5]

    # 重复提交同一区间：幂等返回原裁决结果
    again = decide_gap(client, "c1", 2, 3)
    assert again["applied"] is True
    assert again["idempotent"] is True
    assert again["reason"] is None
    assert again["watermark"] == 5
    assert again["skipped"] == {"start_seq": 2, "end_seq": 3}
    assert [i["seq"] for i in again["finalized"]] == [4, 5]
    assert again["finalized"][0]["text"] == "four"


def test_idempotent_gap_without_released_fragments(client):
    decide_gap(client, "c1", 1, 2)
    again = decide_gap(client, "c1", 1, 2)
    assert again["applied"] is True
    assert again["idempotent"] is True
    assert again["watermark"] == 2
    assert again["finalized"] == []


def test_partial_overlap_with_decided_gap_is_rejected(client, conn):
    submit(client, "c1", 1, 1)
    decide_gap(client, "c1", 2, 4)  # 水位线推进到 4

    # 与既有裁决部分重叠（含）
    body = decide_gap(client, "c1", 3, 4)
    assert body["applied"] is False
    assert body["reason"] == "gap_overlap"
    assert body["watermark"] == 4
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM gap_decisions WHERE channel = 'c1'"
    ).fetchone()["c"] == 1  # 未新增裁决

    # 完全落在已定稿区域、且不匹配任何缺口
    body = decide_gap(client, "c1", 1, 1)
    assert body["applied"] is False
    assert body["reason"] == "already_finalized"


def test_repeated_rejected_gap_decision_writes_nothing(client, conn):
    submit(client, "c1", 1, 1)
    for _ in range(3):
        body = decide_gap(client, "c1", 3, 5)
        assert body["reason"] == "start_not_at_watermark"
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM gap_decisions WHERE channel = 'c1'"
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM gaps WHERE channel = 'c1'"
    ).fetchone()["c"] == 0


def test_finalized_query_interleaves_subtitles_and_gap_markers(client):
    submit(client, "c1", 1, 1, "one")
    submit(client, "c1", 5, 1, "five")
    decide_gap(client, "c1", 2, 4)

    resp = client.get("/channels/c1/finalized").json()
    assert resp["watermark"] == 5
    kinds = [(i["kind"], i["seq"]) for i in resp["items"]]
    assert kinds == [
        ("subtitle", 1),
        ("gap", 2),
        ("gap", 3),
        ("gap", 4),
        ("subtitle", 5),
    ]
    gap = resp["items"][1]
    assert gap["start_seq"] == 2
    assert gap["end_seq"] == 4
    assert gap["text"] is None
    assert gap["revision"] is None

    # after 增量拉取对字幕与缺口标记统一按序号计数
    resp = client.get("/channels/c1/finalized", params={"after": 3}).json()
    assert [(i["kind"], i["seq"]) for i in resp["items"]] == [
        ("gap", 4),
        ("subtitle", 5),
    ]


def test_multiple_gaps_and_submissions_interleave(client):
    submit(client, "c1", 1, 1, "one")
    decide_gap(client, "c1", 2, 2)
    submit(client, "c1", 3, 1, "three")
    submit(client, "c1", 6, 1, "six")
    decide_gap(client, "c1", 4, 5)

    resp = client.get("/channels/c1/finalized").json()
    assert resp["watermark"] == 6
    assert [(i["kind"], i["seq"]) for i in resp["items"]] == [
        ("subtitle", 1),
        ("gap", 2),
        ("subtitle", 3),
        ("gap", 4),
        ("gap", 5),
        ("subtitle", 6),
    ]

    # 两个相邻的独立缺口区间不可合并为一次幂等区间
    body = decide_gap(client, "c1", 2, 5)
    assert body["applied"] is False
    assert body["reason"] == "gap_overlap"
    # 各自仍可单独幂等返回
    assert decide_gap(client, "c1", 2, 2)["idempotent"] is True
    assert decide_gap(client, "c1", 4, 5)["idempotent"] is True


def test_gap_validation_rejects_malformed_payload(client):
    resp = client.post("/channels/c1/gaps", json={"start_seq": 0, "end_seq": 2})
    assert resp.status_code == 422
    resp = client.post("/channels/c1/gaps", json={"start_seq": 5, "end_seq": 3})
    assert resp.status_code == 422


def test_gap_decisions_are_isolated_per_channel(client):
    decide_gap(client, "a", 1, 2)
    submit(client, "b", 1, 1, "b-one")
    resp = client.get("/channels/a/finalized").json()
    assert resp["watermark"] == 2
    assert all(i["kind"] == "gap" for i in resp["items"])
    resp = client.get("/channels/b/finalized").json()
    assert resp["watermark"] == 1
    assert [i["kind"] for i in resp["items"]] == ["subtitle"]


def test_gap_on_unknown_channel_starts_at_one(client):
    body = decide_gap(client, "fresh", 1, 3)
    assert body["applied"] is True
    assert body["watermark"] == 3
    assert body["skipped"] == {"start_seq": 1, "end_seq": 3}

    body = decide_gap(client, "fresh", 5, 6)
    assert body["applied"] is False
    assert body["reason"] == "start_not_at_watermark"
