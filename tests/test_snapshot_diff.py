"""快照差异接口测试。

覆盖：
- 按 seq 合并两份快照：added / removed / changed 三类差异；
- 字幕以冻结生效文本 + correction_version 判定（文本相同但版本不同也算变更）；
- 缺口以裁决区间端点判定（两端点一致的缺口不返回）；
- 未变化条目不返回；after/limit 分页只计差异条目，next_after 指向本页
  最后一条差异，稀疏差异逐页拉取不漏项；
- 快照不存在（基准或 other）-> 404，跨频道 -> 409；
- 参数校验（缺 other、after/limit 越界）-> 422。
"""


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


def correct(client, channel, seq, expected_version, text, reason="r"):
    resp = client.post(
        f"/channels/{channel}/finalized/{seq}/corrections",
        json={
            "expected_version": expected_version,
            "text": text,
            "reason": reason,
        },
    )
    assert resp.status_code == 200
    return resp.json()


def make_snapshot(client, channel, key):
    resp = client.post(
        f"/channels/{channel}/snapshots", json={"idempotency_key": key}
    )
    assert resp.status_code == 200
    return resp.json()


def diff(client, snapshot_id, other, **params):
    return client.get(f"/snapshots/{snapshot_id}/diff", params={"other": other, **params})


def build_base_channel(client, channel="news"):
    """1=字幕（2 个更正）、2-3=缺口、4=字幕（1 个更正）、5=字幕（无更正），快照 A。"""
    submit(client, channel, 1, 1, "第一条")
    submit(client, channel, 4, 1, "第四条")
    submit(client, channel, 5, 1, "第五条")
    decide_gap(client, channel, 2, 3)
    correct(client, channel, 1, 0, "第一条-改")
    correct(client, channel, 1, 1, "第一条-再改")
    correct(client, channel, 4, 0, "第四条-改")
    return make_snapshot(client, channel, "snap-a")


def advance_channel(client, channel="news"):
    """在快照 A 之后推进频道：再更正 1、首更正 5、新字幕 6 与 9、新缺口 7-8，快照 B。"""
    correct(client, channel, 1, 2, "第一条-三改")
    correct(client, channel, 5, 0, "第五条-改")
    submit(client, channel, 6, 1, "第六条")
    submit(client, channel, 9, 1, "第九条")
    decide_gap(client, channel, 7, 8)
    return make_snapshot(client, channel, "snap-b")


# ---------- 三类差异 ----------


def test_diff_added_removed_changed(client):
    snap_a = build_base_channel(client)
    snap_b = advance_channel(client)

    resp = diff(client, snap_a["snapshot_id"], snap_b["snapshot_id"])
    assert resp.status_code == 200
    body = resp.json()
    assert body["snapshot_id"] == snap_a["snapshot_id"]
    assert body["other_snapshot_id"] == snap_b["snapshot_id"]
    assert body["channel"] == "news"
    assert body["watermark"] == 5
    assert body["other_watermark"] == 9
    assert body["next_after"] == 9

    items = body["items"]
    assert [(i["change"], i["seq"]) for i in items] == [
        ("changed", 1),
        ("changed", 5),
        ("added", 6),
        ("added", 7),
        ("added", 8),
        ("added", 9),
    ]

    # changed：字幕按冻结生效文本与 correction_version 判定，两侧内容都返回
    first = items[0]
    assert first["base"]["kind"] == "subtitle"
    assert first["base"]["text"] == "第一条-再改"
    assert first["base"]["correction_version"] == 2
    assert first["other"]["text"] == "第一条-三改"
    assert first["other"]["correction_version"] == 3
    assert first["base"]["revision"] == first["other"]["revision"] == 1

    fifth = items[1]
    assert fifth["base"]["text"] == "第五条"
    assert fifth["base"]["correction_version"] == 0
    assert fifth["other"]["text"] == "第五条-改"
    assert fifth["other"]["correction_version"] == 1

    # added：base 为 null，other 携带冻结内容（含缺口区间端点）
    added_sub = items[2]
    assert added_sub["base"] is None
    assert added_sub["other"] == {
        "kind": "subtitle",
        "seq": 6,
        "revision": 1,
        "text": "第六条",
        "start_seq": None,
        "end_seq": None,
        "correction_version": 0,
    }
    added_gap = items[3]
    assert added_gap["base"] is None
    assert added_gap["other"]["kind"] == "gap"
    assert added_gap["other"]["start_seq"] == 7
    assert added_gap["other"]["end_seq"] == 8
    assert added_gap["other"]["correction_version"] is None

    # 反向比较：added 变 removed，changed 两侧互换
    body = diff(client, snap_b["snapshot_id"], snap_a["snapshot_id"]).json()
    assert body["watermark"] == 9
    assert body["other_watermark"] == 5
    assert [(i["change"], i["seq"]) for i in body["items"]] == [
        ("changed", 1),
        ("changed", 5),
        ("removed", 6),
        ("removed", 7),
        ("removed", 8),
        ("removed", 9),
    ]
    first = body["items"][0]
    assert first["base"]["correction_version"] == 3
    assert first["other"]["correction_version"] == 2
    removed = body["items"][2]
    assert removed["other"] is None
    assert removed["base"]["seq"] == 6


def test_unchanged_items_are_not_returned(client):
    """端点一致的缺口与未再更正的字幕不出现在差异里。"""
    snap_a = build_base_channel(client)
    # 快照之间只推进水位线，不触碰 1..5 的任何内容
    submit(client, "news", 6, 1, "第六条")
    snap_b = make_snapshot(client, "news", "snap-b")

    body = diff(client, snap_a["snapshot_id"], snap_b["snapshot_id"]).json()
    # 2-3 缺口（端点相同）与 4（无新更正）均未返回，只有新增的 6
    assert [(i["change"], i["seq"]) for i in body["items"]] == [("added", 6)]


def test_correction_with_same_text_still_counts_as_changed(client):
    """生效文本相同但 correction_version 不同也算内容变更。"""
    submit(client, "c1", 1, 1, "原文")
    snap_a = make_snapshot(client, "c1", "a")
    correct(client, "c1", 1, 0, "原文")  # 文本不变，版本 0 -> 1
    snap_b = make_snapshot(client, "c1", "b")

    body = diff(client, snap_a["snapshot_id"], snap_b["snapshot_id"]).json()
    assert [(i["change"], i["seq"]) for i in body["items"]] == [("changed", 1)]
    entry = body["items"][0]
    assert entry["base"]["text"] == entry["other"]["text"] == "原文"
    assert entry["base"]["correction_version"] == 0
    assert entry["other"]["correction_version"] == 1


def test_identical_snapshots_have_empty_diff(client):
    snap_a = build_base_channel(client)
    # 与自己比较
    body = diff(client, snap_a["snapshot_id"], snap_a["snapshot_id"]).json()
    assert body["items"] == []
    assert body["next_after"] is None
    # 内容完全一致的两个快照（期间频道无任何变化）
    snap_b = make_snapshot(client, "news", "snap-b")
    body = diff(client, snap_a["snapshot_id"], snap_b["snapshot_id"]).json()
    assert body["items"] == []
    assert body["next_after"] is None


def test_empty_snapshot_diff(client):
    empty_a = make_snapshot(client, "ghost", "a")
    empty_b = make_snapshot(client, "ghost", "b")
    body = diff(client, empty_a["snapshot_id"], empty_b["snapshot_id"]).json()
    assert body["watermark"] == 0
    assert body["other_watermark"] == 0
    assert body["items"] == []
    assert body["next_after"] is None

    # 空快照 vs 有内容的同频道快照：全部记为 added / removed
    submit(client, "ghost", 1, 1, "one")
    snap_c = make_snapshot(client, "ghost", "c")
    body = diff(client, empty_a["snapshot_id"], snap_c["snapshot_id"]).json()
    assert [(i["change"], i["seq"]) for i in body["items"]] == [("added", 1)]
    body = diff(client, snap_c["snapshot_id"], empty_a["snapshot_id"]).json()
    assert [(i["change"], i["seq"]) for i in body["items"]] == [("removed", 1)]


# ---------- 分页：limit 只计差异，稀疏变化不漏项 ----------


def test_diff_pagination_counts_only_diff_entries(client):
    # 1..10 全字幕，快照 A 后只更正 2 与 9（稀疏差异）
    for seq in range(1, 11):
        submit(client, "c1", seq, 1, f"第{seq}条")
    snap_a = make_snapshot(client, "c1", "a")
    correct(client, "c1", 2, 0, "第2条-改")
    correct(client, "c1", 9, 0, "第9条-改")
    snap_b = make_snapshot(client, "c1", "b")
    sid, oid = snap_a["snapshot_id"], snap_b["snapshot_id"]

    # limit=2 一页拿全：中间 8 条未变化条目不占 limit 额度
    body = diff(client, sid, oid, limit=2).json()
    assert [i["seq"] for i in body["items"]] == [2, 9]
    assert body["next_after"] == 9

    # limit=1 逐页拉取：next_after 指向本页最后一条差异
    page1 = diff(client, sid, oid, limit=1).json()
    assert [i["seq"] for i in page1["items"]] == [2]
    assert page1["next_after"] == 2

    page2 = diff(client, sid, oid, after=page1["next_after"], limit=1).json()
    assert [i["seq"] for i in page2["items"]] == [9]
    assert page2["next_after"] == 9

    page3 = diff(client, sid, oid, after=page2["next_after"], limit=1).json()
    assert page3["items"] == []
    assert page3["next_after"] is None

    # 游标式翻页汇总，确认无漏项、无重复
    seen, after = [], 0
    while True:
        page = diff(client, sid, oid, after=after, limit=1).json()
        if not page["items"]:
            break
        seen.extend(i["seq"] for i in page["items"])
        after = page["next_after"]
    assert seen == [2, 9]


def test_diff_pagination_after_skips_earlier_diffs(client):
    snap_a = build_base_channel(client)
    snap_b = advance_channel(client)
    sid, oid = snap_a["snapshot_id"], snap_b["snapshot_id"]

    # after=5：跳过 seq<=5 的 changed，只返回新增部分
    body = diff(client, sid, oid, after=5).json()
    assert [(i["change"], i["seq"]) for i in body["items"]] == [
        ("added", 6),
        ("added", 7),
        ("added", 8),
        ("added", 9),
    ]
    assert body["next_after"] == 9

    # after 越过最大差异序号：空页
    body = diff(client, sid, oid, after=9).json()
    assert body["items"] == []
    assert body["next_after"] is None


# ---------- 错误路径与参数校验 ----------


def test_diff_unknown_snapshot_returns_404(client):
    snap = make_snapshot(client, "c1", "a")
    resp = diff(client, "no-such-base", snap["snapshot_id"])
    assert resp.status_code == 404
    assert resp.json()["error"] == "snapshot_not_found"

    resp = diff(client, snap["snapshot_id"], "no-such-other")
    assert resp.status_code == 404
    assert resp.json()["error"] == "snapshot_not_found"
    assert "no-such-other" in resp.json()["message"]


def test_diff_cross_channel_rejected(client):
    submit(client, "c1", 1, 1, "one")
    submit(client, "c2", 1, 1, "two")
    snap_a = make_snapshot(client, "c1", "a")
    snap_b = make_snapshot(client, "c2", "b")

    resp = diff(client, snap_a["snapshot_id"], snap_b["snapshot_id"])
    assert resp.status_code == 409
    assert resp.json()["error"] == "snapshot_channel_mismatch"

    resp = diff(client, snap_b["snapshot_id"], snap_a["snapshot_id"])
    assert resp.status_code == 409
    assert resp.json()["error"] == "snapshot_channel_mismatch"


def test_diff_param_validation(client):
    snap = make_snapshot(client, "c1", "a")
    sid = snap["snapshot_id"]
    assert client.get(f"/snapshots/{sid}/diff").status_code == 422  # 缺 other
    assert diff(client, sid, sid, after=-1).status_code == 422
    assert diff(client, sid, sid, limit=0).status_code == 422
    assert diff(client, sid, sid, limit=10001).status_code == 422
