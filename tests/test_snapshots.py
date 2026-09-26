"""发布快照测试。

覆盖：
- 单事务冻结：快照包含创建时刻 1..watermark 的原始 / 生效字幕、缺口标记
  及各序号全部更正版本；
- idempotency_key 重试（含跨线程并发）始终返回同一 snapshot_id；
- 快照分页 after/limit（字幕与缺口按序号统一计数）；
- 快照创建后的新定稿、新更正、新缺口不改变既有快照（原始视图与生效视图
  均稳定，快照更正审计亦冻结）；
- 不存在的频道可生成 watermark=0 的空快照，且不创建频道行；
- 快照不存在 / 序号越界 / 缺口序号审计的错误码；
- 三张快照表在存储层禁止 UPDATE/DELETE。
"""

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from app import service
from app.db import write_tx


def submit(client, channel, seq, revision, text="t"):
    resp = client.post(
        f"/channels/{channel}/fragments",
        json={"seq": seq, "revision": revision, "text": text},
    )
    assert resp.status_code == 200
    return resp.json()


def decide_gap(client, channel, start_seq, end_seq):
    return client.post(
        f"/channels/{channel}/gaps",
        json={"start_seq": start_seq, "end_seq": end_seq},
    )


def correct(client, channel, seq, expected_version, text, reason):
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


def build_mixed_channel(client, channel="news"):
    """1=字幕（有 2 个更正）、2-3=缺口、4=字幕（有 1 个更正）、5=字幕（无更正）。"""
    submit(client, channel, 1, 1, "第一条")
    submit(client, channel, 4, 1, "第四条")
    submit(client, channel, 5, 1, "第五条")
    decide_gap(client, channel, 2, 3)
    correct(client, channel, 1, 0, "第一条-改", "r1")
    correct(client, channel, 1, 1, "第一条-再改", "r2")
    correct(client, channel, 4, 0, "第四条-改", "r1")
    return make_snapshot(client, channel, "snap-1")


# ---------- 创建与冻结内容 ----------


def test_snapshot_freezes_original_and_gap_markers(client):
    snap = build_mixed_channel(client)
    assert snap["watermark"] == 5
    assert snap["idempotent"] is False
    assert snap["snapshot_id"]
    assert snap["created_at"]

    resp = client.get(f"/snapshots/{snap['snapshot_id']}/items")
    assert resp.status_code == 200
    body = resp.json()
    assert body["snapshot_id"] == snap["snapshot_id"]
    assert body["channel"] == "news"
    assert body["watermark"] == 5
    assert "view" not in body or body["view"] == "original"
    assert "correction_version" not in body
    assert [(i["kind"], i["seq"]) for i in body["items"]] == [
        ("subtitle", 1),
        ("gap", 2),
        ("gap", 3),
        ("subtitle", 4),
        ("subtitle", 5),
    ]
    # original 视图冻结原始定稿文本，且不返回 correction_version
    first = body["items"][0]
    assert first["text"] == "第一条"
    assert first["revision"] == 1
    assert "correction_version" not in first
    gap = body["items"][1]
    assert gap["start_seq"] == 2
    assert gap["end_seq"] == 3
    assert gap["text"] is None
    assert gap["revision"] is None


def test_snapshot_effective_view_freezes_latest_corrections(client):
    snap = build_mixed_channel(client)
    body = client.get(
        f"/snapshots/{snap['snapshot_id']}/items", params={"view": "effective"}
    ).json()
    assert body["view"] == "effective"
    assert body["correction_version"] == 2  # 本页最大快照更正版本
    items = body["items"]
    assert items[0]["text"] == "第一条-再改"
    assert items[0]["correction_version"] == 2
    assert items[0]["revision"] == 1
    assert items[1]["text"] is None
    assert items[1]["correction_version"] is None
    assert items[3]["text"] == "第四条-改"
    assert items[3]["correction_version"] == 1
    assert items[4]["text"] == "第五条"
    assert items[4]["correction_version"] == 0


def test_snapshot_corrections_audit_is_frozen_history(client):
    snap = build_mixed_channel(client)
    resp = client.get(f"/snapshots/{snap['snapshot_id']}/items/1/corrections")
    assert resp.status_code == 200
    body = resp.json()
    assert body["snapshot_id"] == snap["snapshot_id"]
    assert body["channel"] == "news"
    assert body["seq"] == 1
    assert body["current_version"] == 2
    assert [(i["seq"], i["version"], i["text"]) for i in body["items"]] == [
        (1, 1, "第一条-改"),
        (1, 2, "第一条-再改"),
    ]
    assert all(i["reason"] and i["created_at"] for i in body["items"])

    # 无更正的字幕序号：current_version=0、空列表
    body = client.get(
        f"/snapshots/{snap['snapshot_id']}/items/5/corrections"
    ).json()
    assert body["current_version"] == 0
    assert body["items"] == []


def test_empty_snapshot_for_unknown_channel_does_not_create_channel(client, conn):
    snap = make_snapshot(client, "ghost", "empty-1")
    assert snap["watermark"] == 0
    assert snap["idempotent"] is False
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM channels WHERE channel = 'ghost'"
    ).fetchone()["c"] == 0

    body = client.get(f"/snapshots/{snap['snapshot_id']}/items").json()
    assert body["watermark"] == 0
    assert body["items"] == []
    body = client.get(
        f"/snapshots/{snap['snapshot_id']}/items", params={"view": "effective"}
    ).json()
    assert body["correction_version"] == 0
    assert body["items"] == []


# ---------- 幂等 ----------


def test_same_idempotency_key_returns_same_snapshot(client):
    submit(client, "c1", 1, 1, "one")
    first = make_snapshot(client, "c1", "dup-key")
    assert first["idempotent"] is False

    # 频道推进后用相同 key 重试：仍返回创建时水位线 1 的原快照
    submit(client, "c1", 2, 1, "two")
    again = make_snapshot(client, "c1", "dup-key")
    assert again["idempotent"] is True
    assert again["snapshot_id"] == first["snapshot_id"]
    assert again["watermark"] == 1
    assert again["created_at"] == first["created_at"]
    assert again["idempotency_key"] == "dup-key"

    body = client.get(f"/snapshots/{first['snapshot_id']}/items").json()
    assert [i["seq"] for i in body["items"]] == [1]

    # 不同 key 创建新快照，锁定新水位线
    second = make_snapshot(client, "c1", "other-key")
    assert second["idempotent"] is False
    assert second["snapshot_id"] != first["snapshot_id"]
    assert second["watermark"] == 2


def test_idempotency_keys_are_scoped_per_channel(client):
    submit(client, "a", 1, 1, "a1")
    snap_a = make_snapshot(client, "a", "shared")
    snap_b = make_snapshot(client, "b", "shared")  # b 不存在 -> 空快照
    assert snap_a["snapshot_id"] != snap_b["snapshot_id"]
    assert snap_a["watermark"] == 1
    assert snap_b["watermark"] == 0
    # 各自重试都命中各自的快照
    assert make_snapshot(client, "a", "shared")["snapshot_id"] == snap_a["snapshot_id"]
    assert make_snapshot(client, "b", "shared")["snapshot_id"] == snap_b["snapshot_id"]


def test_concurrent_snapshot_creation_with_same_key_creates_one(client):
    submit(client, "c1", 1, 1, "one")

    def attempt(_):
        try:
            return service.create_snapshot("c1", "race")
        except Exception as exc:  # noqa: BLE001
            return exc

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(attempt, range(20)))
    assert all(isinstance(r, dict) for r in results)
    ids = {r["snapshot_id"] for r in results}
    assert len(ids) == 1
    created = [r for r in results if not r["idempotent"]]
    assert len(created) == 1


def test_snapshot_idempotency_key_validation(client):
    resp = client.post("/channels/c1/snapshots", json={"idempotency_key": ""})
    assert resp.status_code == 422
    resp = client.post("/channels/c1/snapshots", json={"idempotency_key": "   "})
    assert resp.status_code == 422
    resp = client.post("/channels/c1/snapshots", json={})
    assert resp.status_code == 422


# ---------- 分页 ----------


def test_snapshot_pagination_after_and_limit(client):
    snap = build_mixed_channel(client)
    sid = snap["snapshot_id"]

    body = client.get(f"/snapshots/{sid}/items", params={"after": 0, "limit": 2}).json()
    assert [(i["kind"], i["seq"]) for i in body["items"]] == [
        ("subtitle", 1),
        ("gap", 2),
    ]

    body = client.get(f"/snapshots/{sid}/items", params={"after": 2, "limit": 2}).json()
    assert [(i["kind"], i["seq"]) for i in body["items"]] == [
        ("gap", 3),
        ("subtitle", 4),
    ]

    # 逐页拉全
    seen, after = [], 0
    while True:
        page = client.get(
            f"/snapshots/{sid}/items", params={"after": after, "limit": 2}
        ).json()["items"]
        if not page:
            break
        seen.extend(page)
        after = page[-1]["seq"]
    assert [i["seq"] for i in seen] == [1, 2, 3, 4, 5]

    # after 越过水位线：空页；watermark 仍是快照锁定值
    body = client.get(f"/snapshots/{sid}/items", params={"after": 5}).json()
    assert body["items"] == []
    assert body["watermark"] == 5


def test_snapshot_pagination_validate_params(client):
    snap = make_snapshot(client, "c1", "k")
    assert client.get(f"/snapshots/{snap['snapshot_id']}/items?after=-1").status_code == 422
    assert client.get(f"/snapshots/{snap['snapshot_id']}/items?limit=0").status_code == 422
    assert client.get(
        f"/snapshots/{snap['snapshot_id']}/items", params={"view": "draft"}
    ).status_code == 422


# ---------- 不可变：后续变化不影响快照 ----------


def test_snapshot_immutable_after_later_submissions_corrections_and_gaps(client):
    snap = build_mixed_channel(client)
    sid = snap["snapshot_id"]

    # 快照之后：旧序号再更正、缓冲新片段、裁决新缺口
    correct(client, "news", 1, 2, "第一条-快照后再改", "r3")
    correct(client, "news", 5, 0, "第五条-改", "r1")
    submit(client, "news", 8, 1, "第八条")
    decide_gap(client, "news", 6, 7)

    # 实时定稿视图已推进
    live = client.get("/channels/news/finalized", params={"view": "effective"}).json()
    assert live["watermark"] == 8
    assert live["items"][0]["text"] == "第一条-快照后再改"
    assert live["items"][0]["correction_version"] == 3

    # 既有快照 original / effective / 审计均保持创建时刻内容
    original = client.get(f"/snapshots/{sid}/items").json()
    assert original["watermark"] == 5
    assert [(i["kind"], i["seq"]) for i in original["items"]] == [
        ("subtitle", 1),
        ("gap", 2),
        ("gap", 3),
        ("subtitle", 4),
        ("subtitle", 5),
    ]
    assert original["items"][0]["text"] == "第一条"

    effective = client.get(
        f"/snapshots/{sid}/items", params={"view": "effective"}
    ).json()
    assert effective["watermark"] == 5
    assert effective["items"][0]["text"] == "第一条-再改"
    assert effective["items"][0]["correction_version"] == 2
    assert effective["items"][4]["text"] == "第五条"
    assert effective["items"][4]["correction_version"] == 0
    assert len(effective["items"]) == 5

    audit = client.get(f"/snapshots/{sid}/items/1/corrections").json()
    assert audit["current_version"] == 2
    assert [i["version"] for i in audit["items"]] == [1, 2]
    assert client.get(f"/snapshots/{sid}/items/5/corrections").json()["items"] == []

    # 同 key 重试仍返回同一快照（水位线仍为 5）
    again = make_snapshot(client, "news", "snap-1")
    assert again["snapshot_id"] == sid
    assert again["watermark"] == 5
    assert again["idempotent"] is True


def test_snapshot_freezes_before_watermark_advance_in_same_tx(client):
    """创建快照与后续写入串行化：快照只含提交时刻已定稿的前缀。"""
    submit(client, "c1", 1, 1, "one")
    snap_before = make_snapshot(client, "c1", "k1")
    assert snap_before["watermark"] == 1
    # 乱序缓冲 3 不会影响任何快照
    submit(client, "c1", 3, 1, "three")
    assert client.get(
        f"/snapshots/{snap_before['snapshot_id']}/items"
    ).json()["items"] == [
        {"kind": "subtitle", "seq": 1, "revision": 1, "text": "one",
         "start_seq": None, "end_seq": None}
    ]
    snap_mid = make_snapshot(client, "c1", "k2")
    assert snap_mid["watermark"] == 1
    # 2 到达后释放 2、3，旧快照不变，新快照水位线为 3
    submit(client, "c1", 2, 1, "two")
    assert client.get(
        f"/snapshots/{snap_before['snapshot_id']}/items"
    ).json()["watermark"] == 1
    snap_after = make_snapshot(client, "c1", "k3")
    assert snap_after["watermark"] == 3
    assert [
        i["seq"]
        for i in client.get(
            f"/snapshots/{snap_after['snapshot_id']}/items"
        ).json()["items"]
    ] == [1, 2, 3]


# ---------- 错误路径 ----------


def test_get_unknown_snapshot_returns_404(client):
    resp = client.get("/snapshots/does-not-exist/items")
    assert resp.status_code == 404
    assert resp.json()["error"] == "snapshot_not_found"


def test_snapshot_corrections_error_paths(client):
    snap = build_mixed_channel(client)
    sid = snap["snapshot_id"]

    assert client.get("/snapshots/no-such/items/1/corrections").status_code == 404

    # 超出快照水位线（即使该序号后来已定稿）
    resp = client.get(f"/snapshots/{sid}/items/6/corrections")
    assert resp.status_code == 404
    assert resp.json()["error"] == "seq_out_of_snapshot"

    # 缺口序号
    resp = client.get(f"/snapshots/{sid}/items/2/corrections")
    assert resp.status_code == 409
    assert resp.json()["error"] == "seq_is_gap"


# ---------- 存储层不可变 ----------


def test_snapshot_tables_reject_update_and_delete_at_storage(client, conn):
    snap = build_mixed_channel(client)
    sid = snap["snapshot_id"]

    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute(
                "UPDATE snapshots SET watermark = 99 WHERE snapshot_id = ?", (sid,)
            )
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute("DELETE FROM snapshots WHERE snapshot_id = ?", (sid,))
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute(
                "UPDATE snapshot_items SET original_text = 'hacked' "
                "WHERE snapshot_id = ? AND seq = 1",
                (sid,),
            )
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute("DELETE FROM snapshot_items WHERE snapshot_id = ?", (sid,))
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute(
                "UPDATE snapshot_corrections SET text = 'hacked' "
                "WHERE snapshot_id = ? AND seq = 1 AND version = 1",
                (sid,),
            )
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute("DELETE FROM snapshot_corrections WHERE snapshot_id = ?", (sid,))

    # 触发器回滚后快照内容仍可读且未被篡改
    body = client.get(f"/snapshots/{sid}/items").json()
    assert body["watermark"] == 5
    assert body["items"][0]["text"] == "第一条"


def test_snapshot_rows_are_complete_in_storage(client, conn):
    snap = build_mixed_channel(client)
    sid = snap["snapshot_id"]
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM snapshot_items WHERE snapshot_id = ?", (sid,)
    ).fetchone()["c"] == 5
    # seq 1 有 2 个冻结版本，seq 4 有 1 个
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM snapshot_corrections WHERE snapshot_id = ?", (sid,)
    ).fetchone()["c"] == 3
