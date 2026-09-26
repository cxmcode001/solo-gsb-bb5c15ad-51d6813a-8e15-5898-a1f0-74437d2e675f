"""追加式更正层测试。

覆盖：
- 仅已定稿字幕可更正；未定稿序号 / 缺口序号拒绝且不写入；
- expected_version 乐观并发：首更正必须为 0，之后严格递增，冲突返回 409；
- 原始定稿永不改写；view=original 默认兼容，view=effective 合并最新文本；
- 更正审计按 version 升序；
- 多线程竞争下每个版本恰好落一条；
- 触发器在存储层禁止 UPDATE/DELETE。
"""

import sqlite3
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


def correct(client, channel, seq, expected_version, text, reason):
    return client.post(
        f"/channels/{channel}/finalized/{seq}/corrections",
        json={
            "expected_version": expected_version,
            "text": text,
            "reason": reason,
        },
    )


def decide_gap(client, channel, start_seq, end_seq):
    return client.post(
        f"/channels/{channel}/gaps",
        json={"start_seq": start_seq, "end_seq": end_seq},
    )


# ---------- 基本追加与版本 ----------


def test_first_correction_requires_expected_version_zero(client):
    submit(client, "c1", 1, 1, "原文")

    resp = correct(client, "c1", 1, 1, "更正一", "错字")
    assert resp.status_code == 409
    assert resp.json()["error"] == "version_conflict"

    body = correct(client, "c1", 1, 0, "更正一", "错字").json()
    assert body["correction_version"] == 1
    assert body["text"] == "更正一"
    assert body["reason"] == "错字"
    assert body["seq"] == 1
    assert body["channel"] == "c1"
    assert body["created_at"]


def test_versions_append_monotonically_and_optimistic_lock(client):
    submit(client, "c1", 1, 1, "v0")

    assert correct(client, "c1", 1, 0, "v1", "r1").json()["correction_version"] == 1
    # 过期的 expected_version 一律冲突
    stale = correct(client, "c1", 1, 0, "old", "r")
    assert stale.status_code == 409
    assert stale.json()["error"] == "version_conflict"

    assert correct(client, "c1", 1, 1, "v2", "r2").json()["correction_version"] == 2
    again = correct(client, "c1", 1, 1, "stale", "r")
    assert again.status_code == 409
    assert correct(client, "c1", 1, 2, "v3", "r3").json()["correction_version"] == 3


def test_rejected_correction_writes_nothing(client, conn):
    submit(client, "c1", 1, 1, "定稿一")
    submit(client, "c1", 2, 1, "定稿二")  # seq 3 未定稿
    decide_gap(client, "c1", 3, 3)

    # 未定稿序号（水位线之外）
    resp = correct(client, "c1", 99, 0, "x", "r")
    assert resp.status_code == 409
    assert resp.json()["error"] == "not_finalized"

    # 缺口序号
    resp = correct(client, "c1", 3, 0, "x", "r")
    assert resp.status_code == 409
    assert resp.json()["error"] == "seq_is_gap"

    # 已定稿但版本号错误
    resp = correct(client, "c1", 1, 5, "x", "r")
    assert resp.status_code == 409

    # 频道不存在
    resp = correct(client, "ghost", 1, 0, "x", "r")
    assert resp.status_code == 409
    assert resp.json()["error"] == "not_finalized"

    assert conn.execute("SELECT COUNT(*) AS c FROM corrections").fetchone()["c"] == 0
    # 原始定稿不受影响
    assert conn.execute(
        "SELECT text FROM finalized WHERE channel = 'c1' AND seq = 1"
    ).fetchone()["text"] == "定稿一"


def test_correction_validation_rejects_blank_and_negative(client):
    submit(client, "c1", 1, 1, "原文")
    assert correct(client, "c1", 1, 0, "", "r").status_code == 422
    assert correct(client, "c1", 1, 0, "x", "").status_code == 422
    assert correct(client, "c1", 1, 0, "   ", "r").status_code == 422
    resp = correct(client, "c1", 1, -1, "x", "r")
    assert resp.status_code == 422


# ---------- original / effective 视图 ----------


def test_default_view_is_original_and_stays_compatible(client):
    submit(client, "c1", 1, 1, "原文")
    correct(client, "c1", 1, 0, "更正一", "r1")
    correct(client, "c1", 1, 1, "更正二", "r2")

    resp = client.get("/channels/c1/finalized").json()
    assert "view" not in resp
    assert "correction_version" not in resp
    assert resp["items"][0]["text"] == "原文"
    assert "correction_version" not in resp["items"][0]

    resp = client.get("/channels/c1/finalized", params={"view": "original"}).json()
    assert resp["items"][0]["text"] == "原文"


def test_effective_view_merges_latest_text_and_version(client):
    submit(client, "c1", 1, 1, "第一条")
    submit(client, "c1", 4, 1, "第四条")
    decide_gap(client, "c1", 2, 3)
    correct(client, "c1", 1, 0, "第一条-改", "r1")
    correct(client, "c1", 4, 0, "第四条-改", "r1")
    correct(client, "c1", 1, 1, "第一条-再改", "r2")

    resp = client.get(
        "/channels/c1/finalized", params={"view": "effective"}
    ).json()
    assert resp["view"] == "effective"
    assert resp["correction_version"] == 2  # 本页最大更正版本
    items = resp["items"]
    assert [(i["kind"], i["seq"]) for i in items] == [
        ("subtitle", 1),
        ("gap", 2),
        ("gap", 3),
        ("subtitle", 4),
    ]
    assert items[0]["text"] == "第一条-再改"
    assert items[0]["correction_version"] == 2
    assert items[0]["revision"] == 1  # 原始修订号保留
    assert items[1]["correction_version"] is None
    assert items[1]["text"] is None
    assert items[3]["text"] == "第四条-改"
    assert items[3]["correction_version"] == 1


def test_effective_view_without_corrections_uses_version_zero(client):
    submit(client, "c1", 1, 1, "第一条")
    resp = client.get("/channels/c1/finalized", params={"view": "effective"}).json()
    assert resp["correction_version"] == 0
    assert resp["items"][0]["text"] == "第一条"
    assert resp["items"][0]["correction_version"] == 0


def test_effective_view_after_incremental_and_page_max(client):
    for seq in range(1, 5):
        submit(client, "c1", seq, 1, f"line{seq}")
    correct(client, "c1", 1, 0, "c1", "r")  # version 1
    correct(client, "c1", 2, 0, "c2", "r")  # version 1
    correct(client, "c1", 2, 1, "c2b", "r")  # version 2

    resp = client.get(
        "/channels/c1/finalized", params={"view": "effective", "after": 2}
    ).json()
    assert [(i["seq"], i["correction_version"]) for i in resp["items"]] == [
        (3, 0),
        (4, 0),
    ]
    # 页内无更正时顶层版本为 0（页级统计，不代表整个频道）
    assert resp["correction_version"] == 0


def test_invalid_view_value_returns_422(client):
    resp = client.get("/channels/c1/finalized", params={"view": "draft"})
    assert resp.status_code == 422


# ---------- 审计查询 ----------


def test_audit_listing_versions_ascending(client):
    submit(client, "c1", 1, 1, "原文")
    correct(client, "c1", 1, 0, "v1", "原因一")
    correct(client, "c1", 1, 1, "v2", "原因二")

    resp = client.get("/channels/c1/finalized/1/corrections")
    assert resp.status_code == 200
    body = resp.json()
    assert body["channel"] == "c1"
    assert body["seq"] == 1
    assert body["current_version"] == 2
    assert [(i["version"], i["text"], i["reason"]) for i in body["items"]] == [
        (1, "v1", "原因一"),
        (2, "v2", "原因二"),
    ]
    assert all(i["created_at"] for i in body["items"])
    # 版本严格升序
    versions = [i["version"] for i in body["items"]]
    assert versions == sorted(versions)


def test_audit_empty_for_finalized_without_corrections(client):
    submit(client, "c1", 1, 1, "原文")
    body = client.get("/channels/c1/finalized/1/corrections").json()
    assert body["current_version"] == 0
    assert body["items"] == []


def test_audit_unknown_or_gap_seq_returns_404(client):
    submit(client, "c1", 1, 1)
    decide_gap(client, "c1", 2, 2)

    assert client.get("/channels/c1/finalized/2/corrections").status_code == 404
    assert client.get("/channels/c1/finalized/99/corrections").status_code == 404
    assert client.get("/channels/ghost/finalized/1/corrections").status_code == 404


# ---------- 并发 ----------


def test_concurrent_corrections_exactly_one_wins_per_version(client):
    """同一 expected_version 的并发竞争：恰好一个成功，其余 version_conflict。"""
    submit(client, "c1", 1, 1, "原文")

    def attempt(i):
        try:
            return service.submit_correction("c1", 1, 0, f"text-{i}", f"r{i}")
        except service.CorrectionError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(attempt, range(20)))

    ok = [r for r in results if not isinstance(r, service.CorrectionError)]
    conflicts = [
        r for r in results
        if isinstance(r, service.CorrectionError) and r.error == "version_conflict"
    ]
    assert len(ok) == 1
    assert len(conflicts) == 19
    assert ok[0]["correction_version"] == 1


def test_concurrent_chain_each_version_appears_once(client, conn):
    """多批次竞争：每个版本号在 corrections 中恰好出现一次，无空洞。"""
    submit(client, "c1", 1, 1, "原文")
    current = 0
    for ver in range(1, 11):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(
                    lambda i: _try_correct("c1", 1, current, f"v{ver}-{i}", f"r{i}"),
                    range(8),
                )
            )
        winners = [r for r in results if r is not None]
        assert len(winners) == 1
        current = winners[0]["correction_version"]
        assert current == ver

    rows = conn.execute(
        "SELECT version FROM corrections WHERE channel = 'c1' AND seq = 1 ORDER BY version"
    ).fetchall()
    assert [r["version"] for r in rows] == list(range(1, 11))


def _try_correct(channel, seq, expected_version, text, reason):
    try:
        return service.submit_correction(
            channel, seq, expected_version, text, reason
        )
    except service.CorrectionError:
        return None


# ---------- 存储层不可变与频道隔离 ----------


def test_corrections_table_rejects_update_and_delete_at_storage(conn):
    from app.db import write_tx

    service.submit_fragment("c1", 1, 1, "原文")
    service.submit_correction("c1", 1, 0, "更正", "原因")

    # 触发器在存储层拒绝任何改写/删除（即便绕过 service 直连数据库）
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute("UPDATE corrections SET text = 'hacked' WHERE seq = 1")
    with pytest.raises(sqlite3.DatabaseError):
        with write_tx() as c:
            c.execute("DELETE FROM corrections WHERE seq = 1")

    # 原始 finalized 表文本依旧
    assert conn.execute(
        "SELECT text FROM finalized WHERE channel = 'c1' AND seq = 1"
    ).fetchone()["text"] == "原文"
    assert tuple(conn.execute(
        "SELECT text, reason FROM corrections WHERE channel = 'c1' AND seq = 1"
    ).fetchone()) == ("更正", "原因")


def test_original_finalized_never_rewritten_across_views(client, conn):
    submit(client, "c1", 1, 1, "原文")
    correct(client, "c1", 1, 0, "v1", "r")
    correct(client, "c1", 1, 1, "v2", "r")
    assert conn.execute(
        "SELECT text FROM finalized WHERE channel = 'c1' AND seq = 1"
    ).fetchone()["text"] == "原文"
    # 片段提交路径依旧不能覆盖定稿
    body = submit(client, "c1", 1, 99, "迟到的高修订")
    assert body["applied"] is False
    assert body["reason"] == "already_finalized"


def test_corrections_are_isolated_per_channel(client):
    submit(client, "a", 1, 1, "a原文")
    submit(client, "b", 1, 1, "b原文")
    correct(client, "a", 1, 0, "a更正", "r")

    eff_a = client.get("/channels/a/finalized", params={"view": "effective"}).json()
    eff_b = client.get("/channels/b/finalized", params={"view": "effective"}).json()
    assert eff_a["items"][0]["text"] == "a更正"
    assert eff_a["items"][0]["correction_version"] == 1
    assert eff_b["items"][0]["text"] == "b原文"
    assert eff_b["items"][0]["correction_version"] == 0
    assert client.get("/channels/b/finalized/1/corrections").json()["items"] == []
