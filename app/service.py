"""核心业务：片段接收、水位线推进、永久缺口裁决、追加式更正与发布快照。

不变式（由单事务保证）：
- finalized 与 gaps 中每个频道的序号恰好是 1..watermark 的连续前缀，
  每个序号要么是定稿字幕，要么是永久缺口标记；无缺口的片段只存在于
  fragments 缓冲区；
- 同一序号只保留最高修订号，定稿后不再接受任何修改；
- 缺口裁决区间起点必须等于当前水位线下一位，且区间内不得存在已缓冲
  片段，校验失败不产生任何写入；
- 重复提交已完整裁决的同一区间幂等返回当前结果，部分重叠一律拒绝。

更正层（corrections）不变式：
- 只允许更正 finalized 中的定稿字幕；缺口序号与未定稿序号一律拒绝且不写入；
- corrections 表只追加：version 在每个 (channel, seq) 内从 1 起单调递增，
  UPDATE/DELETE 被触发器禁止，原始定稿文本永远不被改写；
- expected_version 必须等于当前更正版本（无更正时为 0），否则返回
  version_conflict；“读取当前版本 -> 校验 -> 追加下一版本”在同一个
  BEGIN IMMEDIATE 事务内完成，竞争请求只有一个能成功。

批量更正（correction_batches/correction_batch_items）不变式：
- 每批 1~100 项，同一批内 seq 必须互不相同，重复 seq 整批拒绝（409
  batch_conflict，冲突明细按输入顺序返回）且零写入；
- “逐项核对（已定稿字幕、非缺口、expected_version 等于当前版本）->
  按输入顺序追加各序号的新更正版本 -> 记录批次与逐项冻结结果”全部在
  单个 BEGIN IMMEDIATE 事务内完成；任一项校验失败即整批回滚，
  corrections 表不增加任何版本；
- (channel, idempotency_key) 唯一：相同频道、相同幂等键且请求内容
  （逐项 seq/expected_version/text/reason 及输入顺序，经指纹比对）
  完全相同时重试，原样返回原批次结果（idempotent=true，同一 batch_id）；
  键相同而内容不同返回 409 idempotency_key_conflict；
- 幂等记录只追加：correction_batches / correction_batch_items 与
  corrections 一样在存储层禁止 UPDATE/DELETE。

发布快照（snapshots/snapshot_items/snapshot_corrections）不变式：
- “锁定当前 watermark -> 固化 1..watermark 的字幕 / 缺口及各条更正
  历史 -> 写入快照”在单个 BEGIN IMMEDIATE 事务内完成，冻结结果与
  创建时刻的定稿状态严格一致；
- 快照完全追加且不可变：同一频道的同一 idempotency_key 重试不重新
  冻结，始终返回原 snapshot_id；此后新定稿、新更正、新缺口只写入
  原表，任何既有快照内容都不随之改变；
- 不存在的频道也可创建 watermark=0 的空快照（不创建频道行）。

快照差异（diff_snapshots）不变式：
- 只允许比较同一频道的两个既有快照；任一快照不存在 -> 404，
  频道不一致 -> 409，校验失败不产生任何写入；
- 差异基于快照冻结的生效内容：字幕以 effective_text 与
  correction_version 判定，缺口以裁决区间端点 (start_seq, end_seq)
  判定，两侧完全一致（含 kind 相同且内容相同）的序号不返回；
- 按 seq 升序合并两份快照条目，差异分 added（仅对方有）/ removed
  （仅基准有）/ changed（双方都有但内容不同）三类；after/limit 分页
  只计差异条目，next_after 指向本页最后一条差异的 seq，稀疏差异
  也不会因中间大量未变化条目而漏项。

命名消费者拉取游标（consumers）不变式：
- 每个 (channel, consumer) 的 cursor 只能经确认接口单调推进，且只能
  推进到当前租约批次 [lease_from, lease_to] 内的序号；
- 同一消费者同一时刻至多一个未过期租约：领取与确认都在 BEGIN IMMEDIATE
  事务内完成“校验 -> 写入”，并发下只有一个请求能改写租约 / 游标；
- 租约过期（lease_expires_at 到期）后同一未确认批次可被重新领取并签发
  新令牌，旧令牌随之失效；确认成功即清空租约，下一批从新游标后继续；
- 确认接口的所有校验（游标、令牌、过期、批次范围）先于任何写入，
  失败时不改写消费者状态。
"""

import hashlib
import json
import sqlite3
import time
import uuid
from datetime import datetime, timezone

from .db import read_tx, write_tx


class CorrectionError(Exception):
    """更正 / 审计请求在业务前置条件上被拒绝；HTTP 层映射为 409/404。"""

    def __init__(
        self,
        error: str,
        message: str,
        status_code: int = 409,
        extra: "dict | None" = None,
    ):
        super().__init__(message)
        self.error = error
        self.message = message
        self.status_code = status_code
        # 附加字段（如批量更正的逐项 conflicts），原样并入错误响应体
        self.extra = extra


class SnapshotError(Exception):
    """快照分页 / 审计请求在业务前置条件上被拒绝；HTTP 层映射为 404/409。"""

    def __init__(self, error: str, message: str, status_code: int = 404):
        super().__init__(message)
        self.error = error
        self.message = message
        self.status_code = status_code


class ConsumerError(Exception):
    """消费者领取 / 确认 / 查询在业务前置条件上被拒绝；HTTP 层映射为 409/404。"""

    def __init__(self, error: str, message: str, status_code: int = 409):
        super().__init__(message)
        self.error = error
        self.message = message
        self.status_code = status_code


def _now() -> float:
    """当前 epoch 秒；测试可替换以确定性控制租约过期。"""
    return time.time()


def _iso_utc(ts: float) -> str:
    """epoch 秒转 UTC ISO8601 字符串（与既有 created_at 风格一致）。"""
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _watermark_of(conn, channel: str) -> int:
    """读取频道当前水位线；频道不存在时为 0。"""
    row = conn.execute(
        "SELECT watermark FROM channels WHERE channel = ?", (channel,)
    ).fetchone()
    return row["watermark"] if row else 0


def submit_fragment(channel: str, seq: int, revision: int, text: str) -> dict:
    """提交一个字幕片段，返回是否生效、最新水位线及本次新定稿的片段。"""
    with write_tx() as conn:
        conn.execute(
            "INSERT INTO channels (channel, watermark) VALUES (?, 0) "
            "ON CONFLICT(channel) DO NOTHING",
            (channel,),
        )
        watermark = conn.execute(
            "SELECT watermark FROM channels WHERE channel = ?", (channel,)
        ).fetchone()["watermark"]

        applied = False
        reason = None
        if seq <= watermark:
            # 已定稿（或已裁决为缺口）的序号不可再修改
            reason = "already_finalized"
        else:
            row = conn.execute(
                "SELECT revision FROM fragments WHERE channel = ? AND seq = ?",
                (channel, seq),
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO fragments (channel, seq, revision, text) "
                    "VALUES (?, ?, ?, ?)",
                    (channel, seq, revision, text),
                )
                applied = True
            elif revision > row["revision"]:
                conn.execute(
                    "UPDATE fragments SET revision = ?, text = ? "
                    "WHERE channel = ? AND seq = ?",
                    (revision, text, channel, seq),
                )
                applied = True
            else:
                reason = "stale_revision"

        newly = _consume_contiguous(conn, channel, watermark)
        new_watermark = newly[-1]["seq"] if newly else watermark

        return {
            "channel": channel,
            "seq": seq,
            "applied": applied,
            "reason": reason,
            "watermark": new_watermark,
            "finalized": newly,
        }


def _consume_contiguous(conn, channel: str, watermark: int) -> list:
    """从 watermark+1 起消费连续的缓冲片段：移入定稿表并更新水位线。

    遇到第一个缺失（缺口或未到达）即停止，保证不会越过缺口定稿；
    缺口补齐（片段到达或被裁决为永久缺口）的操作会在一次事务里
    释放整段连续片段。
    """
    newly = []
    seq = watermark + 1
    while True:
        row = conn.execute(
            "SELECT revision, text FROM fragments WHERE channel = ? AND seq = ?",
            (channel, seq),
        ).fetchone()
        if row is None:
            break
        conn.execute(
            "INSERT INTO finalized (channel, seq, revision, text) VALUES (?, ?, ?, ?)",
            (channel, seq, row["revision"], row["text"]),
        )
        conn.execute(
            "DELETE FROM fragments WHERE channel = ? AND seq = ?", (channel, seq)
        )
        newly.append(
            {
                "kind": "subtitle",
                "seq": seq,
                "revision": row["revision"],
                "text": row["text"],
                "start_seq": None,
                "end_seq": None,
            }
        )
        seq += 1
    if newly:
        conn.execute(
            "UPDATE channels SET watermark = ? WHERE channel = ?",
            (newly[-1]["seq"], channel),
        )
    return newly


def decide_gap(channel: str, start_seq: int, end_seq: int) -> dict:
    """裁决 [start_seq, end_seq] 为永久缺口。

    起点必须等于当前水位线下一位，区间内不得存在已缓冲片段。
    成功后在同一事务中记录被跳过序号、推进水位线，并继续释放紧随
    其后的连续缓冲片段。重复提交已完整裁决的同一区间幂等返回；
    与既有裁决部分重叠则拒绝。任何校验失败都不产生写入。
    """
    with write_tx() as conn:
        row = conn.execute(
            "SELECT watermark FROM channels WHERE channel = ?", (channel,)
        ).fetchone()
        watermark = row["watermark"] if row else 0

        if start_seq <= watermark:
            return _historical_gap_response(conn, channel, start_seq, end_seq, watermark)

        if start_seq != watermark + 1:
            return _rejected(channel, watermark, "start_not_at_watermark")

        buffered = conn.execute(
            "SELECT seq FROM fragments WHERE channel = ? AND seq BETWEEN ? AND ? "
            "ORDER BY seq LIMIT 1",
            (channel, start_seq, end_seq),
        ).fetchone()
        if buffered is not None:
            # 区间内已有缓冲片段，必须等片段定稿而不能裁决丢失
            return _rejected(channel, watermark, "buffered_fragments")

        # 校验通过：以下写入与水位线推进在同一事务内完成
        conn.execute(
            "INSERT INTO channels (channel, watermark) VALUES (?, ?) "
            "ON CONFLICT(channel) DO UPDATE SET watermark = excluded.watermark",
            (channel, end_seq),
        )
        conn.execute(
            "INSERT INTO gap_decisions (channel, start_seq, end_seq, released_until) "
            "VALUES (?, ?, ?, ?)",
            (channel, start_seq, end_seq, end_seq),
        )
        conn.executemany(
            "INSERT INTO gaps (channel, seq, start_seq, end_seq) VALUES (?, ?, ?, ?)",
            [(channel, seq, start_seq, end_seq) for seq in range(start_seq, end_seq + 1)],
        )

        released = _consume_contiguous(conn, channel, end_seq)
        if released:
            conn.execute(
                "UPDATE gap_decisions SET released_until = ? "
                "WHERE channel = ? AND start_seq = ? AND end_seq = ?",
                (released[-1]["seq"], channel, start_seq, end_seq),
            )
        new_watermark = released[-1]["seq"] if released else end_seq

        return {
            "channel": channel,
            "applied": True,
            "idempotent": False,
            "reason": None,
            "watermark": new_watermark,
            "skipped": {"start_seq": start_seq, "end_seq": end_seq},
            "finalized": released,
        }


def _rejected(channel: str, watermark: int, reason: str) -> dict:
    """构造拒绝响应：不执行任何写入。"""
    return {
        "channel": channel,
        "applied": False,
        "idempotent": False,
        "reason": reason,
        "watermark": watermark,
        "skipped": None,
        "finalized": [],
    }


def _historical_gap_response(
    conn, channel: str, start_seq: int, end_seq: int, watermark: int
) -> dict:
    """处理起点位于水位线之前的提交：完全相同的区间幂等返回，否则拒绝。"""
    decision = conn.execute(
        "SELECT end_seq, released_until FROM gap_decisions "
        "WHERE channel = ? AND start_seq = ? AND end_seq = ?",
        (channel, start_seq, end_seq),
    ).fetchone()
    if decision is not None:
        released = _fetch_finalized(
            conn, channel, after=end_seq, upto=decision["released_until"]
        )
        return {
            "channel": channel,
            "applied": True,
            "idempotent": True,
            "reason": None,
            "watermark": watermark,
            "skipped": {"start_seq": start_seq, "end_seq": end_seq},
            "finalized": released,
        }

    overlap = conn.execute(
        "SELECT 1 FROM gap_decisions "
        "WHERE channel = ? AND start_seq <= ? AND end_seq >= ? LIMIT 1",
        (channel, end_seq, start_seq),
    ).fetchone()
    reason = "gap_overlap" if overlap is not None else "already_finalized"
    return _rejected(channel, watermark, reason)


def _fetch_finalized(conn, channel: str, after: int, upto: int) -> list:
    """读取 (after, upto] 区间内随某次裁决/提交新定稿的字幕片段。"""
    rows = conn.execute(
        "SELECT seq, revision, text FROM finalized "
        "WHERE channel = ? AND seq > ? AND seq <= ? ORDER BY seq",
        (channel, after, upto),
    ).fetchall()
    return [
        {
            "kind": "subtitle",
            "seq": row["seq"],
            "revision": row["revision"],
            "text": row["text"],
            "start_seq": None,
            "end_seq": None,
        }
        for row in rows
    ]


def get_finalized(
    channel: str, after: int = 0, limit: int = 1000, view: str = "original"
) -> dict:
    """查询频道定稿结果与当前水位线，按序混合返回字幕与缺口标记。

    after 用于增量拉取（按序号过滤，字幕与缺口标记统一计数）。
    view=original（默认）只返回不可变的原始定稿；view=effective 合并
    每个序号的最新更正文本并附带 correction_version，原始表不被读取改写。
    """
    with read_tx() as conn:
        row = conn.execute(
            "SELECT watermark FROM channels WHERE channel = ?", (channel,)
        ).fetchone()
        watermark = row["watermark"] if row else 0

        if view == "original":
            return {"channel": channel, "watermark": watermark,
                    "items": _fetch_finalized_items(conn, channel, after, limit)}

        # effective：左连接每个序号的最新更正版本，未更正者 version 为 0
        rows = conn.execute(
            """
            SELECT base.seq AS seq, base.kind AS kind, base.revision AS revision,
                   base.text AS original_text, base.start_seq AS start_seq,
                   base.end_seq AS end_seq,
                   COALESCE(c.version, 0) AS correction_version,
                   COALESCE(c.text, base.text) AS effective_text
            FROM (
                SELECT f.seq AS seq, 'subtitle' AS kind, f.revision AS revision,
                       f.text AS text, NULL AS start_seq, NULL AS end_seq, 0 AS ord
                FROM finalized f
                WHERE f.channel = ? AND f.seq > ?
                UNION ALL
                SELECT g.seq AS seq, 'gap' AS kind, NULL AS revision, NULL AS text,
                       g.start_seq AS start_seq, g.end_seq AS end_seq, 1 AS ord
                FROM gaps g
                WHERE g.channel = ? AND g.seq > ?
            ) base
            LEFT JOIN corrections c
                   ON c.channel = ? AND c.seq = base.seq
                  AND c.version = (
                      SELECT MAX(c2.version) FROM corrections c2
                      WHERE c2.channel = ? AND c2.seq = base.seq
                  )
            ORDER BY base.seq, base.ord
            LIMIT ?
            """,
            (channel, after, channel, after, channel, channel, limit),
        ).fetchall()

        items = []
        page_max_version = 0
        for row in rows:
            if row["kind"] == "gap":
                items.append(
                    {
                        "kind": "gap",
                        "seq": row["seq"],
                        "revision": None,
                        "text": None,
                        "start_seq": row["start_seq"],
                        "end_seq": row["end_seq"],
                        "correction_version": None,
                    }
                )
            else:
                cv = row["correction_version"]
                page_max_version = max(page_max_version, cv)
                items.append(
                    {
                        "kind": "subtitle",
                        "seq": row["seq"],
                        "revision": row["revision"],
                        "text": row["effective_text"],
                        "start_seq": None,
                        "end_seq": None,
                        "correction_version": cv,
                    }
                )
        return {
            "channel": channel,
            "watermark": watermark,
            "view": "effective",
            "correction_version": page_max_version,
            "items": items,
        }


def _fetch_finalized_items(conn, channel: str, after: int, limit: int) -> list:
    """读取按序混合的字幕与缺口标记；effective 仅在 original 视图之外复用。"""
    rows = conn.execute(
        """
        SELECT f.seq AS seq, f.revision AS revision, f.text AS text,
               NULL AS start_seq, NULL AS end_seq, 0 AS ord
        FROM finalized f
        WHERE f.channel = ? AND f.seq > ?
        UNION ALL
        SELECT g.seq AS seq, NULL AS revision, NULL AS text,
               g.start_seq AS start_seq, g.end_seq AS end_seq, 1 AS ord
        FROM gaps g
        WHERE g.channel = ? AND g.seq > ?
        ORDER BY seq, ord LIMIT ?
        """,
        (channel, after, channel, after, limit),
    ).fetchall()
    items = []
    for row in rows:
        if row["start_seq"] is None:
            items.append(
                {
                    "kind": "subtitle",
                    "seq": row["seq"],
                    "revision": row["revision"],
                    "text": row["text"],
                    "start_seq": None,
                    "end_seq": None,
                }
            )
        else:
            items.append(
                {
                    "kind": "gap",
                    "seq": row["seq"],
                    "revision": None,
                    "text": None,
                    "start_seq": row["start_seq"],
                    "end_seq": row["end_seq"],
                }
            )
    return items


def submit_correction(
    channel: str, seq: int, expected_version: int, text: str, reason: str
) -> dict:
    """对已定稿字幕原子追加一条更正。

    仅当 (channel, seq) 存在于 finalized（不是缺口、不是未定稿序号），
    且 expected_version 等于当前更正版本（无更正为 0）时才写入；
    任何前置条件不满足都先于写入返回，corrections 表与 finalized 表
    均保持不变。
    """
    with write_tx() as conn:
        finalized = conn.execute(
            "SELECT 1 FROM finalized WHERE channel = ? AND seq = ?",
            (channel, seq),
        ).fetchone()
        if finalized is None:
            gap = conn.execute(
                "SELECT 1 FROM gaps WHERE channel = ? AND seq = ?",
                (channel, seq),
            ).fetchone()
            if gap is not None:
                raise CorrectionError(
                    "seq_is_gap",
                    f"序号 {seq} 已裁决为永久缺口，不能更正",
                )
            raise CorrectionError(
                "not_finalized",
                f"序号 {seq} 尚未定稿，只有已定稿字幕允许更正",
            )

        current = conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM corrections "
            "WHERE channel = ? AND seq = ?",
            (channel, seq),
        ).fetchone()["v"]
        if expected_version != current:
            raise CorrectionError(
                "version_conflict",
                f"expected_version={expected_version} 与当前更正版本 {current} 不一致",
            )

        next_version = current + 1
        try:
            row = conn.execute(
                "INSERT INTO corrections (channel, seq, version, text, reason) "
                "VALUES (?, ?, ?, ?, ?) "
                "RETURNING created_at",
                (channel, seq, next_version, text, reason),
            ).fetchone()
        except sqlite3.IntegrityError as exc:  # 极端竞争下的最后防线
            raise CorrectionError(
                "version_conflict",
                f"更正版本 {next_version} 已被并发请求占用",
            ) from exc

        return {
            "channel": channel,
            "seq": seq,
            "correction_version": next_version,
            "text": text,
            "reason": reason,
            "created_at": row["created_at"],
        }


def list_corrections(channel: str, seq: int) -> dict:
    """返回某定稿序号的更正审计记录（按 version 升序）。

    缺口或未定稿序号返回 404——这类序号不存在可审计的更正链。
    """
    with read_tx() as conn:
        finalized = conn.execute(
            "SELECT 1 FROM finalized WHERE channel = ? AND seq = ?",
            (channel, seq),
        ).fetchone()
        if finalized is None:
            raise CorrectionError(
                "not_finalized",
                f"序号 {seq} 不存在已定稿字幕，没有更正审计记录",
                status_code=404,
            )

        rows = conn.execute(
            "SELECT version, text, reason, created_at FROM corrections "
            "WHERE channel = ? AND seq = ? ORDER BY version",
            (channel, seq),
        ).fetchall()
        return {
            "channel": channel,
            "seq": seq,
            "current_version": rows[-1]["version"] if rows else 0,
            "items": [
                {
                    "channel": channel,
                    "seq": seq,
                    "version": row["version"],
                    "text": row["text"],
                    "reason": row["reason"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ],
        }


# ---------- 批量更正 ----------


def _batch_request_fingerprint(items: list) -> str:
    """对整批请求按输入顺序做指纹：字段含 seq/expected_version/text/reason。

    text/reason 为经 schema 去除首尾空白后的字符串；JSON 以确定性分隔符
    序列化（ensure_ascii=False、无多余空白），使“完全相同请求”逐字节可比。
    """
    payload = [
        [item["seq"], item["expected_version"], item["text"], item["reason"]]
        for item in items
    ]
    blob = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), sort_keys=False
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def submit_correction_batch(channel: str, idempotency_key: str, items: list) -> dict:
    """在单个写事务内原子提交一批（1~100 条）追加式更正。

    先按幂等键查既有批次：完全相同请求（指纹一致）原样返回原批次结果，
    键相同而内容不同抛 idempotency_key_conflict。随后逐项核对——同批
    重复 seq、未定稿序号、缺口序号、expected_version 不匹配均收集为
    冲突项；任一项失败则整批拒绝（不插入任何更正版本），冲突明细按
    输入顺序返回。全部通过才按输入顺序追加各序号的新版本并冻结批次
    记录，返回 batch_id 与逐项结果。
    """
    fingerprint = _batch_request_fingerprint(items)

    with write_tx() as conn:
        existing = conn.execute(
            "SELECT batch_id, created_at, request_fingerprint FROM correction_batches "
            "WHERE channel = ? AND idempotency_key = ?",
            (channel, idempotency_key),
        ).fetchone()
        if existing is not None:
            if existing["request_fingerprint"] != fingerprint:
                raise CorrectionError(
                    "idempotency_key_conflict",
                    f"频道 {channel} 的幂等键 {idempotency_key} 已被内容不同的请求占用，"
                    "请更换幂等键后重试",
                )
            return _replay_batch(conn, existing, channel, idempotency_key)

        # 逐项核对：任何一项失败都不写入，冲突按输入顺序收集
        seen: "dict[int, int]" = {}
        conflicts = []
        for index, item in enumerate(items):
            seq = item["seq"]
            if seq in seen:
                conflicts.append(
                    {
                        "index": index,
                        "seq": seq,
                        "error": "duplicate_seq",
                        "message": (
                            f"序号 {seq} 在本批中重复：与第 {seen[seq]} 项"
                            "（下标从 0 起）序号相同"
                        ),
                    }
                )
                continue

            finalized = conn.execute(
                "SELECT 1 FROM finalized WHERE channel = ? AND seq = ?",
                (channel, seq),
            ).fetchone()
            if finalized is None:
                gap = conn.execute(
                    "SELECT 1 FROM gaps WHERE channel = ? AND seq = ?",
                    (channel, seq),
                ).fetchone()
                if gap is not None:
                    err, msg = (
                        "seq_is_gap",
                        f"序号 {seq} 已裁决为永久缺口，不能更正",
                    )
                else:
                    err, msg = (
                        "not_finalized",
                        f"序号 {seq} 尚未定稿，只有已定稿字幕允许更正",
                    )
            else:
                current = conn.execute(
                    "SELECT COALESCE(MAX(version), 0) AS v FROM corrections "
                    "WHERE channel = ? AND seq = ?",
                    (channel, seq),
                ).fetchone()["v"]
                if item["expected_version"] != current:
                    err, msg = (
                        "version_conflict",
                        f"第 {index} 项 expected_version="
                        f"{item['expected_version']} 与序号 {seq} 的当前更正版本 "
                        f"{current} 不一致",
                    )
                else:
                    err, msg = None, None

            if err is not None:
                conflicts.append(
                    {"index": index, "seq": seq, "error": err, "message": msg}
                )
            else:
                seen[seq] = index

        if conflicts:
            raise CorrectionError(
                "batch_conflict",
                f"{len(conflicts)} 个更正项未通过校验，整批拒绝且无任何写入",
                extra={"conflicts": conflicts},
            )

        # 全部通过：批次行先落库（UNIQUE(channel, idempotency_key) 是并发
        # 重试的最后防线），再按输入顺序逐项追加更正版本并冻结结果
        batch_id = str(uuid.uuid4())
        try:
            conn.execute(
                "INSERT INTO correction_batches "
                "(batch_id, channel, idempotency_key, request_fingerprint) "
                "VALUES (?, ?, ?, ?)",
                (batch_id, channel, idempotency_key, fingerprint),
            )
        except sqlite3.IntegrityError:
            # 极端并发下另一请求已用同键提交：重读既有批次，内容相同则
            # 幂等返回，不同则冲突（本事务随即随异常回滚，零写入）
            existing = conn.execute(
                "SELECT batch_id, created_at, request_fingerprint "
                "FROM correction_batches WHERE channel = ? AND idempotency_key = ?",
                (channel, idempotency_key),
            ).fetchone()
            if existing["request_fingerprint"] == fingerprint:
                return _replay_batch(conn, existing, channel, idempotency_key)
            raise CorrectionError(
                "idempotency_key_conflict",
                f"频道 {channel} 的幂等键 {idempotency_key} 已被内容不同的请求占用，"
                "请更换幂等键后重试",
            )

        results = []
        for index, item in enumerate(items):
            seq = item["seq"]
            next_version = item["expected_version"] + 1
            try:
                row = conn.execute(
                    "INSERT INTO corrections (channel, seq, version, text, reason) "
                    "VALUES (?, ?, ?, ?, ?) RETURNING created_at",
                    (
                        channel,
                        seq,
                        next_version,
                        item["text"],
                        item["reason"],
                    ),
                ).fetchone()
            except sqlite3.IntegrityError as exc:  # 极端竞争下的最后防线
                raise CorrectionError(
                    "version_conflict",
                    f"序号 {seq} 的更正版本 {next_version} 已被并发请求占用，整批拒绝",
                ) from exc
            conn.execute(
                "INSERT INTO correction_batch_items "
                "(batch_id, item_index, seq, expected_version, correction_version, "
                "text, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    index,
                    seq,
                    item["expected_version"],
                    next_version,
                    item["text"],
                    item["reason"],
                    row["created_at"],
                ),
            )
            results.append(
                {
                    "index": index,
                    "seq": seq,
                    "correction_version": next_version,
                    "text": item["text"],
                    "reason": item["reason"],
                    "created_at": row["created_at"],
                }
            )

        created_at = conn.execute(
            "SELECT created_at FROM correction_batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()["created_at"]
        return {
            "batch_id": batch_id,
            "channel": channel,
            "idempotency_key": idempotency_key,
            "idempotent": False,
            "created_at": created_at,
            "results": results,
        }


def _replay_batch(conn, batch_row, channel: str, idempotency_key: str) -> dict:
    """按冻结的批次逐项记录构造原样重试响应（只读，不追加任何更正版本）。"""
    rows = conn.execute(
        "SELECT item_index, seq, correction_version, text, reason, created_at "
        "FROM correction_batch_items WHERE batch_id = ? ORDER BY item_index",
        (batch_row["batch_id"],),
    ).fetchall()
    return {
        "batch_id": batch_row["batch_id"],
        "channel": channel,
        "idempotency_key": idempotency_key,
        "idempotent": True,
        "created_at": batch_row["created_at"],
        "results": [
            {
                "index": row["item_index"],
                "seq": row["seq"],
                "correction_version": row["correction_version"],
                "text": row["text"],
                "reason": row["reason"],
                "created_at": row["created_at"],
            }
            for row in rows
        ],
    }


# ---------- 发布快照 ----------


def create_snapshot(channel: str, idempotency_key: str) -> dict:
    """为频道创建发布快照：单事务锁定当前 watermark 并冻结 1..watermark。

    同一 (channel, idempotency_key) 重试不重新冻结，返回既有快照
    （idempotent=true）。不存在的频道生成 watermark=0 的空快照，且
    不会在 channels 表中创建频道行。
    """
    with write_tx() as conn:
        existing = conn.execute(
            "SELECT snapshot_id, watermark, created_at FROM snapshots "
            "WHERE channel = ? AND idempotency_key = ?",
            (channel, idempotency_key),
        ).fetchone()
        if existing is not None:
            return _snapshot_meta(
                existing, channel, idempotency_key, idempotent=True
            )

        row = conn.execute(
            "SELECT watermark FROM channels WHERE channel = ?", (channel,)
        ).fetchone()
        watermark = row["watermark"] if row else 0

        snapshot_id = str(uuid.uuid4())
        try:
            conn.execute(
                "INSERT INTO snapshots "
                "(snapshot_id, channel, watermark, idempotency_key) "
                "VALUES (?, ?, ?, ?)",
                (snapshot_id, channel, watermark, idempotency_key),
            )
        except sqlite3.IntegrityError:
            # 极端并发下同一幂等键竞争：唯一约束是最后防线，转幂等返回
            existing = conn.execute(
                "SELECT snapshot_id, watermark, created_at FROM snapshots "
                "WHERE channel = ? AND idempotency_key = ?",
                (channel, idempotency_key),
            ).fetchone()
            return _snapshot_meta(
                existing, channel, idempotency_key, idempotent=True
            )

        if watermark > 0:
            # 字幕行：同时固化原始文本与创建时刻的生效文本（最新更正）
            conn.execute(
                """
                INSERT INTO snapshot_items (
                    snapshot_id, seq, kind, revision, original_text,
                    effective_text, correction_version, start_seq, end_seq
                )
                SELECT ?, f.seq, 'subtitle', f.revision, f.text,
                       COALESCE(c.text, f.text), COALESCE(c.version, 0),
                       NULL, NULL
                FROM finalized f
                LEFT JOIN corrections c
                       ON c.channel = f.channel AND c.seq = f.seq
                      AND c.version = (
                          SELECT MAX(c2.version) FROM corrections c2
                          WHERE c2.channel = f.channel AND c2.seq = f.seq
                      )
                WHERE f.channel = ? AND f.seq BETWEEN 1 AND ?
                """,
                (snapshot_id, channel, watermark),
            )
            # 缺口行：固化所属裁决区间端点
            conn.execute(
                """
                INSERT INTO snapshot_items (
                    snapshot_id, seq, kind, revision, original_text,
                    effective_text, correction_version, start_seq, end_seq
                )
                SELECT ?, seq, 'gap', NULL, NULL, NULL, NULL, start_seq, end_seq
                FROM gaps
                WHERE channel = ? AND seq BETWEEN 1 AND ?
                """,
                (snapshot_id, channel, watermark),
            )
            # 各条更正版本完整冻结（按版本升序，供快照内审计）
            conn.execute(
                """
                INSERT INTO snapshot_corrections
                    (snapshot_id, seq, version, text, reason, created_at)
                SELECT ?, seq, version, text, reason, created_at
                FROM corrections
                WHERE channel = ? AND seq BETWEEN 1 AND ?
                """,
                (snapshot_id, channel, watermark),
            )

        created_at = conn.execute(
            "SELECT created_at FROM snapshots WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()["created_at"]
        return {
            "snapshot_id": snapshot_id,
            "channel": channel,
            "watermark": watermark,
            "idempotency_key": idempotency_key,
            "idempotent": False,
            "created_at": created_at,
        }


def _snapshot_meta(row, channel: str, idempotency_key: str, idempotent: bool) -> dict:
    """从 snapshots 行构造创建/重试响应。"""
    return {
        "snapshot_id": row["snapshot_id"],
        "channel": channel,
        "watermark": row["watermark"],
        "idempotency_key": idempotency_key,
        "idempotent": idempotent,
        "created_at": row["created_at"],
    }


def _get_snapshot_meta(conn, snapshot_id: str):
    meta = conn.execute(
        "SELECT channel, watermark FROM snapshots WHERE snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()
    if meta is None:
        raise SnapshotError(
            "snapshot_not_found",
            f"快照 {snapshot_id} 不存在",
            status_code=404,
        )
    return meta


def get_snapshot(
    snapshot_id: str, after: int = 0, limit: int = 1000, view: str = "original"
) -> dict:
    """按 snapshot_id 分页拉取快照条目（after/limit 按序号）。

    返回的是快照时刻冻结的内容：快照创建后新定稿的字幕、新追加的更正
    或新裁决的缺口都不会影响任何既有快照。
    """
    with read_tx() as conn:
        meta = _get_snapshot_meta(conn, snapshot_id)
        channel, watermark = meta["channel"], meta["watermark"]

        if view == "original":
            rows = conn.execute(
                """
                SELECT seq, kind, revision, original_text, start_seq, end_seq
                FROM snapshot_items
                WHERE snapshot_id = ? AND seq > ?
                ORDER BY seq LIMIT ?
                """,
                (snapshot_id, after, limit),
            ).fetchall()
            return {
                "snapshot_id": snapshot_id,
                "channel": channel,
                "watermark": watermark,
                "view": "original",
                "items": [_snapshot_item_dict(row, effective=False) for row in rows],
            }

        rows = conn.execute(
            """
            SELECT seq, kind, revision, original_text, effective_text,
                   correction_version, start_seq, end_seq
            FROM snapshot_items
            WHERE snapshot_id = ? AND seq > ?
            ORDER BY seq LIMIT ?
            """,
            (snapshot_id, after, limit),
        ).fetchall()
        items = []
        page_max_version = 0
        for row in rows:
            item = _snapshot_item_dict(row, effective=True)
            if item["correction_version"] is not None:
                page_max_version = max(
                    page_max_version, item["correction_version"]
                )
            items.append(item)
        return {
            "snapshot_id": snapshot_id,
            "channel": channel,
            "watermark": watermark,
            "view": "effective",
            "correction_version": page_max_version,
            "items": items,
        }


def _snapshot_item_dict(row, effective: bool) -> dict:
    """将快照条目行转为响应 dict；effective=True 时返回冻结的生效文本。"""
    if row["kind"] == "gap":
        return {
            "kind": "gap",
            "seq": row["seq"],
            "revision": None,
            "text": None,
            "start_seq": row["start_seq"],
            "end_seq": row["end_seq"],
            **(
                {"correction_version": None}
                if effective
                else {}
            ),
        }
    item = {
        "kind": "subtitle",
        "seq": row["seq"],
        "revision": row["revision"],
        "text": row["effective_text"] if effective else row["original_text"],
        "start_seq": None,
        "end_seq": None,
    }
    if effective:
        item["correction_version"] = row["correction_version"]
    return item


def list_snapshot_corrections(snapshot_id: str, seq: int) -> dict:
    """返回快照内某字幕序号冻结的更正历史（按 version 升序）。

    快照不存在 -> 404 snapshot_not_found；序号超出快照锁定水位线 ->
    404 seq_out_of_snapshot；序号在快照中是缺口 -> 409 seq_is_gap。
    """
    with read_tx() as conn:
        meta = _get_snapshot_meta(conn, snapshot_id)
        channel, watermark = meta["channel"], meta["watermark"]

        if seq < 1 or seq > watermark:
            raise SnapshotError(
                "seq_out_of_snapshot",
                f"序号 {seq} 超出快照 {snapshot_id} 的水位线 {watermark}",
                status_code=404,
            )

        item = conn.execute(
            "SELECT kind FROM snapshot_items WHERE snapshot_id = ? AND seq = ?",
            (snapshot_id, seq),
        ).fetchone()
        if item["kind"] == "gap":
            raise SnapshotError(
                "seq_is_gap",
                f"序号 {seq} 在快照 {snapshot_id} 中是永久缺口，没有更正历史",
                status_code=409,
            )

        rows = conn.execute(
            "SELECT version, text, reason, created_at FROM snapshot_corrections "
            "WHERE snapshot_id = ? AND seq = ? ORDER BY version",
            (snapshot_id, seq),
        ).fetchall()
        return {
            "snapshot_id": snapshot_id,
            "channel": channel,
            "seq": seq,
            "current_version": rows[-1]["version"] if rows else 0,
            "items": [
                {
                    "seq": seq,
                    "version": row["version"],
                    "text": row["text"],
                    "reason": row["reason"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ],
        }


def diff_snapshots(
    snapshot_id: str, other_snapshot_id: str, after: int = 0, limit: int = 1000
) -> dict:
    """比较同一频道两个快照的冻结生效内容，按 seq 合并返回差异。

    路径快照为基准（base），other_snapshot_id 为对方（other）：
    - added：seq 只存在于对方快照（基准冻结水位线之外的新内容）；
    - removed：seq 只存在于基准快照（对方是更早快照时才会出现）；
    - changed：两侧都有但冻结内容不同——字幕以生效文本（effective_text）
      与更正版本（correction_version）判定，缺口以裁决区间端点
      (start_seq, end_seq) 判定，kind 不同同样视为变更。
    两侧完全一致的序号不返回。after/limit 只对差异条目计数，next_after
    为本页最后一条差异的 seq（无差异时为 None），供下一页续拉。
    """
    with read_tx() as conn:
        base = _get_snapshot_meta(conn, snapshot_id)
        other = conn.execute(
            "SELECT channel, watermark FROM snapshots WHERE snapshot_id = ?",
            (other_snapshot_id,),
        ).fetchone()
        if other is None:
            raise SnapshotError(
                "snapshot_not_found",
                f"快照 {other_snapshot_id} 不存在",
                status_code=404,
            )
        if other["channel"] != base["channel"]:
            raise SnapshotError(
                "snapshot_channel_mismatch",
                f"快照 {snapshot_id}（频道 {base['channel']}）与快照 "
                f"{other_snapshot_id}（频道 {other['channel']}）不属于同一频道，"
                "不能比较",
                status_code=409,
            )

        # FULL OUTER JOIN 由两段 LEFT JOIN 的 UNION ALL 模拟：第一段覆盖
        # “基准有 / 双方都有（且有差异）”，第二段补齐“仅对方有”。
        # 差异过滤与 LIMIT 都落在合并后的 seq 序列上，因此未变化条目
        # 不占 limit，再稀疏的差异也会被继续扫描出来。
        # IS NOT 是 SQLite 的 NULL 安全不等号（NULL IS NOT NULL -> 0）。
        rows = conn.execute(
            """
            SELECT * FROM (
                SELECT b.seq                  AS seq,
                       b.kind                 AS base_kind,
                       b.revision             AS base_revision,
                       b.effective_text       AS base_effective_text,
                       b.correction_version   AS base_correction_version,
                       b.start_seq            AS base_start_seq,
                       b.end_seq              AS base_end_seq,
                       o.kind                 AS other_kind,
                       o.revision             AS other_revision,
                       o.effective_text       AS other_effective_text,
                       o.correction_version   AS other_correction_version,
                       o.start_seq            AS other_start_seq,
                       o.end_seq              AS other_end_seq
                FROM snapshot_items b
                LEFT JOIN snapshot_items o
                       ON o.snapshot_id = ? AND o.seq = b.seq
                WHERE b.snapshot_id = ? AND b.seq > ?
                  AND (o.seq IS NULL
                       OR b.kind               IS NOT o.kind
                       OR b.effective_text     IS NOT o.effective_text
                       OR b.correction_version IS NOT o.correction_version
                       OR b.start_seq          IS NOT o.start_seq
                       OR b.end_seq            IS NOT o.end_seq)
                UNION ALL
                SELECT o.seq                  AS seq,
                       NULL, NULL, NULL, NULL, NULL, NULL,
                       o.kind                 AS other_kind,
                       o.revision             AS other_revision,
                       o.effective_text       AS other_effective_text,
                       o.correction_version   AS other_correction_version,
                       o.start_seq            AS other_start_seq,
                       o.end_seq              AS other_end_seq
                FROM snapshot_items o
                LEFT JOIN snapshot_items b
                       ON b.snapshot_id = ? AND b.seq = o.seq
                WHERE o.snapshot_id = ? AND o.seq > ?
                  AND b.seq IS NULL
            )
            ORDER BY seq
            LIMIT ?
            """,
            (
                other_snapshot_id, snapshot_id, after,
                snapshot_id, other_snapshot_id, after,
                limit,
            ),
        ).fetchall()

        items = []
        for row in rows:
            base_item = _diff_side_item(row, "base")
            other_item = _diff_side_item(row, "other")
            if base_item is None:
                change = "added"
            elif other_item is None:
                change = "removed"
            else:
                change = "changed"
            items.append(
                {
                    "change": change,
                    "seq": row["seq"],
                    "base": base_item,
                    "other": other_item,
                }
            )

        return {
            "snapshot_id": snapshot_id,
            "other_snapshot_id": other_snapshot_id,
            "channel": base["channel"],
            "watermark": base["watermark"],
            "other_watermark": other["watermark"],
            "items": items,
            "next_after": items[-1]["seq"] if items else None,
        }


def _diff_side_item(row, side: str):
    """从差异查询的 base_* / other_* 列构造一侧的冻结生效条目；该侧无此 seq 时为 None。"""
    kind = row[f"{side}_kind"]
    if kind is None:
        return None
    seq = row["seq"]
    if kind == "gap":
        return {
            "kind": "gap",
            "seq": seq,
            "revision": None,
            "text": None,
            "start_seq": row[f"{side}_start_seq"],
            "end_seq": row[f"{side}_end_seq"],
            "correction_version": None,
        }
    return {
        "kind": "subtitle",
        "seq": seq,
        "revision": row[f"{side}_revision"],
        "text": row[f"{side}_effective_text"],
        "start_seq": None,
        "end_seq": None,
        "correction_version": row[f"{side}_correction_version"],
    }


# ---------- 命名消费者拉取游标 ----------


def claim_batch(channel: str, consumer: str, limit: int, lease_seconds: float) -> dict:
    """领取连续批次：从已确认游标后返回至多 limit 条（含缺口标记）并签发租约。

    单个 BEGIN IMMEDIATE 事务内完成“注册消费者 -> 校验无未过期租约 ->
    截取连续批次 -> 写入新租约”，因此并发领取同一消费者时只有一个能
    拿到租约，其余得到 409 lease_active。租约过期后允许重新领取同一
    未确认批次（覆盖旧租约、签发新令牌）；无新内容时不签发租约。
    """
    with write_tx() as conn:
        conn.execute(
            "INSERT INTO consumers (channel, consumer) VALUES (?, ?) "
            "ON CONFLICT(channel, consumer) DO NOTHING",
            (channel, consumer),
        )
        row = conn.execute(
            "SELECT cursor, lease_token, lease_from, lease_to, lease_expires_at "
            "FROM consumers WHERE channel = ? AND consumer = ?",
            (channel, consumer),
        ).fetchone()

        now = _now()
        if row["lease_token"] is not None and row["lease_expires_at"] > now:
            raise ConsumerError(
                "lease_active",
                f"消费者 {consumer} 持有未过期租约（批次 "
                f"{row['lease_from']}..{row['lease_to']}），到期前不得重复领取",
            )

        watermark = _watermark_of(conn, channel)
        cursor = row["cursor"]
        items = _fetch_finalized_items(conn, channel, cursor, limit)
        if not items:
            # 无新内容：返回空批次且不签发租约（过期租约留待下次领取覆盖，
            # 状态查询仍可见其 expired=true）
            return {
                "channel": channel,
                "consumer": consumer,
                "cursor": cursor,
                "watermark": watermark,
                "lease_token": None,
                "lease_expires_at": None,
                "items": [],
            }

        token = str(uuid.uuid4())
        expires_at = now + lease_seconds
        conn.execute(
            "UPDATE consumers SET lease_token = ?, lease_from = ?, lease_to = ?, "
            "lease_expires_at = ?, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
            "WHERE channel = ? AND consumer = ?",
            (token, cursor + 1, items[-1]["seq"], expires_at, channel, consumer),
        )
        return {
            "channel": channel,
            "consumer": consumer,
            "cursor": cursor,
            "watermark": watermark,
            "lease_token": token,
            "lease_expires_at": _iso_utc(expires_at),
            "items": items,
        }


def ack_batch(
    channel: str,
    consumer: str,
    lease_token: str,
    expected_cursor: int,
    new_cursor: int,
) -> dict:
    """确认批次：校验租约与游标后把已确认游标推进到 new_cursor。

    全部校验先于写入且在单个 BEGIN IMMEDIATE 事务内完成：过期、旧令牌、
    越界或并发落后（expected_cursor 与当前游标不一致）的请求一律拒绝，
    不改写任何状态；成功时推进游标并清空租约，下一批从新游标后继续。
    """
    with write_tx() as conn:
        row = conn.execute(
            "SELECT cursor, lease_token, lease_from, lease_to, lease_expires_at "
            "FROM consumers WHERE channel = ? AND consumer = ?",
            (channel, consumer),
        ).fetchone()
        if row is None:
            raise ConsumerError(
                "no_active_lease",
                f"消费者 {consumer} 在频道 {channel} 从未领取批次，没有可确认的租约",
            )
        if expected_cursor != row["cursor"]:
            raise ConsumerError(
                "cursor_conflict",
                f"expected_cursor={expected_cursor} 与当前已确认游标 "
                f"{row['cursor']} 不一致（可能已有并发确认推进了游标）",
            )
        if row["lease_token"] is None:
            raise ConsumerError(
                "no_active_lease",
                f"消费者 {consumer} 当前没有待确认的租约",
            )
        if lease_token != row["lease_token"]:
            raise ConsumerError(
                "lease_token_mismatch",
                "lease_token 与当前租约不匹配（旧令牌或他人令牌）",
            )
        if row["lease_expires_at"] <= _now():
            raise ConsumerError(
                "lease_expired",
                "租约已过期，请重新领取批次后再确认",
            )
        if not row["lease_from"] <= new_cursor <= row["lease_to"]:
            raise ConsumerError(
                "cursor_out_of_range",
                f"new_cursor={new_cursor} 不在本批次 "
                f"{row['lease_from']}..{row['lease_to']} 范围内",
            )

        conn.execute(
            "UPDATE consumers SET cursor = ?, lease_token = NULL, lease_from = NULL, "
            "lease_to = NULL, lease_expires_at = NULL, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
            "WHERE channel = ? AND consumer = ?",
            (new_cursor, channel, consumer),
        )
        return {
            "channel": channel,
            "consumer": consumer,
            "previous_cursor": row["cursor"],
            "cursor": new_cursor,
            "watermark": _watermark_of(conn, channel),
        }


def get_consumer_status(channel: str, consumer: str) -> dict:
    """查询消费者状态：已确认游标、水位线、滞后量与当前租约（含是否已过期）。"""
    with read_tx() as conn:
        row = conn.execute(
            "SELECT cursor, lease_token, lease_from, lease_to, lease_expires_at "
            "FROM consumers WHERE channel = ? AND consumer = ?",
            (channel, consumer),
        ).fetchone()
        if row is None:
            raise ConsumerError(
                "consumer_not_found",
                f"消费者 {consumer} 在频道 {channel} 不存在（尚未领取过批次）",
                status_code=404,
            )

        watermark = _watermark_of(conn, channel)
        lease = None
        if row["lease_token"] is not None:
            lease = {
                "from_seq": row["lease_from"],
                "to_seq": row["lease_to"],
                "expires_at": _iso_utc(row["lease_expires_at"]),
                "expired": row["lease_expires_at"] <= _now(),
            }
        return {
            "channel": channel,
            "consumer": consumer,
            "cursor": row["cursor"],
            "watermark": watermark,
            "lag": watermark - row["cursor"],
            "lease": lease,
        }
