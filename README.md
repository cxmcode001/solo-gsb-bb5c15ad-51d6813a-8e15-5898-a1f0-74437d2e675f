# Live Subtitle Finalizer — 直播字幕定稿服务

基于 FastAPI + SQLite 的直播字幕定稿服务。客户端按频道提交带**序号 / 修订号 / 文本**的字幕片段，服务将乱序到达的片段放入缓冲区，只从序号 1 起按**连续前缀**推进水位线并生成**不可再修改**的定稿字幕。操作员可对确认永久丢失的序号区间发起**缺口裁决**，服务在同一事务中记录缺口并释放紧随其后的连续缓冲片段。定稿之后如需改字，可通过**追加式更正层**提交不可变的更正版本——原始定稿永远保留，下游可在原始视图与生效视图之间选择；需要一次改多处时还可调用**批量更正接口**：单事务内逐项核对、全部通过才按输入顺序追加，任一项失败整批零写入，并以频道范围内的幂等键支持安全重试。需要让下游分批拉取**同一时刻的稳定字幕**时，可对频道**发布快照**：单事务锁定水位线并冻结 1..watermark 的原始 / 生效结果、缺口标记与各条更正版本，此后任何定稿、更正或缺口都不会改变既有快照；任意两个同频道快照还可经**快照差异接口**按 seq 合并比较，只返回新增 / 移除 / 内容变更的条目。下游也可以注册**命名消费者拉取游标**：每个命名消费者在频道内持有独立的已确认游标，领取接口在事务中返回游标后的连续批次（含缺口标记）并签发带过期时间的租约令牌，确认后游标才推进；租约过期未确认的批次可被重新领取，实现至少一次交付。

## 语义规则

- 片段可乱序到达；同一序号只接受**更高修订号**（更低或相等的修订号被拒绝）。
- 水位线 = 已定稿的最大连续序号（定稿字幕与永久缺口共同构成连续前缀）。遇到缺口即停，**不会越过缺口**定稿。
- 缺口被补齐的提交会在**同一事务**内一次释放整段连续片段。
- 序号 ≤ 当前水位线的提交一律不生效（定稿不可变，已裁决为缺口的序号同样不可再提交）。
- 所有提交 / 裁决在 `BEGIN IMMEDIATE` 事务内串行执行，并发下不会重复定稿或跳过缺口。

### 永久缺口裁决规则

- 区间 `[start_seq, end_seq]`（含两端）的**起点必须等于当前水位线下一位**；
- 区间内**不得存在已缓冲片段**（有缓冲说明片段只是迟到，应等待定稿而不是裁决丢失）；
- 任一校验失败返回 `applied=false` 且**不产生任何写入**；
- 裁决成功：同一事务内记录被跳过序号、推进水位线到区间终点，并继续释放紧随其后的连续缓冲片段；
- 重复提交**已完整裁决的同一区间**幂等返回当前结果（`idempotent=true`）；与既有裁决**部分重叠**则拒绝（`gap_overlap`）。

### 追加式更正规则

- 定稿不可改写；需要修改已定稿字幕时只能**追加**一条更正，原始文本永久保留。
- 仅**已定稿字幕**可被更正；未定稿序号与永久缺口序号一律拒绝（409）且不写入。
- 每个序号的更正版本从 1 起单调递增；提交时必须携带 `expected_version`，无更正时为 `0`，否则须等于当前最新版本。
- 版本不匹配（含并发竞争失败）返回 409 `version_conflict`；校验与追加在同一 `BEGIN IMMEDIATE` 事务内原子完成。
- 定稿查询默认 `view=original`（响应结构不变）；`view=effective` 合并最新更正文本并返回每条字幕的 `correction_version`。
- 更正历史可通过审计接口按版本升序查询。

### 批量更正规则

- 接口 `POST /channels/{channel}/corrections/batch` 一次接收 **1～100 条**更正，每项含 `seq`、`expected_version`、`text`、`reason`，请求顶层另带频道范围内的 `idempotency_key`。
- **同一批内 `seq` 不得重复**；出现重复时整批拒绝（409 `batch_conflict`，重复项标记 `duplicate_seq`）。
- 「逐项核对（序号确为已定稿字幕、不是永久缺口、`expected_version` 等于该序号当前版本）→ 按输入顺序追加各序号的新更正版本 → 记录批次」全部在**单个 `BEGIN IMMEDIATE` 事务**内完成；**任一项失败，409 响应的 `conflicts` 都按输入顺序给出各项冲突原因，整批零写入**（即使批次中其他项本身合法也不追加）。
- 全部通过时按**输入顺序**追加，返回批次号 `batch_id` 与逐项 `results`（含每项的下标、新 `correction_version` 等）。
- **幂等**：相同频道、相同 `idempotency_key` 且请求完全相同（逐项字段与顺序一致）的重试原样返回原批次结果（同一 `batch_id`、`idempotent=true`），不会重复追加版本；键相同而**内容不同**返回 409 `idempotency_key_conflict`。幂等键按频道隔离；被拒绝的失败批次不占用幂等键。
- 批量与**单条更正接口**、定稿原文（`view=original`）及更正审计互不影响，可混用：同一序号经两种方式追加的版本仍连续递增。

### 发布快照规则

- 创建快照在**单个 `BEGIN IMMEDIATE` 事务**内完成「锁定频道当前 watermark → 固化 `1..watermark` 的字幕（原始文本 + 创建时刻的生效文本）与缺口标记 → 冻结各字幕序号当时存在的全部更正版本」，冻结结果与创建时刻的定稿状态严格一致。
- 快照**整体不可变**：创建之后新定稿的字幕、新追加的更正或新裁决的缺口只写入原表，任何既有快照的内容（含分页与更正历史）都不随之变化。
- 同一频道携带相同 `idempotency_key` 重试始终返回**同一个 `snapshot_id`**（响应 `idempotent=true`），不重新冻结；幂等键按频道隔离（不同频道可复用同一字符串）。
- 不存在的频道也可创建 `watermark=0` 的空快照（不创建频道行）；之后可分页拉到空列表。
- 快照表只追加，存储层触发器禁止任何 `UPDATE` / `DELETE`。

### 快照差异规则

- 只允许比较**同一频道**的两个既有快照：任一快照不存在返回 404，频道不一致返回 409，比较本身为只读操作，不产生任何写入。
- 差异基于快照**冻结的生效内容**：字幕以生效文本与 `correction_version` 判定，缺口以裁决区间端点判定；两侧完全一致的序号不返回。
- 两份快照按 `seq` 升序合并，差异分 `added` / `removed` / `changed` 三类；`limit` 只计差异条目，`next_after` 指向本页最后一条差异，稀疏差异不会漏项。

### 命名消费者拉取游标规则

- 每个 `(channel, consumer)` 维护独立的已确认游标 `cursor`（初始 0）与至多一个未过期租约；消费者之间互不影响。
- **领取（claim）** 在单个 `BEGIN IMMEDIATE` 事务内完成「读取游标与现有租约 → 校验 → 截取连续批次 → 写入新租约」：从 `cursor` 后返回至多 `limit` 条的连续批次（字幕与缺口标记按序混合），并签发带过期时间的 `lease_token`；同一消费者持有未过期租约时重复领取返回 409 `lease_active`。
- **确认（ack）** 必须同时携带 `lease_token` 与 `expected_cursor`，且 `new_cursor` 必须落在本批次序号范围内（允许部分确认）；过期（`lease_expired`）、旧令牌（`lease_token_mismatch`）、越界（`cursor_out_of_range`）、并发落后（`cursor_conflict`）或无租约（`no_active_lease`）一律返回 409 且**不改写任何状态**。
- 确认成功后游标推进到 `new_cursor`、租约清空，下一次领取从新游标后继续；租约过期未确认时同一批次可被重新领取（内容相同、签发新令牌，旧令牌随之失效），实现至少一次交付。
- 无新内容时领取返回空批次且不签发租约（`lease_token=null`）。

## 快速启动（单条命令）

```bash
docker compose up --build
```

服务监听 **8000** 端口（容器内 `uvicorn --host 0.0.0.0 --port 8000`，Compose 映射 `8000:8000`），启动时自动初始化数据库（建表幂等，包括更正层、快照层与消费者游标新表及触发器，旧实例重启自动补建）。

- 交互式 API 文档：http://localhost:8000/docs
- 健康检查：http://localhost:8000/health
- 提交片段：`POST /channels/{channel}/fragments`
- 缺口裁决：`POST /channels/{channel}/gaps`
- 定稿查询（`view=original|effective`）：`GET /channels/{channel}/finalized`
- 追加更正：`POST /channels/{channel}/finalized/{seq}/corrections`
- 批量更正（幂等）：`POST /channels/{channel}/corrections/batch`
- 更正审计（版本升序）：`GET /channels/{channel}/finalized/{seq}/corrections`
- 创建发布快照：`POST /channels/{channel}/snapshots`
- 分页拉取快照（`view=original|effective`）：`GET /snapshots/{snapshot_id}/items`
- 快照内更正历史（版本升序）：`GET /snapshots/{snapshot_id}/items/{seq}/corrections`
- 比较两个快照的差异：`GET /snapshots/{snapshot_id}/diff?other={other_id}`
- 领取消费者批次：`POST /channels/{channel}/consumers/{consumer}/claims`
- 确认消费者批次：`POST /channels/{channel}/consumers/{consumer}/acks`
- 查询消费者状态：`GET /channels/{channel}/consumers/{consumer}`

## 配置

| 环境变量   | 默认值                 | 说明               |
| ---------- | ---------------------- | ------------------ |
| `DB_PATH`  | `/data/subtitles.db`   | SQLite 数据库文件路径 |

Compose 默认将 `/data` 挂载到命名卷 `subtitles-data`，容器重启数据不丢失。端口映射可在 `docker-compose.yml` 的 `ports` 中调整。

## API

### 提交字幕片段

```bash
curl -X POST http://localhost:8000/channels/news/fragments \
  -H 'Content-Type: application/json' \
  -d '{"seq": 1, "revision": 1, "text": "各位观众晚上好"}'
```

响应（`applied` 表示本次提交是否生效，`watermark` 为处理后的最新水位线，`finalized` 为本次新定稿的片段）：

```json
{
  "channel": "news",
  "seq": 1,
  "applied": true,
  "reason": null,
  "watermark": 1,
  "finalized": [{"seq": 1, "revision": 1, "text": "各位观众晚上好"}]
}
```

`applied=false` 时 `reason` 取值：

- `stale_revision` — 修订号不高于已缓冲的同序号片段；
- `already_finalized` — 该序号已定稿（或已裁决为永久缺口），不可再修改。

### 裁决永久缺口

当确认某些序号永久丢失（例如推流中断），操作员可对缺失区间发起裁决：

```bash
curl -X POST http://localhost:8000/channels/news/gaps \
  -H 'Content-Type: application/json' \
  -d '{"start_seq": 3, "end_seq": 4}'
```

裁决成功的响应（`watermark` 为裁决后的新水位线，`skipped` 为本次跳过区间，`finalized` 为随裁决一并释放的连续缓冲片段）：

```json
{
  "channel": "news",
  "applied": true,
  "idempotent": false,
  "reason": null,
  "watermark": 6,
  "skipped": {"start_seq": 3, "end_seq": 4},
  "finalized": [
    {"kind": "subtitle", "seq": 5, "revision": 1, "text": "第五条字幕", "start_seq": null, "end_seq": null},
    {"kind": "subtitle", "seq": 6, "revision": 1, "text": "第六条字幕", "start_seq": null, "end_seq": null}
  ]
}
```

重复提交**完全相同**的已裁决区间时，幂等返回原裁决结果，`idempotent` 为 `true`。`applied=false` 时 `reason` 取值：

- `start_not_at_watermark` — 区间起点不是当前水位线下一位（不能跳过未裁决的序号）；
- `buffered_fragments` — 区间内存在已缓冲片段，应等待其定稿；
- `already_finalized` — 区间已完全定稿且不匹配任何缺口；
- `gap_overlap` — 与既有缺口裁决部分重叠（完全相同的区间才允许幂等提交）。

请求体校验（如 `start_seq < 1`、`end_seq < start_seq`）返回 HTTP 422。

### 查询频道定稿

定稿查询按序号升序**混合返回字幕与缺口标记**，`kind` 字段供下游区分丢失内容。通过 `view` 参数在原始文本与更正后文本之间选择，**默认 `original`，响应结构与旧版完全一致**：

```bash
curl 'http://localhost:8000/channels/news/finalized?after=0&limit=1000&view=original'
```

```json
{
  "channel": "news",
  "watermark": 6,
  "items": [
    {"kind": "subtitle", "seq": 1, "revision": 1, "text": "第一条字幕", "start_seq": null, "end_seq": null},
    {"kind": "subtitle", "seq": 2, "revision": 1, "text": "第二条字幕", "start_seq": null, "end_seq": null},
    {"kind": "gap",      "seq": 3, "revision": null, "text": null, "start_seq": 3, "end_seq": 4},
    {"kind": "gap",      "seq": 4, "revision": null, "text": null, "start_seq": 3, "end_seq": 4},
    {"kind": "subtitle", "seq": 5, "revision": 1, "text": "第五条字幕", "start_seq": null, "end_seq": null},
    {"kind": "subtitle", "seq": 6, "revision": 1, "text": "第六条字幕", "start_seq": null, "end_seq": null}
  ]
}
```

- `kind=subtitle`：定稿字幕，取 `revision` / `text`；
- `kind=gap`：永久缺口标记，`start_seq` / `end_seq` 标明所属裁决区间（同一区间内每个被跳过序号各占一条标记）；
- `after` 用于增量拉取，对字幕与缺口标记统一按序号计数。

`view=effective` 时，每个字幕条目合并其**最新更正文本**，并附带该序号的当前 `correction_version`（`0` = 未更正；缺口条目为 `null`）；顶层 `correction_version` 是本页返回范围内出现过的最大更正版本。原始定稿不受任何影响：

```bash
curl 'http://localhost:8000/channels/news/finalized?view=effective'
```

```json
{
  "channel": "news",
  "watermark": 6,
  "view": "effective",
  "correction_version": 1,
  "items": [
    {"kind": "subtitle", "seq": 1, "revision": 1, "text": "第一条字幕（已更正）", "start_seq": null, "end_seq": null, "correction_version": 1},
    {"kind": "gap",      "seq": 3, "revision": null, "text": null, "start_seq": 3, "end_seq": 4, "correction_version": null}
  ]
}
```

非法的 `view` 值返回 HTTP 422。

### 追加更正（不可变更正层）

定稿字幕如有错字 / 翻译问题，可对**单个已定稿序号**追加更正。更正不修改任何旧数据，只向 `corrections` 表追加一个新版本：

```bash
curl -X POST http://localhost:8000/channels/news/finalized/1/corrections \
  -H 'Content-Type: application/json' \
  -d '{"expected_version": 0, "text": "各位观众晚上好", "reason": "原稿漏字"}'
```

请求字段：

- `expected_version`（≥0，必填）：客户端认为的该序号**当前更正版本**。没有更正时当前版本为 `0`，首个更正必须提交 `0`；此后必须等于最新版本号；
- `text`（非空字符串）：更正后的字幕文本；
- `reason`（非空字符串）：更正原因，随审计记录永久保留。

成功响应（原子追加下一版本，返回该序号新的更正版本）：

```json
{
  "channel": "news",
  "seq": 1,
  "correction_version": 1,
  "text": "各位观众晚上好",
  "reason": "原稿漏字",
  "created_at": "2026-09-20T12:00:00.000Z"
}
```

规则：

- **仅允许更正已定稿字幕**：未定稿序号（尚未进入连续前缀）或已裁决为永久缺口的序号一律拒绝，返回 HTTP 409 且**不产生任何写入**；
- **乐观版本控制**：`expected_version` 与当前版本不一致时返回 HTTP 409 `version_conflict`，客户端应重新拉取生效视图 / 审计记录后携带新版本重试；
- 「读取当前版本 → 校验 → 追加下一版本」在单个 `BEGIN IMMEDIATE` 事务内完成，并发竞争请求中只有一个成功，其余全部得到版本冲突；
- 更正只追加：每个 `(channel, seq)` 的版本号从 1 起严格递增、无空洞；存储层触发器禁止 `UPDATE` / `DELETE`，**原始定稿文本永远不被改写**，随时可用 `view=original` 取回。

409 响应体（更正接口）：

| `error`             | 含义                                                    |
| ------------------- | ------------------------------------------------------- |
| `not_finalized`     | 序号未定稿（频道不存在 / 序号还在缓冲区或水位线之外）   |
| `seq_is_gap`        | 序号已裁决为永久缺口，缺口不可更正                      |
| `version_conflict`  | `expected_version` 与当前更正版本不一致（竞争失败）     |

```json
{"error": "version_conflict", "message": "expected_version=0 与当前更正版本 1 不一致"}
```

### 查询更正审计

按更正版本**升序**返回某定稿序号的全部更正记录（不可变、可追溯）：

```bash
curl http://localhost:8000/channels/news/finalized/1/corrections
```

```json
{
  "channel": "news",
  "seq": 1,
  "current_version": 2,
  "items": [
    {"channel": "news", "seq": 1, "version": 1, "text": "各位观众晚上好", "reason": "原稿漏字", "created_at": "2026-09-20T12:00:00.000Z"},
    {"channel": "news", "seq": 1, "version": 2, "text": "各位观众朋友们晚上好", "reason": "主持人改口", "created_at": "2026-09-20T12:05:00.000Z"}
  ]
}
```

没有更正的定稿序号返回 `current_version=0` 与空 `items`；缺口 / 未定稿 / 不存在的序号返回 HTTP 404。

### 批量更正（单事务、可幂等重试）

需要一次性更正多条已定稿字幕时，调用批量接口。每批 1～100 条，服务在**单个写事务**内逐项核对：任一项不满足条件都整批拒绝、零写入；全部通过才按输入顺序追加各序号的新版本。

```bash
curl -X POST http://localhost:8000/channels/news/corrections/batch \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotency_key": "fix-2026-09-26T20:00",
    "corrections": [
      {"seq": 3, "expected_version": 0, "text": "第三条（批量更正）", "reason": "错字"},
      {"seq": 1, "expected_version": 0, "text": "第一条（批量更正）", "reason": "译名统一"},
      {"seq": 2, "expected_version": 0, "text": "第二条（批量更正）", "reason": "标点"}
    ]
  }'
```

成功响应（`results` 顺序与请求数组一致，`index` 为项下标；本例按输入顺序处理，与序号大小无关）：

```json
{
  "batch_id": "6f2a1c9e-....",
  "channel": "news",
  "idempotency_key": "fix-2026-09-26T20:00",
  "idempotent": false,
  "created_at": "2026-09-26T20:00:00.000Z",
  "results": [
    {"index": 0, "seq": 3, "correction_version": 1, "text": "第三条（批量更正）", "reason": "错字", "created_at": "2026-09-26T20:00:00.000Z"},
    {"index": 1, "seq": 1, "correction_version": 1, "text": "第一条（批量更正）", "reason": "译名统一", "created_at": "2026-09-26T20:00:00.000Z"},
    {"index": 2, "seq": 2, "correction_version": 1, "text": "第二条（批量更正）", "reason": "标点", "created_at": "2026-09-26T20:00:00.000Z"}
  ]
}
```

**幂等重试**：相同频道、相同 `idempotency_key` 且请求体逐项字段与顺序完全相同时，重发始终返回同一批次结果（`batch_id` 不变、`idempotent=true`），即使相关序号的更正版本此后又被推进也不会误判冲突、不会重复追加：

```json
{
  "batch_id": "6f2a1c9e-....",
  "channel": "news",
  "idempotency_key": "fix-2026-09-26T20:00",
  "idempotent": true,
  "created_at": "2026-09-26T20:00:00.000Z",
  "results": [ /* 与首次完全一致的逐项结果 */ ]
}
```

相同频道复用同一 `idempotency_key` 但**内容不同**（任何一项的 seq / expected_version / text / reason 或输入顺序不同）返回 409：

```json
{"error": "idempotency_key_conflict", "message": "频道 news 的幂等键 fix-2026-09-26T20:00 已被内容不同的请求占用，请更换幂等键后重试"}
```

**整批拒绝**：同批出现重复 `seq`，或任何一项未定稿 / 为缺口 / 版本不符时，返回 409 `batch_conflict`，`conflicts` 按输入顺序列出**所有**冲突项，且整批零写入（合法项也不会落库）：

```bash
curl -X POST http://localhost:8000/channels/news/corrections/batch \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotency_key": "fix-bad",
    "corrections": [
      {"seq": 2, "expected_version": 0, "text": "通过项但不会写入", "reason": "r"},
      {"seq": 99, "expected_version": 0, "text": "未定稿", "reason": "r"},
      {"seq": 1, "expected_version": 0, "text": "版本已落后", "reason": "r"},
      {"seq": 3, "expected_version": 0, "text": "缺口序号", "reason": "r"},
      {"seq": 2, "expected_version": 0, "text": "批内重复 seq", "reason": "r"}
    ]
  }'
```

```json
{
  "error": "batch_conflict",
  "message": "4 个更正项未通过校验，整批拒绝且无任何写入",
  "conflicts": [
    {"index": 1, "seq": 99, "error": "not_finalized", "message": "序号 99 尚未定稿，只有已定稿字幕允许更正"},
    {"index": 2, "seq": 1, "error": "version_conflict", "message": "第 2 项 expected_version=0 与序号 1 的当前更正版本 1 不一致"},
    {"index": 3, "seq": 3, "error": "seq_is_gap", "message": "序号 3 已裁决为永久缺口，不能更正"},
    {"index": 4, "seq": 2, "error": "duplicate_seq", "message": "序号 2 在本批中重复：与第 0 项（下标从 0 起）序号相同"}
  ]
}
```

`conflicts[].error` 取值：

| `error`             | 含义                                                              |
| ------------------- | ----------------------------------------------------------------- |
| `not_finalized`     | 序号未定稿（频道不存在 / 序号还在缓冲区或水位线之外）             |
| `seq_is_gap`        | 序号已裁决为永久缺口，缺口不可更正                                |
| `version_conflict`  | `expected_version` 与该序号当前更正版本不一致                     |
| `duplicate_seq`     | 与本批中更靠前的某项序号重复                                      |

请求体校验失败（批次数为 0 或超过 100、`seq<1`、`expected_version<0`、`text`/`reason`/`idempotency_key` 为空白等）返回 HTTP 422。被拒绝的批次不占用幂等键，可用原键提交修正后的请求。批量与单条更正可混用，版本链在每个序号上保持连续；原始定稿仍只经 `view=original` 获取。

### 创建发布快照

对频道当前定稿状态做一次不可变冻结，供下游按 `snapshot_id` 分批拉取同一时刻的稳定字幕：

```bash
curl -X POST http://localhost:8000/channels/news/snapshots \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key": "run-2026-09-20T12:00"}'
```

```json
{
  "snapshot_id": "b1f3c2a4-....",
  "channel": "news",
  "watermark": 6,
  "idempotency_key": "run-2026-09-20T12:00",
  "idempotent": false,
  "created_at": "2026-09-20T12:00:00.000Z"
}
```

- 请求只需 `idempotency_key`（非空字符串，去除首尾空白后不得为空）；
- `idempotent=false` 表示本次新建；相同频道携带相同 key 重试时不重新冻结，返回原快照且 `idempotent=true`（即使频道水位线已推进，`watermark` 仍是创建时刻锁定的值）；
- 不存在的频道可直接创建快照，得到 `watermark=0` 的空快照。

### 分页拉取快照

按 `snapshot_id` + `after`（序号游标，字幕与缺口标记统一计数）+ `limit`（默认 1000，1..10000）分页，按序号升序混合返回冻结的字幕与缺口标记。默认 `view=original`，响应结构与定稿查询一致（不带 `view` / `correction_version` 字段）：

```bash
curl 'http://localhost:8000/snapshots/b1f3c2a4-..../items?after=0&limit=1000'
```

```json
{
  "snapshot_id": "b1f3c2a4-....",
  "channel": "news",
  "watermark": 6,
  "view": "original",
  "items": [
    {"kind": "subtitle", "seq": 1, "revision": 1, "text": "第一条字幕", "start_seq": null, "end_seq": null},
    {"kind": "gap",      "seq": 3, "revision": null, "text": null, "start_seq": 3, "end_seq": 4}
  ]
}
```

`view=effective` 返回**快照创建时刻冻结**的最新更正文本（不是查询时刻的最新更正），并附带 `correction_version`（字幕 0=快照时未更正；缺口为 null），顶层 `correction_version` 为本页最大值：

```bash
curl 'http://localhost:8000/snapshots/b1f3c2a4-..../items?view=effective&after=0&limit=2'
```

```json
{
  "snapshot_id": "b1f3c2a4-....",
  "channel": "news",
  "watermark": 6,
  "view": "effective",
  "correction_version": 1,
  "items": [
    {"kind": "subtitle", "seq": 1, "revision": 1, "text": "第一条字幕（快照时的更正）", "start_seq": null, "end_seq": null, "correction_version": 1},
    {"kind": "gap",      "seq": 3, "revision": null, "text": null, "start_seq": 3, "end_seq": 4, "correction_version": null}
  ]
}
```

快照不存在返回 HTTP 404 `snapshot_not_found`；`after<0`、`limit` 越界、非法 `view` 返回 HTTP 422。

### 查询快照内更正历史

返回某字幕序号在**快照时刻冻结**的全部更正版本（按 `version` 升序），与实时审计接口结构对应：

```bash
curl http://localhost:8000/snapshots/b1f3c2a4-..../items/1/corrections
```

```json
{
  "snapshot_id": "b1f3c2a4-....",
  "channel": "news",
  "seq": 1,
  "current_version": 1,
  "items": [
    {"seq": 1, "version": 1, "text": "第一条字幕（快照时的更正）", "reason": "原稿漏字", "created_at": "2026-09-20T11:00:00.000Z"}
  ]
}
```

| 状态码 | `error`                | 含义                                                       |
| ------ | ---------------------- | ---------------------------------------------------------- |
| 404    | `snapshot_not_found`   | 快照不存在                                                 |
| 404    | `seq_out_of_snapshot`  | 序号小于 1 或超出快照锁定的 watermark（空快照全部越界）    |
| 409    | `seq_is_gap`           | 该序号在快照中是永久缺口，缺口没有更正历史                 |

### 比较两个快照（差异）

比较**同一频道**任意两个快照的冻结生效内容。以路径快照为基准（base）、`other` 查询参数为对方，按 `seq` 升序合并两份快照条目，只返回有差异的条目：

```bash
curl 'http://localhost:8000/snapshots/b1f3c2a4-..../diff?other=c2e4d5b6-....&after=0&limit=1000'
```

```json
{
  "snapshot_id": "b1f3c2a4-....",
  "other_snapshot_id": "c2e4d5b6-....",
  "channel": "news",
  "watermark": 5,
  "other_watermark": 9,
  "items": [
    {
      "change": "changed",
      "seq": 1,
      "base":  {"kind": "subtitle", "seq": 1, "revision": 1, "text": "第一条字幕", "start_seq": null, "end_seq": null, "correction_version": 2},
      "other": {"kind": "subtitle", "seq": 1, "revision": 1, "text": "第一条字幕（再改）", "start_seq": null, "end_seq": null, "correction_version": 3}
    },
    {
      "change": "added",
      "seq": 7,
      "base": null,
      "other": {"kind": "gap", "seq": 7, "revision": null, "text": null, "start_seq": 7, "end_seq": 8, "correction_version": null}
    }
  ],
  "next_after": 7
}
```

差异类型与判定规则：

- `added`：序号仅存在于 `other` 快照（`base` 为 null）；`removed`：序号仅存在于基准快照（`other` 为 null，对方是更早的快照时才会出现）；`changed`：两侧都有但冻结内容不同；
- **字幕**按快照冻结的**生效文本**（`view=effective` 同款）与 `correction_version` 判定——即使文本相同，只要更正版本不同也算变更；**缺口**按裁决区间端点 `start_seq` / `end_seq` 判定；
- 两侧完全一致的序号**不返回**；与自身比较或期间无任何变化时返回空 `items`；
- 每条差异的 `base` / `other` 携带该侧快照冻结的生效条目（结构同快照 `view=effective` 条目）。

分页规则：

- `after`（默认 0）为序号游标，`limit`（默认 1000，1..10000）**只计差异条目**——未变化的条目不占额度，差异再稀疏也不会被漏掉；
- `next_after` 指向本页最后一条差异的 `seq`，把它作为下一页的 `after` 续拉即可逐页拉全；本页为空（无更多差异）时 `next_after=null`。

错误：

| 状态码 | `error`                      | 含义                                       |
| ------ | ---------------------------- | ------------------------------------------ |
| 404    | `snapshot_not_found`         | 基准快照或 `other` 快照不存在              |
| 409    | `snapshot_channel_mismatch`  | 两个快照分属不同频道，不能比较             |
| 422    | —                            | 缺少 `other`、`after<0` 或 `limit` 越界    |

### 领取消费者批次（claim）

命名消费者从自己的已确认游标后领取连续批次，服务在同一事务内签发带过期时间的租约令牌：

```bash
curl -X POST http://localhost:8000/channels/news/consumers/worker-1/claims \
  -H 'Content-Type: application/json' \
  -d '{"limit": 100, "lease_seconds": 30}'
```

```json
{
  "channel": "news",
  "consumer": "worker-1",
  "cursor": 0,
  "watermark": 6,
  "lease_token": "7c9e2f1a-....",
  "lease_expires_at": "2026-09-21T12:00:30.000Z",
  "items": [
    {"kind": "subtitle", "seq": 1, "revision": 1, "text": "第一条字幕", "start_seq": null, "end_seq": null},
    {"kind": "gap",      "seq": 3, "revision": null, "text": null, "start_seq": 3, "end_seq": 4}
  ]
}
```

- 请求体可整体省略（默认 `limit=100`、`lease_seconds=30`）；`limit` 取值 1..10000，`lease_seconds` 取值 (0, 3600]；
- 批次为该消费者已确认游标后的连续前缀（`cursor+1` 起，字幕与缺口标记按序混合，最多 `limit` 条），只包含已定稿内容，不含缓冲区中未到连续前缀的片段；
- 同一消费者持有**未过期租约**时重复领取返回 409 `lease_active`；租约过期后可重新领取同一未确认批次（返回相同内容、签发新令牌）；
- 无新内容时返回 `"items": []`、`"lease_token": null`，不签发租约；首次领取即注册消费者（之后可查状态）。

### 确认消费者批次（ack）

确认已处理的批次并把已确认游标推进到 `new_cursor`（允许部分确认，但必须落在本批次范围内）：

```bash
curl -X POST http://localhost:8000/channels/news/consumers/worker-1/acks \
  -H 'Content-Type: application/json' \
  -d '{"lease_token": "7c9e2f1a-....", "expected_cursor": 0, "new_cursor": 3}'
```

```json
{"channel": "news", "consumer": "worker-1", "previous_cursor": 0, "cursor": 3, "watermark": 6}
```

- `lease_token` 必须等于当前租约令牌；`expected_cursor` 必须等于当前已确认游标（乐观并发控制）；`new_cursor` 必须落在本批次 `[cursor+1, 批次末序号]` 范围内；
- 成功后游标推进、租约清空，下一次领取从新游标后继续；
- 失败一律返回 409 且**不改写任何状态**：

| `error`                 | 含义                                                     |
| ----------------------- | -------------------------------------------------------- |
| `lease_expired`         | 租约已过期，需重新领取后再确认                           |
| `lease_token_mismatch`  | 令牌与当前租约不匹配（旧令牌或他人令牌）                 |
| `cursor_out_of_range`   | `new_cursor` 不在本批次序号范围内                        |
| `cursor_conflict`       | `expected_cursor` 与当前游标不一致（并发确认已推进游标） |
| `no_active_lease`       | 消费者不存在，或当前没有待确认的租约                     |

### 查询消费者状态

```bash
curl http://localhost:8000/channels/news/consumers/worker-1
```

```json
{
  "channel": "news",
  "consumer": "worker-1",
  "cursor": 3,
  "watermark": 6,
  "lag": 3,
  "lease": {"from_seq": 4, "to_seq": 6, "expires_at": "2026-09-21T12:00:30.000Z", "expired": false}
}
```

- `lag` = 未确认条目数（`watermark - cursor`）；`lease` 为当前租约（含批次范围、过期时刻与查询时刻是否已过期），无租约时为 `null`；
- 消费者不存在（从未领取过批次）返回 404 `consumer_not_found`。

## 本地开发与测试

```bash
pip install -r requirements-dev.txt
pytest                     # 含乱序、修订竞争、缺口裁决、幂等/重叠、单条/批量更正、批量整批拒绝/幂等重试/并发、版本冲突、视图合并、审计、发布快照冻结/幂等/分页/不可变、快照差异合并/分页/错误、消费者领取/确认/租约过期/并发、并发事务等用例
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

服务默认监听 **8000** 端口；本地开发时 uvicorn 绑定 `0.0.0.0:8000`，从本机访问 http://localhost:8000/docs 即可打开交互式文档。

## 实现要点

- **事务化提交 / 裁决 / 更正 / 快照**：`app/service.py` 中「校验 → 写入」在单个 `BEGIN IMMEDIATE` 事务内完成；缺口裁决、单条 / 批量更正与快照发布的所有校验先于任何写入执行，失败即只读返回（批量更正随事务回滚），不产生副作用。`finalized` / `gaps` 表主键 `(channel, seq)`、`corrections` 表主键 `(channel, seq, version)`、`correction_batches` 的 `UNIQUE(channel, idempotency_key)`、`snapshots` 的 `UNIQUE(channel, idempotency_key)` 分别作为防重复定稿 / 裁决 / 版本号重复 / 批量幂等键重复 / 快照幂等键重复的最后防线。
- **连续前缀推进**：`_consume_contiguous` 从给定水位线的下一位起逐条消费缓冲区，遇到第一个缺失即停止；片段补齐与缺口裁决都复用该逻辑。
- **缺口存储**：`gap_decisions` 记录每次裁决区间及其当时释放到的序号（用于幂等返回原结果），`gaps` 按被跳过序号逐行落表，定稿查询用 `UNION ALL` 按序合并字幕与缺口标记。
- **追加式更正层**：`corrections(channel, seq, version, text, reason, created_at)` 仅通过 `INSERT` 追加，`(channel, seq)` 外键引用 `finalized`——缺口或未定稿序号在存储层即无法插入；版本号按序号从 1 递增，`expected_version` 乐观并发控制 + `BEGIN IMMEDIATE` 保证竞争下唯一成功；`corrections_no_update` / `corrections_no_delete` 触发器在存储层禁止任何改写与删除，原始定稿保持不可变。生效视图用相关子查询连接每个序号的最新版本（`MAX(version)`）合并文本，审计查询按 `version` 升序返回完整历史。
- **批量更正**：`correction_batches(batch_id, channel, idempotency_key, request_fingerprint, created_at)` 以 `UNIQUE(channel, idempotency_key)` 约束频道内幂等键；`correction_batch_items(batch_id, item_index, seq, expected_version, correction_version, text, reason, created_at)` 按输入顺序（`item_index` 从 0 起）冻结逐项请求与结果，供重试逐字重放。`submit_correction_batch` 在单个 `BEGIN IMMEDIATE` 事务内先查幂等键——命中且指纹（逐项 seq/expected_version/text/reason 及顺序的 SHA-256）一致则只读重放原批次，键相同指纹不同抛 `idempotency_key_conflict`；未命中时逐项核对批内重复 seq（重复出现项标 `duplicate_seq`）、定稿存在性（`not_finalized` / `seq_is_gap`）与当前版本（`version_conflict`），冲突按输入顺序全部收集后一次性抛 `batch_conflict` 并随事务回滚，做到**零写入**；全部通过才先插入批次行（唯一约束是并发同键重试的最后防线），再按输入顺序逐项 `INSERT ... RETURNING created_at` 追加 `corrections` 版本并写入冻结记录，与单条更正共用同一版本链。两张批次表均配 `no_update` / `no_delete` 触发器，幂等记录在存储层不可变。
- **发布快照**：`snapshots(snapshot_id, channel, watermark, idempotency_key, created_at)` 记录冻结时刻，`snapshot_items(snapshot_id, seq, …)` 对 `1..watermark` 每个序号落一行——字幕行同时存 `original_text`、创建时刻经 `MAX(version)` 连接冻结的 `effective_text` 及其 `correction_version`，缺口行存裁决区间端点；`snapshot_corrections(snapshot_id, seq, version, …)` 逐版本复制快照时刻已有的全部更正记录。创建先按 `(channel, idempotency_key)` 查重，命中即幂等返回；全部插入在一个写事务内完成，故冻结视图与锁定水位线严格一致。三张快照表各自配备 `no_update` / `no_delete` 触发器，快照一旦生成在存储层即不可变；分页与快照内审计只读取快照表，后续定稿、更正、缺口对其零影响。不存在的频道不写 `channels` 行，直接生成 `watermark=0` 的空快照。
- **快照差异**：`GET /snapshots/{snapshot_id}/diff?other=…` 在单个只读事务内校验两快照存在且同频道（不存在 404、跨频道 409），随后用两段 `LEFT JOIN` 的 `UNION ALL` 模拟全外连接，把两份 `snapshot_items` 按 `seq` 合并：第一段覆盖“基准有 / 双方都有且有差异”，第二段补齐“仅对方有”。差异判定用 SQLite 的 NULL 安全不等号 `IS NOT` 比较冻结的 `effective_text`、`correction_version`（字幕）与 `start_seq` / `end_seq`（缺口），完全一致的行在 SQL 层即被过滤，`ORDER BY seq LIMIT` 只作用于差异行——因此 `limit` 天然只计差异条目，稀疏差异也不会漏项；`next_after` 取本页最后一条差异的 `seq` 供续拉。
- **命名消费者拉取游标**：`consumers(channel, consumer, cursor, lease_token, lease_from, lease_to, lease_expires_at, …)` 每消费者一行。领取在单个 `BEGIN IMMEDIATE` 事务内「注册消费者 → 校验无未过期租约 → 复用定稿查询截取 `cursor` 后的连续批次（含缺口标记）→ 写入新租约（UUID 令牌 + 过期时刻 epoch 秒）」，并发领取同一消费者只有一个能拿到租约；确认同样单事务完成全部校验（游标一致 → 有租约 → 令牌匹配 → 未过期 → `new_cursor` 在批次范围内）后推进游标并清空租约，任何一步失败都不改写状态。租约过期由应用层按 `lease_expires_at` 判定，过期后同一未确认批次可被重新领取并签发新令牌（旧令牌随之失效），确认成功即清空租约、下一批从新游标继续，实现至少一次交付。
- **存储**：`channels`（水位线）、`fragments`（待定稿缓冲）、`finalized`（定稿，不可变）、`gap_decisions` / `gaps`（永久缺口）、`corrections`（追加式更正）、`correction_batches` / `correction_batch_items`（批量更正批次与逐项冻结记录，不可变）、`snapshots` / `snapshot_items` / `snapshot_corrections`（不可变发布快照）、`consumers`（命名消费者拉取游标）十二张表；WAL 模式 + `busy_timeout`。建表语句与触发器均幂等（`CREATE TABLE/TRIGGER IF NOT EXISTS`），已部署实例重启后自动补建新表与触发器，原有数据与接口行为不受影响。
- **容器化**：`docker compose up --build` 一键启动，数据库落在命名卷 `subtitles-data`；服务固定监听 **8000** 端口。
