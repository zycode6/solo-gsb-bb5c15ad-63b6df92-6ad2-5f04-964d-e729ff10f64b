# Dependency-Aware Task Lease API

基于 **Python + FastAPI + SQLite** 的本地任务调度服务：提交由任务与依赖边构成的
DAG，工作器原子领取「所有前置均已成功」的任务，凭借唯一租约令牌回报结果。

## 核心语义

- **DAG 校验**：提交时拒绝缺失依赖（`422 missing_dependency`）与环
  （`422 cycle_detected`，返回环上的节点序列），重复任务键同样被拒。
- **依赖门控的原子领取**：只有全部前置任务为 `succeeded` 的任务可被领取；
  领取在单事务 `BEGIN IMMEDIATE` 下完成，并发工作器中只有一个能拿到同一任务。
- **跨 DAG 并发组限流**：任务可选声明 `concurrency_group`（组名）与
  `concurrency_limit`（正整数上限），二者必须**同时**提供，只给其一会被 422 拒绝。
  同名组**跨所有 DAG 全局共享**一个容量：领取时按全库「未过期的 running 租约」
  计数，租约成功、失败（含回队与最终失败）或过期后名额自动释放；心跳续租会
  继续占用名额。组上限在首次提交时**持久化**，之后任何提交声明同名组都必须使用
  **相同**上限，否则整批提交回滚（`409 concurrency_group_conflict`，
  不留 DAG/任务/事件痕迹）；同一批内自相矛盾（同名组不同上限）返回 422。
  一次批量领取中新任务立即占位，已满的组会被**跳过并继续**寻找其他可领取任务
  （其他组或无组任务），尽量填满 `limit`。未声明组的任务行为完全不变。
- **递增 attempt 与唯一租约令牌**：每次领取生成新的 `attempt`（从 1 起递增）
  和新的随机 `lease_token`，并写入 attempts 历史。
- **租约过期与旧令牌失效**：租约到期后任务可被再次领取（新尝试、新令牌）；
  旧令牌的续租（heartbeat）或结果回报一律返回 **409**，不可能覆盖新尝试。
- **重试**：任务可配置 `max_retries`（额外重试次数）。只有工作器**显式回报失败**
  （`success=false`）才原子递增独立的 `failure_count` 并消耗重试预算；租约过期后
  被重新领取只增加 `attempt`，不影响失败计数。失败后回到 `pending` 等待下一次领取；
  `failure_count` 达到上限后置为 `failed`，并递归阻塞所有直接/间接后继
  （状态 `blocked`，带 `blocked_reason`），后继永远无法被领取。
- **幂等回报**：回报接口必须携带客户端生成的 `completion_id`。相同
  `task_id + completion_id` 且 `lease_token`、`success`、`result`、`error`
  等价的请求只生效一次——即使任务此后已被重新领取，重放也逐字节返回首次响应
  （同样的 HTTP 状态码与响应体），不会再次递增 `failure_count` 或重复阻塞后继；
  相同键但内容不同返回 `409 idempotency_conflict`；不同键仍执行当前租约校验。
  JSON 对象键序不同不影响等价判定。幂等记录持久化在 SQLite 中，服务重启后仍有效。
- **剩余额度**：状态响应中的 `retries_remaining = max(0, max_retries - failure_count)`，
  与显式失败计数一致，不因租约过期重领而减少。
- **持久化事件流**：所有关键状态变迁都在**同一事务**内向只增（append-only）的事件流
  追加一条不可变事件——`dag.created`（DAG 创建）以及
  `task.claimed`（领取/过期重领）、`task.requeued`（失败重排）、
  `task.succeeded`（成功）、`task.failed`（最终失败）、`task.blocked`（后继被阻塞）。
  事件与业务变更同生共死：事务回滚（如图校验之后的插入冲突）不留任何事件；
  幂等重放与 4xx 错误路径不产生事件。事件含全局严格递增的 `sequence`、`dag_id`、
  可空的 `task_id`/`attempt`/`from_state`/`to_state` 与 `occurred_at`
  （`dag.created` 的可空字段均为 `null`）。事件流持久化在 SQLite 中并由数据库触发器
  禁止 `UPDATE`/`DELETE`，可通过 `GET /events` 按 `sequence` 升序键集分页拉取，
  跨页与重启均不漏项、不重复。
- **状态接口**：DAG 概览给出各状态计数；任务详情列出每次尝试的状态、租约令牌、
  过期时间、心跳时间、错误信息，以及当前租约是否已过期和阻塞原因。
- **只读分析**：`GET /dags/{dag_id}/analysis` 按任务声明的 `duration_seconds`
  （缺省 1 秒）在依赖图上做关键路径分析（CPM）：每个任务的最早开始/完成时间、
  最晚开始/完成时间与总时差、总工期、关键路径、关键任务集合与并行层级
  （同层任务互不依赖、可并行执行）。分析在**单个只读事务**内读取一致快照，
  随后纯内存计算——不改变任务状态、不产生事件，可并发/重复调用且结果确定。
  DAG 不存在返回 404；存储数据中出现非正时长或图异常（环、悬空依赖）返回 422。

## 一条命令启动（端口 8000）

```bash
docker compose up --build
```

服务启动后监听 **http://localhost:8000** ，SQLite 数据持久化在 `task-data` 卷中。
交互式文档（Swagger UI）： http://localhost:8000/docs

不用 Docker 时：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

可用环境变量：`DB_PATH`（默认 `./data/tasks.db`）、`DEFAULT_LEASE_SECONDS`
（默认 30，任务可用 `lease_seconds` 单独覆盖）、`DEFAULT_DURATION_SECONDS`
（默认 1，任务可用 `duration_seconds` 单独覆盖，用于只读分析）。

## 调用示例

以下示例使用 `curl` 与 `jq`。

### 1. 提交 DAG

```bash
curl -s -X POST http://localhost:8000/dags \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "etl",
    "tasks": [
      {"key": "extract",   "lease_seconds": 60, "duration_seconds": 3},
      {"key": "transform", "deps": ["extract"], "max_retries": 2, "lease_seconds": 60, "duration_seconds": 5},
      {"key": "load",      "deps": ["transform"]}
    ]
  }'
```

非法图会被拒绝（422）：

```bash
# 缺失依赖
curl -i -X POST http://localhost:8000/dags -H 'Content-Type: application/json' \
  -d '{"tasks":[{"key":"a","deps":["ghost"]}]}'
# -> 422 {"error":{"code":"missing_dependency", ...}}

# 环
curl -i -X POST http://localhost:8000/dags -H 'Content-Type: application/json' \
  -d '{"tasks":[{"key":"a","deps":["c"]},{"key":"b","deps":["a"]},{"key":"c","deps":["b"]}]}'
# -> 422 {"error":{"code":"cycle_detected","details":{"cycle":["a","c","b","a"]}}}

# 并发组字段只给了一个（必须同时提供）
curl -i -X POST http://localhost:8000/dags -H 'Content-Type: application/json' \
  -d '{"tasks":[{"key":"a","concurrency_group":"db-writers"}]}'
# -> 422

# concurrency_limit 必须是正整数
curl -i -X POST http://localhost:8000/dags -H 'Content-Type: application/json' \
  -d '{"tasks":[{"key":"a","concurrency_group":"g","concurrency_limit":0}]}'
# -> 422
```

### 2. 工作器领取任务

只有 `extract` 没有前置，可以被领取；`transform`/`load` 被门控：

```bash
curl -s -X POST http://localhost:8000/tasks/claim \
  -H 'Content-Type: application/json' \
  -d '{"worker_id": "worker-1", "dag_id": "<DAG_ID>", "limit": 10}'
```

```json
{
  "claimed": [
    {
      "task_id": "…", "dag_id": "…", "task_key": "extract",
      "attempt": 1,
      "lease_token": "f1a3…", "lease_expires_at": "2026-09-17T11:00:00Z",
      "lease_seconds": 60.0, "payload": null,
      "concurrency_group": null
    }
  ],
  "count": 1
}
```

#### 跨 DAG 并发组（concurrency_group / concurrency_limit）

需要限制「同一类任务」在**所有 DAG 之间**的全局并发数时（例如最多 2 个任务同时写
同一个外部数据库），给任务声明同名组和正整数上限——两个字段必须同时出现：

```bash
# DAG A：两个写库任务，加入 db-writers 组，上限 2
curl -s -X POST http://localhost:8000/dags -H 'Content-Type: application/json' -d '{
  "id": "dag-a",
  "tasks": [
    {"key": "write-a1", "concurrency_group": "db-writers", "concurrency_limit": 2},
    {"key": "write-a2", "concurrency_group": "db-writers", "concurrency_limit": 2}
  ]
}'

# DAG B（可在任意时间、由任意调用方提交）：同名组必须声明相同上限 2
curl -s -X POST http://localhost:8000/dags -H 'Content-Type: application/json' -d '{
  "id": "dag-b",
  "tasks": [
    {"key": "write-b1", "concurrency_group": "db-writers", "concurrency_limit": 2},
    {"key": "cleanup"}
  ]
}'
```

- 上限 **2** 全局生效：跨 `dag-a`/`dag-b`，组内未过期的 running 租约最多只有 2 个。
  已占满时，领取会**跳过**组内任务、继续领取 `cleanup` 等其他可领取任务，尽量填满
  `limit`，而不是整批返回空：

```bash
# write-a1、write-a2 占位成功（同批立即计入占用）
curl -s -X POST http://localhost:8000/tasks/claim -H 'Content-Type: application/json' \
  -d '{"dag_id": "dag-a", "limit": 10}' | jq '.claimed[].task_key'
# "write-a1"
# "write-a2"

# 组已满：write-b1 被跳过，cleanup 照常领取
curl -s -X POST http://localhost:8000/tasks/claim -H 'Content-Type: application/json' \
  -d '{"dag_id": "dag-b", "limit": 10}' | jq '.claimed[].task_key'
# "cleanup"
```

- 名额在租约**成功回报、显式失败（回队或最终 failed）或租约过期**后自动释放；
  心跳续租期间名额继续占用。无组任务不参与任何限流。
- 同名组的上限**持久化且必须一致**：之后再提交 `db-writers` 但写成
  `concurrency_limit: 3` 会整体回滚（`409 concurrency_group_conflict`），
  该次提交的全部任务与事件都不会落库：

```bash
curl -i -X POST http://localhost:8000/dags -H 'Content-Type: application/json' -d '{
  "id": "dag-c",
  "tasks": [{"key": "write-c1", "concurrency_group": "db-writers", "concurrency_limit": 3}]
}'
# HTTP/1.1 409
# {"error":{"code":"concurrency_group_conflict",
#   "details":{"group":"db-writers","existing_limit":2,"submitted_limit":3}}}
```

任务查询（`GET /tasks/{id}`、`GET /tasks`、`GET /dags/{id}`）在任务对象上返回
`concurrency` 字段：有组时为 `{"group": "db-writers", "limit": 2}`，无组时为 `null`；
领取响应中的每个任务也带 `concurrency_group`（无组为 `null`）。

### 3. 续租（心跳）

任务可能运行超过租约时长时，在到期前凭当前令牌续租：

```bash
curl -s -X POST http://localhost:8000/tasks/<TASK_ID>/heartbeat \
  -H 'Content-Type: application/json' \
  -d '{"lease_token": "f1a3…", "extend_seconds": 120}'
```

租约过期并被他人重新领取后，再用旧令牌续租会得到 `409 lease_expired` /
`409 invalid_lease`。

### 4. 回报结果

回报必须携带客户端生成的幂等键 `completion_id`（建议用 UUID 等全局唯一值）：

```bash
# 成功
curl -s -X POST http://localhost:8000/tasks/<TASK_ID>/complete \
  -H 'Content-Type: application/json' \
  -d '{"completion_id": "3f1c…", "lease_token": "f1a3…", "success": true, "result": {"rows": 42}}'

# 失败（仍有重试额度时回到 pending；额度耗尽则 failed 并阻塞后继）
curl -s -X POST http://localhost:8000/tasks/<TASK_ID>/complete \
  -H 'Content-Type: application/json' \
  -d '{"completion_id": "9b22…", "lease_token": "…", "success": false, "error": "connection reset"}'
```

幂等语义：

- 同一 `task_id + completion_id` 且 `lease_token`/`success`/`result`/`error`
  等价的重复请求（网络重试等）直接返回**首次响应**（同状态码、同响应体）；
  即使任务租约已过期并被其他工作器重新领取，重放也不经过当前租约校验，
  既不会重复递增 `failure_count`，也不会重复阻塞后继。
- 相同 `task_id + completion_id` 但上述任一字段不等价 →
  `409 {"error":{"code":"idempotency_conflict"}}`。`result` 为 JSON 对象时键序
  不同视为等价（规范化比较）。
- 不同的 `completion_id` 视为新回报，仍须通过当前租约校验（旧令牌照例 409）。
- 幂等记录持久化在 `task-data` 卷的 SQLite 中，服务/容器重启后重放仍然有效。

> 租约过期后旧令牌**首次**使用新 `completion_id` 的回报返回 **409**，
> 不会作用于已开始的新尝试；只有曾成功生效过的幂等键才会回放首次响应。

### 5. 状态查询

```bash
# DAG 概览（状态计数 + 任务列表，含阻塞原因）
curl -s http://localhost:8000/dags/<DAG_ID> | jq '.states, .tasks[] | {key,state,attempt,blocked_reason}'

# 任务详情（含每次尝试与当前租约）
curl -s http://localhost:8000/tasks/<TASK_ID> | jq

# 全局任务列表，可按 dag_id / state 过滤
curl -s 'http://localhost:8000/tasks?state=blocked'
```

任务详情示例：

```json
{
  "key": "transform", "state": "failed", "attempt": 3, "max_retries": 2,
  "lease": null, "blocked_reason": null,
  "concurrency": null,
  "attempts": [
    {"attempt": 1, "state": "failed",    "error": "connection reset", "lease_token": "…", "expires_at": "…", "heartbeat_at": null, "expired": false},
    {"attempt": 2, "state": "expired",   "error": null,               "lease_token": "…", "expires_at": "…", "heartbeat_at": null, "expired": true},
    {"attempt": 3, "state": "failed",    "error": "fatal",            "lease_token": "…", "expires_at": "…", "heartbeat_at": null, "expired": false}
  ]
}
```

被阻塞后继示例：`{"key": "load", "state": "blocked", "blocked_reason":
"upstream 'transform' permanently failed", "lease": null, "concurrency": null}`。

并发组任务示例：`{"key": "write-a1", "state": "running",
"concurrency": {"group": "db-writers", "limit": 2}, "lease": {"expired": false, …}}`。

### 6. 事件流

事件流只增不改，记录全部关键状态变迁。每条事件形如：

```json
{
  "sequence": 7,
  "type": "task.claimed",
  "dag_id": "9f1c…",
  "task_id": "2b7e…",
  "attempt": 1,
  "from_state": "pending",
  "to_state": "running",
  "occurred_at": "2026-09-17T11:00:00Z"
}
```

- `type` ∈ `dag.created` / `task.claimed` / `task.requeued` /
  `task.succeeded` / `task.failed` / `task.blocked`；
- `sequence` 为全局严格递增游标，事件按其升序返回；`dag.created` 事件的
  `task_id`、`attempt`、`from_state`、`to_state` 均为 `null`；
- 事件与对应业务变更在同一事务提交，回滚不留事件；存储层禁止
  `UPDATE`/`DELETE`，保证不可变。

拉取接口采用键集分页，`after` 为不透明游标，`limit` 取 1～100
（默认 `after=0`、`limit=50`），可选 `dag_id` 只看单个 DAG：

```bash
curl -s 'http://localhost:8000/events?after=0&limit=50'
```

```json
{
  "events": [
    {"sequence": 1, "type": "dag.created", "dag_id": "d1", "task_id": null,
     "attempt": null, "from_state": null, "to_state": null,
     "occurred_at": "2026-09-17T10:59:59Z"},
    {"sequence": 2, "type": "task.claimed", "dag_id": "d1", "task_id": "…",
     "attempt": 1, "from_state": "pending", "to_state": "running",
     "occurred_at": "2026-09-17T11:00:00Z"}
  ],
  "next_after": 2
}
```

翻页时把上一页的 `next_after` 原样作为下次请求的 `after`；空页
（已到流末尾，或指定 `dag_id` 下暂无新事件）的 `next_after` 沿用传入的 `after`。
事件与游标都持久化在 `task-data` 卷中，逐页追赶在跨页、服务重启后都不会漏项或重复。

```bash
# 只订阅某个 DAG 的事件，每页 100 条
curl -s 'http://localhost:8000/events?dag_id=<DAG_ID>&after=0&limit=100'
```

### 7. 只读分析（关键路径）

按任务声明的 `duration_seconds`（缺省 1）对整个 DAG 做关键路径分析，
与任务当前运行状态无关，纯粹回答「如果现在开始，理想排程长什么样」：

```bash
curl -s http://localhost:8000/dags/<DAG_ID>/analysis | jq
```

```json
{
  "dag_id": "…",
  "task_count": 3,
  "total_duration_seconds": 9.0,
  "critical_path": ["extract", "transform", "load"],
  "critical_tasks": ["extract", "transform", "load"],
  "levels": [["extract"], ["transform"], ["load"]],
  "tasks": [
    {"key": "extract",   "duration_seconds": 3.0, "earliest_start": 0.0, "earliest_finish": 3.0,
     "latest_start": 0.0, "latest_finish": 3.0, "slack": 0.0, "critical": true,  "level": 0},
    {"key": "transform", "duration_seconds": 5.0, "earliest_start": 3.0, "earliest_finish": 8.0,
     "latest_start": 3.0, "latest_finish": 8.0, "slack": 0.0, "critical": true,  "level": 1},
    {"key": "load",      "duration_seconds": 1.0, "earliest_start": 8.0, "earliest_finish": 9.0,
     "latest_start": 8.0, "latest_finish": 9.0, "slack": 0.0, "critical": true,  "level": 2}
  ]
}
```

- `earliest_start` / `earliest_finish`：所有前置完成即可开始口径下的最早时间；
  `latest_start` / `latest_finish`：不拖延总工期口径下的最晚时间；
  `slack` 为总时差，`slack ≈ 0` 的任务构成 `critical_tasks`（关键任务集合）。
- `critical_path` 为一条从源点到汇点的关键路径（并列时按任务键字典序取确定的一条）；
  `levels` 为并行层级：第 i 层任务的全部前置都在更浅的层，同层任务可并行执行。
- 分析在单个只读事务内读取一致快照后纯内存计算：不改任务状态、不产生事件，
  可并发/重复调用，结果确定。DAG 不存在 → `404 dag_not_found`；
  存储数据中出现非正 `duration_seconds` → `422 invalid_duration`，
  图异常（环、悬空依赖）→ `422 cycle_detected` / `422 missing_dependency`。

状态变迁与事件 `type` 的对应关系：

| 业务动作 | 事件 type | from_state → to_state |
| --- | --- | --- |
| DAG 创建 | `dag.created` | 可空字段均为 `null` |
| 首次领取 / 过期重领 | `task.claimed` | `pending`（或 `running`）→ `running` |
| 显式失败、仍有重试额度 | `task.requeued` | `running` → `pending` |
| 回报成功 | `task.succeeded` | `running` → `succeeded` |
| 显式失败、重试耗尽 | `task.failed` | `running` → `failed` |
| 递归阻塞后继 | `task.blocked` | 原状态（`pending`/`running`）→ `blocked` |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/dags` | 提交 DAG（校验缺失依赖与环） |
| `GET` | `/dags/{dag_id}` | DAG 概览：状态计数与任务列表 |
| `GET` | `/dags/{dag_id}/analysis` | 只读分析：最早/最晚时间、总工期、关键路径与并行层级 |
| `POST` | `/tasks/claim` | 原子领取前置全部成功的任务（跳过已满的跨 DAG 并发组，尽量填满 limit） |
| `GET` | `/tasks?dag_id=&state=` | 任务列表（可过滤） |
| `GET` | `/events?after=&limit=&dag_id=` | 不可变事件流：按 sequence 升序键集分页 |
| `GET` | `/tasks/{task_id}` | 任务详情：尝试历史、租约、阻塞原因 |
| `POST` | `/tasks/{task_id}/heartbeat` | 凭当前租约令牌续租 |
| `POST` | `/tasks/{task_id}/complete` | 凭租约令牌 + `completion_id` 幂等回报成功/失败 |
| `GET` | `/health` | 健康检查 |

## 任务状态机

```
pending ──claim──▶ running ──complete(success)──────────▶ succeeded
  ▲                  │  │
  │ fail (可重试)    │  └─complete(fail, 重试耗尽)─▶ failed ──▶ blocked(所有后继)
  └──────────────────┘
                     └─租约过期─▶ 可被再次 claim（attempt+1，新令牌；旧令牌全部 409）
```

## 开发与测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

测试覆盖：DAG 校验（缺失依赖/环/重复键）、依赖门控、并发原子领取、
跨 DAG 并发组（字段必须成对且为正整数、组容量占用与同批即时占位、满组跳过继续填满
limit、跨 DAG 共享容量、同名组上限持久化一致与冲突整批回滚、成功/失败/过期释放名额、
心跳续租保持占用、重启后上限与限流仍生效、无组任务不受影响、旧库迁移补 NULL）、
attempt 递增与令牌唯一、过期后旧令牌 409、心跳续租、重试与耗尽后递归阻塞、
幂等回报（重放同响应、内容冲突 409、键序等价、重启后持久化）、旧 SQLite 数据迁移、
状态接口、持久化事件流（各事件类型与状态对、同事务回滚不留事件、重放不产生事件、
键集分页无漏项/重复、dag_id 过滤、参数校验、重启后不重置、存储层不可变）、
只读分析（最早/最晚时间与总时差、总工期、关键路径与关键任务集合、并行层级、
默认与小数时长、并列关键路径的确定性、只读不产生事件、404/422 错误路径、
旧库迁移后默认时长）。
