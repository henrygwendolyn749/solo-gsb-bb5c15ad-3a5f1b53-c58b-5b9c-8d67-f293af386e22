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
- **只读的领取决策预演（claim-plan）**：`POST /tasks/claim-plan` 的请求参数与
  `POST /tasks/claim` **完全一致**（`worker_id`/`dag_id`/`limit`），在**单个
  SQLite 只读事务**的一致快照上**模拟**真实领取的完整裁决——同样的候选排序
  （rowid）、依赖门控、退避时刻、并发组容量（全库跨 DAG）与工作器能力门控，
  **同批模拟选中的任务即时占用并发组容量**，因此选中序列与同一快照下的真实
  批量领取一致。接口**绝不写入**任务、attempt 或事件（连接置 `query_only`
  兜底，事务以 ROLLBACK 收尾），可安全并发/重复调用。响应在 `claimed` 中按
  模拟选中顺序返回任务（结构与真实领取相同，但**不签发真实租约令牌**：
  `lease_token` 恒为 `null`，`lease_expires_at` 仅为按当前 `lease_seconds`
  的模拟值），并在 `unselected` 中为每个**未选中的 pending 任务**给出**首个
  阻塞原因**及相关字段，原因为以下之一（按门控顺序）：
  `dependency_not_succeeded`（`blocked_by` 列出每个未 `succeeded` 的前置及其
  模拟后状态）、`backoff_not_elapsed`（携带 `next_eligible_at`）、
  `concurrency_group_full`（携带 `concurrency_group`/`group_limit`/
  `group_occupied`，占用含本批模拟占位）、`capability_mismatch`（携带任务
  `requires` 与工作器当前 `capabilities`）、`limit_reached`（携带本次
  `limit`）。指定 `dag_id` 不存在返回 `404 dag_not_found`；批量填满之前遇到
  requires 候选但领取者无存活能力登记时，与真实领取同语义返回
  `409 worker_not_registered`（details 相同，同样不落任何写入）；候选在限额
  填满之后才被遇到则不触发 409，仅记为 `limit_reached`。`succeeded`/
  `running`/`failed`/`blocked`/`canceled` 任务不属于 pending，不出现在
  `unselected`（其中过期租约的 running 任务可被模拟重领，计入 `claimed`）。
  该模块只复用领取裁决与快照，**不改变 `/tasks/claim` 及任何既有接口的行为**。
- **工作器能力登记与能力路由**：工作器通过 `PUT /workers/{worker_id}` 登记
  **去重后的能力标签**（`capabilities`，按首次出现顺序持久化，空数组合法）与
  **正数** `ttl_seconds`（登记存活时长，非正返回 422）。重复 PUT 是覆盖语义：
  **整体替换能力标签并刷新过期时刻**（`registered_at` 保留首次登记时间）。
  `GET /workers/{worker_id}` 查询登记内容与实时 `alive`（`expires_at >= now`），
  从未登记返回 `404 worker_not_found`；过期登记行仍可查询，只是 `alive: false`。
  任务提交时可选声明 `requires: ["标签", …]`（同样去重持久化；空数组或缺省表示
  **不要求能力**）。领取时该候选必须**同时**满足依赖门控、退避时刻、并发组容量与
  能力门控：能力校验在领取的**同一 `BEGIN IMMEDIATE` 事务**内进行（
  `requires` 与 worker 登记取同一事务的一致状态），领取者须携带**存活**登记的
  `worker_id` 且其 `capabilities` **覆盖全部** requires 标签（超集即可，标签
  区分大小写与空格）。**能力不匹配的候选被跳过，领取继续向后寻找其他任务填满
  `limit`**（与满并发组同样的跳过语义）；**登记缺失或已过期（含匿名领取）时，
  一旦存在依赖/退避/并发组门控均满足、仅缺能力登记的候选，整次领取返回
  `409 worker_not_registered` 且不领取任何任务**（同事务回滚；details 携带
  `worker_id`/`requires`/`task_id`/`task_key`/`dag_id`）。任务详情、DAG/任务列表
  与领取结果均返回 `requires`（未声明为空数组）；**未声明 `requires` 的历史任务
  完全兼容现有领取行为**——匿名、无登记或登记过期的 worker_id 都能领取。
- **递增 attempt 与唯一租约令牌**：每次领取生成新的 `attempt`（从 1 起递增）
  和新的随机 `lease_token`，并写入 attempts 历史。
- **租约过期与旧令牌失效**：租约到期后任务可被再次领取（新尝试、新令牌）；
  旧令牌的续租（heartbeat）或结果回报一律返回 **409**，不可能覆盖新尝试。
- **重试**：任务可配置 `max_retries`（额外重试次数）。只有工作器**显式回报失败**
  （`success=false`）才原子递增独立的 `failure_count` 并消耗重试预算；租约过期后
  被重新领取只增加 `attempt`，不影响失败计数。失败后回到 `pending` 等待下一次领取；
  `failure_count` 达到上限后置为 `failed`，并递归阻塞所有直接/间接后继
  （状态 `blocked`，带 `blocked_reason`），使其无法被领取——除非之后恢复失败的上游
  （见下）。
- **失败恢复**：`POST /tasks/{task_id}/resume` 仅接受最终失败（`failed`）的任务，
  在**单个事务**内将其置回 `pending`（清除租约与 `finished_at`，**不重置**
  `failure_count`/重试预算与 `attempt` 历史），追加 `task.resumed` 事件
  （`failed → pending`），并按依赖图**递归检查被阻塞的后继**：仅当某后继的全部前置
  均已 `succeeded` 或处于 `pending`（被恢复任务本身与本次已连锁解锁的后继按拓扑序
  先更新，同样视为 `pending`）时，才将其解锁为 `pending`（清除 `blocked_reason`，
  同样追加 `task.resumed` 事件，`blocked → pending`）；前置中仍有
  `running`/`failed`/`blocked` 的后继保留 `blocked` 及原阻塞原因。恢复后的任务
  立即可被领取（不触发退避）；再次显式失败会因预算已耗尽立即回到 `failed` 并重新
  阻塞后继。非 `failed` 状态（包括恢复成功后的重复调用）返回
  `409 task_not_failed`，且不新增任何事件；任务不存在返回 `404 task_not_found`。
- **任务取消**：`POST /tasks/{task_id}/cancel` 仅允许取消 `pending`、`running`
  或 `blocked` 的任务，在**单个事务**内把目标置为终态 `canceled`（清除租约与
  `next_eligible_at`/`blocked_reason`，写入 `finished_at`）。目标正在运行时
  **立即终止当前 attempt**（active → expired）并清除租约：旧 `lease_token`
  此后的心跳与完成回报一律返回 **409**，不会作用于该任务。取消后按依赖图
  **递归取消所有尚未结束的后继**：每个直接/间接后继只要处于
  `pending`/`running`/`blocked` 就连锁置为 `canceled`（运行中的后继同样终止
  attempt、清除租约，并发组名额随之释放）；已 `succeeded`/`failed`/`canceled`
  的后继保持不变。每个实际发生的状态变化在同一事务内追加一条
  `task.canceled` 事件，已结束的任务不重复写入；重复取消（目标已是 `canceled`）
  与取消 `succeeded`/`failed` 任务返回 `409 task_not_cancelable`，不产生任何
  事件；任务不存在返回 `404 task_not_found`。`canceled` 是终态，任务永不再
  进入领取候选，DAG 状态计数与任务列表/详情查询均支持该状态。
- **持久化指数退避**：任务可在提交时配置 `retry_backoff`：
  `{"initial_seconds": >0, "multiplier": >=1, "max_seconds": >= initial_seconds}`，
  随任务持久化（`max_seconds < initial_seconds` 等非法组合在提交时 422 拒绝）。
  **显式失败且仍有重试额度**时，在回报的同一事务内按
  `delay = min(max_seconds, initial_seconds * multiplier^(failure_count-1))`
  （首次失败 `failure_count=1`，幂次从 0 起）计算延迟，把
  `next_eligible_at = 回报时刻 + delay` 与状态回队一起落库并在回报响应中返回；
  领取只选取该时刻**已到**（`<= now`）的 `pending` 任务，未到期一律跳过。
  **租约过期重领不触发退避**：退避只作用于显式失败，过期任务立即可被重领，且领取时
  清空 `next_eligible_at`。未配置 `retry_backoff` 的任务（含历史数据）保持**立即重试**
  （`next_eligible_at` 为 `null`）。任务查询（`GET /tasks/{id}`、`GET /tasks`、
  `GET /dags/{id}`）返回持久化的 `retry_backoff` 策略与当前 `next_eligible_at`；
  `task.requeued` 事件携带 `next_eligible_at`（其他事件该字段为 `null`）。
  策略与下次可领时刻均持久化在 SQLite 中，服务重启后仍然有效。
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
  `task.succeeded`（成功）、`task.failed`（最终失败）、`task.blocked`（后继被阻塞）、
  `task.resumed`（失败恢复与后继连锁解锁）、`task.canceled`（取消与后继连锁取消）。
  事件与业务变更同生共死：事务回滚（如图校验之后的插入冲突）不留任何事件；
  幂等重放与 4xx 错误路径不产生事件。事件含全局严格递增的 `sequence`、`dag_id`、
  可空的 `task_id`/`attempt`/`from_state`/`to_state` 与 `occurred_at`
  （`dag.created` 的可空字段均为 `null`）。事件流持久化在 SQLite 中并由数据库触发器
  禁止 `UPDATE`/`DELETE`，可通过 `GET /events` 按 `sequence` 升序键集分页拉取，
  跨页与重启均不漏项、不重复。
- **可靠事件订阅（投递—确认租约）**：在只读事件流之上提供 `POST /subscriptions`
  订阅，创建时保存 `committed_sequence`（默认 `0`，从头订阅），可选 `dag_id`
  只订阅单个 DAG（过滤口径与 `GET /events?dag_id=` 一致）。`POST /subscriptions/{id}/poll`
  接受 `limit`（1～100，默认 50）与正数 `lease_seconds`，从 `committed_sequence`
  之后按 `sequence` 升序返回下一批事件及随机 `delivery_token`。**同一订阅同时只允许
  一个未确认批次**：租约未过期时重复 `poll` 逐字节原样回放该批次（同令牌、同事件集，
  忽略新的 `limit`/`lease_seconds`）；租约过期后旧批次失效，下次 `poll` 以**新随机
  令牌**和新租约重新投递同一窗口（游标未推进，未确认事件原样重投，绝不跳过）。
  `POST /subscriptions/{id}/ack` 仅接受**当前**批次令牌，把游标推进到该批最后一条
  `sequence`；令牌旧（批次已过期重投）、错误或没有在途批次一律返回 **409**
  （`invalid_delivery_token` / `no_pending_batch`），游标保持不动，因此不可能越过
  任何未确认事件。订阅、在途批次（含令牌、租约与首次响应原文）与游标全部持久化在
  SQLite 中，**重启后语义不变**（未过期批次仍原样回放，其令牌仍可确认）。另有
  `GET /subscriptions`（可按 `dag_id` 过滤）与 `GET /subscriptions/{id}`
  （含在途批次及其 `expired` 状态）查询。该模块只复用 `events.sequence`，
  不改变既有 `/events` 与任务接口的任何行为。
- **可复用 DAG 模板（版本化快照与参数化实例化）**：`POST /templates` 按名称创建
  模板并立即发布**不可变版本 1**（模板名重复 `409 template_exists`）；
  `POST /templates/{name}/versions` 按名称发布新版本，版本号在模板内从 1 起
  单调递增，每个版本独立保存发布时刻的任务图快照（JSON），**发布后永不更新**
  （后续发布不影响旧版本；另有 `GET /templates`、`GET /templates/{name}` 与
  `GET /templates/{name}/versions/{version}` 查询版本与快照）。创建/发布的图
  校验（重复键、缺失依赖、环、同批并发组矛盾）**完全复用直接
  `POST /dags` 的校验口径与错误码**（422），非法请求不产生模板/版本。
  `POST /templates/{name}/versions/{version}/instantiations` 用调用方参数
  实例化版本：任务 **payload 值**中的 `${name}` 占位符被实参替换（对象键不替换；
  值整体恰为一个占位符时保留实参原类型，嵌入更长字符串时按文本替换）。
  实参集合必须与版本中提取出的占位符集合**一一对应**：缺参
  （`422 missing_template_parameter`）或多参（`422 unexpected_template_parameter`）
  一律拒绝且**不落库**。实例化必须携带非空 `idempotency_key`：相同
  **模板 + 版本 + 键**且参数等价（JSON 键序不影响等价判定）的重复调用返回首次
  创建的**同一 DAG**（首次 `201`，重放 `200`，响应结构与 `GET /dags/{id}` 一致）；
  同键但参数不同返回 `409 idempotency_conflict`。**参数校验、占位符替换、
  DAG 图校验/落库（含跨 DAG 并发组冲突检测与 `dag.created` 事件）与幂等记录
  写入在单个 SQLite 事务内完成**——任何 422/409 整体回滚，不留 DAG、事件或
  幂等记录；模板/版本不存在分别返回 404 `template_not_found` /
  `template_version_not_found`。模板、快照与幂等记录均持久化在 SQLite 中，
  重启后重放与冲突语义不变。该模块只复用 DAG 校验、落库与响应结构，
  **不改变直接 `POST /dags` 的任何行为**（直接提交不做占位符替换）。
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
      {"key": "transform", "deps": ["extract"], "max_retries": 2, "lease_seconds": 60, "duration_seconds": 5,
       "retry_backoff": {"initial_seconds": 2, "multiplier": 3, "max_seconds": 60}},
      {"key": "load",      "deps": ["transform"]}
    ]
  }'
```

`transform` 显式失败后按指数退避重试：第 1 次失败等 2s、第 2 次等 6s
（`min(60, 2·3^(failure_count-1))`，受 `max_seconds=60` 封顶），
到期前不会被任何领取请求选中；不配置 `retry_backoff` 的任务失败后立即可重领。
非法策略（`initial_seconds<=0`、`multiplier<1`、`max_seconds<initial_seconds`）
在提交时返回 422：

```bash
# max_seconds 小于 initial_seconds
curl -i -X POST http://localhost:8000/dags -H 'Content-Type: application/json' \
  -d '{"tasks":[{"key":"a","retry_backoff":{"initial_seconds":10,"multiplier":2,"max_seconds":5}}]}'
# -> 422
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

### 3. 工作器能力登记与能力路由（workers / requires）

需要「只有具备某种能力的工作器才能领取某类任务」时（例如只有 GPU 机器能跑训练、
只有持有证书的进程能访问外部系统），先让工作器**登记能力**，再在提交任务时用
`requires` 声明标签。

登记/刷新工作器能力（标签自动去重，`ttl_seconds` 必须为正数）：

```bash
# w-gpu 具备 gpu 能力，登记 60 秒内存活
curl -s -X PUT http://localhost:8000/workers/w-gpu \
  -H 'Content-Type: application/json' \
  -d '{"capabilities": ["gpu"], "ttl_seconds": 60}'
# {
#   "worker_id": "w-gpu", "capabilities": ["gpu"], "ttl_seconds": 60.0,
#   "registered_at": "2026-09-19T10:00:00Z",
#   "expires_at": "2026-09-19T10:01:00Z", "alive": true
# }

# 查询登记与存活状态（从未登记 -> 404 worker_not_found；过期行仍返回 alive=false）
curl -s http://localhost:8000/workers/w-gpu

# 重复 PUT 覆盖能力并刷新过期时刻（registered_at 不变，expires_at 顺延）
curl -s -X PUT http://localhost:8000/workers/w-gpu \
  -H 'Content-Type: application/json' \
  -d '{"capabilities": ["gpu", "io"], "ttl_seconds": 60}'
```

提交带能力要求的任务（标签同样去重）：

```bash
curl -s -X POST http://localhost:8000/dags -H 'Content-Type: application/json' -d '{
  "id": "cap-dag",
  "tasks": [
    {"key": "prepare"},
    {"key": "train",  "deps": ["prepare"], "requires": ["gpu"]},
    {"key": "export", "deps": ["train"],   "requires": ["gpu", "io"]}
  ]
}'
```

领取语义（能力校验与依赖、退避、并发组在**同一事务**内完成）：

```bash
# 匿名/未登记领取：候选中存在 requires 任务（其余门控已满足）-> 409，整批不领取
curl -i -X POST http://localhost:8000/tasks/claim -H 'Content-Type: application/json' \
  -d '{"limit": 10}'
# HTTP/1.1 409
# {"error":{"code":"worker_not_registered",
#   "details":{"worker_id":null,"requires":["gpu"],"task_key":"train", …}}}

# 只有 gpu 能力：prepare/train 可领；export 要求 gpu+io，能力不匹配被跳过，
# 领取继续向后寻找其他可领任务填满 limit（不会因 export 卡住整批）
curl -s -X POST http://localhost:8000/tasks/claim -H 'Content-Type: application/json' \
  -d '{"worker_id": "w-gpu", "limit": 10}' | jq '.claimed[].task_key'
# "prepare"
# "train"

# 工作器续登覆盖为 [gpu, io] 后，export 即可被领取
curl -s -X PUT http://localhost:8000/workers/w-gpu -H 'Content-Type: application/json' \
  -d '{"capabilities": ["gpu", "io"], "ttl_seconds": 60}'
curl -s -X POST http://localhost:8000/tasks/claim -H 'Content-Type: application/json' \
  -d '{"worker_id": "w-gpu"}' | jq '.claimed[].task_key'
# "export"
```

- `requires` 为空/缺省的任务**不做能力校验**：任何 worker_id（含匿名、无登记、
  登记过期）都可领取，与历史行为完全一致；旧版 SQLite 库迁移后历史任务
  `requires` 为空数组。
- 能力超集即可匹配：worker 持有 `["gpu","io","cpu"]` 可领取 `requires: ["gpu"]`
  的任务；标签区分大小写与前后空格（`"GPU"` ≠ `"gpu"`）。
- 登记过期（超过 `expires_at` 未续登）后领取 requires 任务同样返回
  `409 worker_not_registered`；过期前重新 PUT 可不断刷新存活窗口。
- 被依赖、退避或满并发组挡住的 requires 任务**不在**可领取候选内，不会触发
  409（门控顺序：依赖/退避 → 并发组容量 → 能力）。
- 领取结果与任务详情均携带 `requires`：

```bash
curl -s http://localhost:8000/tasks/<TASK_ID> | jq '{key, requires, state}'
# {"key":"train","requires":["gpu"],"state":"running"}
```

#### 只读预演：POST /tasks/claim-plan

想在**不实际领取**的情况下查看「此刻发起一次批量领取会选中哪些任务、其余
pending 任务为什么选不上」时，调用 `claim-plan`：请求体与 `/tasks/claim`
完全相同，整个计算在单个 SQLite 只读事务内完成，不改任务、不产生 attempt
或事件，可以反复调用。

```bash
# 沿用上面 cap-dag（prepare / train[gpu] / export[gpu,io]）与 w-gpu（仅 gpu）
curl -s -X POST http://localhost:8000/tasks/claim-plan \
  -H 'Content-Type: application/json' \
  -d '{"worker_id": "w-gpu", "dag_id": "cap-dag", "limit": 10}' | jq
```

```json
{
  "simulated": true,
  "worker_id": "w-gpu",
  "dag_id": "cap-dag",
  "limit": 10,
  "evaluated_at": "2026-09-19T12:00:00Z",
  "claimed": [
    {"task_id": "…", "dag_id": "cap-dag", "task_key": "prepare", "attempt": 1,
     "lease_token": null, "lease_expires_at": "2026-09-19T12:00:30Z",
     "lease_seconds": 30.0, "concurrency_group": null, "requires": []},
    {"task_id": "…", "dag_id": "cap-dag", "task_key": "train", "attempt": 1,
     "lease_token": null, "lease_expires_at": "2026-09-19T12:00:30Z",
     "lease_seconds": 30.0, "concurrency_group": null, "requires": ["gpu"]}
  ],
  "count": 2,
  "unselected": [
    {"task_id": "…", "dag_id": "cap-dag", "task_key": "export",
     "reason": "capability_mismatch",
     "message": "工作器存活登记的能力不覆盖任务要求的全部标签",
     "requires": ["gpu", "io"], "capabilities": ["gpu"]}
  ]
}
```

- `claimed` 是按真实领取的候选排序与门控**模拟选中**的序列；同批选中的任务会
  **即时占用并发组容量**，因此紧接着对同一数据库调用 `/tasks/claim` 会得到
  相同的 `task_key` 序列（预演不改变状态，真实领取随后正常落库）。
- 模拟任务**不签发租约**：`lease_token` 恒为 `null`，不能用于心跳或回报；
  `lease_expires_at` 仅为按 `evaluated_at + lease_seconds` 的模拟值。
- `unselected` 只包含**未选中的 pending 任务**，按候选行顺序给出首个阻塞原因：

| reason | 含义 | 相关字段 |
| --- | --- | --- |
| `dependency_not_succeeded` | 仍有前置未 `succeeded`（最先裁决） | `blocked_by`：每个未完成前置的 `task_key` 与模拟后 `state` |
| `backoff_not_elapsed` | 显式失败的退避时刻未到 | `next_eligible_at` |
| `concurrency_group_full` | 并发组已满（含本批模拟占位，先于能力门控） | `concurrency_group`、`group_limit`、`group_occupied` |
| `capability_mismatch` | 工作器存活登记不覆盖全部 requires 标签 | `requires`、`capabilities` |
| `limit_reached` | 本批 `limit` 个名额已被前面的任务占满 | `limit` |

并发组同批即时占位的例子（组上限 2，三个组任务 + 两个无组任务，`limit=5`）：

```bash
curl -s -X POST http://localhost:8000/tasks/claim-plan -H 'Content-Type: application/json' \
  -d '{"dag_id": "<DAG_ID>", "limit": 5}' | jq '{claimed: [.claimed[].task_key],
       unselected: [.unselected[] | {task_key, reason, group_occupied}]}'
# {
#   "claimed": ["g1", "g2", "u1", "u2"],
#   "unselected": [
#     {"task_key": "g3", "reason": "concurrency_group_full", "group_occupied": 2}
#   ]
# }
```

依赖 + 退避的例子：`a` 失败退避中、`b` 依赖 `a`、`c` 依赖 `b` 时，预演返回
`a → backoff_not_elapsed`、`b/c → dependency_not_succeeded`（`blocked_by`
中前置的 `state` 把本批模拟选中的任务记为 `running`）。匿名（或登记过期）预演
在批量填满前遇到 requires 候选时与真实领取一样返回
`409 worker_not_registered`；指定不存在的 `dag_id` 返回 `404 dag_not_found`。
预演后任务状态、attempt 历史与事件流均不发生任何变化。

### 4. 续租（心跳）

任务可能运行超过租约时长时，在到期前凭当前令牌续租：

```bash
curl -s -X POST http://localhost:8000/tasks/<TASK_ID>/heartbeat \
  -H 'Content-Type: application/json' \
  -d '{"lease_token": "f1a3…", "extend_seconds": 120}'
```

租约过期并被他人重新领取后，再用旧令牌续租会得到 `409 lease_expired` /
`409 invalid_lease`。

### 5. 回报结果

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

显式失败但仍有重试额度时，响应携带失败计数、剩余额度与下次可领时刻；任务配置了
`retry_backoff` 时 `next_eligible_at` 为退避到期时刻（此前领取不会选中它），
未配置时为 `null`（立即重试）：

```json
{
  "task_id": "…", "state": "pending", "attempt": 1,
  "failure_count": 1, "retries_remaining": 2, "blocked_descendants": 0,
  "next_eligible_at": "2026-09-18T11:00:02Z"
}
```

任务查询返回持久化的退避策略与当前可领时刻：

```bash
curl -s http://localhost:8000/tasks/<TASK_ID> | jq '{state, retry_backoff, next_eligible_at}'
# {"state": "pending",
#  "retry_backoff": {"initial_seconds": 2.0, "multiplier": 3.0, "max_seconds": 60.0},
#  "next_eligible_at": "2026-09-18T11:00:02Z"}
```

> 退避只作用于**显式失败**：租约过期后的重新领取立即生效，不等待 `next_eligible_at`；
> 任务被领取后该字段清空。最终失败或成功时 `next_eligible_at` 同样为 `null`。

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

### 6. 恢复最终失败的任务

重试耗尽进入 `failed` 的任务可以恢复回 `pending` 重新参与调度；被它连累阻塞的
后继会在**同一事务**内按依赖图递归检查，全部前置均已 `succeeded` 或 `pending`
（含被恢复任务本身与本次连锁解锁的后继）的后继一并解锁为 `pending`：

```bash
curl -s -X POST http://localhost:8000/tasks/<TASK_ID>/resume
```

```json
{ "task_id": "…", "state": "pending", "attempt": 3, "unblocked_descendants": 2 }
```

- 仅 `failed` 状态可恢复：`pending`/`running`/`succeeded`/`blocked` 一律返回
  `409 task_not_failed`；任务不存在返回 `404 task_not_found`。
- 恢复在单事务内清除租约与 `finished_at`，任务立即可被重新领取（不触发退避）；
  `attempt`、`failure_count` 与重试预算**不重置**——再次显式失败会立即回到
  `failed` 并重新阻塞后继。
- 仍有 `running`/`failed`/`blocked` 前置的后继**保持 blocked 与原阻塞原因**；
  恢复其余失败上游时可再次触发检查并将其解锁。
- 重复恢复同一任务（已回到 `pending`）返回 409，且不新增任何事件。
- 恢复与解锁的每个状态变更都会在同一事务内追加 `task.resumed` 事件
  （根任务 `failed → pending`，被解锁后继 `blocked → pending`）。

### 7. 取消任务

取消 `pending`/`running`/`blocked` 任务；正在运行的任务会被立即终止，所有尚未
结束的后继在同一事务内连锁取消：

```bash
curl -s -X POST http://localhost:8000/tasks/<TASK_ID>/cancel
```

```json
{ "task_id": "…", "state": "canceled", "attempt": 1, "canceled_descendants": 2 }
```

- 仅 `pending`/`running`/`blocked` 可取消：`succeeded`/`failed`/`canceled`
  一律返回 `409 task_not_cancelable`；任务不存在返回 `404 task_not_found`。
- 运行中任务的当前 attempt 立即终止、租约清除：旧 `lease_token` 的心跳与完成
  回报均返回 **409**；`canceled` 是终态，永不再被领取，占用的并发组名额立即释放。
- 递归后继中已 `succeeded`/`failed`/`canceled` 的保持不变、不产生事件；
  其余（含 `blocked`、`running`）连锁取消。
- 重复取消返回 409，且不新增任何事件。
- 每个实际状态变更在同一事务内追加 `task.canceled` 事件
  （如 `running → canceled`、`blocked → canceled`）。

### 8. 状态查询

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

退避等待中的任务示例：`{"key": "transform", "state": "pending",
"retry_backoff": {"initial_seconds": 2.0, "multiplier": 3.0, "max_seconds": 60.0},
"next_eligible_at": "2026-09-18T11:00:02Z", "lease": null}`；
未配置退避时 `retry_backoff` 与 `next_eligible_at` 均为 `null`。

### 9. 事件流

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
  "next_eligible_at": null,
  "occurred_at": "2026-09-17T11:00:00Z"
}
```

- `type` ∈ `dag.created` / `task.claimed` / `task.requeued` /
  `task.succeeded` / `task.failed` / `task.blocked` / `task.resumed` /
  `task.canceled`；
- `sequence` 为全局严格递增游标，事件按其升序返回；`dag.created` 事件的
  `task_id`、`attempt`、`from_state`、`to_state` 均为 `null`；
- `next_eligible_at` 仅 `task.requeued` 携带：配置了 `retry_backoff` 时为退避后的
  下次可领时刻，未配置时为 `null`；其他事件类型该字段始终为 `null`；
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
     "next_eligible_at": null,
     "occurred_at": "2026-09-17T10:59:59Z"},
    {"sequence": 2, "type": "task.claimed", "dag_id": "d1", "task_id": "…",
     "attempt": 1, "from_state": "pending", "to_state": "running",
     "next_eligible_at": null,
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

### 10. 可靠事件订阅（投递 / 确认）

`GET /events` 适合临时翻页；需要「至少一次、不跳过、可重放」的持续消费时使用订阅。
订阅在创建时固化起始游标 `committed_sequence`（默认 `0`），可选 `dag_id` 过滤：

```bash
# 从头订阅全库事件流
curl -s -X POST http://localhost:8000/subscriptions \
  -H 'Content-Type: application/json' -d '{}'

# 只订阅某个 DAG，且从 sequence=10 之后开始
curl -s -X POST http://localhost:8000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"dag_id": "<DAG_ID>", "committed_sequence": 10}'
```

```json
{"id": "9f2a…", "dag_id": null, "committed_sequence": 0,
 "created_at": "2026-09-19T10:00:00Z", "in_flight": null}
```

`poll` 按 `sequence` 升序投出游标之后的下一批（`limit` 1～100，`lease_seconds`
必须为正数），并生成随机 `delivery_token` 与租约：

```bash
SID=9f2a…
curl -s -X POST http://localhost:8000/subscriptions/$SID/poll \
  -H 'Content-Type: application/json' \
  -d '{"limit": 50, "lease_seconds": 30}'
```

```json
{
  "subscription_id": "9f2a…",
  "delivery_token": "4b7e…",
  "lease_expires_at": "2026-09-19T10:00:30Z",
  "events": [ {"sequence": 1, "type": "dag.created", "…": "…"} ],
  "last_sequence": 1,
  "committed_sequence": 0
}
```

- **租约内重复 poll 原样回放**：在 `lease_expires_at` 之前再次 `poll`
  （即使传入不同的 `limit`/`lease_seconds`）返回与首次投递**逐字节相同**的响应——
  同一个 `delivery_token`、同一批事件，不会发出第二批。
- **租约过期后重新投递**：过期后 `poll` 删除旧批次并以**新随机令牌**重发同一窗口
  （仍从已确认游标之后开始）。旧令牌此后 `ack` 返回 `409 invalid_delivery_token`。
- **确认只推进当前批次**：

```bash
curl -s -X POST http://localhost:8000/subscriptions/$SID/ack \
  -H 'Content-Type: application/json' -d '{"delivery_token": "4b7e…"}'
# {"subscription_id":"9f2a…","committed_sequence":1}
```

  只有当前在途批次的令牌能确认，游标恰好推进到该批最后一条 `sequence`；
  错误/旧令牌返回 `409 invalid_delivery_token`，没有在途批次（已确认、从未 poll、
  过期后尚未重投）返回 `409 no_pending_batch`，且两者都不推进游标——未确认的事件
  永远不会被越过。
- 追平事件流后 `poll` 返回空批次：`events: []`、`delivery_token: null`，
  不占用在途槽位；之后有新事件时可直接再次 `poll`。
- 订阅与在途批次持久化在 `task-data` 卷中：服务/容器重启后，未过期批次仍原样回放、
  其令牌仍可确认，已确认游标不重置。

列表与详情（详情包含在途批次的令牌、租约过期时间、`expired`、批次范围）：

```bash
curl -s http://localhost:8000/subscriptions        # 全部订阅
curl -s 'http://localhost:8000/subscriptions?dag_id=<DAG_ID>'
curl -s http://localhost:8000/subscriptions/$SID
```

### 11. 只读分析（关键路径）

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

### 12. 可复用 DAG 模板（版本发布与参数化实例化）

创建模板（立即发布不可变版本 1），随后按名称发布新版本；每个版本保存发布时的
任务图快照，实例化时用参数替换任务 payload 中的 `${name}` 占位符：

```bash
# 创建模板并发布版本 1（payload 值中的 ${name} 即占位符；图非法同样 422）
curl -s -X POST http://localhost:8000/templates \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "nightly-etl",
    "tasks": [
      {"key": "extract", "payload": {"source": "${source}", "rows": "${rows}",
                                     "path": "s3://${bucket}/data"}},
      {"key": "load", "deps": ["extract"], "requires": ["io"], "max_retries": 2}
    ]
  }' | jq
# {
#   "template_name": "nightly-etl", "version": 1, "task_count": 2,
#   "parameters": ["bucket", "rows", "source"],
#   "created_at": "2026-09-19T10:00:00Z",
#   "snapshot": [ …发布时规范化的任务定义，之后不可变… ]
# }

# 按名称发布新版本（版本号自动递增；旧版本快照不受影响）
curl -s -X POST http://localhost:8000/templates/nightly-etl/versions \
  -H 'Content-Type: application/json' \
  -d '{"tasks": [
        {"key": "extract", "payload": {"source": "${source}"}},
        {"key": "verify", "deps": ["extract"]},
        {"key": "load", "deps": ["verify"], "requires": ["io"]}
      ]}' | jq
# -> 201 {"version": 2, "parameters": ["source"], …}

# 查询：模板列表 / 模板详情（全部版本元数据）/ 单个版本（含不可变快照）
curl -s http://localhost:8000/templates | jq
curl -s http://localhost:8000/templates/nightly-etl | jq
curl -s http://localhost:8000/templates/nightly-etl/versions/1 | jq

# 实例化版本 1：必须携带 idempotency_key，参数与占位符集合一一对应
curl -s -X POST http://localhost:8000/templates/nightly-etl/versions/1/instantiations \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key": "2026-09-19-nightly",
       "parameters": {"source": "orders-db", "rows": 5000, "bucket": "etl-prod"}}' | jq
# -> 201，响应体与 GET /dags/{id} 完全一致；extract 的 payload 替换为：
#    {"source": "orders-db", "rows": 5000, "path": "s3://etl-prod/data"}
# 整体占位符保留实参类型（rows 仍是数字）；嵌入占位符按文本替换。

# 同模板版本 + 同键 + 等价参数重复调用 -> 200，返回首次创建的同一 DAG（同一 id）
curl -s -X POST http://localhost:8000/templates/nightly-etl/versions/1/instantiations \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key": "2026-09-19-nightly",
       "parameters": {"rows": 5000, "bucket": "etl-prod", "source": "orders-db"}}'
# -> 200（JSON 键序不影响参数等价判定）

# 缺参 / 多参 -> 422 且不落库（无 DAG、无事件、无幂等记录）
#   {"error":{"code":"missing_template_parameter",
#             "details":{"missing":["bucket"], "required":[…]}}}
#   {"error":{"code":"unexpected_template_parameter",
#             "details":{"unexpected":["extra"], "required":[…]}}}
# 同键但参数不同 -> 409
#   {"error":{"code":"idempotency_conflict",
#             "details":{"template_name":"nightly-etl","version":1,
#                        "idempotency_key":"2026-09-19-nightly"}}}
# 模板 / 版本不存在 -> 404 template_not_found / template_version_not_found
```

要点：

- **不可变快照**：每个版本保存发布时规范化的完整任务定义（含 `deps`、`requires`、
  重试/退避/并发组等全部字段），之后的发布、实例化都不会修改它；实例化发生在
  快照的副本上。`parameters` 为发布时从任务 payload **值**中提取并排序去重的
  占位符名（`${合法标识符}`；对象键与 `${}`、`${1a}` 等非法形态不视为占位符）。
- **单事务原子性**：参数校验 → 占位符替换 → DAG 图校验 → DAG/任务/事件落库 →
  幂等记录写入，全部在同一个 `BEGIN IMMEDIATE` 事务内完成；任一步失败整体回滚，
  绝不留下半个 DAG 或抢先写入的幂等记录。跨 DAG 并发组上限冲突同样整批回滚。
- **幂等作用域**：幂等键的唯一作用域是 `(模板名, 版本号, idempotency_key)`；
  不同版本或不同模板可复用同一个键。实例化支持可选 `name`（实例 DAG 名称，
  缺省用模板名）与 `id`（自定义 DAG id，已存在则 `409 dag_exists` 回滚，
  不占幂等键）。

状态变迁与事件 `type` 的对应关系：

| 业务动作 | 事件 type | from_state → to_state |
| --- | --- | --- |
| DAG 创建 | `dag.created` | 可空字段均为 `null` |
| 首次领取 / 过期重领 | `task.claimed` | `pending`（或 `running`）→ `running` |
| 显式失败、仍有重试额度 | `task.requeued` | `running` → `pending`；携带 `next_eligible_at`（无退避为 `null`） |
| 回报成功 | `task.succeeded` | `running` → `succeeded` |
| 显式失败、重试耗尽 | `task.failed` | `running` → `failed` |
| 递归阻塞后继 | `task.blocked` | 原状态（`pending`/`running`）→ `blocked` |
| 恢复最终失败任务 | `task.resumed` | `failed` → `pending` |
| 恢复时连锁解锁后继 | `task.resumed` | `blocked` → `pending` |
| 取消任务 | `task.canceled` | `pending`/`running`/`blocked` → `canceled`（含后继连锁取消） |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/dags` | 提交 DAG（校验缺失依赖与环） |
| `GET` | `/dags/{dag_id}` | DAG 概览：状态计数与任务列表 |
| `GET` | `/dags/{dag_id}/analysis` | 只读分析：最早/最晚时间、总工期、关键路径与并行层级 |
| `POST` | `/templates` | 创建 DAG 模板并发布不可变版本 1（重名 409；图非法 422） |
| `GET` | `/templates` | 模板列表（名称、时间戳、版本数与最新版本） |
| `GET` | `/templates/{name}` | 模板详情：全部已发布版本的元数据（不存在 404） |
| `POST` | `/templates/{name}/versions` | 按名称发布新版本：版本号递增并保存不可变任务图快照（模板不存在 404；图非法 422） |
| `GET` | `/templates/{name}/versions/{version}` | 查询指定版本：元数据 + 不可变快照（版本不存在 404） |
| `POST` | `/templates/{name}/versions/{version}/instantiations` | 参数化实例化（替换 payload 中的 `${name}`）；必须携带 idempotency_key，同键同参返回同一 DAG、参数不同 409、缺参/多参 422 |
| `POST` | `/tasks/claim` | 原子领取前置全部成功的任务（同事务校验能力登记；跳过满并发组与能力不匹配候选，尽量填满 limit；登记缺失/过期 409） |
| `POST` | `/tasks/claim-plan` | 只读预演领取决策：参数与 `/tasks/claim` 一致，单一只读事务模拟同批即时占位，返回模拟选中序列与未选中 pending 任务的首个阻塞原因；不写任务/attempt/事件（dag 不存在 404，登记缺失/过期 409） |
| `PUT` | `/workers/{worker_id}` | 登记/刷新工作器能力：去重能力标签 + 正数 ttl_seconds；重复 PUT 覆盖能力并刷新过期时刻 |
| `GET` | `/workers/{worker_id}` | 查询登记内容与 alive 存活状态；从未登记 404 |
| `GET` | `/tasks?dag_id=&state=` | 任务列表（可过滤） |
| `GET` | `/events?after=&limit=&dag_id=` | 不可变事件流：按 sequence 升序键集分页 |
| `POST` | `/subscriptions` | 创建可靠事件订阅（保存 committed_sequence，可选 dag_id 过滤） |
| `GET` | `/subscriptions?dag_id=` | 订阅列表（可按 dag_id 过滤） |
| `GET` | `/subscriptions/{subscription_id}` | 订阅详情：游标与在途批次（含 expired 状态） |
| `POST` | `/subscriptions/{subscription_id}/poll` | 投递下一批事件与随机 delivery_token；租约内重复 poll 原样回放，过期后重新投递 |
| `POST` | `/subscriptions/{subscription_id}/ack` | 凭当前 delivery_token 确认批次并推进游标；旧/错令牌 409 |
| `GET` | `/tasks/{task_id}` | 任务详情：尝试历史、租约、阻塞原因 |
| `POST` | `/tasks/{task_id}/heartbeat` | 凭当前租约令牌续租 |
| `POST` | `/tasks/{task_id}/complete` | 凭租约令牌 + `completion_id` 幂等回报成功/失败 |
| `POST` | `/tasks/{task_id}/resume` | 恢复最终失败的任务为 `pending`，并连锁解锁前置已满足的被阻塞后继 |
| `POST` | `/tasks/{task_id}/cancel` | 取消 pending/running/blocked 任务，终止运行中 attempt，并连锁取消未结束后继 |
| `GET` | `/health` | 健康检查 |

## 任务状态机

```
pending ──claim──▶ running ──complete(success)──────────▶ succeeded
  │ ▲                │  │
  │ │ fail (可重试;  │  └─complete(fail, 重试耗尽)─▶ failed ──▶ blocked(所有后继)
  │ │ retry_backoff  │                                  │            │
  │ │ 到期才可重领)  │                                  └─ resume ───┴─ 前置全部 succeeded
  │ └────────────────┘                                   (仅最终失败任务;  或 pending 时连锁
  │                      └─租约过期─▶ 可被再次 claim          清除租约/完成时间) 解锁 ─▶ pending
  │                        （attempt+1，新令牌；旧令牌全部 409；不触发退避）
  │
  └──────────────── cancel ──▶ canceled（终态；running 立即终止 attempt/清除租约，
                                  旧令牌心跳与回报 409；递归连锁取消所有未结束后继）
  running / blocked 同样可 cancel；succeeded/failed/canceled 取消返回 409
```

## 开发与测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

测试覆盖：DAG 校验（缺失依赖/环/重复键）、依赖门控、并发原子领取、
工作器能力登记与能力路由（PUT 去重标签/正数 TTL 校验、GET 查询与 404、
重复 PUT 覆盖能力并刷新过期时刻且保留首次 registered_at、TTL 过期 alive=false、
任务 requires 去重并在 DAG/列表/详情/领取结果返回、登记缺失/过期（含匿名）
领取 requires 候选返回 409 且整批不领取也不产生事件、存活但能力不匹配跳过并继续
填满 limit、超集匹配、标签大小写/空格敏感、能力门控与依赖/退避/并发组的优先级
（依赖未满足/退避未到期/组满时不触发 409、组满跳过先于能力门控）、无 requires
历史任务对匿名/过期登记 worker 完全兼容、租约过期重领同样校验登记、登记与 requires
重启后持久化、旧库迁移补 requires NULL 且 workers 表可用、OpenAPI 文档包含新接口）、
跨 DAG 并发组（字段必须成对且为正整数、组容量占用与同批即时占位、满组跳过继续填满
limit、跨 DAG 共享容量、同名组上限持久化一致与冲突整批回滚、成功/失败/过期释放名额、
心跳续租保持占用、重启后上限与限流仍生效、无组任务不受影响、旧库迁移补 NULL）、
attempt 递增与令牌唯一、过期后旧令牌 409、心跳续租、重试与耗尽后递归阻塞、
指数退避（参数校验、延迟按 `initial*multiplier^(n-1)` 增长与 `max_seconds` 封顶、
未到期不可领取、领取清空 `next_eligible_at`、租约过期重领不触发退避、无退避立即重试、
策略与下次可领时刻在任务视图/回报/事件中返回、重启持久化、旧库迁移补 NULL）、
幂等回报（重放同响应、内容冲突 409、键序等价、重启后持久化）、旧 SQLite 数据迁移、
状态接口、持久化事件流（各事件类型与状态对、同事务回滚不留事件、重放不产生事件、
键集分页无漏项/重复、dag_id 过滤、参数校验、重启后不重置、存储层不可变）、
只读分析（最早/最晚时间与总时差、总工期、关键路径与关键任务集合、并行层级、
默认与小数时长、并列关键路径的确定性、只读不产生事件、404/422 错误路径、
旧库迁移后默认时长）、失败恢复（仅 `failed` 可恢复、非失败状态与未知任务
409/404、清除租约与完成时间、不重置失败计数与重试预算、`task.resumed` 事件、
后继按前置条件连锁解锁或保留 `blocked` 与原阻塞原因、重复恢复 409 且不新增事件、
恢复后可重新领取并再次失败、OpenAPI 文档包含恢复接口）、
任务取消（pending/running/blocked 三类目标取消、running 立即终止 attempt 并使旧
令牌心跳/回报 409、递归连锁取消未结束后继且终态后继不变、每个变化仅一条
`task.canceled` 事件、重复取消与终态取消 409 不产生事件、状态计数与列表过滤
支持 canceled、退避/阻塞原因与租约字段清除、并发组名额释放、旧库迁移后可用、
OpenAPI 文档包含取消接口）、
可靠事件订阅（创建并保存 committed_sequence、可选 dag_id 过滤、poll 按 sequence
升序返回下一批事件与随机 delivery_token、limit 1～100 与正数 lease_seconds 校验、
租约内重复 poll 逐字节回放同一批次、租约过期后以新令牌重投同一窗口且旧令牌 ack
返回 409、ack 仅认当前令牌并把游标推进到该批最后一条 sequence、无在途批次/错误
令牌返回 409 且游标不动、空批次无令牌不占槽位、不可能越过未确认事件、订阅列表与
详情含在途 expired 状态、订阅/在途批次/游标重启后持久化且语义不变、与既有
GET /events 及任务接口并存且行为不变、OpenAPI 文档包含订阅接口）、
可复用 DAG 模板（创建即发布版本 1、重名 409、创建/发布复用 DAG 图校验错误码
且失败不写库、按名称发布版本号递增、旧版本快照不可变、模板/版本列表与详情
查询、404 与非整数版本号路径、占位符从 payload 值提取且不替换对象键、非法
占位符形态按字面保留、整体占位符保留实参类型与嵌入文本替换、嵌套 payload
替换、缺参 422 与多参 422 均不落 DAG/事件/幂等记录、无占位符版本只接受空参数、
实例化 DAG 与直接提交同结构且依赖/requires/并发组生效、幂等键必填、同键等价
参数（含键序差异）重放返回同一 DAG 且只产生一次 dag.created、同键参数不同
409、幂等作用域按模板与版本隔离、缺参重放不返回首响、自定义 name/id 与已存在
id 409 回滚不占键、跨 DAG 并发组冲突整体回滚、并发同键异参只创建一个 DAG、
并发同键同参 200/201 同一 DAG、模板/快照/幂等记录重启后持久化且语义不变、
直接 POST /dags 行为与响应结构不变、OpenAPI 文档包含模板接口）、
只读领取决策预演（模拟选中序列与真实领取在同批并发组即时占位/跳过/依赖/退避/
能力门控上一致、已有未过期租约与跨 DAG 组占用计入、未选中 pending 任务的首个
阻塞原因按依赖→退避→组满→能力→限额顺序给出及相关字段、依赖前置按模拟后
running 状态报告、满组先于能力门控不触发 409、登记缺失在批量填满前 409 且不
落事件、限额填满后遇到 requires 候选不触发 409 而记 limit_reached、过期租约
可模拟重领且不计入 unselected、预演不写任务/attempt/事件且不签发租约令牌、
重复调用确定性、参数校验与 404 同 claim 口径、OpenAPI 文档包含新接口且
/tasks/claim 行为不变）。
