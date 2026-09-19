"""核心业务逻辑：提交校验、原子领取、租约心跳、结果回报、失败恢复、状态查询与只读分析。"""

from __future__ import annotations

import heapq
import json
import math
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .db import get_conn, get_default_duration_seconds, get_default_lease_seconds

# 任务状态
PENDING = "pending"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
BLOCKED = "blocked"
CANCELED = "canceled"

# 终态：不再随上游失败/取消而改变；取消递归时一律跳过
TERMINAL_STATES = (SUCCEEDED, FAILED, CANCELED)
# 允许取消的状态
CANCELABLE_STATES = (PENDING, RUNNING, BLOCKED)

# 尝试状态
A_ACTIVE = "active"
A_EXPIRED = "expired"
A_SUCCEEDED = "succeeded"
A_FAILED = "failed"

# 不可变事件类型
EVT_DAG_CREATED = "dag.created"
EVT_TASK_CLAIMED = "task.claimed"
EVT_TASK_REQUEUED = "task.requeued"
EVT_TASK_SUCCEEDED = "task.succeeded"
EVT_TASK_FAILED = "task.failed"
EVT_TASK_BLOCKED = "task.blocked"
EVT_TASK_RESUMED = "task.resumed"
EVT_TASK_CANCELED = "task.canceled"


def _append_event(
    conn,
    event_type: str,
    dag_id: str,
    occurred_at: str,
    task_id: str | None = None,
    attempt: int | None = None,
    from_state: str | None = None,
    to_state: str | None = None,
    next_eligible_at: str | None = None,
) -> None:
    """在调用方的当前事务内追加一条不可变事件。

    不自行提交：事件必须与其记录的业务变更同生共死，事务回滚则不留事件。
    dag.created 不携带任务信息，可空字段一律为 NULL。
    next_eligible_at 仅 task.requeued 携带（退避后的下次可领时刻）。
    """
    conn.execute(
        """INSERT INTO events (event_type, dag_id, task_id, attempt,
               from_state, to_state, next_eligible_at, occurred_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (event_type, dag_id, task_id, attempt, from_state, to_state,
         next_eligible_at, occurred_at),
    )


class APIError(Exception):
    def __init__(self, status_code: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


class ReplayedResponse:
    """幂等命中：携带首次有效回报保存的 HTTP 状态码与原始响应体。

    路由层必须将其原样回放（同样的状态码、逐字节的响应体），不得重新序列化。
    """

    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat().replace("+00:00", "Z") if dt is not None else None


def _parse(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _dumps(value: Any) -> str | None:
    return None if value is None else json.dumps(value, ensure_ascii=False)


def _canonical(value: Any) -> str:
    """规范化 JSON：递归排序对象键，使键序不同但内容相同的请求视为等价。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _backoff_delay_seconds(row, failure_count: int) -> float | None:
    """按持久化策略计算本次显式失败后的重试延迟（秒）；未配置退避返回 None。

    delay = min(max_seconds, initial_seconds * multiplier^(failure_count-1))。
    failure_count 为本次失败后的计数（首次失败为 1，幂次从 0 起）。
    """
    initial = row["backoff_initial_seconds"]
    if initial is None:
        return None
    multiplier = row["backoff_multiplier"]
    maximum = row["backoff_max_seconds"]
    try:
        raw = initial * (multiplier ** (failure_count - 1))
    except OverflowError:
        raw = float("inf")
    # 非有限（极端倍数溢出）或超过上限时，统一收敛到持久化上限
    return maximum if not math.isfinite(raw) or raw > maximum else raw


# ---------------------------------------------------------------- DAG 提交

def _find_cycle(keys: set[str], deps: dict[str, list[str]]) -> list[str]:
    """DFS 检测环，返回构成环的任务键序列（首尾相同），无环返回 []。"""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {k: WHITE for k in keys}
    stack: list[str] = []

    def visit(node: str) -> list[str]:
        color[node] = GRAY
        stack.append(node)
        for dep in deps[node]:
            if dep not in keys:  # 缺失依赖已单独处理
                continue
            if color[dep] == GRAY:
                start = stack.index(dep)
                return stack[start:] + [dep]
            if color[dep] == WHITE:
                found = visit(dep)
                if found:
                    return found
        stack.pop()
        color[node] = BLACK
        return []

    for key in keys:
        if color[key] == WHITE:
            found = visit(key)
            if found:
                return found
    return []


def _prepare_dag_tasks(raw_tasks: list[dict[str, Any]]) -> tuple[
    list[str],
    dict[str, list[str]],
    dict[str, dict[str, Any]],
    dict[str, int],
]:
    """复用的 DAG 提交前校验：重复键 / 缺失依赖 / 环 / 同批并发组矛盾。

    纯内存、不触库：直接 POST /dags 与模板版本发布共用同一套图校验口径。
    返回 (有序 key 列表, 依赖映射, key->任务定义, 并发组名->上限)。
    校验失败抛 422 APIError（与 submit_dag 原行为一致）。
    """
    keys: list[str] = []
    seen: set[str] = set()
    duplicates: list[str] = []
    deps_map: dict[str, list[str]] = {}
    tasks_by_key: dict[str, dict[str, Any]] = {}

    for t in raw_tasks:
        key = t["key"]
        if key in seen:
            duplicates.append(key)
            continue
        seen.add(key)
        keys.append(key)
        deps_map[key] = list(dict.fromkeys(t.get("deps") or []))
        # 能力标签与依赖同口径去重（按首次出现顺序）；空声明视为不要求能力（NULL）
        t["requires"] = list(dict.fromkeys(t.get("requires") or [])) or None
        tasks_by_key[key] = t

    if duplicates:
        raise APIError(422, "duplicate_task_key", "任务键在同一 DAG 中重复", {"keys": duplicates})

    # 缺失依赖（包含指向不存在任务的边）
    missing = sorted({d for d_list in deps_map.values() for d in d_list if d not in seen})
    if missing:
        raise APIError(
            422, "missing_dependency", "存在指向未声明任务的依赖边", {"missing": missing}
        )

    # 环检测
    cycle = _find_cycle(seen, deps_map)
    if cycle:
        raise APIError(422, "cycle_detected", "依赖图存在环，必须是 DAG", {"cycle": cycle})

    # 本次提交声明的并发组：同名组在同一批任务中必须声明相同上限，
    # 否则整批拒绝（请求自身矛盾，422）。
    declared: dict[str, int] = {}
    for key in keys:
        group = tasks_by_key[key].get("concurrency_group")
        if group is None:
            continue
        limit = int(tasks_by_key[key]["concurrency_limit"])
        if group in declared and declared[group] != limit:
            raise APIError(
                422,
                "concurrency_group_conflict",
                f"并发组 {group} 在同一提交中声明了不同上限",
                {
                    "group": group,
                    "existing_limit": declared[group],
                    "submitted_limit": limit,
                },
            )
        declared[group] = limit

    return keys, deps_map, tasks_by_key, declared


def _persist_dag(
    conn,
    dag_id: str,
    name: str,
    keys: list[str],
    deps_map: dict[str, list[str]],
    tasks_by_key: dict[str, dict[str, Any]],
    declared: dict[str, int],
    now: str,
) -> None:
    """在调用方已开启的写事务内插入 DAG、并发组、dag.created 事件、任务与依赖边。

    不自行提交/回滚：直接提交与模板实例化共用同一落库路径，后者必须把幂等记录
    与这些行放在同一个事务内。dags 主键冲突就近转成 409 dag_exists APIError；
    并发组持久化上限冲突抛 409 APIError；其余异常原样抛出——调用方统一回滚。
    """
    # DAG 主键冲突就近转 409（调用方负责回滚整个事务）；其余 IntegrityError
    # （如并发组竞争）不在此吞，以保留原始错误语义
    try:
        conn.execute(
            "INSERT INTO dags (id, name, created_at) VALUES (?, ?, ?)",
            (dag_id, name, now),
        )
    except sqlite3.IntegrityError:
        raise APIError(409, "dag_exists", f"DAG id {dag_id} 已存在")

    # 跨 DAG 一致性：同名组的上限首次声明即持久化，之后必须一致。
    # 冲突在插入任何任务行之前检出，整体回滚，不留 DAG/任务/事件痕迹。
    for group, limit in declared.items():
        existing = conn.execute(
            "SELECT limit_value FROM concurrency_groups WHERE name = ?",
            (group,),
        ).fetchone()
        if existing is not None and existing["limit_value"] != limit:
            raise APIError(
                409,
                "concurrency_group_conflict",
                f"并发组 {group} 已持久化上限 {existing['limit_value']}，与本次声明 {limit} 冲突",
                {
                    "group": group,
                    "existing_limit": existing["limit_value"],
                    "submitted_limit": limit,
                },
            )
        if existing is None:
            conn.execute(
                "INSERT INTO concurrency_groups (name, limit_value, created_at) VALUES (?, ?, ?)",
                (group, limit, now),
            )

    default_lease = get_default_lease_seconds()
    default_duration = get_default_duration_seconds()

    # dag.created 与 DAG/任务行同事务：创建失败（回滚）不留事件；
    # 可空字段 task_id/attempt/from_state/to_state 一律为 NULL
    _append_event(conn, EVT_DAG_CREATED, dag_id, now)
    for key in keys:
        t = tasks_by_key[key]
        lease = float(t.get("lease_seconds") or default_lease)
        duration = float(t.get("duration_seconds") or default_duration)
        backoff = t.get("retry_backoff")
        conn.execute(
            """INSERT INTO tasks (id, dag_id, task_key, name, payload,
                   max_retries, lease_seconds, duration_seconds, state, attempt,
                   created_at, concurrency_group, requires,
                   backoff_initial_seconds, backoff_multiplier, backoff_max_seconds)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)""",
            (
                uuid.uuid4().hex,
                dag_id,
                key,
                t.get("name") or key,
                _dumps(t.get("payload")),
                int(t.get("max_retries") or 0),
                lease,
                duration,
                PENDING,
                now,
                t.get("concurrency_group"),
                _dumps(t.get("requires")),
                float(backoff["initial_seconds"]) if backoff else None,
                float(backoff["multiplier"]) if backoff else None,
                float(backoff["max_seconds"]) if backoff else None,
            ),
        )
        for dep in deps_map[key]:
            conn.execute(
                "INSERT INTO dependencies (dag_id, task_key, dep_key) VALUES (?, ?, ?)",
                (dag_id, key, dep),
            )


def submit_dag(body: dict[str, Any]) -> dict[str, Any]:
    raw_tasks = body["tasks"]
    name = body.get("name") or "dag"
    dag_id = body.get("id") or uuid.uuid4().hex

    keys, deps_map, tasks_by_key, declared = _prepare_dag_tasks(raw_tasks)
    now = _iso(_now())

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            _persist_dag(conn, dag_id, name, keys, deps_map, tasks_by_key, declared, now)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return get_dag(dag_id)


# ---------------------------------------------------------------- 原子领取

# 可领取候选行（依赖门控），按 rowid 稳定排序（同事务插入的任务 created_at
# 可能相同，rowid 严格递增且全局唯一，适合作为键集翻页游标）。领取端按此顺序
# 翻页：遇到并发组已满的候选时跳过，继续向后寻找其他任务以尽量填满 limit。
_CLAIMABLE_PAGE_SQL = """
    SELECT t.rowid AS rowid, t.*
    FROM tasks t
    WHERE (:dag_id IS NULL OR t.dag_id = :dag_id)
      AND (
            -- pending（从未领取或失败后回队）：退避时刻必须已到。
            -- next_eligible_at 为 NULL 表示立即可领（未配置退避的任务与
            -- 租约过期重领前的任务均属此类）；显式失败设置的退避未到期一律跳过。
            (t.state = 'pending'
                AND (t.next_eligible_at IS NULL OR t.next_eligible_at <= :now))
            -- 租约已过期，可被（重新）领取，产生新的尝试（不触发退避）
            OR (t.state = 'running' AND t.lease_expires_at IS NOT NULL
                AND t.lease_expires_at < :now)
          )
      AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks p ON p.dag_id = d.dag_id AND p.task_key = d.dep_key
            WHERE d.dag_id = t.dag_id AND d.task_key = t.task_key
              AND p.state <> 'succeeded'
          )
      AND (:after_rowid IS NULL OR t.rowid > :after_rowid)
    ORDER BY t.rowid
    LIMIT :page_size
"""

# 每轮键集翻页的候选数；单次领取的 limit 上限是 100，500 足以覆盖
# 大量候选因并发组占满被跳过的情形
_CLAIM_PAGE_SIZE = 500


def _load_worker_caps(conn, worker_id: str | None, now_iso: str) -> set[str] | None:
    """在调用方当前事务内核验领取者的能力登记，返回其能力标签集合。

    与依赖、退避、并发组规则一样取同一事务内的一致状态：worker 为 None（匿名）、
    登记缺失或已过期（expires_at < now）一律返回 None，表示无存活能力登记，
    真实领取/预演遇到其余门控均满足的 requires 候选时即裁决为 409。
    """
    if worker_id is None:
        return None
    worker_row = conn.execute(
        "SELECT * FROM workers WHERE worker_id = ?", (worker_id,)
    ).fetchone()
    if worker_row is not None and worker_row["expires_at"] >= now_iso:
        return set(json.loads(worker_row["capabilities"]))
    return None


def _load_group_snapshot(conn, now_iso: str) -> tuple[dict[str, int], dict[str, int]]:
    """同一事务内取并发组的 (持久化上限映射, 当前占用映射)。

    占用按全库（跨 DAG，不按 dag_id 收敛）未过期的 running 租约计数；
    过期 running 租约不算占用——它们已可被（重新）领取，名额视为已释放。
    真实领取与只读预演共用同一份快照口径。
    """
    limits: dict[str, int] = {
        r["name"]: r["limit_value"]
        for r in conn.execute("SELECT name, limit_value FROM concurrency_groups").fetchall()
    }
    occupied: dict[str, int] = {
        r["concurrency_group"]: r["n"]
        for r in conn.execute(
            """SELECT concurrency_group, COUNT(*) AS n
               FROM tasks
               WHERE state = 'running'
                 AND lease_token IS NOT NULL
                 AND lease_expires_at IS NOT NULL
                 AND lease_expires_at >= ?
                 AND concurrency_group IS NOT NULL
               GROUP BY concurrency_group""",
            (now_iso,),
        ).fetchall()
    }
    return limits, occupied


def _scan_claimable_candidates(
    conn,
    *,
    dag_id: str | None,
    now: datetime,
    now_iso: str,
    limit: int,
    worker_id: str | None,
    worker_caps: set[str] | None,
    limits: dict[str, int],
    occupied: dict[str, int],
    on_select,
    collect_skips: bool = False,
    on_skip=None,
) -> int:
    """真实领取与预演共用的候选扫描裁决：候选排序、依赖/退避（SQL）、并发组
    容量、能力门控与同批即时占位全部在此，保证两条路径的裁决口径逐候选一致。

    候选按 _CLAIMABLE_PAGE_SQL 的 rowid 顺序键集翻页。对每个候选：
      - 并发组已满（本批新选中的任务已即时计入 occupied）→ 跳过；
      - requires 非空且无存活登记 → 409（与真实领取同语义，整批无结果）；
      - requires 非空但能力不覆盖 → 跳过；
      - 其余门控通过：批量已满（仅 collect_skips 时可能）记为限额阻塞，
        否则调用 on_select（返回 False 表示该候选未落位，不计占用不计数）。

    collect_skips=False（真实领取）：选中 limit 个即停止翻页，与历史行为一致。
    collect_skips=True（预演）：继续扫描全部候选，通过 on_skip 记录每个未选中
    候选的首个阻塞原因（concurrency_group_full / capability_mismatch /
    limit_reached），依赖与退避阻塞的 pending 任务不在候选集内，由调用方另行判定。
    返回已选中（落位）的候选数。
    """
    after_rowid: int | None = None
    selected = 0
    while collect_skips or selected < limit:
        rows = conn.execute(
            _CLAIMABLE_PAGE_SQL,
            {
                "dag_id": dag_id,
                "now": now_iso,
                "after_rowid": after_rowid,
                "page_size": _CLAIM_PAGE_SIZE,
            },
        ).fetchall()
        if not rows:
            break

        for row in rows:
            after_rowid = row["rowid"]
            required = json.loads(row["requires"]) if row["requires"] else []

            # 并发组容量与依赖/退避同属「该候选此刻是否可领取」的门控（依赖、
            # 退避已在 SQL 内过滤；组容量在 Python 侧逐候选跳过）。组已满的
            # 候选对任何领取者都不可得，先于能力门控跳过。
            group = row["concurrency_group"]
            if group is not None and occupied.get(group, 0) >= limits[group]:
                if collect_skips:
                    on_skip(
                        row,
                        "concurrency_group_full",
                        {
                            "concurrency_group": group,
                            "group_limit": limits[group],
                            "group_occupied": occupied.get(group, 0),
                        },
                    )
                continue

            # 能力门控（在同事务内核验登记，与依赖/退避/并发组一起裁决）：
            # 声明了 requires 的候选要求领取者持有存活登记且覆盖全部标签。
            if required:
                if worker_caps is None:
                    if selected < limit:
                        # 登记缺失或已过期（含匿名领取）且该候选在批量填满之前被
                        # 遇到、其余门控均已满足：整次领取 409 且不领取任何任务——
                        # 预演同样直接返回 409（真实领取扫描到这里也必失败）。
                        raise APIError(
                            409,
                            "worker_not_registered",
                            "要求能力的任务只能由持有存活能力登记的工作器领取",
                            {
                                "worker_id": worker_id,
                                "requires": required,
                                "task_id": row["id"],
                                "task_key": row["task_key"],
                                "dag_id": row["dag_id"],
                            },
                        )
                    # 批量已填满后才遇到的候选：真实领取此刻已结束扫描、不会 409。
                    # 预演中标记为「本批名额已满」（若用更大 limit 则会触发同样的 409）。
                    if collect_skips:
                        on_skip(row, "limit_reached", {"limit": limit})
                    continue
                if not set(required) <= worker_caps:
                    # 登记存活但能力不覆盖：跳过该候选，继续向后寻找其他任务，
                    # 不消耗并发组名额。
                    if collect_skips:
                        on_skip(
                            row,
                            "capability_mismatch",
                            {"requires": required, "capabilities": sorted(worker_caps)},
                        )
                    continue

            attempt_no = row["attempt"] + 1
            expires = now + timedelta(seconds=row["lease_seconds"])

            if selected >= limit:
                # 预演模式下批量已满：后续通过全部门控的候选只因本批限额未被选中。
                # 不模拟占用（真实领取此刻已结束，不会再增加组占用）。
                if collect_skips:
                    on_skip(row, "limit_reached", {"limit": limit})
                continue

            if on_select(row, required, attempt_no, expires) is False:
                # 真实领取的防御路径：UPDATE 未命中（被并发事务改动）则放弃本候选
                continue

            if group is not None:
                # 立即占位：同批后续候选必须看到该占用，杜绝超额
                occupied[group] = occupied.get(group, 0) + 1
            selected += 1
            if not collect_skips and selected >= limit:
                break

        if len(rows) < _CLAIM_PAGE_SIZE:
            break

    return selected


def claim_tasks(worker_id: str | None, dag_id: str | None, limit: int) -> dict[str, Any]:
    if dag_id is not None:
        with get_conn() as conn:
            if not conn.execute("SELECT 1 FROM dags WHERE id = ?", (dag_id,)).fetchone():
                raise APIError(404, "dag_not_found", f"DAG {dag_id} 不存在")

    claimed: list[dict[str, Any]] = []
    now = _now()
    now_iso = _iso(now)

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")  # 保留锁：同一时刻只有一个领取者
        try:
            # 能力路由：领取者的能力登记必须在同一保留事务内核验（与依赖、退避、
            # 并发组规则一样取同一事务内的一致状态）。worker 为 None 表示匿名领取；
            # 登记缺失或过期时 worker_caps 为 None：遇到要求能力的候选即 409。
            worker_caps = _load_worker_caps(conn, worker_id, now_iso)
            limits, occupied = _load_group_snapshot(conn, now_iso)

            def on_select(row, required: list[str], attempt_no: int, expires: datetime) -> bool:
                token = uuid.uuid4().hex

                # 旧尝试（若存在）标记过期，保证历史尝试不再有效
                conn.execute(
                    "UPDATE attempts SET state = 'expired' WHERE task_id = ? AND state = 'active'",
                    (row["id"],),
                )
                cur = conn.execute(
                    """UPDATE tasks
                       SET state = 'running', attempt = ?, lease_token = ?,
                           lease_expires_at = ?, worker_id = ?, started_at = COALESCE(started_at, ?),
                           next_eligible_at = NULL
                       WHERE id = ?""",
                    (attempt_no, token, _iso(expires), worker_id, now_iso, row["id"]),
                )
                if cur.rowcount != 1:  # 被并发事务改动则放弃本任务
                    return False
                conn.execute(
                    """INSERT INTO attempts (task_id, attempt_no, lease_token, worker_id,
                           leased_at, expires_at, state)
                       VALUES (?, ?, ?, ?, ?, ?, 'active')""",
                    (row["id"], attempt_no, token, worker_id, now_iso, _iso(expires)),
                )
                # task.claimed 与领取状态变更同事务；attempt=0 为首次领取，否则为过期重领
                _append_event(
                    conn,
                    EVT_TASK_CLAIMED,
                    row["dag_id"],
                    now_iso,
                    task_id=row["id"],
                    attempt=attempt_no,
                    from_state=PENDING if row["attempt"] == 0 else row["state"],
                    to_state=RUNNING,
                )
                claimed.append(
                    {
                        "task_id": row["id"],
                        "dag_id": row["dag_id"],
                        "task_key": row["task_key"],
                        "name": row["name"],
                        "payload": json.loads(row["payload"]) if row["payload"] else None,
                        "attempt": attempt_no,
                        "lease_token": token,
                        "lease_expires_at": _iso(expires),
                        "lease_seconds": row["lease_seconds"],
                        "concurrency_group": row["concurrency_group"],
                        "requires": required,
                    }
                )
                return True

            _scan_claimable_candidates(
                conn,
                dag_id=dag_id,
                now=now,
                now_iso=now_iso,
                limit=limit,
                worker_id=worker_id,
                worker_caps=worker_caps,
                limits=limits,
                occupied=occupied,
                on_select=on_select,
            )

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {"claimed": claimed, "count": len(claimed)}


# ---------------------------------------------------------------- 领取预演（只读）

# 预演阻塞原因码 -> 人类可读说明（首个阻塞原因按门控顺序：
# 依赖 → 退避 → 并发组容量 → 能力 → 本批限额）
_PLAN_REASON_MESSAGES = {
    "dependency_not_succeeded": "仍有前置任务未处于 succeeded",
    "backoff_not_elapsed": "显式失败后的退避时刻尚未到达",
    "concurrency_group_full": "并发组容量已满（含本批次模拟选中的即时占用）",
    "capability_mismatch": "工作器存活登记的能力不覆盖任务要求的全部标签",
    "limit_reached": "本次批量领取的 limit 个名额已全部选中",
}


def _plan_selected_dict(row, required: list[str], attempt_no: int, expires: datetime) -> dict[str, Any]:
    """模拟选中任务的对外视图：与真实领取响应同构，但不携带租约令牌（预演不发令牌）。"""
    return {
        "task_id": row["id"],
        "dag_id": row["dag_id"],
        "task_key": row["task_key"],
        "name": row["name"],
        "payload": json.loads(row["payload"]) if row["payload"] else None,
        "attempt": attempt_no,
        # 预演不产生真实租约：lease_token 缺省；lease_expires_at 仅为按当前
        # lease_seconds 模拟的结果，真实领取时刻不同会得到不同值
        "lease_token": None,
        "lease_expires_at": _iso(expires),
        "lease_seconds": row["lease_seconds"],
        "concurrency_group": row["concurrency_group"],
        "requires": required,
    }


def _plan_pending_blocker(
    row,
    now_iso: str,
    task_states: dict[tuple[str, str], str],
    deps_by_task: dict[tuple[str, str], list[str]],
) -> dict[str, Any] | None:
    """判定不在可领取候选集内的 pending 任务的首个阻塞原因。

    候选 SQL 只按「依赖未满足」「退避未到期」两类过滤 pending 行，因此此处按
    同一顺序裁决：先依赖（前置状态取模拟后的快照——本批选中的 pending 前置
    视为 running），再退避。理论上不会返回 None（防御外部写入的异常数据时
    返回 None，调用方跳过该行）。
    """
    dep_keys = deps_by_task.get((row["dag_id"], row["task_key"]), [])
    blocking = [
        {"task_key": key, "state": task_states.get((row["dag_id"], key))}
        for key in dep_keys
        if task_states.get((row["dag_id"], key)) != SUCCEEDED
    ]
    if blocking:
        return {
            "task_id": row["id"],
            "dag_id": row["dag_id"],
            "task_key": row["task_key"],
            "name": row["name"],
            "reason": "dependency_not_succeeded",
            "message": _PLAN_REASON_MESSAGES["dependency_not_succeeded"],
            "blocked_by": blocking,
        }
    if row["next_eligible_at"] is not None and row["next_eligible_at"] > now_iso:
        return {
            "task_id": row["id"],
            "dag_id": row["dag_id"],
            "task_key": row["task_key"],
            "name": row["name"],
            "reason": "backoff_not_elapsed",
            "message": _PLAN_REASON_MESSAGES["backoff_not_elapsed"],
            "next_eligible_at": row["next_eligible_at"],
        }
    return None


def plan_claims(worker_id: str | None, dag_id: str | None, limit: int) -> dict[str, Any]:
    """只读的任务领取决策预演：POST /tasks/claim-plan 的业务实现。

    完全沿用真实领取的候选排序（rowid）与依赖、退避、并发组、工作器能力门控，
    在单个 SQLite 只读事务的一致快照上完成：
      - 不写入任务、attempt 或事件（连接置 query_only 兜底，结束以 ROLLBACK 收尾）；
      - 同一批次内模拟选中的任务即时占用并发组容量，使选中序列与同一快照下的
        真实批量领取一致；
      - 返回按顺序模拟选中的任务（无真实租约令牌），并为每个未选中的 pending
        任务给出首个阻塞原因及相关字段。
    请求参数与 claim_tasks 一致；dag_id 不存在返回 404；requires 候选遇到无存活
    登记的领取者同样返回 409 worker_not_registered（与真实领取同语义，不落任何写入）。
    """
    now = _now()
    now_iso = _iso(now)

    with get_conn() as conn:
        # 兜底：整次预演只读，任何意外写操作都会被 SQLite 直接拒绝
        conn.execute("PRAGMA query_only = ON")
        conn.execute("BEGIN")  # 只读事务：本次预演的所有读取看到同一快照
        try:
            if dag_id is not None and not conn.execute(
                "SELECT 1 FROM dags WHERE id = ?", (dag_id,)
            ).fetchone():
                raise APIError(404, "dag_not_found", f"DAG {dag_id} 不存在")

            worker_caps = _load_worker_caps(conn, worker_id, now_iso)
            limits, occupied = _load_group_snapshot(conn, now_iso)

            selected: list[tuple[Any, list[str], int, datetime]] = []
            candidate_skips: dict[str, dict[str, Any]] = {}

            def on_select(row, required: list[str], attempt_no: int, expires: datetime) -> None:
                selected.append((row, required, attempt_no, expires))

            def on_skip(row, reason: str, extra: dict[str, Any]) -> None:
                candidate_skips[row["id"]] = {
                    "task_id": row["id"],
                    "dag_id": row["dag_id"],
                    "task_key": row["task_key"],
                    "name": row["name"],
                    "reason": reason,
                    "message": _PLAN_REASON_MESSAGES[reason],
                    **extra,
                }

            _scan_claimable_candidates(
                conn,
                dag_id=dag_id,
                now=now,
                now_iso=now_iso,
                limit=limit,
                worker_id=worker_id,
                worker_caps=worker_caps,
                limits=limits,
                occupied=occupied,
                on_select=on_select,
                collect_skips=True,
                on_skip=on_skip,
            )

            selected_ids = {row["id"] for row, _r, _a, _e in selected}

            # 全部 pending 任务（候选集外的行在此补齐阻塞原因），按 rowid 稳定排序
            pending_rows = conn.execute(
                """SELECT t.rowid AS rowid, t.*
                   FROM tasks t
                   WHERE (:dag_id IS NULL OR t.dag_id = :dag_id)
                     AND t.state = 'pending'
                   ORDER BY t.rowid""",
                {"dag_id": dag_id},
            ).fetchall()

            # 依赖判定所需的（dag_id, task_key) -> state：先取存储快照，再把本批
            # 模拟选中的 pending 任务视为 running（与真实批量领取后的状态一致）
            task_states: dict[tuple[str, str], str] = {
                (r["dag_id"], r["task_key"]): r["state"]
                for r in conn.execute(
                    "SELECT dag_id, task_key, state FROM tasks"
                ).fetchall()
            }
            for row, _r, _a, _e in selected:
                task_states[(row["dag_id"], row["task_key"])] = RUNNING
            deps_by_task: dict[tuple[str, str], list[str]] = {}
            for d in conn.execute(
                "SELECT dag_id, task_key, dep_key FROM dependencies ORDER BY dag_id, task_key, dep_key"
            ).fetchall():
                deps_by_task.setdefault((d["dag_id"], d["task_key"]), []).append(d["dep_key"])

            # 未选中原因表：候选扫描产生的组满/能力不匹配/限额原因优先，
            # 其余 pending 行按依赖 → 退避补齐首个阻塞原因
            reasons = dict(candidate_skips)
            for prow in pending_rows:
                if prow["id"] in selected_ids or prow["id"] in reasons:
                    continue
                entry = _plan_pending_blocker(prow, now_iso, task_states, deps_by_task)
                if entry is not None:
                    reasons[prow["id"]] = entry

            # 未选中列表按 rowid 排序输出（只含 pending 任务；过期重领中的 running
            # 任务不属于 pending，即使在候选扫描中被跳过也不在此列）
            unselected = [
                reasons[prow["id"]]
                for prow in pending_rows
                if prow["id"] not in selected_ids and prow["id"] in reasons
            ]

            claimed = [
                _plan_selected_dict(row, required, attempt_no, expires)
                for row, required, attempt_no, expires in selected
            ]
            result = {
                "simulated": True,
                "worker_id": worker_id,
                "dag_id": dag_id,
                "limit": limit,
                "evaluated_at": now_iso,
                "claimed": claimed,
                "count": len(claimed),
                "unselected": unselected,
            }
        finally:
            conn.execute("ROLLBACK")  # 纯读取：不提交任何变更

    return result


# ---------------------------------------------------------------- 工作器能力登记

def _worker_dict(row, now_iso: str | None = None) -> dict[str, Any]:
    """workers 行 -> 对外登记视图；alive 按当前时刻动态计算（与租约/订阅视图同口径）。"""
    now_iso = now_iso or _iso(_now())
    return {
        "worker_id": row["worker_id"],
        "capabilities": json.loads(row["capabilities"]),
        "ttl_seconds": row["ttl_seconds"],
        "registered_at": row["registered_at"],
        "expires_at": row["expires_at"],
        # 到期判定与领取一致：expires_at >= now 视为存活，过期行仍可查询
        "alive": row["expires_at"] >= now_iso,
    }


def register_worker(worker_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """登记/刷新工作器能力：UPSERT，重复登记覆盖能力并刷新过期时刻。

    能力标签按首次出现顺序去重（与任务依赖/requires 的去重口径一致）。
    ttl_seconds 必须为正（请求模型已校验）。registered_at 保留首次登记时刻，
    expires_at 以本次 PUT 时刻 + ttl 覆盖刷新。
    """
    capabilities = list(dict.fromkeys(body["capabilities"]))
    ttl = float(body["ttl_seconds"])
    now = _now()
    now_iso = _iso(now)
    expires_iso = _iso(now + timedelta(seconds=ttl))

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                """INSERT INTO workers (worker_id, capabilities, ttl_seconds, registered_at, expires_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(worker_id) DO UPDATE SET
                       capabilities = excluded.capabilities,
                       ttl_seconds = excluded.ttl_seconds,
                       expires_at = excluded.expires_at""",
                (worker_id, _dumps(capabilities), ttl, now_iso, expires_iso),
            )
            row = conn.execute(
                "SELECT * FROM workers WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return _worker_dict(row, now_iso)


def get_worker(worker_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM workers WHERE worker_id = ?", (worker_id,)
        ).fetchone()
        if row is None:
            raise APIError(404, "worker_not_found", f"工作器 {worker_id} 尚未登记")
        return _worker_dict(row)


# ---------------------------------------------------------------- 租约工具

def _load_for_token(conn, lease_token: str) -> dict[str, Any]:
    """校验令牌并返回任务行；令牌错误/任务已不在租约中一律 409。"""
    row = conn.execute("SELECT * FROM tasks WHERE lease_token = ?", (lease_token,)).fetchone()
    if row is None:
        raise APIError(
            409, "invalid_lease", "租约令牌无效：租约已过期、被新尝试取代或任务已结束"
        )
    return row


def _assert_active(row) -> None:
    expires = _parse(row["lease_expires_at"])
    if expires is not None and expires <= _now():
        raise APIError(
            409,
            "lease_expired",
            f"租约已于 {row['lease_expires_at']} 过期，该任务已可被重新领取",
        )


def heartbeat(task_id: str, body: dict[str, Any]) -> dict[str, Any]:
    token = body["lease_token"]
    extend = body.get("extend_seconds")
    now = _now()

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = _load_for_token(conn, token)
            if row["id"] != task_id:
                raise APIError(409, "invalid_lease", "租约令牌与任务不匹配或已失效")
            _assert_active(row)
            seconds = float(extend) if extend is not None else row["lease_seconds"]
            new_expires = now + timedelta(seconds=seconds)
            conn.execute(
                "UPDATE tasks SET lease_expires_at = ? WHERE id = ?",
                (_iso(new_expires), row["id"]),
            )
            conn.execute(
                """UPDATE attempts SET expires_at = ?, heartbeat_at = ?
                   WHERE lease_token = ? AND state = 'active'""",
                (_iso(new_expires), _iso(now), token),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {
        "task_id": row["id"],
        "attempt": row["attempt"],
        "lease_token": token,
        "lease_expires_at": _iso(new_expires),
    }


# ---------------------------------------------------------------- 结果回报

def _block_descendants(conn, dag_id: str, root_key: str, occurred_at: str) -> int:
    """永久失败后，递归阻塞全部直接/间接后继。返回阻塞任务数。"""
    rows = conn.execute(
        """
        WITH RECURSIVE descendants(task_key) AS (
            SELECT task_key
            FROM dependencies
            WHERE dag_id = ? AND dep_key = ?
            UNION
            SELECT d.task_key
            FROM dependencies d
            JOIN descendants ds ON d.dag_id = ? AND d.dep_key = ds.task_key
        )
        SELECT DISTINCT task_key FROM descendants
        """,
        (dag_id, root_key, dag_id),
    ).fetchall()

    count = 0
    for r in rows:
        target = conn.execute(
            "SELECT * FROM tasks WHERE dag_id = ? AND task_key = ?",
            (dag_id, r["task_key"]),
        ).fetchone()
        # 已结束的任务不改变；运行中的任务即使租约有效也视为被阻塞
        if target["state"] in (SUCCEEDED, FAILED, BLOCKED, CANCELED):
            continue
        reason = f"upstream '{root_key}' permanently failed"
        conn.execute(
            """UPDATE tasks
               SET state = 'blocked', blocked_reason = ?,
                   lease_token = NULL, lease_expires_at = NULL, worker_id = NULL,
                   next_eligible_at = NULL
               WHERE id = ?""",
            (reason, target["id"]),
        )
        conn.execute(
            "UPDATE attempts SET state = 'expired' WHERE task_id = ? AND state = 'active'",
            (target["id"],),
        )
        # task.blocked 与阻塞更新同事务，只记录实际发生的状态改变
        _append_event(
            conn,
            EVT_TASK_BLOCKED,
            dag_id,
            occurred_at,
            task_id=target["id"],
            attempt=target["attempt"],
            from_state=target["state"],
            to_state=BLOCKED,
        )
        count += 1
    return count


def complete_task(task_id: str, body: dict[str, Any]) -> dict[str, Any] | ReplayedResponse:
    completion_id = body["completion_id"]
    token = body["lease_token"]
    success = bool(body["success"])
    result = body.get("result")
    error = body.get("error")
    # 规范化指纹：JSON 对象键序不影响等价判定
    fingerprint = _canonical(
        {"lease_token": token, "success": success, "result": result, "error": error}
    )
    now = _now()

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 幂等记录必须在同一保留事务内检查：并发同键回报只有一个能成为首次
            existing = conn.execute(
                "SELECT * FROM completions WHERE task_id = ? AND completion_id = ?",
                (task_id, completion_id),
            ).fetchone()
            if existing is not None:
                # 重放优先于当前租约状态：任务之后是否被重新领取都不影响回放
                conn.execute("ROLLBACK")
                if existing["fingerprint"] != fingerprint:
                    raise APIError(
                        409,
                        "idempotency_conflict",
                        "相同 task_id + completion_id 已用于内容不同的回报",
                        {
                            "task_id": task_id,
                            "completion_id": completion_id,
                        },
                    )
                return ReplayedResponse(existing["status_code"], existing["response_body"])

            # 不同键（或首次请求）：执行当前租约校验
            row = _load_for_token(conn, token)
            if row["id"] != task_id:
                raise APIError(409, "invalid_lease", "租约令牌与任务不匹配或已失效")
            _assert_active(row)
            task_id = row["id"]
            blocked_count = 0
            next_eligible_at: str | None = None

            if success:
                conn.execute(
                    """UPDATE tasks SET state = 'succeeded', lease_token = NULL,
                           lease_expires_at = NULL, finished_at = ?, next_eligible_at = NULL
                       WHERE id = ?""",
                    (_iso(now), task_id),
                )
                conn.execute(
                    "UPDATE attempts SET state = 'succeeded' WHERE lease_token = ?",
                    (token,),
                )
                # task.succeeded 与状态变更同事务
                _append_event(
                    conn,
                    EVT_TASK_SUCCEEDED,
                    row["dag_id"],
                    _iso(now),
                    task_id=task_id,
                    attempt=row["attempt"],
                    from_state=RUNNING,
                    to_state=SUCCEEDED,
                )
                state = SUCCEEDED
            else:
                # 显式失败才消耗重试预算：在同一事务内原子递增独立计数，
                # 租约过期后的重新领取（attempt 增加）不影响该计数。
                cur = conn.execute(
                    "UPDATE tasks SET failure_count = failure_count + 1 WHERE id = ?",
                    (task_id,),
                )
                if cur.rowcount != 1:  # pragma: no cover - 行已在本事务中加载
                    raise APIError(409, "task_changed", "任务在回报期间被改动，请重试")
                failure_count = row["failure_count"] + 1
                will_retry = failure_count <= row["max_retries"]
                if will_retry:
                    # 回到待领取队列；下一次领取产生 attempt+1 的新尝试与新令牌。
                    # 持久化指数退避：延迟在本回报事务内计算并固化为
                    # next_eligible_at；未配置退避时为 NULL（立即可领）。
                    delay = _backoff_delay_seconds(row, failure_count)
                    if delay is not None:
                        next_eligible_at = _iso(now + timedelta(seconds=delay))
                    conn.execute(
                        """UPDATE tasks SET state = 'pending', lease_token = NULL,
                               lease_expires_at = NULL, worker_id = NULL,
                               next_eligible_at = ?
                           WHERE id = ?""",
                        (next_eligible_at, task_id),
                    )
                    conn.execute(
                        "UPDATE attempts SET state = 'failed', error = ? WHERE lease_token = ?",
                        (error, token),
                    )
                    # task.requeued 与状态回队同事务（失败但仍有重试额度），
                    # 携带退避后的下次可领时刻（未配置退避时为 null）
                    _append_event(
                        conn,
                        EVT_TASK_REQUEUED,
                        row["dag_id"],
                        _iso(now),
                        task_id=task_id,
                        attempt=row["attempt"],
                        from_state=RUNNING,
                        to_state=PENDING,
                        next_eligible_at=next_eligible_at,
                    )
                    state = PENDING
                else:
                    conn.execute(
                        """UPDATE tasks SET state = 'failed', lease_token = NULL,
                               lease_expires_at = NULL, finished_at = ?, next_eligible_at = NULL
                           WHERE id = ?""",
                        (_iso(now), task_id),
                    )
                    conn.execute(
                        "UPDATE attempts SET state = 'failed', error = ? WHERE lease_token = ?",
                        (error, token),
                    )
                    # task.failed 先于后继的 task.blocked 落库，保持因果顺序
                    _append_event(
                        conn,
                        EVT_TASK_FAILED,
                        row["dag_id"],
                        _iso(now),
                        task_id=task_id,
                        attempt=row["attempt"],
                        from_state=RUNNING,
                        to_state=FAILED,
                    )
                    state = FAILED
                    blocked_count = _block_descendants(
                        conn, row["dag_id"], row["task_key"], _iso(now)
                    )

            if success:
                retries_remaining = 0
                saved_failure_count = row["failure_count"]
            else:
                # 本次失败后还会被调度的额外尝试次数
                retries_remaining = max(0, row["max_retries"] - failure_count + 1)
                saved_failure_count = failure_count

            response = {
                "task_id": task_id,
                "state": state,
                "attempt": row["attempt"],
                "failure_count": saved_failure_count,
                "retries_remaining": retries_remaining,
                "blocked_descendants": blocked_count,
                # 退避后的下次可领时刻：仅显式失败且仍有额度时可能非空；
                # 成功/最终失败/未配置退避均为 null
                "next_eligible_at": next_eligible_at,
            }
            # 与状态变更同一事务持久化：保存规范化指纹、HTTP 状态码和响应体
            conn.execute(
                """INSERT INTO completions (task_id, completion_id, lease_token, success,
                       fingerprint, status_code, response_body, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    task_id,
                    completion_id,
                    token,
                    1 if success else 0,
                    fingerprint,
                    200,
                    # 与 Starlette JSONResponse 的序列化保持一致，确保重放逐字节相同
                    json.dumps(response, ensure_ascii=False, separators=(",", ":")),
                    _iso(now),
                ),
            )
            conn.execute("COMMIT")
        except Exception:
            # 命中冲突分支时事务已回滚；仅在仍有活动事务时回滚
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    return response


# ---------------------------------------------------------------- 失败恢复

def resume_task(task_id: str) -> dict[str, Any]:
    """恢复最终失败的任务：failed -> pending，并递归解锁满足条件的 blocked 后继。

    单事务内完成：根任务清除租约/完成时间回到 pending 并追加 task.resumed 事件；
    随后按依赖图递归检查被阻塞后继——仅当某后继的全部前置均已 succeeded 或处于
    pending（被恢复任务本身与本次已连锁解锁的后继按拓扑序先更新，同样视为
    pending）时才解锁为 pending（清除 blocked_reason 并追加 task.resumed 事件）；
    前置中仍有 running/failed/blocked 的后继保留 blocked 及原阻塞原因。
    恢复不重置 failure_count/重试预算与 attempt 历史：再次显式失败会因预算
    已耗尽立即回到 failed 并重新阻塞后继。非 failed 状态（含恢复成功后的重复
    调用）返回 409，且不新增任何事件。
    """
    now_iso = _iso(_now())
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if row is None:
                raise APIError(404, "task_not_found", f"任务 {task_id} 不存在")
            if row["state"] != FAILED:
                # 重复恢复（任务已回到 pending）也走到这里：409 且不产生事件
                raise APIError(
                    409,
                    "task_not_failed",
                    "只有最终失败（failed）的任务才能恢复",
                    {"task_id": task_id, "state": row["state"]},
                )
            dag_id = row["dag_id"]
            root_key = row["task_key"]

            # 根任务回到待领取队列：清除租约与完成时间；attempt/failure_count
            # 与重试预算保留，立即可被重新领取（不触发退避）
            conn.execute(
                """UPDATE tasks
                   SET state = 'pending', lease_token = NULL, lease_expires_at = NULL,
                       worker_id = NULL, finished_at = NULL, next_eligible_at = NULL
                   WHERE id = ?""",
                (task_id,),
            )
            # task.resumed 与状态变更同事务；先于后继的解锁事件落库，保持因果顺序
            _append_event(
                conn,
                EVT_TASK_RESUMED,
                dag_id,
                now_iso,
                task_id=task_id,
                attempt=row["attempt"],
                from_state=FAILED,
                to_state=PENDING,
            )

            # 递归检查被阻塞后继（同一事务内）
            task_rows = conn.execute(
                "SELECT id, task_key, state, attempt FROM tasks WHERE dag_id = ?",
                (dag_id,),
            ).fetchall()
            dep_rows = conn.execute(
                "SELECT task_key, dep_key FROM dependencies WHERE dag_id = ?",
                (dag_id,),
            ).fetchall()

            states = {r["task_key"]: r["state"] for r in task_rows}
            states[root_key] = PENDING  # 根任务在本事务内已置为 pending
            ids = {r["task_key"]: r["id"] for r in task_rows}
            attempts = {r["task_key"]: r["attempt"] for r in task_rows}
            deps: dict[str, list[str]] = {k: [] for k in states}
            succ: dict[str, list[str]] = {k: [] for k in states}
            for d in dep_rows:
                deps[d["task_key"]].append(d["dep_key"])
                succ[d["dep_key"]].append(d["task_key"])

            # 只有根的后继可能被本次恢复解锁；其他 blocked 任务（由无关失败
            # 造成）不在检查范围内，保持原状
            descendants: set[str] = set()
            stack = [root_key]
            while stack:
                u = stack.pop()
                for v in succ[u]:
                    if v not in descendants:
                        descendants.add(v)
                        stack.append(v)

            # 拓扑序（Kahn，堆按 key 字典序保证事件顺序确定）：前驱先于后继评估，
            # 本次解锁的后继处于 pending，可让更下游的阻塞节点在同一事务内连锁解锁
            indeg = {k: len(deps[k]) for k in states}
            heap = [k for k in states if indeg[k] == 0]
            heapq.heapify(heap)
            unblocked = 0
            while heap:
                u = heapq.heappop(heap)
                if (
                    u in descendants
                    and states[u] == BLOCKED
                    and all(states[p] in (SUCCEEDED, PENDING) for p in deps[u])
                ):
                    conn.execute(
                        """UPDATE tasks
                           SET state = 'pending', blocked_reason = NULL,
                               lease_token = NULL, lease_expires_at = NULL,
                               worker_id = NULL, next_eligible_at = NULL
                           WHERE id = ?""",
                        (ids[u],),
                    )
                    states[u] = PENDING
                    unblocked += 1
                    # 每个实际解锁的后继与状态变更同事务追加事件（blocked -> pending）；
                    # 保持 blocked 及原阻塞原因的后继不产生事件
                    _append_event(
                        conn,
                        EVT_TASK_RESUMED,
                        dag_id,
                        now_iso,
                        task_id=ids[u],
                        attempt=attempts[u],
                        from_state=BLOCKED,
                        to_state=PENDING,
                    )
                for v in succ[u]:
                    indeg[v] -= 1
                    if indeg[v] == 0:
                        heapq.heappush(heap, v)

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {
        "task_id": task_id,
        "state": PENDING,
        "attempt": row["attempt"],
        "unblocked_descendants": unblocked,
    }


# ---------------------------------------------------------------- 任务取消

def cancel_task(task_id: str) -> dict[str, Any]:
    """取消任务：pending/running/blocked -> canceled，并递归取消所有未结束后继。

    单事务内完成：目标任务与每个实际改变的后继均置为 canceled（终态，不再参与
    领取），并各追加一条 task.canceled 事件。若目标（或某后继）正在运行，立即
    终止当前 attempt（active -> expired）并清除租约令牌/过期时刻/worker：旧
    lease_token 此后的心跳与完成回报都因令牌不再匹配任何租约而返回 409。
    已 succeeded/failed/canceled 的后继保持原样，不产生事件。重复取消（目标已
    处于 canceled 或其他终态/不可取消状态）返回 409 且不产生任何事件。
    """
    now_iso = _iso(_now())
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            root = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if root is None:
                raise APIError(404, "task_not_found", f"任务 {task_id} 不存在")
            if root["state"] not in CANCELABLE_STATES:
                # canceled（重复取消）与 succeeded/failed 均在此：409 且不产生事件
                raise APIError(
                    409,
                    "task_not_cancelable",
                    "只有 pending、running 或 blocked 状态的任务才能取消",
                    {"task_id": task_id, "state": root["state"]},
                )
            dag_id = root["dag_id"]
            root_key = root["task_key"]

            # 先在同一快照内载入整图（状态变更前），保证事件的 from_state 为原值
            task_rows = conn.execute(
                "SELECT id, task_key, state, attempt FROM tasks WHERE dag_id = ?",
                (dag_id,),
            ).fetchall()
            dep_rows = conn.execute(
                "SELECT task_key, dep_key FROM dependencies WHERE dag_id = ?",
                (dag_id,),
            ).fetchall()

            states = {r["task_key"]: r["state"] for r in task_rows}
            ids = {r["task_key"]: r["id"] for r in task_rows}
            attempts = {r["task_key"]: r["attempt"] for r in task_rows}
            deps: dict[str, list[str]] = {k: [] for k in states}
            succ: dict[str, list[str]] = {k: [] for k in states}
            for d in dep_rows:
                deps[d["task_key"]].append(d["dep_key"])
                succ[d["dep_key"]].append(d["task_key"])

            def cancel_one(key: str) -> None:
                """将单个未结束任务置为 canceled，终止其活动尝试并追加事件。"""
                old_state = states[key]
                conn.execute(
                    """UPDATE tasks
                       SET state = 'canceled', blocked_reason = NULL,
                           lease_token = NULL, lease_expires_at = NULL, worker_id = NULL,
                           next_eligible_at = NULL, finished_at = ?
                       WHERE id = ?""",
                    (now_iso, ids[key]),
                )
                # 运行中任务：当前 attempt 立即终止（active -> expired），旧令牌失效
                conn.execute(
                    "UPDATE attempts SET state = 'expired' WHERE task_id = ? AND state = 'active'",
                    (ids[key],),
                )
                states[key] = CANCELED
                _append_event(
                    conn,
                    EVT_TASK_CANCELED,
                    dag_id,
                    now_iso,
                    task_id=ids[key],
                    attempt=attempts[key],
                    from_state=old_state,
                    to_state=CANCELED,
                )

            # 根任务先取消：其事件先于后继落库，保持因果顺序
            cancel_one(root_key)

            # 根的全部直接/间接后继集合
            descendants: set[str] = set()
            stack = [root_key]
            while stack:
                u = stack.pop()
                for v in succ[u]:
                    if v not in descendants:
                        descendants.add(v)
                        stack.append(v)

            # 拓扑序（Kahn，堆按 key 字典序保证事件顺序确定）：前驱先于后继评估，
            # 终态（succeeded/failed/canceled）后继跳过且不产生事件
            indeg = {k: len(deps[k]) for k in states}
            heap = [k for k in states if indeg[k] == 0]
            heapq.heapify(heap)
            canceled_count = 0
            while heap:
                u = heapq.heappop(heap)
                if u in descendants and states[u] not in TERMINAL_STATES:
                    # 依赖门控保证未结束后继只可能是 pending/blocked（running 后继
                    # 仅可能来自异常存储）；终态后继保持原样、不产生事件
                    cancel_one(u)
                    canceled_count += 1
                for v in succ[u]:
                    indeg[v] -= 1
                    if indeg[v] == 0:
                        heapq.heappush(heap, v)

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {
        "task_id": task_id,
        "state": CANCELED,
        "attempt": root["attempt"],
        "canceled_descendants": canceled_count,
    }


# ---------------------------------------------------------------- 状态查询

def _attempts(conn, task_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM attempts WHERE task_id = ? ORDER BY attempt_no", (task_id,)
    ).fetchall()
    out = []
    for a in rows:
        out.append(
            {
                "attempt": a["attempt_no"],
                "state": a["state"],
                "lease_token": a["lease_token"],
                "worker_id": a["worker_id"],
                "leased_at": a["leased_at"],
                "expires_at": a["expires_at"],
                "heartbeat_at": a["heartbeat_at"],
                "expired": a["state"] == A_ACTIVE
                and (now := _parse(a["expires_at"])) is not None
                and now <= _now(),
                "error": a["error"],
            }
        )
    return out


def _group_limits(conn) -> dict[str, int]:
    """全库并发组名 -> 持久化上限（一次查询，供本连接内批量构造任务视图）。"""
    return {
        r["name"]: r["limit_value"]
        for r in conn.execute("SELECT name, limit_value FROM concurrency_groups").fetchall()
    }


def _task_dict(conn, row, with_attempts: bool = True, group_limits: dict[str, int] | None = None) -> dict[str, Any]:
    deps = [
        r["dep_key"]
        for r in conn.execute(
            "SELECT dep_key FROM dependencies WHERE dag_id = ? AND task_key = ? ORDER BY dep_key",
            (row["dag_id"], row["task_key"]),
        ).fetchall()
    ]
    lease = None
    if row["lease_token"] is not None:
        exp = _parse(row["lease_expires_at"])
        lease = {
            "token": row["lease_token"],
            "expires_at": row["lease_expires_at"],
            "expired": exp is not None and exp <= _now(),
            "worker_id": row["worker_id"],
        }
    # 无组任务返回 null；有组任务携带持久化的同名组上限（跨 DAG 一致）。
    # 未传入上限映射时回退到单行查询（get_task 之外的防御路径）。
    concurrency = None
    group = row["concurrency_group"]
    if group is not None:
        if group_limits is not None and group in group_limits:
            limit_value = group_limits[group]
        else:
            limit_value = conn.execute(
                "SELECT limit_value FROM concurrency_groups WHERE name = ?", (group,)
            ).fetchone()["limit_value"]
        concurrency = {"group": group, "limit": limit_value}
    return {
        "id": row["id"],
        "dag_id": row["dag_id"],
        "key": row["task_key"],
        "name": row["name"],
        "payload": json.loads(row["payload"]) if row["payload"] else None,
        "state": row["state"],
        "deps": deps,
        "attempt": row["attempt"],
        "failure_count": row["failure_count"],
        "max_retries": row["max_retries"],
        "duration_seconds": row["duration_seconds"],
        # 持久化指数退避策略；未配置时为 null（显式失败后立即重试）
        "retry_backoff": (
            {
                "initial_seconds": row["backoff_initial_seconds"],
                "multiplier": row["backoff_multiplier"],
                "max_seconds": row["backoff_max_seconds"],
            }
            if row["backoff_initial_seconds"] is not None
            else None
        ),
        # pending 任务的下次可领时刻：退避未到期前不可领取；null 表示立即可领
        "next_eligible_at": row["next_eligible_at"],
        # 剩余重试额度只与显式失败计数挂钩，与 attempt（含租约过期重领）无关
        "retries_remaining": max(0, row["max_retries"] - row["failure_count"]),
        "lease": lease,
        "blocked_reason": row["blocked_reason"],
        "concurrency": concurrency,
        # 能力要求标签：未声明（NULL）或空数组一律返回空数组，声明则按持久化顺序返回
        "requires": json.loads(row["requires"]) if row["requires"] else [],
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "attempts": _attempts(conn, row["id"]) if with_attempts else None,
    }


def get_task(task_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise APIError(404, "task_not_found", f"任务 {task_id} 不存在")
        return _task_dict(conn, row, group_limits=_group_limits(conn))


def _dag_summary(conn, dag_id: str) -> dict[str, Any]:
    counts = {
        s: (
            conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE dag_id = ? AND state = ?", (dag_id, s)
            ).fetchone()[0]
        )
        for s in (PENDING, RUNNING, SUCCEEDED, FAILED, BLOCKED, CANCELED)
    }
    return counts


def get_dag(dag_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        dag = conn.execute("SELECT * FROM dags WHERE id = ?", (dag_id,)).fetchone()
        if dag is None:
            raise APIError(404, "dag_not_found", f"DAG {dag_id} 不存在")
        rows = conn.execute(
            "SELECT * FROM tasks WHERE dag_id = ? ORDER BY created_at, task_key", (dag_id,)
        ).fetchall()
        group_limits = _group_limits(conn)
        return {
            "id": dag["id"],
            "name": dag["name"],
            "created_at": dag["created_at"],
            "task_count": len(rows),
            "states": _dag_summary(conn, dag_id),
            "tasks": [
                _task_dict(conn, r, with_attempts=False, group_limits=group_limits) for r in rows
            ],
        }


# ---------------------------------------------------------------- 只读分析

# 浮点时差判零阈值：关键任务 = 总时差不超过该值
_SLACK_EPS = 1e-9


def analyze_dag(dag_id: str) -> dict[str, Any]:
    """关键路径分析（CPM）：按声明的 duration_seconds 计算排程。

    纯只读：在单个只读事务内取一致快照（WAL 下读写互不阻塞，事务期间
    并发写入不影响本次读取），随后在内存中计算——不改任务状态、不写库、
    不产生事件，可安全地并发/重复调用。
    """
    with get_conn() as conn:
        conn.execute("BEGIN")  # 只读事务：本次分析的所有读取看到同一快照
        try:
            dag = conn.execute("SELECT * FROM dags WHERE id = ?", (dag_id,)).fetchone()
            if dag is None:
                raise APIError(404, "dag_not_found", f"DAG {dag_id} 不存在")
            task_rows = conn.execute(
                "SELECT task_key, duration_seconds FROM tasks WHERE dag_id = ?", (dag_id,)
            ).fetchall()
            dep_rows = conn.execute(
                "SELECT task_key, dep_key FROM dependencies WHERE dag_id = ?", (dag_id,)
            ).fetchall()
        finally:
            conn.execute("ROLLBACK")  # 纯读取：不提交任何变更

    durations = {r["task_key"]: float(r["duration_seconds"]) for r in task_rows}
    deps: dict[str, list[str]] = {k: [] for k in durations}
    for r in dep_rows:
        deps.setdefault(r["task_key"], []).append(r["dep_key"])

    # 防御性校验：正常提交路径已保证时长为正且图合法，但分析对存储内容重新校验，
    # 使历史遗留/外部写入的异常数据也得到明确的 422 而非错误排程。
    bad_duration = sorted(
        k for k, d in durations.items() if not math.isfinite(d) or d <= 0
    )
    if bad_duration:
        raise APIError(
            422, "invalid_duration", "存在非正的 duration_seconds", {"keys": bad_duration}
        )
    referenced = {r["task_key"] for r in dep_rows} | {r["dep_key"] for r in dep_rows}
    missing = sorted(referenced - durations.keys())
    if missing:
        raise APIError(
            422, "missing_dependency", "存在指向未声明任务的依赖边", {"missing": missing}
        )
    cycle = _find_cycle(set(durations), deps)
    if cycle:
        raise APIError(422, "cycle_detected", "依赖图存在环，必须是 DAG", {"cycle": cycle})

    keys = sorted(durations)
    succ: dict[str, list[str]] = {k: [] for k in keys}
    indeg = {k: len(deps[k]) for k in keys}
    for k in keys:
        for d in deps[k]:
            succ[d].append(k)

    # 拓扑序（Kahn，堆按 key 字典序保证结果确定）
    heap = [k for k in keys if indeg[k] == 0]
    heapq.heapify(heap)
    topo: list[str] = []
    while heap:
        u = heapq.heappop(heap)
        topo.append(u)
        for s in succ[u]:
            indeg[s] -= 1
            if indeg[s] == 0:
                heapq.heappush(heap, s)

    # 前向传递：最早开始/完成（所有前置完成后即可开始）
    es: dict[str, float] = {}
    ef: dict[str, float] = {}
    for k in topo:
        es[k] = max((ef[d] for d in deps[k]), default=0.0)
        ef[k] = es[k] + durations[k]
    total = max(ef.values(), default=0.0)

    # 后向传递：最晚开始/完成（汇点的最晚完成 = 总工期）
    ls: dict[str, float] = {}
    lf: dict[str, float] = {}
    for k in reversed(topo):
        lf[k] = min((ls[s] for s in succ[k]), default=total)
        ls[k] = lf[k] - durations[k]

    critical = {k for k in keys if ls[k] - es[k] <= _SLACK_EPS}

    # 并行层级：到源点的最长距离（按边数）。同层任务互不依赖、可并行执行；
    # 层数即任务数口径下的最小串行批次数。
    level: dict[str, int] = {}
    for k in topo:
        level[k] = 0 if not deps[k] else 1 + max(level[d] for d in deps[k])
    levels: list[list[str]] = []
    for k in keys:  # keys 已按字典序排序，层内顺序确定
        while len(levels) <= level[k]:
            levels.append([])
        levels[level[k]].append(k)

    # 关键路径：从零时差的源点出发，沿「最早开始 == 前驱最早完成」的关键后继
    # 走到汇点；平局取较小 key，保证多次调用结果一致。
    path: list[str] = []
    starts = sorted(k for k in critical if es[k] <= _SLACK_EPS)
    if starts:
        path = [starts[0]]
        while True:
            nxt = sorted(
                s for s in succ[path[-1]]
                if s in critical and abs(es[s] - ef[path[-1]]) <= _SLACK_EPS
            )
            if not nxt:
                break
            path.append(nxt[0])

    return {
        "dag_id": dag_id,
        "name": dag["name"],
        "task_count": len(keys),
        "total_duration_seconds": total,
        "critical_path": path,
        "critical_tasks": sorted(critical),
        "levels": levels,
        "tasks": [
            {
                "key": k,
                "duration_seconds": durations[k],
                "earliest_start": es[k],
                "earliest_finish": ef[k],
                "latest_start": ls[k],
                "latest_finish": lf[k],
                "slack": max(0.0, ls[k] - es[k]),
                "critical": k in critical,
                "level": level[k],
            }
            for k in keys
        ],
    }


def list_tasks(dag_id: str | None, state: str | None) -> dict[str, Any]:
    sql = "SELECT * FROM tasks"
    where, params = [], []
    if dag_id is not None:
        where.append("dag_id = ?")
        params.append(dag_id)
    if state is not None:
        where.append("state = ?")
        params.append(state)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at, id"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        group_limits = _group_limits(conn)
        return {
            "tasks": [
                _task_dict(conn, r, with_attempts=False, group_limits=group_limits) for r in rows
            ],
            "count": len(rows),
        }


# ---------------------------------------------------------------- 事件流

def _event_dict(r) -> dict[str, Any]:
    """events 行 -> 对外事件对象。GET /events 与订阅投递共用同一形态。"""
    return {
        "sequence": r["sequence"],
        "type": r["event_type"],
        "dag_id": r["dag_id"],
        "task_id": r["task_id"],
        "attempt": r["attempt"],
        "from_state": r["from_state"],
        "to_state": r["to_state"],
        # 仅 task.requeued 携带退避后的下次可领时刻；其他事件为 null
        "next_eligible_at": r["next_eligible_at"],
        "occurred_at": r["occurred_at"],
    }


def list_events(after: int, limit: int, dag_id: str | None) -> dict[str, Any]:
    """键集分页读取不可变事件流。

    以 sequence 为不透明游标：仅取 sequence > after 的行并按 sequence 升序。
    非空页 next_after 为本页最后一条的 sequence，供下一页使用；
    空页（无剩余或 dag_id 下尚无新事件）next_after 沿用 after。
    """
    sql = "SELECT * FROM events WHERE sequence > ?"
    params: list[Any] = [after]
    if dag_id is not None:
        sql += " AND dag_id = ?"
        params.append(dag_id)
    sql += " ORDER BY sequence ASC LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    events = [_event_dict(r) for r in rows]
    # 非空页游标推进到本页最后一条；空页（无剩余或该 dag_id 下尚无新事件）沿用 after
    next_after = rows[-1]["sequence"] if rows else after
    return {"events": events, "next_after": next_after}


# ------------------------------------------------------- 可靠事件订阅

def _fetch_subscription(conn, subscription_id: str):
    row = conn.execute(
        "SELECT * FROM subscriptions WHERE id = ?", (subscription_id,)
    ).fetchone()
    if row is None:
        raise APIError(404, "subscription_not_found", f"订阅 {subscription_id} 不存在")
    return row


def _delivery_view(row) -> dict[str, Any] | None:
    """在途批次视图：expired 按当前时刻动态计算，租约内的批次可继续 ack。"""
    if row is None:
        return None
    expires = _parse(row["lease_expires_at"])
    return {
        "delivery_token": row["delivery_token"],
        "lease_expires_at": row["lease_expires_at"],
        "expired": expires is not None and expires <= _now(),
        "last_sequence": row["last_sequence"],
        "event_count": row["event_count"],
        "delivered_at": row["delivered_at"],
    }


def _subscription_dict(conn, row) -> dict[str, Any]:
    delivery = conn.execute(
        "SELECT * FROM subscription_deliveries WHERE subscription_id = ?",
        (row["id"],),
    ).fetchone()
    return {
        "id": row["id"],
        "dag_id": row["dag_id"],
        "committed_sequence": row["committed_sequence"],
        "created_at": row["created_at"],
        "in_flight": _delivery_view(delivery),
    }


def create_subscription(body: dict[str, Any]) -> dict[str, Any]:
    """创建订阅并固化初始游标 committed_sequence（默认 0，从头订阅）。"""
    subscription_id = uuid.uuid4().hex
    now_iso = _iso(_now())
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                """INSERT INTO subscriptions (id, dag_id, committed_sequence, created_at)
                   VALUES (?, ?, ?, ?)""",
                (subscription_id, body.get("dag_id"), int(body["committed_sequence"]), now_iso),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return _subscription_dict(conn, _fetch_subscription(conn, subscription_id))


def get_subscription(subscription_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        return _subscription_dict(conn, _fetch_subscription(conn, subscription_id))


def list_subscriptions(dag_id: str | None = None) -> dict[str, Any]:
    sql = "SELECT * FROM subscriptions"
    params: list[Any] = []
    if dag_id is not None:
        sql += " WHERE dag_id = ?"
        params.append(dag_id)
    sql += " ORDER BY created_at, id"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        deliveries = {
            r["subscription_id"]: r
            for r in conn.execute("SELECT * FROM subscription_deliveries").fetchall()
        }
        items = []
        for row in rows:
            view = _subscription_dict(conn, row)
            # 已预取全部在途行，直接复用（避免逐个订阅再查询）
            view["in_flight"] = _delivery_view(deliveries.get(row["id"]))
            items.append(view)
        return {"subscriptions": items, "count": len(items)}


def poll_subscription(subscription_id: str, body: dict[str, Any]) -> dict[str, Any] | ReplayedResponse:
    """拉取下一批事件；同一订阅同时只允许一个未确认批次。

    - 存在在途批次且租约未过期：逐字节回放首次投递的响应（同令牌、同事件集），
      请求中的 limit/lease_seconds 不影响回放结果；
    - 在途批次租约已过期：删除旧批次，以新随机 delivery_token 与新租约重新投递
      （游标未推进，未确认事件原样重投，绝不跳过）；
    - 无在途批次：从 committed_sequence 之后按 sequence 升序取至多 limit 条，
      没有新事件时返回空批次（不生成在途行、不产生令牌）。
    """
    limit = int(body["limit"])
    now = _now()
    now_iso = _iso(now)

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            sub = _fetch_subscription(conn, subscription_id)
            delivery = conn.execute(
                "SELECT * FROM subscription_deliveries WHERE subscription_id = ?",
                (subscription_id,),
            ).fetchone()

            if delivery is not None:
                expires = _parse(delivery["lease_expires_at"])
                if expires is not None and expires > now:
                    # 租约未过期：同一未确认批次必须原样返回
                    conn.execute("ROLLBACK")
                    return ReplayedResponse(200, delivery["response_body"])
                # 租约已过期：旧批次失效，允许重新投递（在同一写事务内删除）
                conn.execute(
                    "DELETE FROM subscription_deliveries WHERE subscription_id = ?",
                    (subscription_id,),
                )

            sql = "SELECT * FROM events WHERE sequence > ?"
            params: list[Any] = [sub["committed_sequence"]]
            if sub["dag_id"] is not None:
                sql += " AND dag_id = ?"
                params.append(sub["dag_id"])
            sql += " ORDER BY sequence ASC LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, params).fetchall()

            if not rows:
                # 无新事件：空批次不占用在途槽位，也不产生令牌；游标保持不变
                conn.execute("COMMIT")
                return {
                    "subscription_id": subscription_id,
                    "delivery_token": None,
                    "lease_expires_at": None,
                    "events": [],
                    "last_sequence": sub["committed_sequence"],
                    "committed_sequence": sub["committed_sequence"],
                }

            token = uuid.uuid4().hex
            lease_expires_at = _iso(now + timedelta(seconds=float(body["lease_seconds"])))
            events = [_event_dict(r) for r in rows]
            last_sequence = rows[-1]["sequence"]
            response = {
                "subscription_id": subscription_id,
                "delivery_token": token,
                "lease_expires_at": lease_expires_at,
                "events": events,
                "last_sequence": last_sequence,
                "committed_sequence": sub["committed_sequence"],
            }
            response_body = json.dumps(response, ensure_ascii=False, separators=(",", ":"))
            conn.execute(
                """INSERT INTO subscription_deliveries
                       (subscription_id, delivery_token, lease_expires_at, last_sequence,
                        event_count, delivered_at, response_body)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    subscription_id,
                    token,
                    lease_expires_at,
                    last_sequence,
                    len(rows),
                    now_iso,
                    response_body,
                ),
            )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        return response


def ack_subscription(subscription_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """凭当前 delivery_token 确认在途批次，游标推进到该批最后一条 sequence。

    仅接受在途批次的当前令牌：没有在途批次（从未投递、已确认或租约过期未重投）、
    令牌旧（批次过期后已重新投递生成新令牌）或令牌错误一律 409，且游标保持不动——
    游标只能推进到当前在途批次的最后一条，绝不可能越过任何未确认事件。
    """
    token = body["delivery_token"]
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            sub = _fetch_subscription(conn, subscription_id)
            delivery = conn.execute(
                "SELECT * FROM subscription_deliveries WHERE subscription_id = ?",
                (subscription_id,),
            ).fetchone()
            if delivery is None:
                raise APIError(
                    409,
                    "no_pending_batch",
                    "没有待确认的投递批次：可能已确认、尚未 poll 或租约已过期",
                )
            if delivery["delivery_token"] != token:
                raise APIError(
                    409,
                    "invalid_delivery_token",
                    "delivery_token 与当前在途批次不匹配（旧令牌或错误令牌）",
                )
            # 游标只可能推进到该批最后一条 sequence；批次窗口从
            # committed_sequence 之后开始，天然不可能越过未确认事件
            committed = delivery["last_sequence"]
            if committed < sub["committed_sequence"]:  # pragma: no cover - 防御外部写入倒退
                raise APIError(
                    409,
                    "cursor_would_go_backwards",
                    "确认批次落后于已提交游标",
                )
            conn.execute(
                "UPDATE subscriptions SET committed_sequence = ? WHERE id = ?",
                (committed, subscription_id),
            )
            conn.execute(
                "DELETE FROM subscription_deliveries WHERE subscription_id = ?",
                (subscription_id,),
            )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    return {"subscription_id": subscription_id, "committed_sequence": committed}
