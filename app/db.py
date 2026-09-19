"""SQLite 连接与表结构。

所有对任务状态的修改都在单个 SQLite 连接的一个事务内完成，
依靠 BEGIN IMMEDIATE 取得的保留锁保证同一时刻只有一个工作器
能从 PENDING 队列领取到给定任务（原子领取）。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

# 默认租约时长（秒），任务可用 lease_seconds 覆盖。
DEFAULT_LEASE_SECONDS = 30.0

# 默认任务时长（秒），任务可用 duration_seconds 覆盖；只读分析据此排程。
DEFAULT_DURATION_SECONDS = 1.0


def db_path() -> Path:
    return Path(os.environ.get("DB_PATH", "./data/tasks.db"))


def get_default_lease_seconds() -> float:
    return float(os.environ.get("DEFAULT_LEASE_SECONDS", DEFAULT_LEASE_SECONDS))


def get_default_duration_seconds() -> float:
    return float(os.environ.get("DEFAULT_DURATION_SECONDS", DEFAULT_DURATION_SECONDS))


def _connect(path: Path | None = None) -> sqlite3.Connection:
    path = path or db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path),
        timeout=30.0,
        isolation_level=None,  # 手工管理事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(path: Path | None = None) -> None:
    with _connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dags (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                created_at    TEXT NOT NULL
            );

            -- 跨 DAG 并发组：同名组的限流上限全局唯一、首次提交时持久化。
            -- 之后任何 DAG 提交同名组必须声明相同上限，否则整批回滚（409）。
            CREATE TABLE IF NOT EXISTS concurrency_groups (
                name          TEXT PRIMARY KEY,
                limit_value   INTEGER NOT NULL CHECK (limit_value > 0),
                created_at    TEXT NOT NULL
            );

            -- 工作器能力登记（能力路由）：worker_id 即主键。
            -- capabilities 为去重后的能力标签 JSON 数组（按首次出现顺序持久化）；
            -- expires_at 为登记过期时刻，过期前该 worker_id 的领取才视为携带存活登记。
            -- 重复 PUT 覆盖 capabilities 并刷新 expires_at（registered_at 保留首次值）。
            CREATE TABLE IF NOT EXISTS workers (
                worker_id      TEXT PRIMARY KEY,
                capabilities   TEXT NOT NULL,       -- JSON 数组字符串，可能为 []
                ttl_seconds    REAL NOT NULL CHECK (ttl_seconds > 0),
                registered_at  TEXT NOT NULL,       -- 首次登记时刻
                expires_at     TEXT NOT NULL        -- 最近一次 PUT 刷新后的过期时刻
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id                  TEXT PRIMARY KEY,
                dag_id              TEXT NOT NULL REFERENCES dags(id),
                task_key            TEXT NOT NULL,
                name                TEXT NOT NULL,
                payload             TEXT,
                max_retries         INTEGER NOT NULL DEFAULT 0,
                lease_seconds       REAL NOT NULL,
                -- 预计执行时长（秒），只读分析用于关键路径/排程计算
                duration_seconds    REAL NOT NULL DEFAULT 1.0,
                state               TEXT NOT NULL,        -- pending/running/succeeded/failed/blocked/canceled
                blocked_reason      TEXT,
                lease_token         TEXT,
                lease_expires_at    TEXT,
                worker_id           TEXT,
                attempt             INTEGER NOT NULL DEFAULT 0,
                -- 独立的显式失败计数：只有 complete(success=false) 才递增；
                -- 租约过期后的重新领取只增加 attempt，不消耗重试预算。
                failure_count       INTEGER NOT NULL DEFAULT 0,
                -- 持久化指数退避策略（提交时固化，三列同为 NULL 或同为非空）。
                -- 仅显式失败且仍有重试额度时生效：延迟
                -- min(max_seconds, initial_seconds * multiplier^(failure_count-1))。
                backoff_initial_seconds REAL,
                backoff_multiplier      REAL,
                backoff_max_seconds     REAL,
                -- 退避后重新进入候选集的时刻：pending 任务须等待
                -- next_eligible_at <= 当前时刻才可被领取；NULL 表示立即可领。
                next_eligible_at    TEXT,
                created_at          TEXT NOT NULL,
                started_at          TEXT,
                finished_at         TEXT,
                -- 跨 DAG 并发组：NULL 表示不限流；非空时全库同名组内未过期
                -- running 租约数不得超过 concurrency_groups 中持久化的上限。
                concurrency_group   TEXT REFERENCES concurrency_groups(name),
                -- 能力要求：JSON 数组（去重后的能力标签，按首次出现顺序持久化）；
                -- NULL 表示不要求能力（历史任务与未声明 requires 的任务），
                -- 任何 worker_id（含无登记/匿名）均可领取，兼容历史行为。
                -- 非空时只有携带存活能力登记、且 capabilities 覆盖全部标签的
                -- worker_id 才能领取；能力不匹配的候选被跳过继续填满 limit，
                -- 登记缺失/过期则整次领取 409 且不领取任何任务。
                requires            TEXT,
                UNIQUE(dag_id, task_key)
            );

            CREATE TABLE IF NOT EXISTS dependencies (
                dag_id     TEXT NOT NULL REFERENCES dags(id),
                task_key   TEXT NOT NULL,
                dep_key    TEXT NOT NULL,
                PRIMARY KEY (dag_id, task_key, dep_key)
            );

            CREATE TABLE IF NOT EXISTS attempts (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id        TEXT NOT NULL REFERENCES tasks(id),
                attempt_no     INTEGER NOT NULL,
                lease_token    TEXT NOT NULL UNIQUE,
                worker_id      TEXT,
                leased_at      TEXT NOT NULL,
                expires_at     TEXT NOT NULL,
                heartbeat_at   TEXT,
                state          TEXT NOT NULL,   -- active/expired/succeeded/failed
                error          TEXT
            );

            -- 结果回报的幂等记录：(task_id, completion_id) 唯一标识一次逻辑回报。
            -- 首次有效回报在租约校验/状态变更的同一事务内写入规范化请求指纹与
            -- 首次响应（HTTP 状态码 + 原始响应体）；重放即使发生在任务被重新领取
            -- 之后也只返回存储的响应，不再改动 failure_count 或阻塞后继。
            CREATE TABLE IF NOT EXISTS completions (
                task_id        TEXT NOT NULL REFERENCES tasks(id),
                completion_id  TEXT NOT NULL,
                lease_token    TEXT NOT NULL,
                success        INTEGER NOT NULL,
                fingerprint    TEXT NOT NULL,   -- lease_token/success/result/error 的规范化 JSON
                status_code    INTEGER NOT NULL,
                response_body  TEXT NOT NULL,   -- 首次响应的原始 JSON 文本
                created_at     TEXT NOT NULL,
                PRIMARY KEY (task_id, completion_id)
            );

            -- 不可变事件流：只允许 INSERT。sequence 为全局单调递增游标，
            -- 与对应业务变更在同一事务内写入，事务回滚则事件一并消失。
            CREATE TABLE IF NOT EXISTS events (
                sequence      INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type    TEXT NOT NULL,   -- dag.created/task.claimed/task.requeued/task.succeeded/task.failed/task.blocked/task.resumed/task.canceled
                dag_id        TEXT NOT NULL,
                task_id       TEXT,            -- dag.created 时为 NULL
                attempt       INTEGER,         -- 任务当前尝试序号；dag.created 时为 NULL
                from_state    TEXT,            -- 变更前状态；dag.created 时为 NULL
                to_state      TEXT,            -- 变更后状态；dag.created 时为 NULL
                -- 退避后的下次可领时刻：仅 task.requeued 携带；其他事件为 NULL
                next_eligible_at TEXT,
                occurred_at   TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_dag_state ON tasks(dag_id, state);
            CREATE INDEX IF NOT EXISTS idx_deps_dep ON dependencies(dag_id, dep_key);
            CREATE INDEX IF NOT EXISTS idx_attempts_task ON attempts(task_id, attempt_no);
            CREATE INDEX IF NOT EXISTS idx_events_dag_seq ON events(dag_id, sequence);

            -- 事件流不可变：任何 UPDATE/DELETE 都被中止（INSERT 与事务回滚不受影响）
            CREATE TRIGGER IF NOT EXISTS events_no_update
            BEFORE UPDATE ON events
            BEGIN
                SELECT RAISE(ABORT, 'event stream is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS events_no_delete
            BEFORE DELETE ON events
            BEGIN
                SELECT RAISE(ABORT, 'event stream is immutable');
            END;

            -- 可靠事件订阅：committed_sequence 为已确认游标（投递严格从
            -- sequence > committed_sequence 开始，只有 ack 才推进，不漏不越）。
            -- dag_id 非空时只投递该 DAG 的事件（与 GET /events 的过滤口径一致），
            -- NULL 表示订阅全库事件流。订阅行持久化，重启后游标语义不变。
            CREATE TABLE IF NOT EXISTS subscriptions (
                id                 TEXT PRIMARY KEY,
                dag_id             TEXT,            -- 可选过滤；NULL 表示全库订阅
                committed_sequence INTEGER NOT NULL DEFAULT 0 CHECK (committed_sequence >= 0),
                created_at         TEXT NOT NULL
            );

            -- 在途投递批次：每个订阅至多一行（subscription_id 即主键），
            -- 强制「同一订阅同时只允许一个未确认批次」。行内存本次批次的随机
            -- delivery_token、租约过期时刻、批次最后一条 sequence、事件条数与
            -- 首次响应原文（租约内重复 poll 逐字节回放）。ack 成功即删行；
            -- 租约过期后的下次 poll 删旧行并以新令牌重新投递（游标仍停在
            -- committed_sequence，未确认事件会被原样重投，绝不跳过）。
            CREATE TABLE IF NOT EXISTS subscription_deliveries (
                subscription_id    TEXT PRIMARY KEY REFERENCES subscriptions(id),
                delivery_token     TEXT NOT NULL,   -- 每次（重）投递新生成的随机令牌
                lease_expires_at   TEXT NOT NULL,   -- 租约过期时刻；过期前重复 poll 回放本批
                last_sequence      INTEGER NOT NULL,-- 本批最后一条事件的 sequence；ack 后游标推进到该值
                event_count        INTEGER NOT NULL,
                delivered_at       TEXT NOT NULL,
                response_body      TEXT NOT NULL    -- 首次投递响应的原始 JSON 文本
            );

            -- ------------------------------------------------------- 可复用 DAG 模板
            -- 模板只承载定义（名称与时间戳）；任务图的不可变快照按版本保存在
            -- dag_template_versions：发布后版本行（含 snapshot_json）永不更新。
            CREATE TABLE IF NOT EXISTS dag_templates (
                name          TEXT PRIMARY KEY,     -- 模板名（创建后不可改）
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL          -- 最近一次发布新版本的时刻
            );

            -- 已发布版本：每个版本保存发布时刻的任务图快照（JSON 文本，不可变）。
            -- version 在同一模板内从 1 起单调递增；snapshot 不随后续发布而改变。
            -- parameters 为发布时从任务 payload 中提取的 ${name} 占位符名集合（
            -- 去重排序后的 JSON 数组），实例化时用于缺参/多参校验。
            CREATE TABLE IF NOT EXISTS dag_template_versions (
                template_name  TEXT NOT NULL REFERENCES dag_templates(name),
                version        INTEGER NOT NULL CHECK (version >= 1),
                snapshot_json  TEXT NOT NULL,        -- 发布时规范化的任务定义数组（不可变）
                parameters     TEXT NOT NULL,        -- JSON 数组字符串：占位符名（去重排序）
                created_at     TEXT NOT NULL,
                PRIMARY KEY (template_name, version)
            );

            -- 模板实例化幂等记录：(模板名, 版本, idempotency_key) 唯一标识一次
            -- 逻辑实例化。首次请求在「校验/替换/DAG 创建」的同一事务内写入参数
            -- 规范化指纹与创建出的 dag_id；同键同参重复调用返回同一 DAG，
            -- 同键参数指纹不同返回 409；任何 422/409 路径回滚，不留记录也不留 DAG。
            CREATE TABLE IF NOT EXISTS dag_template_instantiations (
                template_name    TEXT NOT NULL,
                version          INTEGER NOT NULL,
                idempotency_key  TEXT NOT NULL,
                dag_id           TEXT NOT NULL REFERENCES dags(id),
                parameters_json  TEXT NOT NULL,      -- 规范化参数 JSON（等价判定指纹）
                created_at       TEXT NOT NULL,
                PRIMARY KEY (template_name, version, idempotency_key)
            );
            """
        )

        # 旧版 SQLite 数据库迁移：为已有任务补齐显式失败计数列。
        # ADD COLUMN ... NOT NULL DEFAULT 0 会自动为所有历史任务填入 0
        # —— 这些任务此前尚无独立失败计数，默认从未显式失败，保留原重试预算。
        columns = {r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        if "failure_count" not in columns:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0"
            )
        # 旧库迁移：为已有任务补齐预计时长列，默认 1 秒（与分析缺省口径一致）。
        if "duration_seconds" not in columns:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN duration_seconds REAL NOT NULL DEFAULT 1.0"
            )
        # 旧库迁移：为已有任务补齐并发组列。历史任务均未声明组，默认 NULL —
        # 行为与新增的无组任务一致，不受跨 DAG 限流影响。
        if "concurrency_group" not in columns:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN concurrency_group TEXT REFERENCES concurrency_groups(name)"
            )
        # 旧库迁移：为已有任务补齐指数退避策略列与下次可领时刻。
        # ADD COLUMN 默认 NULL：历史任务未配置退避（保持立即重试），
        # next_eligible_at 为 NULL（可立即被领取），与新库的无退避任务完全一致。
        if "backoff_initial_seconds" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN backoff_initial_seconds REAL")
        if "backoff_multiplier" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN backoff_multiplier REAL")
        if "backoff_max_seconds" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN backoff_max_seconds REAL")
        if "next_eligible_at" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN next_eligible_at TEXT")
        # 旧库迁移：为已有任务补齐能力要求列。历史任务均未声明 requires，
        # 默认 NULL —— 行为与新增的无能力要求任务一致，兼容现有领取。
        if "requires" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN requires TEXT")
        conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_tasks_group_occupancy
                   ON tasks(concurrency_group)
                   WHERE state = 'running' AND lease_token IS NOT NULL"""
        )
        # 旧库迁移：事件流补齐退避可领时刻列。历史事件均不携带该字段，默认 NULL。
        event_columns = {r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()}
        if "next_eligible_at" not in event_columns:
            conn.execute("ALTER TABLE events ADD COLUMN next_eligible_at TEXT")


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    """返回一个开启外键/WAL 的连接，调用方负责 commit/rollback。"""
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()
