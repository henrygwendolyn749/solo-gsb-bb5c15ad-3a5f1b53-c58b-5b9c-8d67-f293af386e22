"""端到端行为测试。

运行: pytest -q （需先 pip install -r requirements-dev.txt）
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as c:
        yield c


def submit(client, tasks, name="dag", dag_id=None):
    body = {"name": name, "tasks": tasks}
    if dag_id:
        body["id"] = dag_id
    r = client.post("/dags", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def by_key(dag, key):
    return next(t for t in dag["tasks"] if t["key"] == key)


# ------------------------------------------------------------- 提交校验

def test_submit_valid_dag(client):
    dag = submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
        ],
    )
    assert dag["task_count"] == 2
    assert dag["states"]["pending"] == 2


def test_submit_rejects_missing_dependency(client):
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["ghost"]}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "missing_dependency"
    assert "ghost" in r.json()["error"]["details"]["missing"]


def test_submit_rejects_self_dependency_cycle(client):
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["a"]}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"


def test_submit_rejects_long_cycle(client):
    r = client.post(
        "/dags",
        json={"tasks": [{"key": "a", "deps": ["c"]}, {"key": "b", "deps": ["a"]}, {"key": "c", "deps": ["b"]}]},
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"
    assert r.json()["error"]["details"]["cycle"][0] == r.json()["error"]["details"]["cycle"][-1]


def test_submit_rejects_duplicate_key(client):
    r = client.post("/dags", json={"tasks": [{"key": "a"}, {"key": "a"}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "duplicate_task_key"


# ------------------------------------------------------------- 依赖门控

def test_claim_respects_dependencies(client):
    dag = submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}])
    r = client.post("/tasks/claim", json={"limit": 10})
    assert r.status_code == 200
    keys = {t["task_key"] for t in r.json()["claimed"]}
    assert keys == {"a"}

    # a 已在 running 且租约未过期，不重复领取；b 的前置未完成，也不可领
    r = client.post("/tasks/claim", json={"dag_id": dag["id"]})
    assert r.json()["count"] == 0


def test_full_dependency_flow(client):
    dag = submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["a"]},
            {"key": "d", "deps": ["b", "c"]},
        ],
    )
    dag_id = dag["id"]

    # 只有 a 可领
    claimed = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert [t["task_key"] for t in claimed] == ["a"]
    token_a = claimed[0]["lease_token"]
    assert claimed[0]["attempt"] == 1

    # 完成 a 后 b、c 同时可领
    assert client.post(f"/tasks/{claimed[0]['task_id']}/complete",
                       json={"completion_id": "cmp-a", "lease_token": token_a, "success": True}).status_code == 200
    claimed = client.post("/tasks/claim", json={"dag_id": dag_id, "limit": 10}).json()["claimed"]
    assert {t["task_key"] for t in claimed} == {"b", "c"}
    for t in claimed:
        assert t["attempt"] == 1

    # d 仍被门控
    assert client.post("/tasks/claim", json={"dag_id": dag_id}).json()["count"] == 0

    for t in claimed:
        client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": f"cmp-{t['task_key']}",
                          "lease_token": t["lease_token"], "success": True})
    d = client.post("/tasks/claim", json={"dag_id": dag_id}).json()["claimed"]
    assert len(d) == 1 and d[0]["task_key"] == "d"


# ------------------------------------------------------------- 尝试与唯一令牌

def test_attempt_increments_and_tokens_unique(client):
    dag = submit(client, [{"key": "a", "lease_seconds": 0.2, "max_retries": 3}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t1["attempt"] == 1
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["task_id"] == t1["task_id"]
    assert t2["attempt"] == 2
    assert t2["lease_token"] != t1["lease_token"]


def test_old_token_operations_return_409_after_reclaim(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    old_token, task_id = t1["lease_token"], t1["task_id"]
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2

    # 旧令牌续租 → 409
    r = client.post(f"/tasks/{task_id}/heartbeat",
                    json={"lease_token": old_token})
    assert r.status_code == 409
    assert r.json()["error"]["code"] in ("invalid_lease", "lease_expired")

    # 旧令牌回报结果 → 409，绝不能覆盖新尝试（completion_id 首次使用也必须先过租约校验）
    r = client.post(f"/tasks/{task_id}/complete",
                    json={"completion_id": "cmp-old", "lease_token": old_token, "success": True})
    assert r.status_code == 409

    status = client.get(f"/tasks/{task_id}").json()
    assert status["state"] == "running"
    assert status["attempt"] == 2
    assert status["lease"]["token"] == t2["lease_token"]


def test_heartbeat_extends_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 0.3}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    time.sleep(0.2)
    r = client.post(f"/tasks/{t['task_id']}/heartbeat",
                    json={"lease_token": t["lease_token"], "extend_seconds": 5})
    assert r.status_code == 200
    time.sleep(0.2)  # 没有心跳的话此刻已过期
    # 租约仍有效：不可再领取，且旧令牌仍可回报
    assert client.post("/tasks/claim", json={}).json()["count"] == 0
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-hb", "lease_token": t["lease_token"], "success": True})
    assert r.status_code == 200


def test_completion_id_is_required(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"lease_token": t["lease_token"], "success": True})
    assert r.status_code == 422


def test_wrong_token_returns_409(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-x",
                          "lease_token": uuid.uuid4().hex, "success": True})
    assert r.status_code == 409


# ------------------------------------------------------------- 幂等回报

def _complete(client, task, cid, **extra):
    body = {"completion_id": cid, "lease_token": task["lease_token"]}
    body.update(extra)
    return client.post(f"/tasks/{task['task_id']}/complete", json=body)


def test_idempotent_replay_returns_identical_response_after_reclaim(client):
    """任务被重新领取后，旧 completion_id 的等价回报仍原样返回首次响应。"""
    submit(client, [{"key": "a", "max_retries": 1, "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]

    payload = {"completion_id": "c1", "lease_token": t1["lease_token"],
               "success": False, "error": "boom"}
    r1 = client.post(f"/tasks/{t1['task_id']}/complete", json=payload)
    assert r1.status_code == 200
    first = r1.json()
    assert first["state"] == "pending" and first["attempt"] == 1
    assert first["failure_count"] == 1 and first["retries_remaining"] == 1

    # 租约过期后被重新领取：新尝试、新令牌
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2 and t2["lease_token"] != t1["lease_token"]

    # 用旧令牌 + 相同 completion_id/内容重放：原样回放，不经过当前租约校验
    r2 = client.post(f"/tasks/{t1['task_id']}/complete", json=payload)
    assert r2.status_code == r1.status_code
    assert r2.text == r1.text  # 逐字节相同的响应体
    assert r2.json() == first

    # 不得再次增加 failure_count，也不影响正在运行的新尝试
    status = client.get(f"/tasks/{t1['task_id']}").json()
    assert status["failure_count"] == 1
    assert status["state"] == "running" and status["attempt"] == 2
    assert status["lease"]["token"] == t2["lease_token"]

    # 新令牌的回报（不同 completion_id）仍走当前租约校验并正常生效
    r3 = _complete(client, t2, "c2", success=True)
    assert r3.status_code == 200 and r3.json()["state"] == "succeeded"
    assert client.get(f"/tasks/{t1['task_id']}").json()["state"] == "succeeded"


def test_idempotent_replay_json_key_order_equivalent(client):
    """result 的 JSON 对象键序不同（含嵌套）视为等价，原样回放首次响应。"""
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]

    r1 = _complete(client, t, "ck", success=True, result={"outer": {"b": 1, "a": [2, 1]}, "k": 0})
    assert r1.status_code == 200
    # 键序调换、空白变化：仍视为同一请求
    r2 = client.post(
        f"/tasks/{t['task_id']}/complete",
        content=json.dumps({"completion_id": "ck", "lease_token": t["lease_token"],
                            "success": True,
                            "result": {"k": 0, "outer": {"a":  [2, 1], "b": 1}}}),
        headers={"Content-Type": "application/json"},
    )
    assert r2.status_code == 200 and r2.text == r1.text
    assert client.get(f"/tasks/{t['task_id']}").json()["state"] == "succeeded"


def test_same_key_different_content_returns_conflict(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r1 = _complete(client, t, "ck", success=True, result={"rows": 1})
    assert r1.status_code == 200

    # success 不同
    r2 = _complete(client, t, "ck", success=False, error="now failed")
    assert r2.status_code == 409
    assert r2.json()["error"]["code"] == "idempotency_conflict"
    assert r2.json()["error"]["details"]["completion_id"] == "ck"

    # 即便租约令牌也无效，只要键存在且内容不同，仍是冲突而非租约错误
    r3 = client.post(
        f"/tasks/{t['task_id']}/complete",
        json={"completion_id": "ck", "lease_token": uuid.uuid4().hex,
              "success": True, "result": {"rows": 2}},
    )
    assert r3.status_code == 409
    assert r3.json()["error"]["code"] == "idempotency_conflict"


def test_different_key_still_requires_valid_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert _complete(client, t1, "c1", success=True).status_code == 200

    # 任务已结束，使用新 completion_id 必须重新通过租约校验 → invalid_lease
    r = client.post(
        f"/tasks/{t1['task_id']}/complete",
        json={"completion_id": "c2", "lease_token": t1["lease_token"], "success": True},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "invalid_lease"


def test_idempotent_exhausted_failure_blocks_descendants_once(client):
    dag = submit(client, [{"key": "a", "max_retries": 0}, {"key": "b", "deps": ["a"]}])
    dag_id = dag["id"]
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    payload = {"completion_id": "cf", "lease_token": t["lease_token"],
               "success": False, "error": "fatal"}
    r1 = client.post(f"/tasks/{t['task_id']}/complete", json=payload)
    assert r1.json()["state"] == "failed"
    assert r1.json()["failure_count"] == 1
    assert r1.json()["blocked_descendants"] == 1

    # 任务结束后用相同键重放：仍是首次响应，计数与阻塞都不重复发生
    r2 = client.post(f"/tasks/{t['task_id']}/complete", json=payload)
    assert r2.status_code == 200 and r2.text == r1.text
    full = client.get(f"/dags/{dag_id}").json()
    assert full["states"]["failed"] == 1 and full["states"]["blocked"] == 1
    assert client.get(f"/tasks/{t['task_id']}").json()["failure_count"] == 1


def test_idempotency_record_survives_process_restart(tmp_path, monkeypatch):
    """幂等记录持久化在 SQLite 中：重新打开应用后重放仍返回首次响应。"""
    db_file = tmp_path / "persist.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        submit(c1, [{"key": "a"}])
        t = c1.post("/tasks/claim", json={}).json()["claimed"][0]
        r1 = c1.post(f"/tasks/{t['task_id']}/complete",
                     json={"completion_id": "restart-1",
                           "lease_token": t["lease_token"], "success": True})
        assert r1.status_code == 200

    # 模拟进程重启：重新走 lifespan/init_db，旧库中的 completions 记录仍然有效
    with TestClient(app) as c2:
        r2 = c2.post(f"/tasks/{t['task_id']}/complete",
                     json={"completion_id": "restart-1",
                           "lease_token": t["lease_token"], "success": True})
        assert r2.status_code == 200 and r2.text == r1.text

        r3 = c2.post(f"/tasks/{t['task_id']}/complete",
                     json={"completion_id": "restart-1",
                           "lease_token": t["lease_token"], "success": False})
        assert r3.status_code == 409
        assert r3.json()["error"]["code"] == "idempotency_conflict"


# ------------------------------------------------------------- 重试与阻塞

def test_retry_then_success(client):
    submit(client, [{"key": "a", "max_retries": 2}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-r1",
                          "lease_token": t["lease_token"], "success": False, "error": "boom"})
    assert r.json()["state"] == "pending"
    assert r.json()["retries_remaining"] == 2

    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2
    assert client.post(f"/tasks/{t2['task_id']}/complete",
                       json={"completion_id": "cmp-r2",
                             "lease_token": t2["lease_token"], "success": True}).status_code == 200


def test_exhausted_retries_blocks_descendants(client):
    dag = submit(
        client,
        [
            {"key": "a", "max_retries": 1},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["b"]},
        ],
    )
    dag_id = dag["id"]

    for expected_attempt in (1, 2):
        t = client.post("/tasks/claim", json={}).json()["claimed"][0]
        assert t["attempt"] == expected_attempt
        r = client.post(f"/tasks/{t['task_id']}/complete",
                        json={"completion_id": f"cmp-block-{expected_attempt}",
                              "lease_token": t["lease_token"], "success": False, "error": "x"})
    assert r.json()["state"] == "failed"
    assert r.json()["blocked_descendants"] == 2

    # 后继永远领取不到
    assert client.post("/tasks/claim", json={"dag_id": dag_id, "limit": 10}).json()["count"] == 0

    full = client.get(f"/dags/{dag_id}").json()
    b = by_key(full, "b")
    c = by_key(full, "c")
    assert b["state"] == "blocked" and "permanently failed" in b["blocked_reason"]
    assert c["state"] == "blocked" and "permanently failed" in c["blocked_reason"]
    assert full["states"]["failed"] == 1 and full["states"]["blocked"] == 2


def test_lease_expiry_reclaim_does_not_consume_retry_budget(client):
    submit(client, [{"key": "a", "max_retries": 1, "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t1["attempt"] == 1
    time.sleep(0.25)

    # 过期后重领：attempt+1、新令牌，但显式失败计数不变、重试预算保留
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2 and t2["lease_token"] != t1["lease_token"]
    status = client.get(f"/tasks/{t2['task_id']}").json()
    assert status["failure_count"] == 0
    assert status["retries_remaining"] == 1

    # 旧令牌回报失败 -> 409，不得计入失败次数（首次未通过租约校验，不产生幂等记录）
    r = client.post(f"/tasks/{t1['task_id']}/complete",
                    json={"completion_id": "cmp-late",
                          "lease_token": t1["lease_token"], "success": False, "error": "late"})
    assert r.status_code == 409

    # 第一次显式失败：仍可重试（回到 pending），attempts 历史保留过期记录
    r = client.post(f"/tasks/{t2['task_id']}/complete",
                    json={"completion_id": "cmp-e1",
                          "lease_token": t2["lease_token"], "success": False, "error": "e1"})
    assert r.json()["state"] == "pending"
    assert r.json()["failure_count"] == 1
    status = client.get(f"/tasks/{t2['task_id']}").json()
    assert status["state"] == "pending" and status["attempt"] == 2
    assert status["failure_count"] == 1 and status["retries_remaining"] == 0
    assert [a["state"] for a in status["attempts"]] == ["expired", "failed"]

    # 第二次显式失败：预算耗尽 -> failed
    t3 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t3["attempt"] == 3
    r = client.post(f"/tasks/{t3['task_id']}/complete",
                    json={"completion_id": "cmp-e2",
                          "lease_token": t3["lease_token"], "success": False, "error": "e2"})
    assert r.json()["state"] == "failed"
    assert r.json()["failure_count"] == 2
    assert r.json()["retries_remaining"] == 0


def test_retries_remaining_tracks_only_explicit_failures(client):
    submit(client, [{"key": "a", "max_retries": 0, "lease_seconds": 0.2}])
    # 多次过期重领均未显式失败：成功后计数仍为 0
    for expected in (1, 2):
        t = client.post("/tasks/claim", json={}).json()["claimed"][0]
        assert t["attempt"] == expected
        time.sleep(0.25)
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t["attempt"] == 3
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-ok", "lease_token": t["lease_token"], "success": True})
    assert r.json()["state"] == "succeeded"
    assert r.json()["failure_count"] == 0
    status = client.get(f"/tasks/{t['task_id']}").json()
    assert status["failure_count"] == 0 and status["retries_remaining"] == 0
    assert [a["state"] for a in status["attempts"]] == ["expired", "expired", "succeeded"]


def test_legacy_sqlite_db_gets_failure_count_default(tmp_path, monkeypatch):
    """旧版 tasks 表（无 failure_count 列）在 init_db 时补齐默认值 0。"""
    import sqlite3

    db_file = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            state TEXT NOT NULL, blocked_reason TEXT, lease_token TEXT, lease_expires_at TEXT,
            worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key TEXT,
            PRIMARY KEY (dag_id, task_key, dep_key));
        CREATE TABLE attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT, attempt_no INTEGER,
            lease_token TEXT UNIQUE, worker_id TEXT, leased_at TEXT, expires_at TEXT,
            heartbeat_at TEXT, state TEXT, error TEXT);
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        -- 旧任务 attempt=3（曾因过期被重领），max_retries=1，处于 pending
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 1, 30, 'pending', 3, '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    # db_path() 每次调用动态读取环境变量，无需重新导入模块
    from app.db import get_conn, init_db
    init_db()  # 不应报错，且只迁移一次
    init_db()

    conn = sqlite3.connect(db_file)
    row = conn.execute(
        "SELECT attempt, failure_count, max_retries FROM tasks WHERE id = 't1'"
    ).fetchone()
    conn.close()
    assert row == (3, 0, 1)  # 兼容默认值：过期重领不追溯消耗失败预算

    from app import service
    t = service.claim_tasks(None, None, 1)["claimed"][0]
    assert t["attempt"] == 4
    # 第一次显式失败仍有重试额度（旧库迁移后 completions 表可用）
    result = service.complete_task(
        t["task_id"],
        {"completion_id": "cmp-legacy", "lease_token": t["lease_token"], "success": False},
    )
    assert result["state"] == "pending" and result["failure_count"] == 1


# ------------------------------------------------------------- 指数退避

BACKOFF = {"initial_seconds": 0.3, "multiplier": 2, "max_seconds": 5}


def test_backoff_delays_reclaim_with_growth_and_caps(client):
    submit(client, [{"key": "a", "max_retries": 4, "retry_backoff": BACKOFF}])

    # 首次失败：延迟 initial * 2^0 = 0.3s，到期前不可领取，到期后可领
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    start = _utcnow()
    r = _complete(client, t1, "b1", success=False, error="e1")
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "pending"
    assert abs((_parse_iso(body["next_eligible_at"]) - start).total_seconds() - 0.3) < 0.2
    # 退避未到期：pending 任务不可被领取
    assert client.post("/tasks/claim", json={}).json()["count"] == 0
    time.sleep(0.35)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2

    # 第二次失败：延迟 0.3 * 2^1 = 0.6s
    r = _complete(client, t2, "b2", success=False, error="e2")
    assert abs((_parse_iso(r.json()["next_eligible_at"]) - start).total_seconds() - (0.3 + 0.6)) < 0.3
    assert client.post("/tasks/claim", json={}).json()["count"] == 0
    time.sleep(0.65)
    t3 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t3["attempt"] == 3

    # 第三次失败：0.3 * 2^2 = 1.2s，但 cap 到 max_seconds=5 前未到上限；
    # 第四次失败：0.3 * 2^3 = 2.4s，仍未达上限
    r = _complete(client, t3, "b3", success=False, error="e3")
    assert abs((_parse_iso(r.json()["next_eligible_at"]) - start).total_seconds()
               - (0.3 + 0.6 + 1.2)) < 0.4
    time.sleep(1.25)
    t4 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = _complete(client, t4, "b4", success=False, error="e4")
    assert r.json()["state"] == "pending"
    # 第五次显式失败将超出 max_retries=4：本次（第 4 次失败）延迟 2.4s 未被 cap
    assert abs((_parse_iso(r.json()["next_eligible_at"]) - start).total_seconds()
               - (0.3 + 0.6 + 1.2 + 2.4)) < 0.5
    # 领取会清掉 next_eligible_at
    time.sleep(2.45)
    t5 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    status = client.get(f"/tasks/{t5['task_id']}").json()
    assert status["state"] == "running" and status["next_eligible_at"] is None
    # 第 5 次显式失败：额度耗尽 -> failed，无 next_eligible_at
    r = _complete(client, t5, "b5", success=False, error="fatal")
    assert r.json()["state"] == "failed" and r.json()["next_eligible_at"] is None


def test_backoff_delay_capped_at_max_seconds(client):
    submit(
        client,
        [{"key": "a", "max_retries": 3,
          "retry_backoff": {"initial_seconds": 0.3, "multiplier": 10, "max_seconds": 1}}],
    )
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    start = _utcnow()
    r = _complete(client, t1, "c1", success=False)
    assert r.status_code == 200
    # 0.3 * 10^0 = 0.3
    assert abs((_parse_iso(r.json()["next_eligible_at"]) - start).total_seconds() - 0.3) < 0.2
    time.sleep(0.35)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    # 第二次：0.3 * 10^1 = 3 -> cap 为 max_seconds=1
    r = _complete(client, t2, "c2", success=False)
    elapsed = (_parse_iso(r.json()["next_eligible_at"]) - start).total_seconds()
    assert abs(elapsed - (0.3 + 1.0)) < 0.3
    time.sleep(1.05)
    t3 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = _complete(client, t3, "c3", success=False)
    # 第三次同样被 cap 为 1
    assert client.post("/tasks/claim", json={}).json()["count"] == 0
    time.sleep(1.05)
    assert client.post("/tasks/claim", json={}).json()["claimed"][0]["attempt"] == 4


def test_backoff_multiplier_one_constant_delay(client):
    submit(
        client,
        [{"key": "a", "max_retries": 2,
          "retry_backoff": {"initial_seconds": 0.3, "multiplier": 1, "max_seconds": 0.3}}],
    )
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    for cid in ("m1", "m2"):
        start = _utcnow()
        r = _complete(client, t, cid, success=False)
        assert r.status_code == 200 and r.json()["state"] == "pending"
        assert abs((_parse_iso(r.json()["next_eligible_at"]) - start).total_seconds() - 0.3) < 0.2
        assert client.post("/tasks/claim", json={}).json()["count"] == 0
        time.sleep(0.35)
        t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t["attempt"] == 3


def test_lease_expiry_reclaim_ignores_backoff(client):
    submit(
        client,
        [{"key": "a", "max_retries": 2, "lease_seconds": 0.2, "retry_backoff": BACKOFF}],
    )
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    time.sleep(0.25)
    # 租约过期重领不触发退避：无需等待 initial_seconds
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2 and t2["lease_token"] != t1["lease_token"]
    # 显式失败后才进入退避
    r = _complete(client, t2, "e1", success=False)
    assert r.json()["state"] == "pending"
    assert r.json()["next_eligible_at"] is not None
    assert client.post("/tasks/claim", json={}).json()["count"] == 0
    time.sleep(0.35)
    t3 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t3["attempt"] == 3
    # 领取后 next_eligible_at 已清空；租约过期可立即重领（无退避）
    time.sleep(0.25)
    t4 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t4["attempt"] == 4


def test_no_backoff_keeps_immediate_retry(client):
    submit(client, [{"key": "a", "max_retries": 1}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = _complete(client, t, "i1", success=False)
    assert r.json()["state"] == "pending" and r.json()["next_eligible_at"] is None
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2
    status = client.get(f"/tasks/{t2['task_id']}").json()
    assert status["retry_backoff"] is None and status["next_eligible_at"] is None


def test_backoff_rejected_when_validation_fails(client):
    bad_bodies = [
        {"initial_seconds": 0, "multiplier": 1, "max_seconds": 1},          # initial 必须 > 0
        {"initial_seconds": -1, "multiplier": 1, "max_seconds": 1},
        {"initial_seconds": 1, "multiplier": 0.5, "max_seconds": 1},        # multiplier >= 1
        {"initial_seconds": 5, "multiplier": 1, "max_seconds": 1},          # max < initial
        {"initial_seconds": 1, "multiplier": 2},                             # 缺 max_seconds
        {"multiplier": 2, "max_seconds": 1},                                 # 缺 initial
    ]
    for backoff in bad_bodies:
        r = client.post("/dags", json={"tasks": [{"key": "a", "retry_backoff": backoff}]})
        assert r.status_code == 422, backoff
    # 合法策略正常提交
    dag = submit(client, [{"key": "a", "retry_backoff":
                          {"initial_seconds": 1, "multiplier": 1.5, "max_seconds": 10}}])
    assert by_key(dag, "a")["retry_backoff"] == {
        "initial_seconds": 1.0, "multiplier": 1.5, "max_seconds": 10.0,
    }


def test_backoff_visible_in_task_views_and_cleared_on_success(client):
    dag = submit(client, [{"key": "a", "max_retries": 2, "retry_backoff": BACKOFF}])
    expected = {"initial_seconds": 0.3, "multiplier": 2.0, "max_seconds": 5.0}
    assert by_key(dag, "a")["retry_backoff"] == expected
    assert by_key(dag, "a")["next_eligible_at"] is None

    listed = client.get("/tasks", params={"dag_id": dag["id"]}).json()["tasks"]
    assert listed[0]["retry_backoff"] == expected

    t = client.post("/tasks/claim", json={"dag_id": dag["id"]}).json()["claimed"][0]
    detail = client.get(f"/tasks/{t['task_id']}").json()
    assert detail["retry_backoff"] == expected and detail["next_eligible_at"] is None

    r = _complete(client, t, "f1", success=False)
    eligible = r.json()["next_eligible_at"]
    detail = client.get(f"/tasks/{t['task_id']}").json()
    assert detail["state"] == "pending" and detail["next_eligible_at"] == eligible

    # 到期领取后成功：退避时刻清空，策略保留
    time.sleep(0.35)
    t2 = client.post("/tasks/claim", json={"dag_id": dag["id"]}).json()["claimed"][0]
    assert _complete(client, t2, "ok", success=True).status_code == 200
    detail = client.get(f"/tasks/{t2['task_id']}").json()
    assert detail["state"] == "succeeded"
    assert detail["next_eligible_at"] is None
    assert detail["retry_backoff"] == expected


def test_backoff_requeued_event_carries_next_eligible_at(client):
    dag = submit(client, [{"key": "a", "max_retries": 1, "retry_backoff": BACKOFF}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = _complete(client, t, "f1", success=False)
    eligible = r.json()["next_eligible_at"]
    assert eligible is not None

    events = client.get("/events", params={"dag_id": dag["id"]}).json()["events"]
    requeued = events[-1]
    assert requeued["type"] == "task.requeued"
    assert requeued["next_eligible_at"] == eligible

    # 其他事件类型不携带该字段（为 null）
    claimed = next(e for e in events if e["type"] == "task.claimed")
    assert claimed["next_eligible_at"] is None


def test_backoff_persists_across_restart(tmp_path, monkeypatch):
    """退避策略与 next_eligible_at 持久化在 SQLite：重启后仍生效。"""
    db_file = tmp_path / "backoff.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        submit(c1, [{"key": "a", "max_retries": 2, "retry_backoff": BACKOFF}], dag_id="bo")
        t = c1.post("/tasks/claim", json={}).json()["claimed"][0]
        r = c1.post(
            f"/tasks/{t['task_id']}/complete",
            json={"completion_id": "f1", "lease_token": t["lease_token"],
                  "success": False, "error": "e"},
        )
        assert r.status_code == 200 and r.json()["next_eligible_at"] is not None
        task_id = t["task_id"]

    # 重启后：退避未到期不可领取，策略仍可读
    with TestClient(app) as c2:
        assert c2.post("/tasks/claim", json={}).json()["count"] == 0
        detail = c2.get(f"/tasks/{task_id}").json()
        assert detail["retry_backoff"] == {
            "initial_seconds": 0.3, "multiplier": 2.0, "max_seconds": 5.0,
        }
        assert detail["state"] == "pending" and detail["next_eligible_at"] is not None
        assert detail["failure_count"] == 1


def test_legacy_sqlite_db_gets_null_backoff(tmp_path, monkeypatch):
    """旧版 tasks/events 表迁移后补齐退避列（NULL）：立即重试、事件字段为 null。"""
    import sqlite3

    db_file = tmp_path / "legacy-backoff.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            state TEXT NOT NULL, blocked_reason TEXT, lease_token TEXT, lease_expires_at TEXT,
            worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0, failure_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key TEXT,
            PRIMARY KEY (dag_id, task_key, dep_key));
        CREATE TABLE attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT, attempt_no INTEGER,
            lease_token TEXT UNIQUE, worker_id TEXT, leased_at TEXT, expires_at TEXT,
            heartbeat_at TEXT, state TEXT, error TEXT);
        CREATE TABLE completions (
            task_id TEXT NOT NULL, completion_id TEXT NOT NULL, lease_token TEXT NOT NULL,
            success INTEGER NOT NULL, fingerprint TEXT NOT NULL, status_code INTEGER NOT NULL,
            response_body TEXT NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY (task_id, completion_id));
        CREATE TABLE events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL,
            dag_id TEXT NOT NULL, task_id TEXT, attempt INTEGER, from_state TEXT,
            to_state TEXT, occurred_at TEXT NOT NULL);
        CREATE TABLE concurrency_groups (name TEXT PRIMARY KEY, limit_value INTEGER NOT NULL,
            created_at TEXT NOT NULL);
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 2, 30, 'pending', 0, '2026-01-01T00:00:00Z');
        INSERT INTO events (event_type, dag_id, occurred_at)
        VALUES ('dag.created', 'd1', '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    from app.db import init_db
    from app import service

    init_db()
    init_db()  # 迁移幂等

    # 旧任务无退避策略、立即可领
    t = service.claim_tasks(None, None, 1)["claimed"][0]
    assert t["attempt"] == 1
    result = service.complete_task(
        t["task_id"],
        {"completion_id": "cmp-legacy-bo", "lease_token": t["lease_token"], "success": False},
    )
    assert result["state"] == "pending"
    assert result["next_eligible_at"] is None  # 未配置退避：立即重试

    detail = service.get_task(t["task_id"])
    assert detail["retry_backoff"] is None and detail["next_eligible_at"] is None
    # 旧事件迁移后字段可读且为 null；新 requeued 事件同样为 null
    events = service.list_events(0, 50, None)["events"]
    assert all(e["next_eligible_at"] is None for e in events)


# ------------------------------------------------------------- 状态接口

def test_status_lists_attempts_and_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 60}])
    t = client.post("/tasks/claim", json={"worker_id": "w-1"}).json()["claimed"][0]
    client.post(f"/tasks/{t['task_id']}/heartbeat",
                json={"lease_token": t["lease_token"]})

    status = client.get(f"/tasks/{t['task_id']}").json()
    assert status["attempt"] == 1
    assert status["state"] == "running"
    assert status["lease"]["token"] == t["lease_token"]
    assert status["lease"]["expired"] is False
    assert status["lease"]["worker_id"] == "w-1"
    att = status["attempts"][0]
    assert att["attempt"] == 1 and att["state"] == "active"
    assert att["heartbeat_at"] is not None

    # 列表接口支持状态过滤
    r = client.get("/tasks", params={"state": "running"})
    assert r.json()["count"] == 1


# ------------------------------------------------------------- 原子领取
def test_concurrent_claim_is_atomic(client):
    submit(client, [{"key": "a"}])
    results = []
    errors = []

    def worker():
        try:
            r = client.post("/tasks/claim", json={"worker_id": f"w-{threading.get_ident()}"})
            results.extend(r.json()["claimed"])
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    assert len(results) == 1
    assert results[0]["attempt"] == 1


# ------------------------------------------------------------- 事件流

def _all_events(client, **params):
    """逐页拉取全部事件，断言跨页无重复/遗漏。"""
    seen = []
    after = params.get("after", 0)
    while True:
        page = client.get("/events", params={**params, "after": after}).json()
        seqs = [e["sequence"] for e in page["events"]]
        assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))
        seen.extend(page["events"])
        if not page["events"]:
            assert page["next_after"] == after  # 空页沿用 after
            break
        assert page["next_after"] == seqs[-1]
        after = page["next_after"]
    return seen


def test_dag_created_event_nulls_task_fields(client):
    dag = submit(client, [{"key": "a"}], dag_id="dag-evt-1")
    events = client.get("/events", params={"dag_id": "dag-evt-1"}).json()["events"]
    assert len(events) == 1
    e = events[0]
    assert e["sequence"] >= 1
    assert e["type"] == "dag.created"
    assert e["dag_id"] == dag["id"]
    assert e["task_id"] is None
    assert e["attempt"] is None
    assert e["from_state"] is None
    assert e["to_state"] is None
    assert e["occurred_at"]  # 非空时间戳


def test_task_lifecycle_event_sequence(client):
    dag = submit(
        client,
        [
            {"key": "a", "max_retries": 1},
            {"key": "b", "deps": ["a"]},
        ],
    )
    dag_id = dag["id"]

    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    claimed = evts[-1]
    assert claimed["type"] == "task.claimed"
    assert claimed["task_id"] == t["task_id"]
    assert claimed["attempt"] == 1
    assert claimed["from_state"] == "pending"
    assert claimed["to_state"] == "running"

    # 显式失败但仍有额度 -> requeued: running -> pending
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "c-fail", "lease_token": t["lease_token"],
                          "success": False, "error": "boom"})
    assert r.status_code == 200
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    requeued = evts[-1]
    assert requeued["type"] == "task.requeued"
    assert requeued["attempt"] == 1
    assert requeued["from_state"] == "running"
    assert requeued["to_state"] == "pending"
    # 未配置退避：事件携带 null
    assert requeued["next_eligible_at"] is None

    # 重新领取 -> claimed（pending -> running，attempt=2）
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    claimed2 = evts[-1]
    assert claimed2["type"] == "task.claimed"
    assert claimed2["attempt"] == 2
    assert claimed2["from_state"] == "pending"
    assert claimed2["to_state"] == "running"

    # 再次失败且额度耗尽（max_retries=1，failure_count=2）-> failed，后继 b -> blocked
    client.post(f"/tasks/{t2['task_id']}/complete",
                json={"completion_id": "c-fail-2", "lease_token": t2["lease_token"],
                      "success": False, "error": "fatal"})
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    failed, blocked = evts[-2], evts[-1]
    assert failed["type"] == "task.failed"
    assert failed["task_id"] == t2["task_id"]
    assert failed["attempt"] == 2
    assert failed["from_state"] == "running" and failed["to_state"] == "failed"
    assert blocked["type"] == "task.blocked"
    assert blocked["task_id"] != t2["task_id"]
    assert blocked["from_state"] == "pending" and blocked["to_state"] == "blocked"

    # sequence 全局严格递增，且 failed 先于 blocked
    seqs = [e["sequence"] for e in evts]
    assert seqs == sorted(set(seqs))
    assert failed["sequence"] < blocked["sequence"]


def test_succeeded_event(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    client.post(f"/tasks/{t['task_id']}/complete",
                json={"completion_id": "c-ok", "lease_token": t["lease_token"], "success": True})
    events = client.get("/events").json()["events"]
    succeeded = events[-1]
    assert succeeded["type"] == "task.succeeded"
    assert succeeded["task_id"] == t["task_id"]
    assert succeeded["attempt"] == 1
    assert succeeded["from_state"] == "running"
    assert succeeded["to_state"] == "succeeded"


def test_claim_after_lease_expiry_event_from_running(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2
    events = client.get("/events").json()["events"]
    claimed2 = events[-1]
    assert claimed2["type"] == "task.claimed"
    assert claimed2["attempt"] == 2
    assert claimed2["from_state"] == "running"  # 过期重领：任务此前停留在 running
    assert claimed2["to_state"] == "running"


def test_events_pagination_no_gaps_or_duplicates(client):
    # 制造超过单页容量的事件：3 个 DAG，每个 2 个任务走完整流程
    for i in range(3):
        dag = submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id=f"page-{i}")
        for _ in range(2):
            t = client.post("/tasks/claim", json={"dag_id": dag["id"]}).json()["claimed"][0]
            client.post(f"/tasks/{t['task_id']}/complete",
                        json={"completion_id": f"c-{i}-{t['task_key']}",
                              "lease_token": t["lease_token"], "success": True})

    full = client.get("/events", params={"limit": 100}).json()["events"]
    assert len(full) > 3

    # 逐页（limit=1）拉取应与一次性拉取完全一致
    paged = _all_events(client, limit=1)
    assert [e["sequence"] for e in paged] == [e["sequence"] for e in full]

    # 末页之后再翻页：空页沿用 after
    last_seq = full[-1]["sequence"]
    tail = client.get("/events", params={"after": last_seq, "limit": 10}).json()
    assert tail == {"events": [], "next_after": last_seq}


def test_events_filter_by_dag_id_across_global_sequence(client):
    d1 = submit(client, [{"key": "a"}], dag_id="f-1")
    d2 = submit(client, [{"key": "x"}], dag_id="f-2")
    for dag in (d1, d2):
        t = client.post("/tasks/claim", json={"dag_id": dag["id"]}).json()["claimed"][0]
        client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": f"c-{dag['id']}",
                          "lease_token": t["lease_token"], "success": True})

    paged = _all_events(client, dag_id="f-1", limit=1)
    assert {e["dag_id"] for e in paged} == {"f-1"}
    assert [e["type"] for e in paged] == ["dag.created", "task.claimed", "task.succeeded"]

    # 过滤后无新事件：空页沿用传入的全局 after
    tail = client.get("/events", params={"dag_id": "f-1", "after": 10_000}).json()
    assert tail == {"events": [], "next_after": 10_000}


def test_events_default_params(client):
    submit(client, [{"key": "a"}])
    body = client.get("/events").json()
    assert body["next_after"] == body["events"][-1]["sequence"]
    assert body["events"][0]["type"] == "dag.created"


def test_events_rejects_invalid_params(client):
    assert client.get("/events", params={"limit": 0}).status_code == 422
    assert client.get("/events", params={"limit": 101}).status_code == 422
    assert client.get("/events", params={"after": -1}).status_code == 422


def test_idempotent_replay_emits_no_event(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    payload = {"completion_id": "c1", "lease_token": t["lease_token"], "success": True}
    assert client.post(f"/tasks/{t['task_id']}/complete", json=payload).status_code == 200
    before = client.get("/events", params={"limit": 100}).json()["events"]

    # 幂等重放不产生新事件
    assert client.post(f"/tasks/{t['task_id']}/complete", json=payload).status_code == 200
    after = client.get("/events", params={"limit": 100}).json()["events"]
    assert [e["sequence"] for e in after] == [e["sequence"] for e in before]


def test_failed_submit_rolls_back_event(client):
    submit(client, [{"key": "a"}], dag_id="dup-dag")
    # 重复 DAG id：dag 行插入即冲突，整个事务回滚，不留第二条创建事件
    r = client.post("/dags", json={"id": "dup-dag", "tasks": [{"key": "b"}]})
    assert r.status_code == 409
    events = client.get("/events", params={"dag_id": "dup-dag"}).json()["events"]
    assert len(events) == 1 and events[0]["type"] == "dag.created"

    # 图校验失败（环）发生在开事务之前：完全不留事件
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["a"]}]})
    assert r.status_code == 422
    all_events = client.get("/events", params={"limit": 100}).json()["events"]
    assert len(all_events) == 1


def test_events_persist_across_restart(tmp_path, monkeypatch):
    db_file = tmp_path / "events.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        submit(c1, [{"key": "a"}], dag_id="restart-dag")

    with TestClient(app) as c2:
        events = c2.get("/events", params={"dag_id": "restart-dag"}).json()["events"]
        assert len(events) == 1
        assert events[0]["type"] == "dag.created"
        assert events[0]["sequence"] == 1  # 重启后 sequence 不重置，不漏不重


def test_event_stream_is_immutable_in_storage(client):
    submit(client, [{"key": "a"}])
    import sqlite3
    from app.db import db_path

    conn = sqlite3.connect(db_path())
    with pytest.raises(sqlite3.Error):
        conn.execute("UPDATE events SET event_type = 'tampered' WHERE sequence = 1")
    conn.rollback()
    with pytest.raises(sqlite3.Error):
        conn.execute("DELETE FROM events")
    conn.rollback()
    conn.close()

    events = client.get("/events").json()["events"]
    assert events[0]["type"] == "dag.created"


# ------------------------------------------------------------- 跨 DAG 并发组限流

def test_concurrency_fields_must_come_together(client):
    # 只给组名不给上限 / 只给上限不给组名 -> 422（pydantic 层）
    r = client.post("/dags", json={"tasks": [{"key": "a", "concurrency_group": "g"}]})
    assert r.status_code == 422

    r = client.post("/dags", json={"tasks": [{"key": "a", "concurrency_limit": 2}]})
    assert r.status_code == 422


def test_concurrency_limit_must_be_positive(client):
    for bad in (0, -1):
        r = client.post(
            "/dags",
            json={"tasks": [{"key": "a", "concurrency_group": "g", "concurrency_limit": bad}]},
        )
        assert r.status_code == 422


def test_same_submit_group_with_different_limits_rejected(client):
    r = client.post(
        "/dags",
        json={
            "tasks": [
                {"key": "a", "concurrency_group": "g", "concurrency_limit": 1},
                {"key": "b", "concurrency_group": "g", "concurrency_limit": 2},
            ]
        },
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "concurrency_group_conflict"
    assert r.json()["error"]["details"] == {"group": "g", "existing_limit": 1, "submitted_limit": 2}
    # 请求自相矛盾在开写事务前拒绝：不留任何痕迹
    assert client.get("/events", params={"limit": 100}).json()["events"] == []


def test_group_cap_holds_within_dag_and_skips_to_fill_limit(client):
    submit(
        client,
        [
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g2", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g3", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "u1"},
            {"key": "u2"},
        ],
    )
    r = client.post("/tasks/claim", json={"limit": 5})
    assert r.status_code == 200
    keys = [t["task_key"] for t in r.json()["claimed"]]
    # 组内只放进 2 个（g3 被跳过），继续向后把无组任务收入，尽量填满 limit
    assert keys == ["g1", "g2", "u1", "u2"]
    for t in r.json()["claimed"]:
        if t["task_key"].startswith("g"):
            assert t["concurrency_group"] == "g"
        else:
            assert t["concurrency_group"] is None

    # 组已满且无其他候选：空领取
    assert client.post("/tasks/claim", json={"limit": 10}).json()["count"] == 0


def test_group_cap_is_cross_dag(client):
    submit(
        client,
        [{"key": "a1", "concurrency_group": "shared", "concurrency_limit": 2}],
        dag_id="dag-a",
    )
    submit(
        client,
        [
            {"key": "b1", "concurrency_group": "shared", "concurrency_limit": 2},
            {"key": "b2", "concurrency_group": "shared", "concurrency_limit": 2},
            {"key": "free"},
        ],
        dag_id="dag-b",
    )

    # 在 dag-a 内占掉 1 个名额
    a = client.post("/tasks/claim", json={"dag_id": "dag-a", "limit": 5}).json()["claimed"]
    assert [t["task_key"] for t in a] == ["a1"]

    # 全局只剩 1 个组名额：b1 占位，b2 跳过，继续用无组 free 填满 limit
    r = client.post("/tasks/claim", json={"dag_id": "dag-b", "limit": 5})
    assert [t["task_key"] for t in r.json()["claimed"]] == ["b1", "free"]

    # 两个 DAG 都不再有可领取任务（b2 被组容量挡住）
    assert client.post("/tasks/claim", json={"dag_id": "dag-a"}).json()["count"] == 0
    assert client.post("/tasks/claim", json={"dag_id": "dag-b"}).json()["count"] == 0
    assert client.post("/tasks/claim", json={}).json()["count"] == 0


def test_persisted_same_group_limit_must_match_and_rolls_back(client):
    submit(client, [{"key": "a", "concurrency_group": "g", "concurrency_limit": 2}], dag_id="d1")

    # 同名组不同上限 -> 409，整批回滚：DAG、任务、事件均不存在
    r = client.post(
        "/dags",
        json={
            "id": "d2",
            "tasks": [
                {"key": "b", "concurrency_group": "g", "concurrency_limit": 3},
            ],
        },
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "concurrency_group_conflict"
    assert r.json()["error"]["details"] == {"group": "g", "existing_limit": 2, "submitted_limit": 3}
    assert client.get("/dags/d2").status_code == 404
    assert client.get("/events", params={"dag_id": "d2"}).json()["events"] == []

    # 同名组相同上限 -> 正常提交
    dag = submit(
        client,
        [{"key": "c", "concurrency_group": "g", "concurrency_limit": 2}],
        dag_id="d3",
    )
    assert dag["task_count"] == 1


def test_persisted_conflict_rolls_back_whole_batch(client):
    submit(
        client,
        [{"key": "a", "concurrency_group": "g", "concurrency_limit": 1}],
        dag_id="p1",
    )
    # 同批里既有冲突组任务也有正常无组任务：整体回滚，无组任务同样不存在
    r = client.post(
        "/dags",
        json={
            "id": "p2",
            "tasks": [
                {"key": "x", "concurrency_group": "g", "concurrency_limit": 5},
                {"key": "y"},
            ],
        },
    )
    assert r.status_code == 409
    assert client.get("/dags/p2").status_code == 404
    # 上限未被改写
    submit(
        client,
        [{"key": "z", "concurrency_group": "g", "concurrency_limit": 1}],
        dag_id="p3",
    )


def test_slot_released_on_success_and_failure(client):
    submit(
        client,
        [{"key": "a", "concurrency_group": "g", "concurrency_limit": 1}],
        dag_id="rel-s",
    )
    submit(
        client,
        [{"key": "b", "concurrency_group": "g", "concurrency_limit": 1}],
        dag_id="rel-s2",
    )
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert client.post("/tasks/claim", json={}).json()["count"] == 0

    # 成功释放名额
    assert (
        client.post(
            f"/tasks/{t['task_id']}/complete",
            json={"completion_id": "ok", "lease_token": t["lease_token"], "success": True},
        ).status_code
        == 200
    )
    nxt = client.post("/tasks/claim", json={}).json()["claimed"]
    assert len(nxt) == 1

    # 显式失败（仍有重试额度，回 pending）同样释放名额，失败任务自己可重领
    submit(
        client,
        [{"key": "c", "concurrency_group": "g2", "concurrency_limit": 1, "max_retries": 1}],
        dag_id="rel-f",
    )
    tc = client.post("/tasks/claim", json={"dag_id": "rel-f"}).json()["claimed"][0]
    assert (
        client.post(
            f"/tasks/{tc['task_id']}/complete",
            json={"completion_id": "fail-1", "lease_token": tc["lease_token"],
                  "success": False, "error": "boom"},
        ).json()["state"]
        == "pending"
    )
    reclaimed = client.post("/tasks/claim", json={"dag_id": "rel-f"}).json()["claimed"]
    assert len(reclaimed) == 1 and reclaimed[0]["task_id"] == tc["task_id"]

    # 永久失败也释放名额（任务不再可领，但组内其他任务可以）
    submit(
        client,
        [{"key": "d", "concurrency_group": "g3", "concurrency_limit": 1, "max_retries": 0}],
        dag_id="rel-f2",
    )
    submit(
        client,
        [{"key": "e", "concurrency_group": "g3", "concurrency_limit": 1}],
        dag_id="rel-f3",
    )
    td = client.post("/tasks/claim", json={"dag_id": "rel-f2"}).json()["claimed"][0]
    client.post(
        f"/tasks/{td['task_id']}/complete",
        json={"completion_id": "fail-2", "lease_token": td["lease_token"],
              "success": False, "error": "fatal"},
    )
    e = client.post("/tasks/claim", json={"dag_id": "rel-f3"}).json()["claimed"]
    assert [t["task_key"] for t in e] == ["e"]


def test_slot_released_after_lease_expiry(client):
    submit(
        client,
        [{"key": "a", "lease_seconds": 0.2, "concurrency_group": "g", "concurrency_limit": 1}],
        dag_id="exp-1",
    )
    submit(
        client,
        [{"key": "b", "concurrency_group": "g", "concurrency_limit": 1}],
        dag_id="exp-2",
    )
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert client.post("/tasks/claim", json={}).json()["count"] == 0

    # 租约未过期时名额仍占用；过期后名额自动释放：b 可被领取（a 同时也回到候选集，
    # 按创建顺序 a 更晚过期可重领——这里断言 b 出现在某次可领取结果即可）
    time.sleep(0.25)
    keys = {t["task_key"] for t in client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]}
    # a 过期重领占回唯一名额，或 b 抢到名额：二者恰有一个在组内被领取
    assert "a" in keys or "b" in keys
    assert len(keys) == 1
    # 再领为空，证明跨 DAG 容量仍为 1
    assert client.post("/tasks/claim", json={"limit": 10}).json()["count"] == 0


def test_ungrouped_tasks_unaffected(client):
    # 无组任务即使存在已满的组，仍可正常批量领取
    submit(
        client,
        [
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 1},
            {"key": "u1"},
            {"key": "u2"},
            {"key": "u3"},
        ],
    )
    first = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert [t["task_key"] for t in first] == ["g1", "u1", "u2", "u3"]

    # 无组任务视图中 concurrency 为 null
    for t in first:
        detail = client.get(f"/tasks/{t['task_id']}").json()
        if t["task_key"] == "g1":
            assert detail["concurrency"] == {"group": "g", "limit": 1}
        else:
            assert detail["concurrency"] is None


def test_concurrency_info_in_task_views(client):
    submit(
        client,
        [{"key": "a", "concurrency_group": "grp", "concurrency_limit": 3}],
        dag_id="view-g",
    )
    dag = client.get("/dags/view-g").json()
    assert by_key(dag, "a")["concurrency"] == {"group": "grp", "limit": 3}

    listed = client.get("/tasks", params={"dag_id": "view-g"}).json()["tasks"]
    assert listed[0]["concurrency"] == {"group": "grp", "limit": 3}


def test_interleaved_groups_skip_full_and_continue_to_other_groups(client):
    # 候选顺序：ga(g 组, 上限1)、gb1/gb2(h 组, 上限2)。g 占满后仍可继续领取 h 组
    submit(
        client,
        [
            {"key": "ga", "concurrency_group": "g", "concurrency_limit": 1},
            {"key": "gb1", "concurrency_group": "h", "concurrency_limit": 2},
            {"key": "gb2", "concurrency_group": "h", "concurrency_limit": 2},
        ],
    )
    keys = [t["task_key"] for t in client.post("/tasks/claim", json={"limit": 3}).json()["claimed"]]
    assert keys == ["ga", "gb1", "gb2"]


def test_heartbeat_keeps_group_slot_occupied(client):
    submit(
        client,
        [{"key": "a", "lease_seconds": 0.3, "concurrency_group": "g", "concurrency_limit": 1}],
        dag_id="hb-1",
    )
    submit(
        client,
        [{"key": "b", "concurrency_group": "g", "concurrency_limit": 1}],
        dag_id="hb-2",
    )
    t = client.post("/tasks/claim", json={"dag_id": "hb-1"}).json()["claimed"][0]
    time.sleep(0.2)
    # 续租把过期时间推后：名额继续占用，b 仍不可领
    assert (
        client.post(f"/tasks/{t['task_id']}/heartbeat", json={"lease_token": t["lease_token"]}).status_code
        == 200
    )
    time.sleep(0.2)  # 原本此刻租约已过期
    assert client.post("/tasks/claim", json={"dag_id": "hb-2"}).json()["count"] == 0


def test_group_limit_persists_across_restart(tmp_path, monkeypatch):
    """并发组上限持久化在 SQLite：重启后同名组不同上限仍被拒绝，限流仍然生效。"""
    db_file = tmp_path / "groups.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        submit(c1, [{"key": "a", "concurrency_group": "g", "concurrency_limit": 2}], dag_id="gd-1")

    with TestClient(app) as c2:
        # 同名组不同上限：重启后持久化的上限仍生效
        r = c2.post(
            "/dags",
            json={"id": "gd-2", "tasks": [{"key": "b", "concurrency_group": "g", "concurrency_limit": 3}]},
        )
        assert r.status_code == 409
        assert r.json()["error"]["details"] == {
            "group": "g", "existing_limit": 2, "submitted_limit": 3
        }
        # 相同上限正常
        submit(c2, [{"key": "c", "concurrency_group": "g", "concurrency_limit": 2}], dag_id="gd-3")
        claimed = c2.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
        assert len(claimed) == 2  # 跨重启后的两个 DAG 共享容量 2


def test_legacy_sqlite_db_gets_concurrency_group_null(tmp_path, monkeypatch):
    """旧版 tasks 表（无 concurrency_group 列）迁移后补 NULL，行为与无组任务一致。"""
    import sqlite3

    db_file = tmp_path / "legacy-group.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            state TEXT NOT NULL, blocked_reason TEXT, lease_token TEXT, lease_expires_at TEXT,
            worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key TEXT,
            PRIMARY KEY (dag_id, task_key, dep_key));
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 0, 30, 'pending', 0, '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    from app.db import init_db
    from app import service

    init_db()
    init_db()  # 迁移幂等

    # 迁移后可声明新并发组，且旧任务（NULL 组）不受限流影响
    service.submit_dag(
        {
            "id": "d2",
            "tasks": [{"key": "b", "concurrency_group": "g", "concurrency_limit": 1}],
        }
    )
    claimed = service.claim_tasks(None, None, 10)["claimed"]
    assert {t["task_key"] for t in claimed} == {"a", "b"}
    old = next(t for t in claimed if t["task_key"] == "a")
    assert old["concurrency_group"] is None


# ------------------------------------------------------------- 只读分析

def test_analysis_computes_schedule_critical_path_and_levels(client):
    submit(
        client,
        [
            {"key": "a", "duration_seconds": 3},
            {"key": "b", "deps": ["a"], "duration_seconds": 2},
            {"key": "c", "deps": ["a"], "duration_seconds": 5},
            {"key": "d", "deps": ["b", "c"]},  # 未声明时长 -> 默认 1
        ],
        dag_id="ana-1",
    )
    r = client.get("/dags/ana-1/analysis")
    assert r.status_code == 200
    body = r.json()
    assert body["dag_id"] == "ana-1"
    assert body["task_count"] == 4

    tasks = {t["key"]: t for t in body["tasks"]}
    # a: 0-3；b: 3-5；c: 3-8；d: 8-9
    assert (tasks["a"]["earliest_start"], tasks["a"]["earliest_finish"]) == (0, 3)
    assert (tasks["b"]["earliest_start"], tasks["b"]["earliest_finish"]) == (3, 5)
    assert (tasks["c"]["earliest_start"], tasks["c"]["earliest_finish"]) == (3, 8)
    assert (tasks["d"]["earliest_start"], tasks["d"]["earliest_finish"]) == (8, 9)
    assert tasks["d"]["duration_seconds"] == 1  # 默认时长

    assert body["total_duration_seconds"] == 9
    assert body["critical_path"] == ["a", "c", "d"]
    assert body["critical_tasks"] == ["a", "c", "d"]
    assert tasks["a"]["critical"] is True and tasks["a"]["slack"] == 0
    assert tasks["b"]["critical"] is False and tasks["b"]["slack"] == 3
    assert tasks["b"]["latest_start"] == 6 and tasks["b"]["latest_finish"] == 8

    # 并行层级：同层互不依赖
    assert body["levels"] == [["a"], ["b", "c"], ["d"]]
    assert [tasks[k]["level"] for k in "abcd"] == [0, 1, 1, 2]


def test_analysis_default_duration_is_one(client):
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="ana-def")
    body = client.get("/dags/ana-def/analysis").json()
    assert body["total_duration_seconds"] == 2
    assert body["critical_path"] == ["a", "b"]
    assert body["critical_tasks"] == ["a", "b"]
    assert body["levels"] == [["a"], ["b"]]


def test_analysis_fractional_durations(client):
    submit(
        client,
        [
            {"key": "a", "duration_seconds": 0.5},
            {"key": "b", "deps": ["a"], "duration_seconds": 0.25},
        ],
        dag_id="ana-frac",
    )
    body = client.get("/dags/ana-frac/analysis").json()
    assert body["total_duration_seconds"] == 0.75
    assert body["critical_path"] == ["a", "b"]


def test_analysis_tied_critical_paths_are_deterministic(client):
    # 两条等长关键路径 a->c 与 b->c：关键任务集合包含全部，路径确定唯一
    submit(
        client,
        [
            {"key": "a", "duration_seconds": 2},
            {"key": "b", "duration_seconds": 2},
            {"key": "c", "deps": ["a", "b"], "duration_seconds": 1},
        ],
        dag_id="ana-tie",
    )
    body = client.get("/dags/ana-tie/analysis").json()
    assert body["total_duration_seconds"] == 3
    assert body["critical_tasks"] == ["a", "b", "c"]
    assert body["critical_path"] == ["a", "c"]  # 平局取较小 key
    assert client.get("/dags/ana-tie/analysis").json() == body  # 可重复且一致


def test_analysis_unknown_dag_returns_404(client):
    r = client.get("/dags/nope/analysis")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "dag_not_found"


def test_submit_rejects_non_positive_duration(client):
    for bad in (0, -2, -0.5):
        r = client.post("/dags", json={"tasks": [{"key": "a", "duration_seconds": bad}]})
        assert r.status_code == 422


def test_analysis_is_read_only(client):
    submit(
        client,
        [{"key": "a", "duration_seconds": 2}, {"key": "b", "deps": ["a"]}],
        dag_id="ana-ro",
    )
    before_events = client.get("/events", params={"dag_id": "ana-ro"}).json()
    before_dag = client.get("/dags/ana-ro").json()

    r1 = client.get("/dags/ana-ro/analysis")
    r2 = client.get("/dags/ana-ro/analysis")
    assert r1.status_code == 200 and r1.json() == r2.json()

    # 分析不改变任务状态，也不产生事件
    assert client.get("/events", params={"dag_id": "ana-ro"}).json() == before_events
    assert client.get("/dags/ana-ro").json() == before_dag


def test_analysis_rejects_corrupt_duration_in_storage(client):
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="ana-bad")
    import sqlite3
    from app.db import db_path

    conn = sqlite3.connect(db_path())
    conn.execute(
        "UPDATE tasks SET duration_seconds = 0 WHERE dag_id = 'ana-bad' AND task_key = 'a'"
    )
    conn.commit()
    conn.close()

    r = client.get("/dags/ana-bad/analysis")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_duration"
    assert r.json()["error"]["details"]["keys"] == ["a"]


def test_analysis_rejects_injected_cycle(client):
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="ana-cyc")
    import sqlite3
    from app.db import db_path

    # 绕过提交校验直接写入一条成环的边
    conn = sqlite3.connect(db_path())
    conn.execute(
        "INSERT INTO dependencies (dag_id, task_key, dep_key) VALUES ('ana-cyc', 'a', 'b')"
    )
    conn.commit()
    conn.close()

    r = client.get("/dags/ana-cyc/analysis")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"


def test_analysis_rejects_injected_missing_dependency(client):
    submit(client, [{"key": "a"}], dag_id="ana-miss")
    import sqlite3
    from app.db import db_path

    conn = sqlite3.connect(db_path())
    conn.execute(
        "INSERT INTO dependencies (dag_id, task_key, dep_key) VALUES ('ana-miss', 'a', 'ghost')"
    )
    conn.commit()
    conn.close()

    r = client.get("/dags/ana-miss/analysis")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "missing_dependency"
    assert "ghost" in r.json()["error"]["details"]["missing"]


def test_duration_seconds_exposed_in_task_views(client):
    dag = submit(client, [{"key": "a", "duration_seconds": 7}], dag_id="ana-view")
    assert by_key(dag, "a")["duration_seconds"] == 7

    overview = client.get("/dags/ana-view").json()
    assert by_key(overview, "a")["duration_seconds"] == 7

    task_id = overview["tasks"][0]["id"]
    detail = client.get(f"/tasks/{task_id}").json()
    assert detail["duration_seconds"] == 7


def test_legacy_sqlite_db_gets_duration_default(tmp_path, monkeypatch):
    """旧版 tasks 表（无 duration_seconds 列）迁移后默认 1，分析可直接使用。"""
    import sqlite3

    db_file = tmp_path / "legacy-analysis.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            state TEXT NOT NULL, blocked_reason TEXT, lease_token TEXT, lease_expires_at TEXT,
            worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key TEXT,
            PRIMARY KEY (dag_id, task_key, dep_key));
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 0, 30, 'pending', 0, '2026-01-01T00:00:00Z');
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t2', 'd1', 'b', 'b', 0, 30, 'pending', 0, '2026-01-01T00:00:00Z');
        INSERT INTO dependencies VALUES ('d1', 'b', 'a');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    from app.db import init_db
    from app import service

    init_db()
    init_db()  # 迁移幂等

    analysis = service.analyze_dag("d1")
    assert analysis["total_duration_seconds"] == 1 + 1  # 历史任务默认时长 1
    assert analysis["critical_path"] == ["a", "b"]
    assert analysis["levels"] == [["a"], ["b"]]


# ------------------------------------------------------------- 失败恢复

def _fail(client, task, cid):
    """显式失败（重试耗尽 -> failed）并断言进入最终失败状态。"""
    r = client.post(
        f"/tasks/{task['task_id']}/complete",
        json={"completion_id": cid, "lease_token": task["lease_token"],
              "success": False, "error": "fatal"},
    )
    assert r.status_code == 200 and r.json()["state"] == "failed"
    return r


def test_resume_failed_task_restores_pending_and_emits_event(client):
    submit(client, [{"key": "a", "max_retries": 0}], dag_id="rs-1")
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    _fail(client, t, "f1")
    detail = client.get(f"/tasks/{t['task_id']}").json()
    assert detail["state"] == "failed" and detail["finished_at"] is not None

    r = client.post(f"/tasks/{t['task_id']}/resume")
    assert r.status_code == 200
    body = r.json()
    assert body["task_id"] == t["task_id"]
    assert body["state"] == "pending"
    assert body["attempt"] == 1
    assert body["unblocked_descendants"] == 0

    # 租约与完成时间已清除；失败计数与重试预算不重置
    detail = client.get(f"/tasks/{t['task_id']}").json()
    assert detail["state"] == "pending"
    assert detail["lease"] is None
    assert detail["finished_at"] is None
    assert detail["failure_count"] == 1
    assert detail["retries_remaining"] == 0

    # task.resumed 事件：failed -> pending
    events = client.get("/events", params={"dag_id": "rs-1"}).json()["events"]
    assert events[-1]["type"] == "task.resumed"
    assert events[-1]["task_id"] == t["task_id"]
    assert events[-1]["from_state"] == "failed"
    assert events[-1]["to_state"] == "pending"

    # 恢复后立即可被重新领取；再次显式失败因预算耗尽直接回到 failed
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["task_id"] == t["task_id"] and t2["attempt"] == 2
    _fail(client, t2, "f2")
    assert client.get(f"/tasks/{t['task_id']}").json()["state"] == "failed"


def test_resume_recursively_unblocks_descendants(client):
    submit(
        client,
        [
            {"key": "a", "max_retries": 0},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["b"]},
            {"key": "d", "deps": ["a"]},
        ],
        dag_id="rs-2",
    )
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = _fail(client, t, "f1")
    assert r.json()["blocked_descendants"] == 3

    r = client.post(f"/tasks/{t['task_id']}/resume")
    assert r.status_code == 200 and r.json()["unblocked_descendants"] == 3

    full = client.get("/dags/rs-2").json()
    assert full["states"]["pending"] == 4
    for key in ("b", "c", "d"):
        task = by_key(full, key)
        assert task["state"] == "pending" and task["blocked_reason"] is None

    # 事件因果顺序：根 failed->pending 在前，后继 blocked->pending 连锁在后
    events = [e for e in client.get("/events", params={"dag_id": "rs-2"}).json()["events"]
              if e["type"] == "task.resumed"]
    assert len(events) == 4
    assert events[0]["task_id"] == t["task_id"]
    assert events[0]["from_state"] == "failed" and events[0]["to_state"] == "pending"
    assert all(e["from_state"] == "blocked" and e["to_state"] == "pending"
               for e in events[1:])

    # 解锁后的链路可正常执行到成功
    for _ in range(4):
        claimed = client.post("/tasks/claim", json={"dag_id": "rs-2", "limit": 10}).json()["claimed"]
        for task in claimed:
            client.post(
                f"/tasks/{task['task_id']}/complete",
                json={"completion_id": f"ok-{task['task_key']}",
                      "lease_token": task["lease_token"], "success": True},
            )
    assert client.get("/dags/rs-2").json()["states"]["succeeded"] == 4


def test_resume_keeps_blocked_while_other_upstream_failed(client):
    submit(
        client,
        [
            {"key": "a", "max_retries": 0},
            {"key": "b", "max_retries": 0},
            {"key": "c", "deps": ["a", "b"]},
        ],
        dag_id="rs-3",
    )
    claimed = client.post("/tasks/claim", json={"dag_id": "rs-3", "limit": 10}).json()["claimed"]
    by = {t["task_key"]: t for t in claimed}
    _fail(client, by["a"], "fa")
    _fail(client, by["b"], "fb")

    c = by_key(client.get("/dags/rs-3").json(), "c")
    assert c["state"] == "blocked"
    reason = c["blocked_reason"]
    assert "permanently failed" in reason

    # 只恢复 a：c 的另一前置 b 仍 failed -> 保持 blocked 与原阻塞原因
    r = client.post(f"/tasks/{by['a']['task_id']}/resume")
    assert r.status_code == 200 and r.json()["unblocked_descendants"] == 0
    c = by_key(client.get("/dags/rs-3").json(), "c")
    assert c["state"] == "blocked" and c["blocked_reason"] == reason

    # 再恢复 b：c 的全部前置均处于 pending -> 解锁
    r = client.post(f"/tasks/{by['b']['task_id']}/resume")
    assert r.status_code == 200 and r.json()["unblocked_descendants"] == 1
    c = by_key(client.get("/dags/rs-3").json(), "c")
    assert c["state"] == "pending" and c["blocked_reason"] is None


def test_resume_unblocks_when_other_upstream_is_plain_pending(client):
    # c 的另一前置 b 是健康的普通 pending（尚未运行）：恢复 a 后 c 不再有任何
    # 失败/阻塞/运行中的前置，随本次恢复解锁为 pending，等待 b 正常完成即可被领取
    submit(
        client,
        [
            {"key": "a", "max_retries": 0},
            {"key": "b"},
            {"key": "c", "deps": ["a", "b"]},
        ],
        dag_id="rs-4",
    )
    t = client.post("/tasks/claim", json={"dag_id": "rs-4"}).json()["claimed"][0]
    assert t["task_key"] == "a"  # 按创建序先领到 a
    _fail(client, t, "fa")
    assert by_key(client.get("/dags/rs-4").json(), "c")["state"] == "blocked"

    r = client.post(f"/tasks/{t['task_id']}/resume")
    assert r.status_code == 200 and r.json()["unblocked_descendants"] == 1
    full = client.get("/dags/rs-4").json()
    assert by_key(full, "c")["state"] == "pending"
    assert by_key(full, "b")["state"] == "pending"

    # 依赖门控仍然生效：a、b 成功前 c 不可领取
    claimed = client.post("/tasks/claim", json={"dag_id": "rs-4", "limit": 10}).json()["claimed"]
    assert {t["task_key"] for t in claimed} == {"a", "b"}


def test_resume_keeps_blocked_while_other_upstream_running(client):
    submit(
        client,
        [
            {"key": "a", "max_retries": 0, "lease_seconds": 30},
            {"key": "b", "lease_seconds": 30},
            {"key": "c", "deps": ["a", "b"]},
        ],
        dag_id="rs-5",
    )
    claimed = client.post("/tasks/claim", json={"dag_id": "rs-5", "limit": 10}).json()["claimed"]
    by = {t["task_key"]: t for t in claimed}
    # a 最终失败（c 被阻塞）时 b 仍在运行（租约有效）
    _fail(client, by["a"], "fa")
    c = by_key(client.get("/dags/rs-5").json(), "c")
    assert c["state"] == "blocked"
    reason = c["blocked_reason"]

    # b 处于 running：不满足 succeeded/pending -> c 保持 blocked 与原阻塞原因
    r = client.post(f"/tasks/{by['a']['task_id']}/resume")
    assert r.status_code == 200 and r.json()["unblocked_descendants"] == 0
    c = by_key(client.get("/dags/rs-5").json(), "c")
    assert c["state"] == "blocked" and c["blocked_reason"] == reason


def test_resume_rejects_non_failed_and_unknown_tasks(client):
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="rs-6")

    # 未知任务 -> 404
    r = client.post("/tasks/nonexistent/resume")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "task_not_found"

    # pending -> 409
    a = by_key(client.get("/dags/rs-6").json(), "a")
    r = client.post(f"/tasks/{a['id']}/resume")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "task_not_failed"
    assert r.json()["error"]["details"]["state"] == "pending"

    # running -> 409
    t = client.post("/tasks/claim", json={"dag_id": "rs-6"}).json()["claimed"][0]
    assert client.post(f"/tasks/{t['task_id']}/resume").status_code == 409

    # succeeded -> 409
    client.post(f"/tasks/{t['task_id']}/complete",
                json={"completion_id": "ok", "lease_token": t["lease_token"], "success": True})
    r = client.post(f"/tasks/{t['task_id']}/resume")
    assert r.status_code == 409
    assert r.json()["error"]["details"]["state"] == "succeeded"

    # blocked -> 409（blocked 不是最终失败，须恢复其失败上游）
    submit(client, [{"key": "x", "max_retries": 0}, {"key": "y", "deps": ["x"]}], dag_id="rs-7")
    tx = client.post("/tasks/claim", json={"dag_id": "rs-7"}).json()["claimed"][0]
    _fail(client, tx, "fx")
    y = by_key(client.get("/dags/rs-7").json(), "y")
    assert y["state"] == "blocked"
    r = client.post(f"/tasks/{y['id']}/resume")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "task_not_failed"


def test_resume_twice_returns_409_and_adds_no_event(client):
    submit(client, [{"key": "a", "max_retries": 0}, {"key": "b", "deps": ["a"]}], dag_id="rs-8")
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    _fail(client, t, "f1")

    r = client.post(f"/tasks/{t['task_id']}/resume")
    assert r.status_code == 200 and r.json()["unblocked_descendants"] == 1
    events = client.get("/events", params={"dag_id": "rs-8"}).json()["events"]
    assert [e["type"] for e in events[-2:]] == ["task.resumed", "task.resumed"]
    n_events = len(events)

    # 重复恢复：任务已回到 pending -> 409，且不新增任何事件
    r = client.post(f"/tasks/{t['task_id']}/resume")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "task_not_failed"
    events_after = client.get("/events", params={"dag_id": "rs-8"}).json()["events"]
    assert len(events_after) == n_events


def test_openapi_documents_resume_endpoint(client):
    spec = client.get("/openapi.json").json()
    path = spec["paths"]["/tasks/{task_id}/resume"]["post"]
    assert "200" in path["responses"]
    assert "404" in path["responses"]
    assert "409" in path["responses"]


# ------------------------------------------------------------- 任务取消

def test_cancel_pending_task_emits_event(client):
    submit(client, [{"key": "a"}], dag_id="cx-1")
    a = by_key(client.get("/dags/cx-1").json(), "a")

    r = client.post(f"/tasks/{a['id']}/cancel")
    assert r.status_code == 200
    body = r.json()
    assert body["task_id"] == a["id"]
    assert body["state"] == "canceled"
    assert body["attempt"] == 0
    assert body["canceled_descendants"] == 0

    detail = client.get(f"/tasks/{a['id']}").json()
    assert detail["state"] == "canceled"
    assert detail["lease"] is None
    assert detail["finished_at"] is not None

    # 取消后的任务不可再被领取
    assert client.post("/tasks/claim", json={"dag_id": "cx-1"}).json()["count"] == 0

    events = client.get("/events", params={"dag_id": "cx-1"}).json()["events"]
    evt = events[-1]
    assert evt["type"] == "task.canceled"
    assert evt["task_id"] == a["id"]
    assert evt["from_state"] == "pending"
    assert evt["to_state"] == "canceled"


def test_cancel_running_task_terminates_attempt_and_invalidates_token(client):
    submit(client, [{"key": "a", "lease_seconds": 60}], dag_id="cx-2")
    t = client.post("/tasks/claim", json={"dag_id": "cx-2"}).json()["claimed"][0]
    token = t["lease_token"]

    r = client.post(f"/tasks/{t['task_id']}/cancel")
    assert r.status_code == 200
    assert r.json() == {
        "task_id": t["task_id"],
        "state": "canceled",
        "attempt": 1,
        "canceled_descendants": 0,
    }

    detail = client.get(f"/tasks/{t['task_id']}").json()
    assert detail["state"] == "canceled"
    assert detail["lease"] is None
    # 当前 attempt 已终止；attempt 历史保留记录
    assert [a["state"] for a in detail["attempts"]] == ["expired"]
    assert detail["finished_at"] is not None

    # 旧 lease_token 的心跳与完成回报一律 409
    r = client.post(f"/tasks/{t['task_id']}/heartbeat", json={"lease_token": token})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "invalid_lease"
    r = client.post(
        f"/tasks/{t['task_id']}/complete",
        json={"completion_id": "late", "lease_token": token, "success": True},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "invalid_lease"

    # 任务不会因租约过期等任何原因重新进入候选集
    assert client.post("/tasks/claim", json={"dag_id": "cx-2", "limit": 10}).json()["count"] == 0

    events = client.get("/events", params={"dag_id": "cx-2"}).json()["events"]
    evt = events[-1]
    assert evt["type"] == "task.canceled"
    assert evt["from_state"] == "running"
    assert evt["to_state"] == "canceled"


def test_cancel_blocked_task_clears_reason(client):
    submit(
        client,
        [{"key": "a", "max_retries": 0}, {"key": "b", "deps": ["a"]}],
        dag_id="cx-3",
    )
    t = client.post("/tasks/claim", json={"dag_id": "cx-3"}).json()["claimed"][0]
    _fail(client, t, "fa")
    b = by_key(client.get("/dags/cx-3").json(), "b")
    assert b["state"] == "blocked"

    r = client.post(f"/tasks/{b['id']}/cancel")
    assert r.status_code == 200 and r.json()["state"] == "canceled"
    detail = client.get(f"/tasks/{b['id']}").json()
    assert detail["state"] == "canceled" and detail["blocked_reason"] is None
    events = client.get("/events", params={"dag_id": "cx-3"}).json()["events"]
    assert events[-1]["type"] == "task.canceled"
    assert events[-1]["from_state"] == "blocked"


def test_cancel_recursively_cancels_unfinished_descendants(client):
    submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["a"]},
            {"key": "d", "deps": ["b", "c"]},
        ],
        dag_id="cx-4",
    )
    a = by_key(client.get("/dags/cx-4").json(), "a")

    r = client.post(f"/tasks/{a['id']}/cancel")
    assert r.status_code == 200
    assert r.json()["canceled_descendants"] == 3

    full = client.get("/dags/cx-4").json()
    assert full["states"]["canceled"] == 4
    for key in "abcd":
        assert by_key(full, key)["state"] == "canceled"

    # 每个状态变化各一条事件（根 + 3 后继），且根事件先于后继，序列严格递增
    canceled = [e for e in client.get("/events", params={"dag_id": "cx-4"}).json()["events"]
                if e["type"] == "task.canceled"]
    assert len(canceled) == 4
    assert canceled[0]["task_id"] == a["id"]
    assert all(e["from_state"] == "pending" for e in canceled)
    assert all(e["to_state"] == "canceled" for e in canceled)
    seqs = [e["sequence"] for e in canceled]
    assert seqs == sorted(set(seqs))

    # 整个 DAG 再无可领取任务
    assert client.post("/tasks/claim", json={"dag_id": "cx-4", "limit": 10}).json()["count"] == 0


def test_cancel_keeps_finished_descendants_untouched_and_emits_no_event_for_them(client):
    submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["a"], "max_retries": 0},
            {"key": "d", "deps": ["b", "c"]},
        ],
        dag_id="cx-5",
    )
    full = client.get("/dags/cx-5").json()
    a, b, c, d = (by_key(full, k) for k in "abcd")

    # b 先成功（其前置 a 仍 pending：直接构造不出来，走正常流程）
    ta = client.post("/tasks/claim", json={"dag_id": "cx-5"}).json()["claimed"][0]
    client.post(
        f"/tasks/{ta['task_id']}/complete",
        json={"completion_id": "ok-a", "lease_token": ta["lease_token"], "success": True},
    )
    # c 最终失败，d 随之 blocked；b 已领取、正在 running
    tc = next(t for t in client.post("/tasks/claim", json={"dag_id": "cx-5", "limit": 10}).json()["claimed"]
              if t["task_key"] == "c")
    tb = client.get(f"/tasks/{b['id']}").json()
    client.post(
        f"/tasks/{tc['task_id']}/complete",
        json={"completion_id": "fail-c", "lease_token": tc["lease_token"],
              "success": False, "error": "boom"},
    )
    full = client.get("/dags/cx-5").json()
    assert by_key(full, "a")["state"] == "succeeded"
    assert by_key(full, "b")["state"] == "running"
    assert by_key(full, "c")["state"] == "failed"
    assert by_key(full, "d")["state"] == "blocked"

    # 取消 running 的 b：后继 d（blocked，未结束）-> canceled；a(succeeded)
    # 与 c(failed) 保持原样且不产生事件。
    r = client.post(f"/tasks/{b['id']}/cancel")
    assert r.status_code == 200
    assert r.json()["canceled_descendants"] == 1
    full = client.get("/dags/cx-5").json()
    assert by_key(full, "b")["state"] == "canceled"
    assert by_key(full, "d")["state"] == "canceled"
    assert by_key(full, "a")["state"] == "succeeded"
    assert by_key(full, "c")["state"] == "failed"
    assert full["states"] == {
        "pending": 0, "running": 0, "succeeded": 1,
        "failed": 1, "blocked": 0, "canceled": 2,
    }

    canceled = [e for e in client.get("/events", params={"dag_id": "cx-5"}).json()["events"]
                if e["type"] == "task.canceled"]
    assert {e["task_id"] for e in canceled} == {b["id"], d["id"]}
    by_id = {e["task_id"]: e for e in canceled}
    assert by_id[b["id"]]["from_state"] == "running"
    assert by_id[d["id"]]["from_state"] == "blocked"

    # b 被取消后其旧令牌立即失效
    r = client.post(f"/tasks/{b['id']}/heartbeat",
                    json={"lease_token": tb["lease"]["token"]})
    assert r.status_code == 409


def test_cancel_running_descendant_releases_concurrency_slot(client):
    """递归取消命中 running 后继（正常门控下不会出现，直接改写存储模拟
    外部写入/历史数据）：活动 attempt 终止、旧令牌 409、并发组名额释放。"""
    import sqlite3
    from app.db import db_path

    submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"], "lease_seconds": 30,
             "concurrency_group": "g", "concurrency_limit": 1},
            {"key": "d", "deps": ["b"]},
        ],
        dag_id="cx-6",
    )
    submit(
        client,
        [{"key": "z", "concurrency_group": "g", "concurrency_limit": 1}],
        dag_id="cx-6b",
    )
    full = client.get("/dags/cx-6").json()
    a, b, d = (by_key(full, k) for k in ("a", "b", "d"))

    # 绕过依赖门控把 b 直接置为 running（未过期租约 + active attempt，占用组名额）
    token = uuid.uuid4().hex
    conn = sqlite3.connect(db_path())
    conn.execute(
        """UPDATE tasks
           SET state = 'running', attempt = 1, lease_token = ?,
               lease_expires_at = '2099-01-01T00:00:00Z', worker_id = 'w-inj'
           WHERE id = ?""",
        (token, b["id"]),
    )
    conn.execute(
        """INSERT INTO attempts (task_id, attempt_no, lease_token, worker_id,
               leased_at, expires_at, state)
           VALUES (?, 1, ?, 'w-inj', '2026-01-01T00:00:00Z', '2099-01-01T00:00:00Z', 'active')""",
        (b["id"], token),
    )
    conn.commit()
    conn.close()

    # b 占着组内唯一名额：其他 DAG 的 z 不可领
    assert client.post("/tasks/claim", json={"dag_id": "cx-6b"}).json()["count"] == 0

    r = client.post(f"/tasks/{a['id']}/cancel")
    assert r.status_code == 200
    assert r.json()["canceled_descendants"] == 2  # b(running) 与 d(pending)

    full = client.get("/dags/cx-6").json()
    assert by_key(full, "a")["state"] == "canceled"
    b_detail = client.get(f"/tasks/{b['id']}").json()
    assert b_detail["state"] == "canceled" and b_detail["lease"] is None
    assert [x["state"] for x in b_detail["attempts"]] == ["expired"]
    assert by_key(full, "d")["state"] == "canceled"

    # b 的旧令牌心跳 409
    r = client.post(f"/tasks/{b['id']}/heartbeat", json={"lease_token": token})
    assert r.status_code == 409 and r.json()["error"]["code"] == "invalid_lease"

    # 组名额随取消释放：其他 DAG 的 z 现在可领；整个 cx-6 再无可领任务
    z = client.post("/tasks/claim", json={"dag_id": "cx-6b"}).json()["claimed"]
    assert [t["task_key"] for t in z] == ["z"]
    assert client.post("/tasks/claim", json={"dag_id": "cx-6", "limit": 10}).json()["count"] == 0


def test_cancel_twice_returns_409_and_adds_no_event(client):
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="cx-7")
    a = by_key(client.get("/dags/cx-7").json(), "a")
    r = client.post(f"/tasks/{a['id']}/cancel")
    assert r.status_code == 200 and r.json()["canceled_descendants"] == 1
    n_events = len(client.get("/events", params={"dag_id": "cx-7"}).json()["events"])

    r = client.post(f"/tasks/{a['id']}/cancel")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "task_not_cancelable"
    assert r.json()["error"]["details"]["state"] == "canceled"
    events = client.get("/events", params={"dag_id": "cx-7"}).json()["events"]
    assert len(events) == n_events


def test_cancel_rejects_finished_states_and_unknown_task(client):
    submit(client, [{"key": "a"}], dag_id="cx-8")
    t = client.post("/tasks/claim", json={"dag_id": "cx-8"}).json()["claimed"][0]
    client.post(
        f"/tasks/{t['task_id']}/complete",
        json={"completion_id": "ok", "lease_token": t["lease_token"], "success": True},
    )
    # succeeded -> 409
    r = client.post(f"/tasks/{t['task_id']}/cancel")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "task_not_cancelable"
    assert r.json()["error"]["details"]["state"] == "succeeded"

    # failed -> 409
    submit(client, [{"key": "f", "max_retries": 0}], dag_id="cx-9")
    tf = client.post("/tasks/claim", json={"dag_id": "cx-9"}).json()["claimed"][0]
    client.post(
        f"/tasks/{tf['task_id']}/complete",
        json={"completion_id": "ff", "lease_token": tf["lease_token"],
              "success": False, "error": "x"},
    )
    r = client.post(f"/tasks/{tf['task_id']}/cancel")
    assert r.status_code == 409
    assert r.json()["error"]["details"]["state"] == "failed"

    # 未知任务 -> 404，不产生事件
    r = client.post("/tasks/nonexistent/cancel")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "task_not_found"
    before = len(client.get("/events", params={"limit": 100}).json()["events"])
    client.post("/tasks/nonexistent/cancel")
    after = client.get("/events", params={"limit": 100}).json()["events"]
    assert len(after) == before


def test_canceled_supported_in_filters_and_counts_and_task_view(client):
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="cx-10")
    a = by_key(client.get("/dags/cx-10").json(), "a")
    client.post(f"/tasks/{a['id']}/cancel")

    # 状态计数
    full = client.get("/dags/cx-10").json()
    assert full["states"]["canceled"] == 2
    assert set(full["states"]) == {"pending", "running", "succeeded", "failed", "blocked", "canceled"}

    # 列表按 state=canceled 过滤
    listed = client.get("/tasks", params={"dag_id": "cx-10", "state": "canceled"}).json()
    assert listed["count"] == 2
    assert all(t["state"] == "canceled" for t in listed["tasks"])
    assert client.get("/tasks", params={"state": "pending"}).json()["count"] == 0


def test_canceled_backoff_pending_clears_next_eligible(client):
    submit(client, [{"key": "a", "max_retries": 2, "retry_backoff": BACKOFF}], dag_id="cx-11")
    t = client.post("/tasks/claim", json={"dag_id": "cx-11"}).json()["claimed"][0]
    _complete(client, t, "f1", success=False, error="e1")
    detail = client.get(f"/tasks/{t['task_id']}").json()
    assert detail["state"] == "pending" and detail["next_eligible_at"] is not None

    assert client.post(f"/tasks/{t['task_id']}/cancel").status_code == 200
    detail = client.get(f"/tasks/{t['task_id']}").json()
    assert detail["state"] == "canceled"
    assert detail["next_eligible_at"] is None
    # 退避到期后也不会被领取
    time.sleep(0.35)
    assert client.post("/tasks/claim", json={"dag_id": "cx-11"}).json()["count"] == 0


def test_canceled_task_old_completion_id_does_not_resurrect(client):
    submit(client, [{"key": "a"}], dag_id="cx-12")
    t = client.post("/tasks/claim", json={"dag_id": "cx-12"}).json()["claimed"][0]
    client.post(f"/tasks/{t['task_id']}/cancel")

    # 取消后旧令牌的新 completion_id 回报：租约校验失败 409，任务保持 canceled
    r = client.post(
        f"/tasks/{t['task_id']}/complete",
        json={"completion_id": "after-cancel", "lease_token": t["lease_token"], "success": True},
    )
    assert r.status_code == 409
    assert client.get(f"/tasks/{t['task_id']}").json()["state"] == "canceled"


def test_openapi_documents_cancel_endpoint(client):
    spec = client.get("/openapi.json").json()
    path = spec["paths"]["/tasks/{task_id}/cancel"]["post"]
    assert "200" in path["responses"]
    assert "404" in path["responses"]
    assert "409" in path["responses"]


def test_cancel_works_on_legacy_sqlite_db(tmp_path, monkeypatch):
    """旧版 SQLite 库（state 为自由文本，无 canceled 概念）迁移后取消可用。"""
    import sqlite3

    db_file = tmp_path / "legacy-cancel.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            state TEXT NOT NULL, blocked_reason TEXT, lease_token TEXT, lease_expires_at TEXT,
            worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0, failure_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key TEXT,
            PRIMARY KEY (dag_id, task_key, dep_key));
        CREATE TABLE attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT, attempt_no INTEGER,
            lease_token TEXT UNIQUE, worker_id TEXT, leased_at TEXT, expires_at TEXT,
            heartbeat_at TEXT, state TEXT, error TEXT);
        CREATE TABLE completions (
            task_id TEXT NOT NULL, completion_id TEXT NOT NULL, lease_token TEXT NOT NULL,
            success INTEGER NOT NULL, fingerprint TEXT NOT NULL, status_code INTEGER NOT NULL,
            response_body TEXT NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY (task_id, completion_id));
        CREATE TABLE events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL,
            dag_id TEXT NOT NULL, task_id TEXT, attempt INTEGER, from_state TEXT,
            to_state TEXT, occurred_at TEXT NOT NULL);
        CREATE TABLE concurrency_groups (name TEXT PRIMARY KEY, limit_value INTEGER NOT NULL,
            created_at TEXT NOT NULL);
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 0, 30, 'pending', 0, '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    from app.db import init_db
    from app import service

    init_db()
    init_db()  # 迁移幂等

    result = service.cancel_task("t1")
    assert result["state"] == "canceled" and result["canceled_descendants"] == 0
    assert service.get_task("t1")["state"] == "canceled"
    events = service.list_events(0, 50, None)["events"]
    assert events[-1]["type"] == "task.canceled"

    # 重复取消 409 且不产生事件
    with pytest.raises(service.APIError) as exc:
        service.cancel_task("t1")
    assert exc.value.status_code == 409
    assert len(service.list_events(0, 50, None)["events"]) == len(events)


# ------------------------------------------------------------- 可靠事件订阅

def _run_dag_full(client, dag_id, n_tasks=1):
    """提交 DAG 并把所有任务跑完，制造 dag.created/task.claimed/task.succeeded 事件。"""
    submit(client, [{"key": f"t{i}"} for i in range(n_tasks)], dag_id=dag_id)
    while True:
        claimed = client.post("/tasks/claim", json={"dag_id": dag_id, "limit": 10}).json()["claimed"]
        if not claimed:
            break
        for t in claimed:
            r = client.post(
                f"/tasks/{t['task_id']}/complete",
                json={"completion_id": f"c-{t['task_id']}",
                      "lease_token": t["lease_token"], "success": True},
            )
            assert r.status_code == 200


def test_create_subscription_stores_cursor_and_filter(client):
    _run_dag_full(client, "sub-d1")
    r = client.post("/subscriptions", json={"dag_id": "sub-d1"})
    assert r.status_code == 201
    sub = r.json()
    assert sub["dag_id"] == "sub-d1"
    assert sub["committed_sequence"] == 0
    assert sub["in_flight"] is None
    assert sub["created_at"] and sub["id"]

    # 显式起始游标
    r = client.post("/subscriptions", json={"committed_sequence": 5})
    assert r.status_code == 201
    assert r.json()["committed_sequence"] == 5
    assert r.json()["dag_id"] is None

    # 非法 dag_id / 负数游标 -> 422
    assert client.post("/subscriptions", json={"dag_id": ""}).status_code == 422
    assert client.post("/subscriptions", json={"committed_sequence": -1}).status_code == 422


def test_subscription_poll_returns_next_batch_in_sequence_order(client):
    _run_dag_full(client, "sub-order-1")
    sid = client.post("/subscriptions", json={}).json()["id"]

    r = client.post(f"/subscriptions/{sid}/poll", json={"limit": 2, "lease_seconds": 30})
    assert r.status_code == 200
    body = r.json()
    assert [e["sequence"] for e in body["events"]] == [1, 2]
    assert body["last_sequence"] == 2
    assert body["committed_sequence"] == 0
    token = body["delivery_token"]
    assert token and body["lease_expires_at"]
    # 事件对象与 GET /events 形态一致
    assert set(body["events"][0]) == {
        "sequence", "type", "dag_id", "task_id", "attempt",
        "from_state", "to_state", "next_eligible_at", "occurred_at",
    }

    # 确认后下一批严格接续，不漏不重
    assert client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": token}).status_code == 200
    r = client.post(f"/subscriptions/{sid}/poll", json={"limit": 100, "lease_seconds": 30})
    seqs = [e["sequence"] for e in r.json()["events"]]
    assert seqs[0] == 3 and seqs == sorted(seqs) and len(seqs) == len(set(seqs))
    client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": r.json()["delivery_token"]})

    # 追平后再 poll：空批次、无令牌、不占在途槽位
    tail = client.post(f"/subscriptions/{sid}/poll", json={"limit": 10, "lease_seconds": 30}).json()
    assert tail["events"] == [] and tail["delivery_token"] is None
    assert tail["lease_expires_at"] is None
    assert client.get(f"/subscriptions/{sid}").json()["in_flight"] is None


def test_subscription_poll_replays_same_batch_byte_for_byte_while_leased(client):
    _run_dag_full(client, "sub-replay-1")
    sid = client.post("/subscriptions", json={}).json()["id"]
    r1 = client.post(f"/subscriptions/{sid}/poll", json={"limit": 2, "lease_seconds": 300})
    first_text = r1.text
    first = r1.json()

    # 租约未过期：不同 limit/lease_seconds 也必须原样回放同一批次
    r2 = client.post(f"/subscriptions/{sid}/poll", json={"limit": 99, "lease_seconds": 1})
    assert r2.status_code == 200
    assert r2.text == first_text  # 逐字节一致
    r3 = client.post(f"/subscriptions/{sid}/poll", json={"limit": 1, "lease_seconds": 1})
    assert r3.text == first_text
    assert r3.json()["delivery_token"] == first["delivery_token"]


def test_subscription_poll_redelivers_after_lease_expiry_with_new_token(client):
    _run_dag_full(client, "sub-exp-1")
    sid = client.post("/subscriptions", json={}).json()["id"]
    p1 = client.post(f"/subscriptions/{sid}/poll", json={"limit": 1, "lease_seconds": 0.2}).json()
    first_seqs = [e["sequence"] for e in p1["events"]]
    old_token = p1["delivery_token"]

    time.sleep(0.1)
    # 未过期仍是回放
    assert client.post(f"/subscriptions/{sid}/poll", json={"limit": 50, "lease_seconds": 10}).text != ""
    rep = client.post(f"/subscriptions/{sid}/poll", json={"limit": 50, "lease_seconds": 10})
    assert rep.json()["delivery_token"] == old_token

    time.sleep(0.25)
    p2 = client.post(f"/subscriptions/{sid}/poll", json={"limit": 50, "lease_seconds": 30}).json()
    assert p2["delivery_token"] != old_token
    # 游标未推进：未确认事件从同一窗口重新投递
    assert [e["sequence"] for e in p2["events"]][:1] == first_seqs

    # 旧令牌确认 -> 409
    r = client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": old_token})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "invalid_delivery_token"
    # 游标未被推进
    assert client.get(f"/subscriptions/{sid}").json()["committed_sequence"] == 0
    # 当前令牌可确认
    r = client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": p2["delivery_token"]})
    assert r.status_code == 200
    assert r.json()["committed_sequence"] == p2["events"][-1]["sequence"]


def test_subscription_ack_rejects_wrong_missing_and_stale_tokens_without_advancing(client):
    _run_dag_full(client, "sub-ack-1")
    sid = client.post("/subscriptions", json={}).json()["id"]

    # 尚未 poll：无在途批次
    r = client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": "whatever"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "no_pending_batch"

    p = client.post(f"/subscriptions/{sid}/poll", json={"limit": 2, "lease_seconds": 30}).json()
    # 错误令牌
    r = client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": "wrong-token"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "invalid_delivery_token"
    # 游标不动、批次仍在
    detail = client.get(f"/subscriptions/{sid}").json()
    assert detail["committed_sequence"] == 0
    assert detail["in_flight"]["delivery_token"] == p["delivery_token"]

    # 正确确认
    r = client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": p["delivery_token"]})
    assert r.status_code == 200
    assert r.json() == {
        "subscription_id": sid,
        "committed_sequence": p["last_sequence"],
    }
    # 重复确认（旧令牌）-> 409 no_pending_batch，游标不变
    r = client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": p["delivery_token"]})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "no_pending_batch"
    assert client.get(f"/subscriptions/{sid}").json()["committed_sequence"] == p["last_sequence"]


def test_subscription_cannot_skip_unacked_events(client):
    _run_dag_full(client, "sub-skip-1")
    sid = client.post("/subscriptions", json={}).json()["id"]
    # 取两批但只确认第二批是做不到的：未确认批次占着槽位，第二批根本发不出来
    p1 = client.post(f"/subscriptions/{sid}/poll", json={"limit": 1, "lease_seconds": 300}).json()
    replay = client.post(f"/subscriptions/{sid}/poll", json={"limit": 100, "lease_seconds": 300})
    assert replay.json()["events"] == p1["events"]
    # 确认第一批后，新批次的第一条 sequence 紧接 p1
    client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": p1["delivery_token"]})
    p2 = client.post(f"/subscriptions/{sid}/poll", json={"limit": 100, "lease_seconds": 30}).json()
    assert p2["events"][0]["sequence"] == p1["last_sequence"] + 1


def test_subscription_dag_filter_only_delivers_matching_events(client):
    _run_dag_full(client, "sub-f-a")
    _run_dag_full(client, "sub-f-b")
    sid = client.post("/subscriptions", json={"dag_id": "sub-f-a"}).json()["id"]
    body = client.post(f"/subscriptions/{sid}/poll", json={"limit": 100, "lease_seconds": 30}).json()
    assert body["events"]
    assert {e["dag_id"] for e in body["events"]} == {"sub-f-a"}
    client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": body["delivery_token"]})
    # 过滤订阅追平后，另一个 DAG 的新事件不会投递给它
    _run_dag_full(client, "sub-f-c")
    tail = client.post(f"/subscriptions/{sid}/poll", json={"limit": 100, "lease_seconds": 30}).json()
    assert tail["events"] == []

    # 被过滤 DAG 自己来新事件（用其专属过滤订阅验证过滤口径）
    sid2 = client.post("/subscriptions", json={"dag_id": "sub-f-c"}).json()["id"]
    body2 = client.post(f"/subscriptions/{sid2}/poll", json={"limit": 100, "lease_seconds": 30}).json()
    assert {e["dag_id"] for e in body2["events"]} == {"sub-f-c"} and body2["events"]


def test_subscription_poll_validation_and_404(client):
    sid = client.post("/subscriptions", json={}).json()["id"]
    # 缺 limit 合法（默认 50）；limit 越界、lease_seconds 非正/缺失均 422
    assert client.post(f"/subscriptions/{sid}/poll",
                       json={"lease_seconds": 1}).status_code == 200
    for bad in ({"limit": 0, "lease_seconds": 1},
                {"limit": 101, "lease_seconds": 1},
                {"limit": 1, "lease_seconds": 0},
                {"limit": 1, "lease_seconds": -5},
                {"limit": 1}):  # 缺 lease_seconds
        assert client.post(f"/subscriptions/{sid}/poll", json=bad).status_code == 422, bad

    for method, path in (("GET", "/subscriptions/missing"),
                         ("POST", "/subscriptions/missing/poll"),
                         ("POST", "/subscriptions/missing/ack")):
        r = client.request(method, path, json={"limit": 1, "lease_seconds": 1,
                                               "delivery_token": "x"})
        assert r.status_code == 404, (method, path, r.status_code)
        assert r.json()["error"]["code"] == "subscription_not_found"


def test_subscription_list_and_detail(client):
    _run_dag_full(client, "sub-list-1")
    s1 = client.post("/subscriptions", json={"dag_id": "sub-list-1"}).json()
    s2 = client.post("/subscriptions", json={}).json()

    # 在途批次出现在详情与列表中
    p = client.post(f"/subscriptions/{s1['id']}/poll", json={"limit": 1, "lease_seconds": 300}).json()
    detail = client.get(f"/subscriptions/{s1['id']}").json()
    assert detail["id"] == s1["id"]
    assert detail["dag_id"] == "sub-list-1"
    inf = detail["in_flight"]
    assert inf["delivery_token"] == p["delivery_token"]
    assert inf["last_sequence"] == p["last_sequence"]
    assert inf["event_count"] == 1
    assert inf["expired"] is False

    all_subs = client.get("/subscriptions").json()
    assert all_subs["count"] >= 2
    ids = {s["id"] for s in all_subs["subscriptions"]}
    assert {s1["id"], s2["id"]} <= ids

    only = client.get("/subscriptions", params={"dag_id": "sub-list-1"}).json()
    assert only["count"] == 1 and only["subscriptions"][0]["id"] == s1["id"]


def test_subscription_persists_cursor_and_inflight_across_restart(tmp_path, monkeypatch):
    db_file = tmp_path / "subs.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        _run_dag_full(c1, "sub-restart-1")
        sid = c1.post("/subscriptions", json={}).json()["id"]
        p = c1.post(f"/subscriptions/{sid}/poll", json={"limit": 2, "lease_seconds": 300}).json()
        token = p["delivery_token"]
        last = p["last_sequence"]

    with TestClient(app) as c2:
        detail = c2.get(f"/subscriptions/{sid}").json()
        assert detail["committed_sequence"] == 0
        assert detail["in_flight"]["delivery_token"] == token
        assert detail["in_flight"]["last_sequence"] == last
        assert detail["in_flight"]["expired"] is False
        # 租约仍有效：重启后重复 poll 原样回放
        rep = c2.post(f"/subscriptions/{sid}/poll", json={"limit": 99, "lease_seconds": 1})
        assert rep.json()["delivery_token"] == token
        assert [e["sequence"] for e in rep.json()["events"]] == [1, 2]
        # 持久化的令牌仍可确认，游标推进
        r = c2.post(f"/subscriptions/{sid}/ack", json={"delivery_token": token})
        assert r.status_code == 200 and r.json()["committed_sequence"] == last
        assert c2.get(f"/subscriptions/{sid}").json()["committed_sequence"] == last
        # 列表也在
        assert c2.get("/subscriptions").json()["count"] >= 1


def test_subscription_openapi_documents_endpoints(client):
    spec = client.get("/openapi.json").json()
    paths = spec["paths"]
    assert "post" in paths["/subscriptions"]
    assert "get" in paths["/subscriptions"]
    assert "get" in paths["/subscriptions/{subscription_id}"]
    poll = paths["/subscriptions/{subscription_id}/poll"]["post"]
    assert {"200", "404"} <= set(poll["responses"])
    ack = paths["/subscriptions/{subscription_id}/ack"]["post"]
    assert {"200", "404", "409"} <= set(ack["responses"])


def test_existing_events_endpoint_still_works_alongside_subscriptions(client):
    _run_dag_full(client, "sub-compat-1")
    sid = client.post("/subscriptions", json={}).json()["id"]
    p = client.post(f"/subscriptions/{sid}/poll", json={"limit": 100, "lease_seconds": 30}).json()
    client.post(f"/subscriptions/{sid}/ack", json={"delivery_token": p["delivery_token"]})
    # 既有 GET /events 不受影响
    events = client.get("/events", params={"after": 0, "limit": 100}).json()
    assert len(events["events"]) == len(p["events"])
    assert [e["sequence"] for e in events["events"]] == [e["sequence"] for e in p["events"]]


# ------------------------------------------------------------- 工作器能力登记（能力路由）

def register(client, worker_id, capabilities, ttl=60):
    return client.put(f"/workers/{worker_id}",
                      json={"capabilities": capabilities, "ttl_seconds": ttl})


def _requires_dag(client, dag_id="cap-1", tasks=None):
    default_tasks = [
        {"key": "plain"},
        {"key": "gpu", "requires": ["gpu"]},
        {"key": "both", "requires": ["gpu", "io"]},
    ]
    return submit(client, tasks if tasks is not None else default_tasks, dag_id=dag_id)


def test_register_worker_persists_dedup_and_ttl(client):
    r = register(client, "w-1", ["gpu", "io", "gpu", "cpu"], ttl=10)
    assert r.status_code == 200
    body = r.json()
    assert body["worker_id"] == "w-1"
    # 标签按首次出现顺序去重
    assert body["capabilities"] == ["gpu", "io", "cpu"]
    assert body["ttl_seconds"] == 10
    assert body["alive"] is True
    assert body["registered_at"]
    assert body["expires_at"]
    assert _parse_iso(body["expires_at"]) > _parse_iso(body["registered_at"])

    # GET 返回相同登记与存活状态
    got = client.get("/workers/w-1")
    assert got.status_code == 200
    assert got.json() == body


def test_register_get_unknown_returns_404(client):
    r = client.get("/workers/nope")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "worker_not_found"


def test_register_rejects_non_positive_ttl_and_blank_tags(client):
    for bad_ttl in (0, -1):
        r = register(client, f"w-ttl-{bad_ttl}", ["gpu"], ttl=bad_ttl)
        assert r.status_code == 422, bad_ttl
    # NaN/Inf 非法：直接在请求模型层校验（FastAPI 默认 422 响应体需回显该输入，
    # NaN 无法被 JSON 序列化，故此处不走 HTTP 文本）
    import pydantic

    from app.schemas import WorkerRegisterIn

    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(pydantic.ValidationError):
            WorkerRegisterIn(capabilities=["gpu"], ttl_seconds=bad)
    # 能力标签必须是非空字符串
    assert register(client, "w-empty", [], ttl=10).status_code == 200  # 空能力集合合法
    # 空字符串标签非法；非字符串标签非法
    assert register(client, "w-blank", [""], ttl=10).status_code == 422
    r = register(client, "w-badtag", [1], ttl=10)
    assert r.status_code == 422
    r = client.put("/workers/w-x", json={"ttl_seconds": 10})  # 缺 capabilities
    assert r.status_code == 422


def test_repeat_put_overwrites_caps_and_refreshes_expiry(client):
    r1 = register(client, "w-1", ["gpu", "io"], ttl=0.3)
    first = r1.json()
    assert first["capabilities"] == ["gpu", "io"]

    time.sleep(0.15)
    # 重复登记：覆盖能力（丢弃 io）、刷新过期时刻；registered_at 保留首次值
    r2 = register(client, "w-1", ["gpu"], ttl=30)
    second = r2.json()
    assert second["capabilities"] == ["gpu"]
    assert second["ttl_seconds"] == 30
    assert second["registered_at"] == first["registered_at"]
    assert _parse_iso(second["expires_at"]) > _parse_iso(first["expires_at"])
    assert second["alive"] is True

    # 初次登记本已接近过期，刷新后仍然存活
    time.sleep(0.25)
    assert client.get("/workers/w-1").json()["alive"] is True

    # 空能力集合同样是覆盖语义
    r3 = register(client, "w-1", [], ttl=30)
    assert r3.json()["capabilities"] == []


def test_worker_alive_false_after_ttl_and_ttl_refresh_keeps_alive(client):
    register(client, "w-ttl", ["gpu"], ttl=0.3)
    time.sleep(0.15)
    # 过期前重新登记刷新 TTL，不会过期
    register(client, "w-ttl", ["gpu"], ttl=0.3)
    time.sleep(0.2)
    assert client.get("/workers/w-ttl").json()["alive"] is True
    time.sleep(0.15)
    # 刷新窗口用尽：过期行仍可查询，alive 为 false（行不删除）
    body = client.get("/workers/w-ttl").json()
    assert body["alive"] is False
    assert body["capabilities"] == ["gpu"]


def test_requires_visible_in_task_views(client):
    dag = _requires_dag(client, "cap-views")
    by = {t["key"]: t for t in dag["tasks"]}
    assert by["plain"]["requires"] == []
    assert by["gpu"]["requires"] == ["gpu"]
    assert by["both"]["requires"] == ["gpu", "io"]

    listed = client.get("/tasks", params={"dag_id": "cap-views"}).json()["tasks"]
    assert {t["key"]: t["requires"] for t in listed} == {
        "plain": [], "gpu": ["gpu"], "both": ["gpu", "io"],
    }
    gpu_id = by["gpu"]["id"]
    assert client.get(f"/tasks/{gpu_id}").json()["requires"] == ["gpu"]


def test_requires_tags_deduped_on_submit(client):
    dag = submit(client, [{"key": "a", "requires": ["x", "x", "y", "x"]}])
    assert by_key(dag, "a")["requires"] == ["x", "y"]


def test_claim_without_registration_returns_409_and_claims_nothing(client):
    _requires_dag(client, "cap-409")

    # 即使无能力要求的 plain 排在最前，遇到 requires 候选时登记缺失 -> 409，
    # 同事务回滚，任何任务（含 plain）都不得被领取
    for body in ({"limit": 10}, {"worker_id": "ghost", "limit": 10}):
        r = client.post("/tasks/claim", json=body)
        assert r.status_code == 409, body
        err = r.json()["error"]
        assert err["code"] == "worker_not_registered"
        assert err["details"]["task_key"] == "gpu"
        assert err["details"]["requires"] == ["gpu"]
        assert client.get("/dags/cap-409").json()["states"]["running"] == 0

    # 409 不产生事件
    events = client.get("/events", params={"dag_id": "cap-409"}).json()["events"]
    assert [e["type"] for e in events] == ["dag.created"]


def test_claim_expired_registration_returns_409(client):
    _requires_dag(client, "cap-expired")
    register(client, "w-old", ["gpu"], ttl=0.2)
    time.sleep(0.25)

    r = client.post("/tasks/claim", json={"worker_id": "w-old", "limit": 10})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "worker_not_registered"
    assert client.get("/dags/cap-expired").json()["states"]["running"] == 0


def test_claim_matches_capabilities_and_skips_mismatch_to_fill_limit(client):
    _requires_dag(client, "cap-match")
    register(client, "w-gpu", ["gpu"], ttl=60)

    # 候选顺序 plain、gpu、both：plain 无要求可领；gpu 被 gpu 覆盖可领；
    # both 需要 gpu+io，能力不匹配 -> 跳过，不在同批卡住
    r = client.post("/tasks/claim", json={"worker_id": "w-gpu", "limit": 10})
    assert r.status_code == 200
    claimed = {t["task_key"]: t for t in r.json()["claimed"]}
    assert set(claimed) == {"plain", "gpu"}
    assert claimed["gpu"]["requires"] == ["gpu"]
    assert claimed["plain"]["requires"] == []

    # both 仍 pending：同一 worker 再次领取为空（仅剩余不匹配候选）
    r = client.post("/tasks/claim", json={"worker_id": "w-gpu", "limit": 10})
    assert r.json()["count"] == 0
    full = client.get("/dags/cap-match").json()
    assert full["states"]["running"] == 2
    assert by_key(full, "both")["state"] == "pending"

    # 补登记 io 能力后 both 可领（重复 PUT 覆盖标签）
    register(client, "w-gpu", ["gpu", "io"], ttl=60)
    r = client.post("/tasks/claim", json={"worker_id": "w-gpu", "limit": 10})
    assert [t["task_key"] for t in r.json()["claimed"]] == ["both"]


def test_superset_capabilities_claim_subset_requires(client):
    submit(client, [{"key": "a", "requires": ["gpu"]}], dag_id="cap-super")
    register(client, "w-super", ["gpu", "io", "cpu"], ttl=60)
    r = client.post("/tasks/claim", json={"worker_id": "w-super"})
    assert r.status_code == 200 and r.json()["count"] == 1


def test_capability_tags_are_case_and_space_sensitive(client):
    submit(client, [{"key": "a", "requires": ["GPU"]}], dag_id="cap-case")
    register(client, "w-case", ["gpu"], ttl=60)
    # "gpu" 不覆盖 "GPU"：跳过；没有其他候选 -> 空领取（不是 409，登记存活）
    assert client.post("/tasks/claim", json={"worker_id": "w-case"}).json()["count"] == 0
    register(client, "w-case", ["GPU"], ttl=60)
    assert client.post("/tasks/claim", json={"worker_id": "w-case"}).json()["count"] == 1


def test_capability_gate_respects_dependencies(client):
    submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"], "requires": ["gpu"]},
        ],
        dag_id="cap-deps",
    )
    register(client, "w-gpu", ["gpu"], ttl=60)
    # a 未完成时 b 不进候选：不触发 409 语义，只领到无要求的 a
    r = client.post("/tasks/claim", json={"worker_id": "w-gpu", "limit": 10})
    assert [t["task_key"] for t in r.json()["claimed"]] == ["a"]
    token = r.json()["claimed"][0]["lease_token"]

    # 完成 a（回报只需有效租约令牌，不校验能力登记）
    a = next(t for t in client.get("/dags/cap-deps").json()["tasks"] if t["key"] == "a")
    assert client.post(
        f"/tasks/{a['id']}/complete",
        json={"completion_id": "ok-a", "lease_token": token, "success": True},
    ).status_code == 200

    # 依赖满足后 b 才进入候选并按能力匹配
    r = client.post("/tasks/claim", json={"worker_id": "w-gpu", "limit": 10})
    assert [t["task_key"] for t in r.json()["claimed"]] == ["b"]


def test_capability_gate_respects_backoff(client):
    submit(
        client,
        [{"key": "a", "max_retries": 2, "requires": ["gpu"],
          "retry_backoff": {"initial_seconds": 0.3, "multiplier": 1, "max_seconds": 0.3}}],
        dag_id="cap-backoff",
    )
    register(client, "w-gpu", ["gpu"], ttl=60)
    t = client.post("/tasks/claim", json={"worker_id": "w-gpu"}).json()["claimed"][0]
    # 显式失败进入退避
    assert _complete(client, t, "f1", success=False, error="e").status_code == 200

    # 退避未到期：requires 候选不在候选集内——无登记 worker 也不会收到 409，
    # 登记过期不影响「没有可匹配候选」的空领取
    assert client.post("/tasks/claim", json={"worker_id": "ghost"}).json()["count"] == 0
    time.sleep(0.35)
    # 退避到期后能力门控恢复：无登记 -> 409，有登记 -> 可领
    r = client.post("/tasks/claim", json={"worker_id": "ghost"})
    assert r.status_code == 409
    r = client.post("/tasks/claim", json={"worker_id": "w-gpu"})
    assert r.json()["claimed"][0]["attempt"] == 2


def test_capability_and_concurrency_group_skip_interaction(client):
    submit(
        client,
        [
            # g 组上限 1，且要求 gpu
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 1, "requires": ["gpu"]},
            {"key": "g2", "concurrency_group": "g", "concurrency_limit": 1},
            {"key": "u1"},
        ],
        dag_id="cap-group",
    )
    register(client, "w-cpu", [], ttl=60)  # 存活但无 gpu 能力

    # g1 能力不匹配跳过；g2 占组唯一名额；u1 无组继续领取以填满 limit
    r = client.post("/tasks/claim", json={"worker_id": "w-cpu", "limit": 10})
    assert [t["task_key"] for t in r.json()["claimed"]] == ["g2", "u1"]

    # 组已满且 g1 能力不匹配：空领取（不是 409）
    assert client.post("/tasks/claim", json={"worker_id": "w-cpu", "limit": 10}).json()["count"] == 0

    # 有 gpu 能力的新 worker：组仍被 g2 占满，g1 因组满跳过 -> 空
    register(client, "w-gpu", ["gpu"], ttl=60)
    assert client.post("/tasks/claim", json={"worker_id": "w-gpu", "limit": 10}).json()["count"] == 0


def test_unclaimed_due_to_missing_capability_can_be_claimed_after_upgrade(client):
    submit(client, [{"key": "a", "requires": ["gpu", "io"]}], dag_id="cap-upgrade")
    register(client, "w-1", ["gpu"], ttl=60)
    assert client.post("/tasks/claim", json={"worker_id": "w-1"}).json()["count"] == 0
    # 任务保持 pending，未产生租约/尝试
    task = client.get("/dags/cap-upgrade").json()["tasks"][0]
    assert task["state"] == "pending" and task["attempt"] == 0
    assert client.get(f"/tasks/{task['id']}").json()["attempts"] == []

    register(client, "w-1", ["gpu", "io"], ttl=60)
    r = client.post("/tasks/claim", json={"worker_id": "w-1"})
    assert r.json()["count"] == 1 and r.json()["claimed"][0]["attempt"] == 1


def test_no_requires_tasks_keep_legacy_claim_behavior(client):
    # 未声明 requires 的历史任务：匿名、无登记、过期登记 worker 均可领取
    submit(client, [{"key": "a"}, {"key": "b"}], dag_id="cap-legacy")
    register(client, "w-exp", ["gpu"], ttl=0.2)
    time.sleep(0.25)

    assert len(client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]) == 2
    # 直接调服务层重置候选较复杂：另起一个无 requires DAG 验证过期登记 worker
    submit(client, [{"key": "a"}, {"key": "b"}], dag_id="cap-legacy-2")
    r = client.post("/tasks/claim", json={"worker_id": "w-exp", "dag_id": "cap-legacy-2", "limit": 10})
    assert r.status_code == 200 and r.json()["count"] == 2


def test_empty_capabilities_worker_claims_only_plain_tasks(client):
    submit(client, [{"key": "a"}, {"key": "b", "requires": ["gpu"]}], dag_id="cap-empty")
    register(client, "w-empty", [], ttl=60)
    r = client.post("/tasks/claim", json={"worker_id": "w-empty", "limit": 10})
    assert [t["task_key"] for t in r.json()["claimed"]] == ["a"]
    # b 需要 gpu：空能力集合不覆盖 -> 再次领取为空
    assert client.post("/tasks/claim", json={"worker_id": "w-empty"}).json()["count"] == 0


def test_lease_reclaim_after_expiry_also_checks_capability_registration(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2, "requires": ["gpu"]}], dag_id="cap-reclaim")
    register(client, "w-1", ["gpu"], ttl=10)
    t1 = client.post("/tasks/claim", json={"worker_id": "w-1"}).json()["claimed"][0]
    time.sleep(0.25)

    # 租约过期后重领：无登记 -> 409（任务虽 running+租约过期，仍是 requires 候选）
    assert client.post("/tasks/claim", json={}).status_code == 409
    # 原 worker 登记仍存活：可重领，attempt=2
    t2 = client.post("/tasks/claim", json={"worker_id": "w-1"}).json()["claimed"][0]
    assert t2["task_id"] == t1["task_id"] and t2["attempt"] == 2

    # worker 登记也过期后：租约过期重领同样 409
    time.sleep(11)
    r = client.post("/tasks/claim", json={"worker_id": "w-1"})
    assert r.status_code == 409
    assert client.get("/dags/cap-reclaim").json()["states"]["running"] == 1  # t2 未被改动


def test_409_when_requires_candidate_only_behind_full_group(client):
    """requires 候选在并发组满时不进可领取集合：无登记领取不触发 409（与门控优先级一致）。"""
    submit(
        client,
        [
            {"key": "holder", "concurrency_group": "g", "concurrency_limit": 1},
            {"key": "need", "concurrency_group": "g", "concurrency_limit": 1, "requires": ["gpu"]},
        ],
        dag_id="cap-gate-order",
    )
    register(client, "w-1", [], ttl=60)
    # w-1 先占满组
    assert [t["task_key"] for t in
            client.post("/tasks/claim", json={"worker_id": "w-1", "limit": 10}).json()["claimed"]] == ["holder"]
    # 匿名再来：holder 运行中、need 被组满挡住（没有可领取候选）——空领取而非 409
    assert client.post("/tasks/claim", json={"limit": 10}).json()["count"] == 0


def test_worker_registration_and_requires_persist_across_restart(tmp_path, monkeypatch):
    db_file = tmp_path / "workers.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        register(c1, "w-persist", ["gpu", "io"], ttl=300)
        submit(c1, [{"key": "a", "requires": ["gpu", "io"]}], dag_id="cap-restart")
        claimed = c1.post("/tasks/claim", json={"worker_id": "w-persist"}).json()["claimed"]
        assert len(claimed) == 1

    with TestClient(app) as c2:
        # 登记、TTL 与 requires 持久化：重启后仍存活、仍按能力路由
        w = c2.get("/workers/w-persist").json()
        assert w["alive"] is True and w["capabilities"] == ["gpu", "io"]
        assert by_key(c2.get("/dags/cap-restart").json(), "a")["requires"] == ["gpu", "io"]
        # a 已在 running，无新候选
        assert c2.post("/tasks/claim", json={"worker_id": "w-persist"}).json()["count"] == 0
        # 未登记 worker 无法在 a 租约过期后重领（用短租约场景另验）
        submit(c2, [{"key": "b", "requires": ["gpu"]}], dag_id="cap-restart-2")
        r = c2.post("/tasks/claim", json={"worker_id": "nobody"})
        assert r.status_code == 409


def test_legacy_sqlite_db_gets_requires_null_and_claims_normally(tmp_path, monkeypatch):
    """旧库（无 requires 列、无 workers 表）迁移后：历史任务 requires 为空、领取行为不变。"""
    import sqlite3

    db_file = tmp_path / "legacy-cap.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE concurrency_groups (name TEXT PRIMARY KEY, limit_value INTEGER NOT NULL,
            created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            duration_seconds REAL NOT NULL DEFAULT 1, state TEXT NOT NULL, blocked_reason TEXT,
            lease_token TEXT, lease_expires_at TEXT, worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
            failure_count INTEGER NOT NULL DEFAULT 0,
            backoff_initial_seconds REAL, backoff_multiplier REAL, backoff_max_seconds REAL,
            next_eligible_at TEXT, created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            concurrency_group TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key TEXT,
            PRIMARY KEY (dag_id, task_key, dep_key));
        CREATE TABLE attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT, attempt_no INTEGER,
            lease_token TEXT UNIQUE, worker_id TEXT, leased_at TEXT, expires_at TEXT,
            heartbeat_at TEXT, state TEXT, error TEXT);
        CREATE TABLE completions (
            task_id TEXT NOT NULL, completion_id TEXT NOT NULL, lease_token TEXT NOT NULL,
            success INTEGER NOT NULL, fingerprint TEXT NOT NULL, status_code INTEGER NOT NULL,
            response_body TEXT NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY (task_id, completion_id));
        CREATE TABLE events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL,
            dag_id TEXT NOT NULL, task_id TEXT, attempt INTEGER, from_state TEXT,
            to_state TEXT, next_eligible_at TEXT, occurred_at TEXT NOT NULL);
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 0, 30, 'pending', 0, '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    from app.db import init_db
    from app import service

    init_db()
    init_db()  # 迁移幂等

    # 历史任务 requires 为空，匿名/无登记照常领取（兼容行为）
    assert service.get_task("t1")["requires"] == []
    t = service.claim_tasks(None, None, 1)["claimed"][0]
    assert t["task_id"] == "t1" and t["requires"] == []

    # 迁移后能力登记可直接使用
    reg = service.register_worker("w-new", {"capabilities": ["gpu"], "ttl_seconds": 30})
    assert reg["alive"] is True
    assert service.get_worker("w-new")["capabilities"] == ["gpu"]
    with pytest.raises(service.APIError) as exc:
        service.get_worker("missing")
    assert exc.value.status_code == 404


def test_worker_endpoints_in_openapi(client):
    spec = client.get("/openapi.json").json()
    paths = spec["paths"]
    assert "put" in paths["/workers/{worker_id}"]
    assert "get" in paths["/workers/{worker_id}"]
    put_op = paths["/workers/{worker_id}"]["put"]
    get_op = paths["/workers/{worker_id}"]["get"]
    assert "200" in put_op["responses"] and "200" in get_op["responses"]
    assert "404" in get_op["responses"]
    claim = paths["/tasks/claim"]["post"]
    assert "409" in claim["responses"]


# ------------------------------------------------------------- 可复用 DAG 模板

def _make_template(client, name="etl", tasks=None):
    tasks = tasks or [
        {"key": "extract",
         "payload": {"src": "${source}", "count": "${count}", "path": "s3://${bucket}/data"}},
        {"key": "load", "deps": ["extract"], "requires": ["io"]},
    ]
    r = client.post("/templates", json={"name": name, "tasks": tasks})
    assert r.status_code == 201, r.text
    return r.json()


def _instantiate(client, name, version, key, params, **extra):
    body = {"idempotency_key": key, "parameters": params}
    body.update(extra)
    return client.post(f"/templates/{name}/versions/{version}/instantiations", json=body)


def _instantiate_named(client, tpl_name, version, key, params, dag_name=None, dag_id=None):
    body: dict = {"idempotency_key": key, "parameters": params}
    if dag_name is not None:
        body["name"] = dag_name
    if dag_id is not None:
        body["id"] = dag_id
    return client.post(f"/templates/{tpl_name}/versions/{version}/instantiations", json=body)


def test_template_create_publishes_version_1_with_snapshot(client):
    tpl = _make_template(client)
    assert tpl["version"] == 1
    assert tpl["task_count"] == 2
    assert tpl["parameters"] == ["bucket", "count", "source"]
    snap = tpl["snapshot"]
    assert [t["key"] for t in snap] == ["extract", "load"]
    # 快照中的任务定义可直接作为 DAG 提交字段（requires 已去重持久化）
    assert snap[1]["requires"] == ["io"] and snap[1]["deps"] == ["extract"]
    assert snap[0]["payload"]["src"] == "${source}"

    # 模板列表与详情可见
    listing = client.get("/templates").json()
    assert listing["count"] == 1
    assert listing["templates"][0]["name"] == "etl"
    assert listing["templates"][0]["latest_version"] == 1
    assert listing["templates"][0]["version_count"] == 1

    detail = client.get("/templates/etl").json()
    assert [v["version"] for v in detail["versions"]] == [1]
    assert detail["versions"][0]["parameters"] == ["bucket", "count", "source"]


def test_template_create_duplicate_name_conflict(client):
    _make_template(client)
    r = client.post("/templates", json={"name": "etl", "tasks": [{"key": "a"}]})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "template_exists"


def test_template_create_validates_graph_like_dag_submit(client):
    # 复用 DAG 校验：重复键 / 缺失依赖 / 环 一律 422，错误码与 /dags 一致
    for bad_tasks, code in [
        ([{"key": "a"}, {"key": "a"}], "duplicate_task_key"),
        ([{"key": "a", "deps": ["ghost"]}], "missing_dependency"),
        ([{"key": "a", "deps": ["a"]}], "cycle_detected"),
    ]:
        r = client.post("/templates", json={"name": f"bad-{code}", "tasks": bad_tasks})
        assert r.status_code == 422, r.text
        assert r.json()["error"]["code"] == code
    # 校验失败不落库：无模板、无版本、无事件
    assert client.get("/templates").json()["count"] == 0
    assert client.get("/events", params={"limit": 100}).json()["events"] == []


def test_template_create_requires_nonempty_tasks(client):
    assert client.post("/templates", json={"name": "empty", "tasks": []}).status_code == 422
    assert client.post("/templates", json={"name": "noname", "tasks": []}).status_code == 422


def test_publish_new_version_by_name(client):
    tpl = _make_template(client)
    assert tpl["version"] == 1

    r = client.post("/templates/etl/versions", json={"tasks": [
        {"key": "extract", "payload": {"src": "${source}"}},
        {"key": "verify", "deps": ["extract"]},
        {"key": "load", "deps": ["verify"]},
    ]})
    assert r.status_code == 201, r.text
    v2 = r.json()
    assert v2["version"] == 2 and v2["parameters"] == ["source"]
    assert [t["key"] for t in v2["snapshot"]] == ["extract", "verify", "load"]

    detail = client.get("/templates/etl").json()
    assert detail["latest_version"] == 2 and detail["version_count"] == 2
    assert [v["version"] for v in detail["versions"]] == [1, 2]
    assert detail["updated_at"] >= detail["created_at"]


def test_publish_version_unknown_template_is_404(client):
    r = client.post("/templates/ghost/versions", json={"tasks": [{"key": "a"}]})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "template_not_found"


def test_publish_version_validates_graph_and_keeps_old_version_immutable(client):
    tpl = _make_template(client)
    v1_keys = [t["key"] for t in client.get("/templates/etl/versions/1").json()["snapshot"]]

    # 非法图：不产生新版本
    r = client.post("/templates/etl/versions", json={"tasks": [{"key": "a", "deps": ["a"]}]})
    assert r.status_code == 422 and r.json()["error"]["code"] == "cycle_detected"
    detail = client.get("/templates/etl").json()
    assert detail["latest_version"] == 1

    # 发布 v2 后，v1 快照不变（不可变）
    client.post("/templates/etl/versions", json={"tasks": [{"key": "only", "payload": {"x": "${p}"}}]})
    v1 = client.get("/templates/etl/versions/1").json()
    assert [t["key"] for t in v1["snapshot"]] == v1_keys
    assert v1["version"] == 1 and v1["parameters"] == ["bucket", "count", "source"]


def test_get_template_version_not_found(client):
    _make_template(client)
    assert client.get("/templates/nope/versions/1").status_code == 404
    r = client.get("/templates/etl/versions/99")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "template_version_not_found"
    assert r.json()["error"]["details"] == {"template_name": "etl", "version": 99}
    # 非整数版本号由路由层 422
    assert client.get("/templates/etl/versions/x").status_code == 422


def test_instantiate_substitutes_placeholders_and_creates_dag(client):
    _make_template(client)
    r = _instantiate(client, "etl", 1, "run-1",
                     {"source": "db-main", "count": 5, "bucket": "b1"})
    assert r.status_code == 201, r.text
    dag = r.json()
    extract = by_key(dag, "extract")
    load = by_key(dag, "load")
    # 整体占位符保留参数原类型（数字仍是数字）
    assert extract["payload"] == {"src": "db-main", "count": 5, "path": "s3://b1/data"}
    # 依赖、能力路由与直接提交口径一致
    assert load["deps"] == ["extract"] and load["requires"] == ["io"]
    assert dag["states"]["pending"] == 2

    # 实例 DAG 可走既有接口：GET /dags、领取（requires 同样生效）
    assert client.get(f"/dags/{dag['id']}").json()["id"] == dag["id"]
    assert client.post("/tasks/claim", json={"dag_id": dag["id"], "limit": 10}).json()["count"] == 1


def test_instantiate_embedded_placeholder_stringifies(client):
    _make_template(
        client, name="embed",
        tasks=[{"key": "a", "payload": {"url": "q=${x}&y", "both": "${x}${y}"}}],
    )
    r = _instantiate(client, "embed", 1, "k", {"x": 7, "y": ["z"]})
    assert r.status_code == 201
    payload = by_key(r.json(), "a")["payload"]
    # 嵌入位置的非字符串参数按 JSON 文本替换
    assert payload == {"url": 'q=7&y', "both": '7["z"]'}


def test_instantiate_nested_payload_placeholders(client):
    _make_template(
        client, name="nested",
        tasks=[{"key": "a", "payload": {"layers": [{"cfg": {"target": "${target}"}}], "flag": "${flag}"}}],
    )
    r = _instantiate(client, "nested", 1, "k", {"target": "gpu", "flag": True})
    assert r.status_code == 201
    payload = by_key(r.json(), "a")["payload"]
    assert payload == {"layers": [{"cfg": {"target": "gpu"}}], "flag": True}


def test_instantiate_requires_idempotency_key(client):
    _make_template(client)
    r = client.post("/templates/etl/versions/1/instantiations",
                    json={"parameters": {"source": "s", "count": 1, "bucket": "b"}})
    assert r.status_code == 422


def test_instantiate_missing_parameter_422_and_no_persistence(client):
    _make_template(client)
    r = _instantiate(client, "etl", 1, "run-x", {"source": "s", "count": 1})
    assert r.status_code == 422
    err = r.json()["error"]
    assert err["code"] == "missing_template_parameter"
    assert err["details"]["missing"] == ["bucket"]

    # 缺参不落库：失败后无 DAG、无事件
    assert client.get("/events").json()["events"] == []
    # 同键随后可用正确参数首次创建（证明未占位幂等记录）
    r2 = _instantiate(client, "etl", 1, "run-x",
                      {"source": "s", "count": 1, "bucket": "b"})
    assert r2.status_code == 201
    assert client.get(f"/dags/{r2.json()['id']}").status_code == 200


def test_instantiate_unexpected_parameter_422_and_no_persistence(client):
    _make_template(client)
    r = _instantiate(client, "etl", 1, "run-y",
                     {"source": "s", "count": 1, "bucket": "b", "extra": 9})
    assert r.status_code == 422
    err = r.json()["error"]
    assert err["code"] == "unexpected_template_parameter"
    assert err["details"]["unexpected"] == ["extra"]
    assert client.get("/events").json()["events"] == []
    # 无实例 DAG 落库
    assert client.get("/tasks", params={"state": "pending"}).json()["count"] == 0


def test_instantiate_no_placeholders_accepts_only_empty_parameters(client):
    _make_template(client, name="plain", tasks=[{"key": "a", "payload": {"v": 1}}])
    assert _instantiate(client, "plain", 1, "k", {}).status_code == 201
    # 多参仍 422
    r = _instantiate(client, "plain", 1, "k2", {"nope": 1})
    assert r.status_code == 422 and r.json()["error"]["code"] == "unexpected_template_parameter"
    # 不传 parameters 字段等价于空参数
    r2 = client.post("/templates/plain/versions/1/instantiations", json={"idempotency_key": "k3"})
    assert r2.status_code == 201


def test_instantiate_unknown_template_or_version_404(client):
    _make_template(client)
    r = _instantiate(client, "ghost", 1, "k", {})
    assert r.status_code == 404 and r.json()["error"]["code"] == "template_not_found"
    r = _instantiate(client, "etl", 99, "k", {})
    assert r.status_code == 404 and r.json()["error"]["code"] == "template_version_not_found"
    assert client.get("/events").json()["events"] == []


def test_instantiate_idempotent_replay_returns_same_dag(client):
    _make_template(client)
    params = {"source": "db", "count": 3, "bucket": "b1"}
    r1 = _instantiate(client, "etl", 1, "same-key", params)
    assert r1.status_code == 201
    first = r1.json()

    # 键序不同的等价参数：重放返回同一 DAG，200
    r2 = _instantiate(client, "etl", 1, "same-key",
                      {"bucket": "b1", "count": 3, "source": "db"})
    assert r2.status_code == 200
    assert r2.json()["id"] == first["id"]

    # 只产生一次 dag.created 事件、一个 DAG
    events = client.get("/events").json()["events"]
    assert len(events) == 1 and events[0]["type"] == "dag.created"
    assert client.get("/tasks", params={"dag_id": first["id"]}).json()["count"] == 2

    # 重放不带 parameters（缺参）仍 422，而不是返回首响
    r3 = client.post("/templates/etl/versions/1/instantiations",
                     json={"idempotency_key": "same-key", "parameters": {}})
    assert r3.status_code == 422 and r3.json()["error"]["code"] == "missing_template_parameter"


def test_instantiate_same_key_different_parameters_conflict(client):
    _make_template(client)
    base = {"source": "db", "count": 3, "bucket": "b1"}
    assert _instantiate(client, "etl", 1, "dup-key", base).status_code == 201

    r = _instantiate(client, "etl", 1, "dup-key", {**base, "source": "other"})
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "idempotency_conflict"
    assert err["details"] == {"template_name": "etl", "version": 1, "idempotency_key": "dup-key"}

    # 冲突不落第二个 DAG
    assert len([e for e in client.get("/events").json()["events"] if e["type"] == "dag.created"]) == 1

    # 原参数仍可重放到同一 DAG
    replay = _instantiate(client, "etl", 1, "dup-key", base)
    assert replay.status_code == 200


def test_instantiate_same_key_scoped_by_template_and_version(client):
    _make_template(client)
    client.post("/templates/etl/versions", json={"tasks": [{"key": "only", "payload": {"p": "${p}"}}]})
    _make_template(client, name="other", tasks=[{"key": "a", "payload": {"q": "${q}"}}])

    shared_key = "shared"
    r1 = _instantiate(client, "etl", 1, shared_key,
                      {"source": "s", "count": 1, "bucket": "b"})
    r2 = _instantiate(client, "etl", 2, shared_key, {"p": "v"})
    r3 = _instantiate(client, "other", 1, shared_key, {"q": "v"})
    assert r1.status_code == 201 and r2.status_code == 201 and r3.status_code == 201
    ids = {r1.json()["id"], r2.json()["id"], r3.json()["id"]}
    assert len(ids) == 3
    # 各自重放均命中自己的 DAG
    assert _instantiate(client, "etl", 2, shared_key, {"p": "v"}).json()["id"] == r2.json()["id"]


def test_instantiate_custom_dag_id_and_name(client):
    _make_template(client, name="named", tasks=[{"key": "a", "payload": {"p": "${p}"}}])
    r = _instantiate_named(client, "named", 1, "k", {"p": "v"},
                           dag_name="run-42", dag_id="custom-dag-id")
    assert r.status_code == 201
    assert r.json()["id"] == "custom-dag-id" and r.json()["name"] == "run-42"

    # 自定义 id 已存在：409 且不写幂等记录（可换 id 用同键重试）
    r2 = _instantiate(client, "named", 1, "k2", {"p": "w"}, id="custom-dag-id")
    assert r2.status_code == 409 and r2.json()["error"]["code"] == "dag_exists"
    r3 = _instantiate(client, "named", 1, "k2", {"p": "w"})
    assert r3.status_code == 201


def test_instantiate_concurrency_group_conflict_rolls_back(client):
    _make_template(
        client, name="grp",
        tasks=[{"key": "a", "concurrency_group": "g", "concurrency_limit": 2}],
    )
    # 先由直接提交持久化同名组上限 1
    submit(client, [{"key": "x", "concurrency_group": "g", "concurrency_limit": 1}], dag_id="d-other")

    # 模板版本声明上限 2：首个实例即命中跨 DAG 组冲突 —— 409，DAG 与幂等记录一同回滚
    r = _instantiate(client, "grp", 1, "g1", {})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "concurrency_group_conflict"
    # 无残留实例（只有 d-other 一个 DAG 的任务）
    assert {t["dag_id"] for t in client.get("/tasks").json()["tasks"]} == {"d-other"}
    # 无 dag.created 残留于失败实例：事件总数即 d-other 的一条
    assert [e["dag_id"] for e in client.get("/events").json()["events"]] == ["d-other"]
    # 同键在冲突解除后可重试（证明幂等记录已随事务回滚，没有占位）：
    # 发布与持久化上限一致的新版本再实例化成功
    client.post("/templates/grp/versions",
                json={"tasks": [{"key": "a", "concurrency_group": "g", "concurrency_limit": 1}]})
    r2 = _instantiate(client, "grp", 2, "g1", {})
    assert r2.status_code == 201


def test_instantiate_concurrent_same_key_creates_one_dag(client):
    _make_template(client, name="conc", tasks=[{"key": "a", "payload": {"p": "${p}"}}])
    responses = []
    winning_param = []
    errors = []
    lock = threading.Lock()

    def worker(i):
        try:
            param = {"p": f"v-{i}"}
            r = _instantiate(client, "conc", 1, "race-key", param)
            with lock:
                responses.append(r)
                if r.status_code == 201:
                    winning_param.append(param)
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert not errors
    # 同键并发：恰有一个请求创建 DAG（201），其余因参数指纹不同 409——
    # 保留锁串行化裁决，绝不允许创建出多个 DAG
    statuses = [r.status_code for r in responses]
    assert statuses.count(201) == 1
    assert set(statuses) <= {201, 409}
    assert len(winning_param) == 1
    dag_events = [e for e in client.get("/events", params={"limit": 100}).json()["events"]
                  if e["type"] == "dag.created"]
    assert len(dag_events) == 1
    # 用创建时实际生效的参数重放：命中同一 DAG
    created = next(r for r in responses if r.status_code == 201)
    replay = _instantiate(client, "conc", 1, "race-key", winning_param[0])
    assert replay.status_code == 200 and replay.json()["id"] == created.json()["id"]


def test_instantiate_concurrent_same_key_same_params_single_dag(client):
    _make_template(client, name="conc2", tasks=[{"key": "a", "payload": {"p": "${p}"}}])
    statuses = []
    dag_ids = []
    barrier = threading.Barrier(6)

    def worker():
        barrier.wait()
        r = _instantiate(client, "conc2", 1, "same-params", {"p": "v"})
        statuses.append(r.status_code)
        if r.status_code in (200, 201):
            dag_ids.append(r.json()["id"])

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert 201 in statuses and set(statuses) <= {200, 201}
    assert len(set(dag_ids)) == 1


def test_templates_persist_across_restart(tmp_path, monkeypatch):
    """模板、版本快照与幂等记录持久化在 SQLite：重启后语义不变。"""
    db_file = tmp_path / "templates.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        _make_template(c1)
        r1 = _instantiate(c1, "etl", 1, "persist-key",
                          {"source": "db", "count": 2, "bucket": "b"})
        assert r1.status_code == 201
        dag_id = r1.json()["id"]

    with TestClient(app) as c2:
        # 版本与快照仍可读且不可变
        v1 = c2.get("/templates/etl/versions/1").json()
        assert v1["parameters"] == ["bucket", "count", "source"]
        assert len(v1["snapshot"]) == 2
        # 幂等重放仍返回同一 DAG
        r2 = _instantiate(c2, "etl", 1, "persist-key",
                          {"source": "db", "count": 2, "bucket": "b"})
        assert r2.status_code == 200 and r2.json()["id"] == dag_id
        # 同键不同参数仍 409
        r3 = _instantiate(c2, "etl", 1, "persist-key",
                          {"source": "db2", "count": 2, "bucket": "b"})
        assert r3.status_code == 409
        # 重启后仍可发布新版本
        r4 = c2.post("/templates/etl/versions", json={"tasks": [{"key": "z"}]})
        assert r4.status_code == 201 and r4.json()["version"] == 2


def test_placeholder_names_extracted_from_values_only(client):
    # 对象键中的 ${...} 不是占位符（只替换 payload 值），参数要求只来自值
    tpl = _make_template(
        client, name="keys",
        tasks=[{"key": "a", "payload": {"${only_in_key}": "v", "real": "${real}"}}],
    )
    assert tpl["parameters"] == ["real"]
    r = _instantiate(client, "keys", 1, "k", {"real": "ok"})
    assert r.status_code == 201
    payload = by_key(r.json(), "a")["payload"]
    assert payload == {"${only_in_key}": "v", "real": "ok"}


def test_illegal_placeholder_shapes_are_literal(client):
    # ${}、${1a}、${a-b} 不匹配占位符语法：不要求参数，原样保留
    tpl = _make_template(
        client, name="lit",
        tasks=[{"key": "a", "payload": {"x": "${}", "y": "${1a}", "z": "${a-b}"}}],
    )
    assert tpl["parameters"] == []
    r = _instantiate(client, "lit", 1, "k", {})
    assert r.status_code == 201
    assert by_key(r.json(), "a")["payload"] == {"x": "${}", "y": "${1a}", "z": "${a-b}"}


def test_direct_dag_submit_unchanged_with_templates(client):
    """新增模板模块不改变直接 POST /dags 的行为与响应结构。"""
    dag = submit(client, [{"key": "a", "payload": "${not_replaced}"}])
    # 直接提交不做占位符替换
    assert by_key(dag, "a")["payload"] == "${not_replaced}"
    _make_template(client, name="t")
    dag2 = submit(client, [{"key": "a"}])
    assert set(dag2.keys()) == set(dag.keys())
    assert dag2["states"]["pending"] == 1


def test_template_endpoints_in_openapi(client):
    spec = client.get("/openapi.json").json()
    paths = spec["paths"]
    assert "post" in paths["/templates"]
    assert "get" in paths["/templates"]
    assert "get" in paths["/templates/{name}"]
    assert "post" in paths["/templates/{name}/versions"]
    assert "get" in paths["/templates/{name}/versions/{version}"]
    inst = paths["/templates/{name}/versions/{version}/instantiations"]["post"]
    assert {"200", "201", "404", "409", "422"} <= set(inst["responses"])
    create = paths["/templates"]["post"]
    assert {"201", "409", "422"} <= set(create["responses"])
    # 既有接口仍在文档中
    assert "post" in paths["/dags"]


# ------------------------------------------------------------- 只读领取预演 claim-plan

def _plan(client, **body):
    return client.post("/tasks/claim-plan", json=body)


def _reason_map(body):
    return {u["task_key"]: u for u in body["unselected"]}


def test_claim_plan_is_readonly(client):
    """预演不写入任务状态、attempt 或事件。"""
    dag = submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}])
    before = client.get(f"/dags/{dag['id']}").json()
    events_before = client.get("/events").json()

    r = _plan(client, dag_id=dag["id"], limit=10)
    assert r.status_code == 200
    body = r.json()
    assert body["simulated"] is True
    assert body["dag_id"] == dag["id"]
    assert body["limit"] == 10
    assert [t["task_key"] for t in body["claimed"]] == ["a"]

    after = client.get(f"/dags/{dag['id']}").json()
    assert before["states"] == after["states"]
    assert before["tasks"][0]["state"] == "pending"
    # 无 attempt 产生
    for t in after["tasks"]:
        assert client.get(f"/tasks/{t['id']}").json()["attempts"] == []
    # 无事件追加
    assert client.get("/events").json()["events"] == events_before["events"]
    # 预演结果不含真实租约令牌
    assert body["claimed"][0]["lease_token"] is None
    assert body["claimed"][0]["lease_expires_at"] is not None


def test_claim_plan_matches_real_claim_selection_order(client):
    """同一快照下预演选中序列与真实批量领取一致（含同批并发组即时占位与跳过）。"""
    submit(
        client,
        [
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g2", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g3", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "u1"},
            {"key": "u2"},
        ],
        dag_id="plan-match",
    )
    plan = _plan(client, dag_id="plan-match", limit=5).json()
    assert [t["task_key"] for t in plan["claimed"]] == ["g1", "g2", "u1", "u2"]
    reasons = _reason_map(plan)
    assert set(reasons) == {"g3"}
    assert reasons["g3"]["reason"] == "concurrency_group_full"
    assert reasons["g3"]["concurrency_group"] == "g"
    assert reasons["g3"]["group_limit"] == 2
    assert reasons["g3"]["group_occupied"] == 2

    # 随后真实领取得到完全相同的选中序列（预演未改变任何状态）
    real = client.post("/tasks/claim", json={"dag_id": "plan-match", "limit": 5}).json()
    assert [t["task_key"] for t in real["claimed"]] == ["g1", "g2", "u1", "u2"]

    # 真实领取后再预演：g3 仍被组容量挡住，且此时无任务被选中
    plan2 = _plan(client, dag_id="plan-match", limit=5).json()
    assert plan2["claimed"] == []
    assert plan2["unselected"][0]["reason"] == "concurrency_group_full"


def test_claim_plan_group_occupied_includes_existing_running_leases(client):
    """预演的组占用同时统计已存在的未过期 running 租约与本批模拟占位。"""
    submit(
        client,
        [{"key": "a1", "concurrency_group": "shared", "concurrency_limit": 2}],
        dag_id="pa",
    )
    submit(
        client,
        [
            {"key": "b1", "concurrency_group": "shared", "concurrency_limit": 2},
            {"key": "b2", "concurrency_group": "shared", "concurrency_limit": 2},
            {"key": "free"},
        ],
        dag_id="pb",
    )
    # 先真实占掉 1 个名额
    client.post("/tasks/claim", json={"dag_id": "pa", "limit": 5})

    plan = _plan(client, dag_id="pb", limit=5).json()
    assert [t["task_key"] for t in plan["claimed"]] == ["b1", "free"]
    reasons = _reason_map(plan)
    assert reasons["b2"]["reason"] == "concurrency_group_full"
    # 已有占用 1（a1）+ 本批 b1 即时占位 1
    assert reasons["b2"]["group_occupied"] == 2


def test_claim_plan_unselected_dependency_and_backoff_reasons(client):
    """未选中 pending 任务给出首个阻塞原因：依赖优先于退避。"""
    submit(
        client,
        [
            {"key": "a", "max_retries": 2, "retry_backoff": BACKOFF},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["b"]},
        ],
        dag_id="plan-deps",
    )
    t = client.post("/tasks/claim", json={"dag_id": "plan-deps"}).json()["claimed"][0]
    # a 显式失败进入退避等待（pending, next_eligible_at 未到）
    _complete(client, t, "fail-1", success=False)

    plan = _plan(client, dag_id="plan-deps", limit=10).json()
    assert plan["claimed"] == []
    reasons = _reason_map(plan)
    # 退避优先（a 无未完成前置）
    assert reasons["a"]["reason"] == "backoff_not_elapsed"
    assert reasons["a"]["next_eligible_at"]
    # b 的前置 a 处于 pending：依赖阻塞先于自身其他门控
    assert reasons["b"]["reason"] == "dependency_not_succeeded"
    assert reasons["b"]["blocked_by"] == [{"task_key": "a", "state": "pending"}]
    assert reasons["c"]["reason"] == "dependency_not_succeeded"
    blocked_c = {d["task_key"]: d["state"] for d in reasons["c"]["blocked_by"]}
    assert blocked_c == {"b": "pending"}


def test_claim_plan_dependency_blocked_by_running_and_terminal_states(client):
    submit(
        client,
        [
            {"key": "ok"},
            {"key": "work"},
            {"key": "late", "deps": ["ok", "work"]},
        ],
        dag_id="plan-blockedby",
    )
    claimed = client.post("/tasks/claim", json={"dag_id": "plan-blockedby", "limit": 10}).json()["claimed"]
    by_token = {t["task_key"]: t for t in claimed}
    # ok 成功；work 保持 running（未过期）
    _complete(client, by_token["ok"], "ok-1", success=True)

    plan = _plan(client, dag_id="plan-blockedby", limit=10).json()
    # work 持有未过期租约，不是候选；late 被依赖挡住
    assert plan["claimed"] == []
    reasons = _reason_map(plan)
    assert reasons["late"]["reason"] == "dependency_not_succeeded"
    blocked = {d["task_key"]: d["state"] for d in reasons["late"]["blocked_by"]}
    # ok 已 succeeded 不在阻塞列表；work running 是唯一阻塞前置
    assert blocked == {"work": "running"}


def test_claim_plan_simulated_running_dependency_unblocks_within_batch_check(client):
    """本批模拟选中的 pending 前置按 running 计入阻塞前置（仍非 succeeded）。"""
    submit(
        client,
        [{"key": "a"}, {"key": "b", "deps": ["a"]}],
        dag_id="plan-sim",
    )
    plan = _plan(client, dag_id="plan-sim", limit=10).json()
    assert [t["task_key"] for t in plan["claimed"]] == ["a"]
    reasons = _reason_map(plan)
    assert reasons["b"]["reason"] == "dependency_not_succeeded"
    assert reasons["b"]["blocked_by"] == [{"task_key": "a", "state": "running"}]


def test_claim_plan_limit_reached_reason(client):
    submit(client, [{"key": "a"}, {"key": "b"}, {"key": "c"}], dag_id="plan-limit")
    plan = _plan(client, dag_id="plan-limit", limit=2).json()
    assert [t["task_key"] for t in plan["claimed"]] == ["a", "b"]
    reasons = _reason_map(plan)
    assert reasons["c"]["reason"] == "limit_reached"
    assert reasons["c"]["limit"] == 2


def test_claim_plan_capability_mismatch_skip_and_parity(client):
    _requires_dag(client, dag_id="plan-cap")
    register(client, "w-gpu", ["gpu"])
    plan = _plan(client, worker_id="w-gpu", dag_id="plan-cap", limit=10).json()
    assert [t["task_key"] for t in plan["claimed"]] == ["plain", "gpu"]
    reasons = _reason_map(plan)
    assert reasons["both"]["reason"] == "capability_mismatch"
    assert reasons["both"]["requires"] == ["gpu", "io"]
    assert "gpu" in reasons["both"]["capabilities"]

    real = client.post(
        "/tasks/claim", json={"worker_id": "w-gpu", "dag_id": "plan-cap", "limit": 10}
    ).json()
    assert [t["task_key"] for t in real["claimed"]] == ["plain", "gpu"]


def test_claim_plan_409_when_worker_not_registered_before_batch_fills(client):
    _requires_dag(client, dag_id="plan-409")
    r = _plan(client, dag_id="plan-409", limit=10)
    assert r.status_code == 409
    body = r.json()["error"]
    assert body["code"] == "worker_not_registered"
    assert body["details"]["worker_id"] is None
    assert body["details"]["requires"] == ["gpu"]
    # 409 同样不写入：任务仍全部 pending、无领取类事件
    dag = client.get("/dags/plan-409").json()
    assert dag["states"]["pending"] == 3
    events = client.get("/events", params={"dag_id": "plan-409"}).json()["events"]
    assert all(e["type"] == "dag.created" for e in events)


def test_claim_plan_no_409_for_requires_candidate_after_limit_reached(client):
    """批量填满后才遇到的 requires 候选不再触发 409（与真实领取扫描边界一致）。"""
    tasks = [{"key": f"p{i}"} for i in range(3)]
    tasks.append({"key": "late-gpu", "requires": ["gpu"]})
    submit(client, tasks, dag_id="plan-late-gpu")
    # limit=3：前 3 个无能力要求任务填满后才轮到 late-gpu
    r = _plan(client, dag_id="plan-late-gpu", limit=3)
    assert r.status_code == 200
    body = r.json()
    assert [t["task_key"] for t in body["claimed"]] == ["p0", "p1", "p2"]
    reasons = _reason_map(body)
    assert reasons["late-gpu"]["reason"] == "limit_reached"


def test_claim_plan_409_parity_with_real_claim_when_capacity_skips_first(client):
    """满组挡住的 requires 候选先被组容量跳过（不触发 409），门控顺序与真实领取一致。"""
    submit(
        client,
        [{"key": "a", "concurrency_group": "g", "concurrency_limit": 1, "requires": ["gpu"]}],
        dag_id="g-full",
    )
    # 占用唯一名额（真实领取需要有能力的 worker）
    register(client, "w-gpu", ["gpu"])
    client.post("/tasks/claim", json={"worker_id": "w-gpu", "dag_id": "g-full", "limit": 5})
    submit(
        client,
        [{"key": "b", "concurrency_group": "g", "concurrency_limit": 1, "requires": ["gpu"]}],
        dag_id="g-full-2",
    )
    # 匿名预演：b 先被满组挡住，不触发能力 409
    plan = _plan(client, dag_id="g-full-2", limit=5).json()
    assert plan["claimed"] == []
    assert plan["unselected"][0]["reason"] == "concurrency_group_full"


def test_claim_plan_expired_lease_reclaimable_and_releases_nothing_pending(client):
    """过期租约任务可被模拟重领；它不是 pending，不出现在 unselected 中。"""
    submit(client, [{"key": "a", "lease_seconds": 0.1}], dag_id="plan-exp")
    client.post("/tasks/claim", json={"dag_id": "plan-exp"})
    time.sleep(0.15)
    plan = _plan(client, dag_id="plan-exp", limit=10).json()
    assert [t["task_key"] for t in plan["claimed"]] == ["a"]
    assert plan["claimed"][0]["attempt"] == 2
    assert plan["unselected"] == []
    # 状态仍是 running、attempt 仍为 1（预演未写）
    dag = client.get("/dags/plan-exp").json()
    assert dag["states"]["running"] == 1
    assert dag["tasks"][0]["attempt"] == 1


def test_claim_plan_unknown_dag_404(client):
    r = _plan(client, dag_id="nope")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "dag_not_found"


def test_claim_plan_uses_claim_request_validation(client):
    """请求参数与 /tasks/claim 一致：同样的 422 校验口径。"""
    assert _plan(client, limit=0).status_code == 422
    assert _plan(client, limit=101).status_code == 422


def test_claim_plan_repeated_calls_are_deterministic_and_readonly(client):
    submit(client, [{"key": "a"}, {"key": "b"}], dag_id="plan-repeat")
    r1 = _plan(client, dag_id="plan-repeat", limit=1).json()
    r2 = _plan(client, dag_id="plan-repeat", limit=1).json()
    assert [t["task_id"] for t in r1["claimed"]] == [t["task_id"] for t in r2["claimed"]]
    assert r1["unselected"] == r2["unselected"]
    dag = client.get("/dags/plan-repeat").json()
    assert dag["states"]["pending"] == 2


def test_claim_plan_unselected_only_contains_pending_tasks(client):
    """succeeded/running/blocked/canceled 任务不出现在未选中说明中。"""
    submit(
        client,
        [
            {"key": "done"},
            {"key": "kid", "deps": ["done"]},
            {"key": "go"},
        ],
        dag_id="plan-states",
    )
    claimed = client.post("/tasks/claim", json={"dag_id": "plan-states", "limit": 10}).json()["claimed"]
    done = next(t for t in claimed if t["task_key"] == "done")
    go = next(t for t in claimed if t["task_key"] == "go")
    _complete(client, done, "done-ok", success=True)
    _complete(client, go, "go-fail", success=False, error="x")  # max_retries=0 -> failed

    plan = _plan(client, dag_id="plan-states", limit=10).json()
    keys = {u["task_key"] for u in plan["unselected"]}
    # kid 因上游 done 已成功而变为可领取（go 的失败不影响 kid）；go 是 failed 非 pending
    assert "go" not in keys
    selected_keys = {t["task_key"] for t in plan["claimed"]}
    assert "kid" in selected_keys


def test_claim_plan_endpoint_in_openapi(client):
    spec = client.get("/openapi.json").json()
    assert "/tasks/claim-plan" in spec["paths"]
    post = spec["paths"]["/tasks/claim-plan"]["post"]
    assert {"200", "404", "409", "422"} <= set(post["responses"])
    # 既有领取接口保持不变
    assert "/tasks/claim" in spec["paths"]

