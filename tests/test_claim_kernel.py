"""领取裁决内核（不可变批次计划）单测与 /tasks/claim ↔ /tasks/claim-plan 一致性测试。

- 内核单测直接调用 service.build_claim_plan / apply_claim_plan / serialize_claim_plan：
  计划内容（稳定顺序的选中项、未选中 pending 任务的首个阻塞原因、并发组即时占位
  结果）、计划不可变、应用计划生成令牌/attempt/事件、任一写入前置状态不匹配时
  整体回滚（不得部分领取）。
- 一致性测试通过 HTTP 接口验证：同一快照下 /tasks/claim-plan 的模拟选中序列与
  /tasks/claim 的真实领取序列完全一致，且 404/409 语义相同。

运行: pytest tests/test_claim_kernel.py -q
"""

from __future__ import annotations

import dataclasses
import sqlite3
import time
import uuid

import pytest

from app import service
from app.db import get_conn, init_db


@pytest.fixture()
def kernel_db(tmp_path, monkeypatch):
    """独立的临时 SQLite 库，供内核级（非 HTTP）测试直接调用 service。"""
    db_file = tmp_path / "kernel.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")
    init_db()
    yield db_file


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


def _build_plan(worker_id=None, dag_id=None, limit=1):
    """在独立只读事务内构建计划并原样返回（计划是不可变值对象，事务外可用）。"""
    now = service._now()
    now_iso = service._iso(now)
    with get_conn() as conn:
        conn.execute("BEGIN")
        try:
            plan = service.build_claim_plan(
                conn,
                worker_id=worker_id,
                dag_id=dag_id,
                limit=limit,
                now=now,
                now_iso=now_iso,
            )
        finally:
            conn.execute("ROLLBACK")
    return plan


def _apply_plan(plan):
    """在 BEGIN IMMEDIATE 事务内应用计划并提交；异常时整体回滚后再抛出。"""
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            claimed = service.apply_claim_plan(conn, plan)
            conn.execute("COMMIT")
            return claimed
        except Exception:
            conn.execute("ROLLBACK")
            raise


def _submit_via_service(tasks, dag_id):
    return service.submit_dag({"id": dag_id, "name": "dag", "tasks": tasks})


# ------------------------------------------------------------- 内核：计划内容

def test_plan_records_selected_unselected_and_group_reservations(kernel_db):
    """计划统一记录：稳定顺序的选中项、未选中 pending 任务的首个阻塞原因、
    并发组即时占位结果。"""
    _submit_via_service(
        [
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g2", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g3", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "u1"},
            {"key": "u2"},
        ],
        "kern-1",
    )
    plan = _build_plan(worker_id="w-1", dag_id="kern-1", limit=5)

    assert isinstance(plan, service.ClaimPlan)
    assert plan.worker_id == "w-1" and plan.dag_id == "kern-1" and plan.limit == 5
    assert plan.evaluated_at

    # 选中项按候选 rowid 稳定顺序：组内只放进 2 个，继续向后填满 limit
    assert [s.task_key for s in plan.selected] == ["g1", "g2", "u1", "u2"]
    g1 = plan.selected[0]
    assert g1.attempt == 1 and g1.concurrency_group == "g"
    assert g1.expected_state == "pending" and g1.expected_attempt == 0
    assert g1.expected_lease_token is None and g1.from_state == "pending"
    assert g1.lease_expires_at and g1.requires == ()

    # 未选中 pending 任务的首个阻塞原因：g3 被并发组容量挡住（含本批即时占位）
    assert [u.task_key for u in plan.unselected] == ["g3"]
    un = plan.unselected[0]
    assert un.reason == "concurrency_group_full"
    assert un.concurrency_group == "g" and un.group_limit == 2 and un.group_occupied == 2

    # 并发组即时占位结果：批前占用 0，本批占位 2
    assert len(plan.groups) == 1
    res = plan.groups[0]
    assert (res.group, res.limit, res.occupied_before, res.reserved) == ("g", 2, 0, 2)
    assert res.occupied_after == 2


def test_plan_records_dependency_and_backoff_reasons(kernel_db):
    """候选集外的 pending 任务按依赖 → 退避补齐首个阻塞原因。"""
    _submit_via_service(
        [
            {"key": "a", "max_retries": 2,
             "retry_backoff": {"initial_seconds": 30, "multiplier": 1, "max_seconds": 30}},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["b"]},
        ],
        "kern-2",
    )
    claimed = service.claim_tasks(None, "kern-2", 1)["claimed"]
    assert [t["task_key"] for t in claimed] == ["a"]
    # a 显式失败进入退避等待（pending, next_eligible_at 未到）
    result = service.complete_task(
        claimed[0]["task_id"],
        {"completion_id": "f1", "lease_token": claimed[0]["lease_token"],
         "success": False, "error": "boom"},
    )
    assert result["state"] == "pending" and result["next_eligible_at"] is not None

    plan = _build_plan(dag_id="kern-2", limit=10)
    assert plan.selected == ()
    reasons = {u.task_key: u for u in plan.unselected}
    assert reasons["a"].reason == "backoff_not_elapsed"
    assert reasons["a"].next_eligible_at == result["next_eligible_at"]
    assert reasons["b"].reason == "dependency_not_succeeded"
    assert reasons["b"].blocked_by == (("a", "pending"),)
    assert reasons["c"].reason == "dependency_not_succeeded"
    assert reasons["c"].blocked_by == (("b", "pending"),)


def test_plan_selected_dependency_reports_simulated_running(kernel_db):
    """本批选中的 pending 前置在未选中原因的 blocked_by 中按 running 报告。"""
    _submit_via_service([{"key": "a"}, {"key": "b", "deps": ["a"]}], "kern-3")
    plan = _build_plan(dag_id="kern-3", limit=10)
    assert [s.task_key for s in plan.selected] == ["a"]
    assert plan.unselected[0].task_key == "b"
    assert plan.unselected[0].blocked_by == (("a", "running"),)


def test_plan_is_immutable(kernel_db):
    """计划与其组成部分均为不可变值对象：任何字段都不可再赋值。"""
    _submit_via_service(
        [{"key": "a", "concurrency_group": "g", "concurrency_limit": 1}], "kern-4"
    )
    plan = _build_plan(dag_id="kern-4", limit=1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.limit = 99
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.selected = ()
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.selected[0].attempt = 7
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.groups[0].reserved = 9
    # 集合类型字段同样是不可变容器
    assert isinstance(plan.selected, tuple) and isinstance(plan.groups, tuple)
    assert isinstance(plan.selected[0].requires, tuple)


def test_build_plan_unknown_dag_raises_404(kernel_db):
    with pytest.raises(service.APIError) as exc:
        _build_plan(dag_id="ghost", limit=1)
    assert exc.value.status_code == 404
    assert exc.value.code == "dag_not_found"


def test_build_plan_raises_409_for_unregistered_worker(kernel_db):
    """无存活登记（含匿名）遇到其余门控均满足的 requires 候选：内核抛 409。"""
    _submit_via_service([{"key": "a", "requires": ["gpu"]}], "kern-5")
    for worker_id in (None, "ghost"):
        with pytest.raises(service.APIError) as exc:
            _build_plan(worker_id=worker_id, dag_id="kern-5", limit=1)
        err = exc.value
        assert err.status_code == 409 and err.code == "worker_not_registered"
        assert err.details["worker_id"] == worker_id
        assert err.details["requires"] == ["gpu"]
        assert err.details["task_key"] == "a"


# ------------------------------------------------------------- 内核：应用计划

def test_apply_plan_generates_tokens_attempts_and_events(kernel_db):
    """应用计划：整批落库，生成唯一令牌、active attempt 与 task.claimed 事件。"""
    _submit_via_service([{"key": "a"}, {"key": "b"}], "kern-6")
    plan = _build_plan(worker_id="w-9", dag_id="kern-6", limit=10)
    claimed = _apply_plan(plan)

    assert [c["task_key"] for c in claimed] == ["a", "b"]
    tokens = [c["lease_token"] for c in claimed]
    assert all(tokens) and len(set(tokens)) == 2  # 令牌唯一且非空
    assert all(c["attempt"] == 1 for c in claimed)

    dag = service.get_dag("kern-6")
    assert dag["states"]["running"] == 2
    for t in dag["tasks"]:
        detail = service.get_task(t["id"])
        assert detail["lease"]["worker_id"] == "w-9"
        assert [a["state"] for a in detail["attempts"]] == ["active"]

    events = service.list_events(0, 50, "kern-6")["events"]
    claimed_events = [e for e in events if e["type"] == "task.claimed"]
    assert len(claimed_events) == 2
    assert all(e["from_state"] == "pending" and e["to_state"] == "running" for e in claimed_events)
    assert {e["task_id"] for e in claimed_events} == {c["task_id"] for c in claimed}


def test_apply_plan_reclaims_expired_lease_with_new_attempt(kernel_db):
    """过期租约重领：计划记录旧状态快照，应用后 attempt+1、旧令牌失效。"""
    _submit_via_service([{"key": "a", "lease_seconds": 0.2}], "kern-7")
    first = service.claim_tasks(None, "kern-7", 1)["claimed"][0]
    time.sleep(0.25)

    plan = _build_plan(dag_id="kern-7", limit=1)
    item = plan.selected[0]
    assert item.attempt == 2
    assert item.expected_state == "running"
    assert item.expected_lease_token == first["lease_token"]
    assert item.from_state == "running"

    claimed = _apply_plan(plan)
    assert claimed[0]["lease_token"] != first["lease_token"]
    detail = service.get_task(first["task_id"])
    assert detail["attempt"] == 2 and detail["state"] == "running"
    assert [a["state"] for a in detail["attempts"]] == ["expired", "active"]
    events = service.list_events(0, 50, "kern-7")["events"]
    last = events[-1]
    assert last["type"] == "task.claimed" and last["from_state"] == "running"


def test_apply_plan_rolls_back_entire_batch_on_task_precondition_mismatch(kernel_db):
    """任一任务的写入前置状态不匹配：整批回滚，不得部分领取。"""
    dag = _submit_via_service([{"key": "a"}, {"key": "b"}, {"key": "c"}], "kern-8")
    plan = _build_plan(dag_id="kern-8", limit=10)
    assert [s.task_key for s in plan.selected] == ["a", "b", "c"]

    # 计划评估后：b 被外部取消（状态前置不再匹配）
    b_id = by_key(dag, "b")["id"]
    service.cancel_task(b_id)

    with pytest.raises(service.APIError) as exc:
        _apply_plan(plan)
    err = exc.value
    assert err.status_code == 409 and err.code == "task_changed"
    assert err.details["task_key"] == "b"

    # 整体回滚：排在前面的 a 也不得被领取——无 running、无 attempt、无领取事件
    full = service.get_dag("kern-8")
    assert full["states"]["pending"] == 2
    assert full["states"]["running"] == 0
    assert full["states"]["canceled"] == 1
    for key in ("a", "c"):
        task = by_key(full, key)
        assert task["state"] == "pending" and task["attempt"] == 0
        assert service.get_task(task["id"])["attempts"] == []
    events = service.list_events(0, 50, "kern-8")["events"]
    assert [e["type"] for e in events] == ["dag.created", "task.canceled"]


def test_apply_plan_rolls_back_on_group_occupancy_mismatch(kernel_db):
    """并发组占用在计划评估后被改动：整批回滚，组内任务一个都不领。"""
    _submit_via_service(
        [
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g2", "concurrency_group": "g", "concurrency_limit": 2},
        ],
        "kern-9a",
    )
    _submit_via_service(
        [{"key": "h1", "concurrency_group": "g", "concurrency_limit": 2}], "kern-9b"
    )
    plan = _build_plan(dag_id="kern-9a", limit=5)
    assert [s.task_key for s in plan.selected] == ["g1", "g2"]
    assert plan.groups[0].occupied_before == 0 and plan.groups[0].reserved == 2

    # 计划评估后：另一个 DAG 的任务真实占掉 1 个组名额
    service.claim_tasks(None, "kern-9b", 1)

    with pytest.raises(service.APIError) as exc:
        _apply_plan(plan)
    err = exc.value
    assert err.status_code == 409 and err.code == "task_changed"
    assert err.details["concurrency_group"] == "g"
    assert err.details["expected_occupied"] == 0
    assert err.details["actual_occupied"] == 1

    # 整体回滚：g1/g2 仍 pending、无 attempt、无领取事件
    full = service.get_dag("kern-9a")
    assert full["states"]["pending"] == 2 and full["states"]["running"] == 0
    events = service.list_events(0, 50, "kern-9a")["events"]
    assert [e["type"] for e in events] == ["dag.created"]


def test_apply_plan_detects_lease_token_precondition(kernel_db):
    """过期重领计划评估后任务被他人重领（令牌已变）：前置不匹配，整批回滚。"""
    _submit_via_service([{"key": "a", "lease_seconds": 0.2}, {"key": "b"}], "kern-10")
    first = service.claim_tasks(None, "kern-10", 1)["claimed"][0]
    assert first["task_key"] == "a"
    time.sleep(0.25)

    plan = _build_plan(dag_id="kern-10", limit=10)
    assert [s.task_key for s in plan.selected] == ["a", "b"]

    # 计划评估后：a 被他人抢先重领（attempt/lease_token 均已变化）
    service.claim_tasks(None, "kern-10", 1)

    with pytest.raises(service.APIError) as exc:
        _apply_plan(plan)
    assert exc.value.status_code == 409 and exc.value.code == "task_changed"

    # b 不得被部分领取：仍 pending、无 attempt
    full = service.get_dag("kern-10")
    assert by_key(full, "b")["state"] == "pending"
    assert by_key(full, "b")["attempt"] == 0
    events = service.list_events(0, 50, "kern-10")["events"]
    # 只有两次真实领取事件（首次 + 他人重领），本批应用未留下任何事件
    assert [e["type"] for e in events].count("task.claimed") == 2


def test_serialize_plan_matches_claim_plan_response_shape(kernel_db):
    """序列化结果与 /tasks/claim-plan 响应结构一致（不签发令牌）。"""
    _submit_via_service(
        [{"key": "a", "payload": {"x": 1}, "requires": ["gpu"]}, {"key": "b"}], "kern-11"
    )
    service.register_worker("w-gpu", {"capabilities": ["gpu"], "ttl_seconds": 60})
    plan = _build_plan(worker_id="w-gpu", dag_id="kern-11", limit=10)
    body = service.serialize_claim_plan(plan)

    assert body["simulated"] is True
    assert body["worker_id"] == "w-gpu" and body["dag_id"] == "kern-11"
    assert body["limit"] == 10 and body["evaluated_at"] == plan.evaluated_at
    assert body["count"] == 2
    assert [t["task_key"] for t in body["claimed"]] == ["a", "b"]
    item = body["claimed"][0]
    assert set(item) == {
        "task_id", "dag_id", "task_key", "name", "payload", "attempt",
        "lease_token", "lease_expires_at", "lease_seconds", "concurrency_group",
        "requires",
    }
    assert item["lease_token"] is None
    assert item["payload"] == {"x": 1} and item["requires"] == ["gpu"]
    assert body["unselected"] == []


# ------------------------------------------------------------- 两接口一致性

def test_plan_then_claim_produces_identical_selection(client):
    """同一快照下：预演选中序列与真实领取序列逐场景一致（含组占位与能力门控）。"""
    # 场景 1：依赖链
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="cc-deps")
    # 场景 2：并发组即时占位（组上限 2，三个组任务 + 两个无组任务）
    submit(
        client,
        [
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g2", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g3", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "u1"},
            {"key": "u2"},
        ],
        dag_id="cc-group",
    )
    # 场景 3：能力门控（plain 无要求；gpu 要求 gpu；both 要求 gpu+io）
    submit(
        client,
        [
            {"key": "plain"},
            {"key": "gpu", "requires": ["gpu"]},
            {"key": "both", "requires": ["gpu", "io"]},
        ],
        dag_id="cc-cap",
    )
    client.put("/workers/w-gpu", json={"capabilities": ["gpu"], "ttl_seconds": 60})

    # 场景 4：limit 截断（独立 DAG，前 2 个入选，第 3 个记 limit_reached）
    submit(client, [{"key": "t0"}, {"key": "t1"}, {"key": "t2"}], dag_id="cc-limit")

    scenarios = [
        ({"dag_id": "cc-deps", "limit": 10}, ["a"]),
        ({"dag_id": "cc-group", "limit": 5}, ["g1", "g2", "u1", "u2"]),
        ({"dag_id": "cc-cap", "limit": 10, "worker_id": "w-gpu"}, ["plain", "gpu"]),
        ({"dag_id": "cc-limit", "limit": 2}, ["t0", "t1"]),
    ]
    for body, expected_keys in scenarios:
        plan = client.post("/tasks/claim-plan", json=body).json()
        assert [t["task_key"] for t in plan["claimed"]] == expected_keys, body
        real = client.post("/tasks/claim", json=body).json()
        assert [t["task_key"] for t in real["claimed"]] == expected_keys, body
        # 选中序列按 task_id 完全一致；预演不签发令牌，真实领取签发唯一令牌
        assert [t["task_id"] for t in plan["claimed"]] == [t["task_id"] for t in real["claimed"]]
        assert all(t["lease_token"] is None for t in plan["claimed"])
        tokens = [t["lease_token"] for t in real["claimed"]]
        assert all(tokens) and len(set(tokens)) == len(tokens)


def test_plan_claimed_entries_match_real_claim_fields(client):
    """除租约字段外，预演选中项与真实领取项的对外字段逐个一致。"""
    submit(
        client,
        [
            {"key": "a", "payload": {"n": 1}, "requires": ["gpu"],
             "concurrency_group": "g", "concurrency_limit": 2, "lease_seconds": 45},
        ],
        dag_id="cc-fields",
    )
    client.put("/workers/w-gpu", json={"capabilities": ["gpu"], "ttl_seconds": 60})
    body = {"dag_id": "cc-fields", "worker_id": "w-gpu", "limit": 5}

    plan_item = client.post("/tasks/claim-plan", json=body).json()["claimed"][0]
    real_item = client.post("/tasks/claim", json=body).json()["claimed"][0]

    for field in ("task_id", "dag_id", "task_key", "name", "payload", "attempt",
                  "lease_seconds", "concurrency_group", "requires"):
        assert plan_item[field] == real_item[field], field
    assert plan_item["lease_token"] is None
    assert real_item["lease_token"]
    # 两者的租约过期时刻都按各自评估时刻 + lease_seconds 计算
    assert plan_item["lease_expires_at"] and real_item["lease_expires_at"]


def test_plan_unselected_tasks_stay_unclaimed_after_real_claim(client):
    """预演判定的未选中任务在真实领取后仍未被领取，原因与最终状态一致。"""
    submit(
        client,
        [
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 1},
            {"key": "g2", "concurrency_group": "g", "concurrency_limit": 1},
            {"key": "kid", "deps": ["g1"]},
        ],
        dag_id="cc-unsel",
    )
    plan = client.post("/tasks/claim-plan", json={"dag_id": "cc-unsel", "limit": 5}).json()
    assert [t["task_key"] for t in plan["claimed"]] == ["g1"]
    reasons = {u["task_key"]: u for u in plan["unselected"]}
    assert reasons["g2"]["reason"] == "concurrency_group_full"
    assert reasons["kid"]["reason"] == "dependency_not_succeeded"
    assert reasons["kid"]["blocked_by"] == [{"task_key": "g1", "state": "running"}]

    real = client.post("/tasks/claim", json={"dag_id": "cc-unsel", "limit": 5}).json()
    assert [t["task_key"] for t in real["claimed"]] == ["g1"]

    # 真实领取后：g2 仍 pending（组满）、kid 仍 pending（依赖未满足）
    full = client.get("/dags/cc-unsel").json()
    assert by_key(full, "g2")["state"] == "pending"
    assert by_key(full, "kid")["state"] == "pending"

    # 再次预演：g1 租约未过期不再是候选，g2 仍被组满挡住（占用来自真实领取）
    plan2 = client.post("/tasks/claim-plan", json={"dag_id": "cc-unsel", "limit": 5}).json()
    assert plan2["claimed"] == []
    reasons2 = {u["task_key"]: u for u in plan2["unselected"]}
    assert reasons2["g2"]["reason"] == "concurrency_group_full"
    assert reasons2["g2"]["group_occupied"] == 1
    assert reasons2["kid"]["blocked_by"] == [{"task_key": "g1", "state": "running"}]


def test_plan_and_claim_share_409_semantics(client):
    """两接口对同一无登记领取返回完全相同的 409 错误体，且都不落任何写入。"""
    submit(
        client,
        [{"key": "plain"}, {"key": "gpu", "requires": ["gpu"]}],
        dag_id="cc-409",
    )
    for body in ({"dag_id": "cc-409", "limit": 10},
                 {"dag_id": "cc-409", "limit": 10, "worker_id": "ghost"}):
        r_plan = client.post("/tasks/claim-plan", json=body)
        r_claim = client.post("/tasks/claim", json=body)
        assert r_plan.status_code == 409 and r_claim.status_code == 409
        assert r_plan.json() == r_claim.json()
        err = r_claim.json()["error"]
        assert err["code"] == "worker_not_registered"
        assert err["details"]["task_key"] == "gpu"

    # 两次 409 之后：无任务被领取、无领取事件
    full = client.get("/dags/cc-409").json()
    assert full["states"]["pending"] == 2 and full["states"]["running"] == 0
    events = client.get("/events", params={"dag_id": "cc-409"}).json()["events"]
    assert [e["type"] for e in events] == ["dag.created"]


def test_plan_and_claim_share_404_semantics(client):
    for path in ("/tasks/claim-plan", "/tasks/claim"):
        r = client.post(path, json={"dag_id": "nope", "limit": 1})
        assert r.status_code == 404
        assert r.json()["error"]["code"] == "dag_not_found"


def test_plan_does_not_consume_batch_and_claim_remains_atomic(client):
    """预演不消耗任何名额：预演后的真实领取照常整批生效；并发领取仍只有一个赢家。"""
    submit(client, [{"key": "a"}], dag_id="cc-atomic")

    for _ in range(3):
        plan = client.post("/tasks/claim-plan", json={"dag_id": "cc-atomic"}).json()
        assert [t["task_key"] for t in plan["claimed"]] == ["a"]

    import threading

    results, errors = [], []

    def worker():
        try:
            r = client.post("/tasks/claim", json={"dag_id": "cc-atomic"})
            results.extend(r.json()["claimed"])
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    assert len(results) == 1 and results[0]["attempt"] == 1
    # 领取后预演为空（租约未过期）
    plan = client.post("/tasks/claim-plan", json={"dag_id": "cc-atomic"}).json()
    assert plan["claimed"] == [] and plan["unselected"] == []


def test_claimed_task_completion_unlocks_dependent_in_both_endpoints(client):
    """真实领取并回报成功后，两接口对下一批可选任务的裁决继续一致。"""
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="cc-flow")
    plan = client.post("/tasks/claim-plan", json={"dag_id": "cc-flow"}).json()
    real = client.post("/tasks/claim", json={"dag_id": "cc-flow"}).json()
    assert [t["task_id"] for t in plan["claimed"]] == [t["task_id"] for t in real["claimed"]]

    t = real["claimed"][0]
    r = client.post(
        f"/tasks/{t['task_id']}/complete",
        json={"completion_id": "ok-a", "lease_token": t["lease_token"], "success": True},
    )
    assert r.status_code == 200

    plan2 = client.post("/tasks/claim-plan", json={"dag_id": "cc-flow"}).json()
    assert [t["task_key"] for t in plan2["claimed"]] == ["b"]
    assert plan2["unselected"] == []
    real2 = client.post("/tasks/claim", json={"dag_id": "cc-flow"}).json()
    assert [t["task_key"] for t in real2["claimed"]] == ["b"]


def test_plan_is_byte_stable_across_repeated_calls_and_claim_unaffected(client):
    """重复预演结果确定（除评估时刻）；预演不改变真实领取结果。"""
    submit(client, [{"key": "a"}, {"key": "b"}], dag_id="cc-repeat")
    r1 = client.post("/tasks/claim-plan", json={"dag_id": "cc-repeat", "limit": 1}).json()
    r2 = client.post("/tasks/claim-plan", json={"dag_id": "cc-repeat", "limit": 1}).json()
    # 除按各自评估时刻计算的 lease_expires_at 外，选中项逐字段一致
    strip = lambda items: [{k: v for k, v in t.items() if k != "lease_expires_at"} for t in items]
    assert strip(r1["claimed"]) == strip(r2["claimed"])
    assert [t["task_id"] for t in r1["claimed"]] == [t["task_id"] for t in r2["claimed"]]
    assert r1["unselected"] == r2["unselected"]
    assert r1["unselected"][0]["reason"] == "limit_reached"

    real = client.post("/tasks/claim", json={"dag_id": "cc-repeat", "limit": 1}).json()
    assert [t["task_id"] for t in real["claimed"]] == [t["task_id"] for t in r1["claimed"]]
