"""领取裁决内核单测与 /tasks/claim ↔ /tasks/claim-plan 一致性测试。

- 内核单测直接驱动 build_claim_plan / apply_claim_plan / serialize_claim_plan：
  计划是显式不可变的值对象，统一记录稳定顺序下的选中项、每个未选中 pending
  任务的首个阻塞原因与并发组即时占位结果；应用阶段任一写入前置状态不匹配
  即整体回滚，不得部分领取。
- 一致性测试通过 HTTP 同时调用两个接口，断言同一快照下计划选中序列与真实
  领取逐候选一致、错误体一致、响应结构与历史口径一致。

运行: pytest -q tests/test_claim_kernel.py
"""

from __future__ import annotations

import dataclasses
import time
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture()
def service(tmp_path, monkeypatch):
    """独立 SQLite 库 + 初始化 schema，直接驱动裁决内核。"""
    monkeypatch.setenv("DB_PATH", str(tmp_path / "kernel.db"))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")
    from app.db import init_db

    init_db()
    from app import service as svc

    return svc


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as c:
        yield c


def _submit(svc, tasks, dag_id):
    return svc.submit_dag({"id": dag_id, "tasks": tasks})


def _build(svc, *, now=None, **kwargs):
    """在只读事务内构建计划（与 /tasks/claim-plan 同事务口径）。"""
    from app.db import get_conn

    with get_conn() as conn:
        conn.execute("BEGIN")
        try:
            plan = svc.build_claim_plan(conn, now=now or svc._now(), **kwargs)
        finally:
            conn.execute("ROLLBACK")
    return plan


def _apply(svc, plan):
    """在 BEGIN IMMEDIATE 事务内应用计划并提交（与 /tasks/claim 同路径）。"""
    from app.db import get_conn

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            claimed = svc.apply_claim_plan(conn, plan, svc._iso(svc._now()))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return claimed


# ------------------------------------------------------------- 内核：计划构建

def test_kernel_plan_is_explicit_and_immutable(service):
    _submit(service, [{"key": "a"}, {"key": "b", "deps": ["a"]}], "k-immut")

    plan = _build(service, worker_id="w", dag_id="k-immut", limit=10)

    # 计划是显式的值对象：请求参数、评估时刻、选中项、未选中原因、组占用统一记录
    assert isinstance(plan, service.ClaimPlan)
    assert (plan.worker_id, plan.dag_id, plan.limit) == ("w", "k-immut", 10)
    assert plan.evaluated_at
    assert isinstance(plan.selected, tuple) and isinstance(plan.unselected, tuple)
    assert [s.row["task_key"] for s in plan.selected] == ["a"]
    assert [b.task_key for b in plan.unselected] == ["b"]

    # 不可变：计划本身、选中项、阻塞项与组占用映射都不可再修改
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.selected = ()
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.selected[0].attempt_no = 99
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.unselected[0].reason = "limit_reached"
    with pytest.raises(TypeError):
        plan.group_occupancy["g"] = 1
    with pytest.raises(TypeError):
        plan.unselected[0].extra["limit"] = 1


def test_kernel_selected_items_carry_stable_order_and_derived_fields(service):
    _submit(
        service,
        [
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g2", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "g3", "concurrency_group": "g", "concurrency_limit": 2},
            {"key": "u1", "lease_seconds": 60},
            {"key": "u2"},
        ],
        "k-order",
    )
    now = datetime.now(timezone.utc)
    plan = _build(service, worker_id=None, dag_id="k-order", limit=5, now=now)

    # 稳定顺序（rowid）：组满跳过后继续向后填满 limit
    assert [s.row["task_key"] for s in plan.selected] == ["g1", "g2", "u1", "u2"]
    # 派生量：attempt 序号与按计划时刻计算的租约过期时刻
    for sel in plan.selected:
        assert sel.attempt_no == 1
        assert sel.expires == now + timedelta(seconds=sel.row["lease_seconds"])
    # 并发组即时占位结果：本批选中后组 g 占用达到上限
    assert plan.group_occupancy["g"] == 2
    # 未选中 pending 任务的首个阻塞原因及相关字段
    (blocker,) = plan.unselected
    assert blocker.task_key == "g3"
    assert blocker.reason == "concurrency_group_full"
    assert blocker.extra == {
        "concurrency_group": "g",
        "group_limit": 2,
        "group_occupied": 2,
    }


def test_kernel_unselected_reasons_follow_gate_order(service):
    """首个阻塞原因按门控顺序裁决：依赖 → 退避（候选集外），组满 → 能力 → 限额（候选内）。"""
    _submit(
        service,
        [
            {
                "key": "a",
                "max_retries": 1,
                "retry_backoff": {"initial_seconds": 300, "multiplier": 1, "max_seconds": 300},
            },
            {"key": "b", "deps": ["a"]},
        ],
        "k-gates",
    )
    # a 显式失败进入退避等待：a 退避未到期，b 被依赖挡住（依赖先于退避）
    t = service.claim_tasks(None, "k-gates", 1)["claimed"][0]
    service.complete_task(
        t["task_id"],
        {"completion_id": "f1", "lease_token": t["lease_token"], "success": False},
    )

    plan = _build(service, worker_id=None, dag_id="k-gates", limit=10)
    assert plan.selected == ()
    reasons = {b.task_key: b for b in plan.unselected}
    assert reasons["a"].reason == "backoff_not_elapsed"
    assert reasons["a"].extra["next_eligible_at"]
    assert reasons["b"].reason == "dependency_not_succeeded"
    assert reasons["b"].extra["blocked_by"] == [{"task_key": "a", "state": "pending"}]


def test_kernel_capability_and_limit_reasons(service):
    _submit(
        service,
        [
            {"key": "p0"},
            {"key": "p1"},
            {"key": "gpu", "requires": ["gpu"]},
            {"key": "gpuio", "requires": ["gpu", "io"]},
        ],
        "k-cap",
    )
    service.register_worker("w1", {"capabilities": ["gpu"], "ttl_seconds": 60})

    # limit=2：p0/p1 填满批量；gpu 能力覆盖但只因限额未选中，
    # gpuio 能力不覆盖（能力门控先于限额裁决）
    plan = _build(service, worker_id="w1", dag_id="k-cap", limit=2)
    assert [s.row["task_key"] for s in plan.selected] == ["p0", "p1"]
    reasons = {b.task_key: b for b in plan.unselected}
    assert reasons["gpu"].reason == "limit_reached"
    assert reasons["gpu"].extra == {"limit": 2}
    assert reasons["gpuio"].reason == "capability_mismatch"
    assert reasons["gpuio"].extra["requires"] == ["gpu", "io"]
    assert reasons["gpuio"].extra["capabilities"] == ["gpu"]

    # limit 放宽后：gpu 被选中，gpuio 仍因能力不匹配未选中
    plan2 = _build(service, worker_id="w1", dag_id="k-cap", limit=4)
    assert [s.row["task_key"] for s in plan2.selected] == ["p0", "p1", "gpu"]
    reasons2 = {b.task_key: b for b in plan2.unselected}
    assert reasons2["gpuio"].reason == "capability_mismatch"


def test_kernel_dependency_reason_uses_simulated_states(service):
    """本批选中的 pending 前置在阻塞报告中按 running 计入（仍非 succeeded）。"""
    _submit(service, [{"key": "a"}, {"key": "b", "deps": ["a"]}], "k-sim")
    plan = _build(service, worker_id=None, dag_id="k-sim", limit=10)
    assert [s.row["task_key"] for s in plan.selected] == ["a"]
    (blocker,) = plan.unselected
    assert blocker.task_key == "b"
    assert blocker.reason == "dependency_not_succeeded"
    assert blocker.extra["blocked_by"] == [{"task_key": "a", "state": "running"}]


def test_kernel_group_occupancy_includes_existing_leases(service):
    """即时占位结果 = 已存在的未过期 running 租约 + 本批选中占位。"""
    _submit(
        service,
        [{"key": "a1", "concurrency_group": "shared", "concurrency_limit": 2}],
        "k-oa",
    )
    _submit(
        service,
        [
            {"key": "b1", "concurrency_group": "shared", "concurrency_limit": 2},
            {"key": "b2", "concurrency_group": "shared", "concurrency_limit": 2},
        ],
        "k-ob",
    )
    service.claim_tasks(None, "k-oa", 5)  # 已存在的 running 租约占 1 个名额

    plan = _build(service, worker_id=None, dag_id="k-ob", limit=5)
    assert [s.row["task_key"] for s in plan.selected] == ["b1"]
    assert plan.group_occupancy["shared"] == 2  # 已有 1 + 本批占位 1
    (blocker,) = plan.unselected
    assert blocker.task_key == "b2"
    assert blocker.reason == "concurrency_group_full"
    assert blocker.extra["group_occupied"] == 2


def test_kernel_raises_409_when_worker_not_registered_before_batch_fills(service):
    _submit(service, [{"key": "gpu", "requires": ["gpu"]}], "k-409")
    with pytest.raises(service.APIError) as exc:
        _build(service, worker_id=None, dag_id="k-409", limit=5)
    assert exc.value.status_code == 409
    assert exc.value.code == "worker_not_registered"
    assert exc.value.details["worker_id"] is None
    assert exc.value.details["requires"] == ["gpu"]
    assert exc.value.details["task_key"] == "gpu"


def test_kernel_requires_candidate_after_limit_is_limit_reached_not_409(service):
    """批量填满后才遇到的 requires 候选不触发 409，记为 limit_reached。"""
    _submit(
        service,
        [{"key": "p0"}, {"key": "p1"}, {"key": "late", "requires": ["gpu"]}],
        "k-late",
    )
    plan = _build(service, worker_id=None, dag_id="k-late", limit=2)
    assert [s.row["task_key"] for s in plan.selected] == ["p0", "p1"]
    assert [(b.task_key, b.reason) for b in plan.unselected] == [("late", "limit_reached")]


def test_kernel_group_full_gate_precedes_capability_409(service):
    """满组挡住的 requires 候选先被组容量跳过（不触发能力 409）。"""
    _submit(
        service,
        [{"key": "a", "concurrency_group": "g", "concurrency_limit": 1, "requires": ["gpu"]}],
        "k-gfull",
    )
    service.register_worker("w1", {"capabilities": ["gpu"], "ttl_seconds": 60})
    service.claim_tasks("w1", "k-gfull", 5)  # 占掉唯一名额
    _submit(
        service,
        [{"key": "b", "concurrency_group": "g", "concurrency_limit": 1, "requires": ["gpu"]}],
        "k-gfull2",
    )
    # 匿名计划：b 先被满组挡住，不触发能力 409
    plan = _build(service, worker_id=None, dag_id="k-gfull2", limit=5)
    assert plan.selected == ()
    assert [(b.task_key, b.reason) for b in plan.unselected] == [("b", "concurrency_group_full")]
    assert plan.group_occupancy["g"] == 1


def test_kernel_raises_404_for_unknown_dag(service):
    with pytest.raises(service.APIError) as exc:
        _build(service, worker_id=None, dag_id="nope", limit=1)
    assert exc.value.status_code == 404
    assert exc.value.code == "dag_not_found"


def test_kernel_expired_lease_is_reclaimable_and_not_unselected(service):
    """过期租约的 running 任务被计划为重领（attempt+1），且不属于 pending 不进 unselected。"""
    _submit(service, [{"key": "a", "lease_seconds": 0.1}], "k-exp")
    service.claim_tasks(None, "k-exp", 1)
    time.sleep(0.15)
    plan = _build(service, worker_id=None, dag_id="k-exp", limit=10)
    assert [s.row["task_key"] for s in plan.selected] == ["a"]
    assert plan.selected[0].attempt_no == 2
    assert plan.unselected == ()


def test_kernel_build_is_pure_read(service):
    """内核在 query_only 连接上运行：构建计划本身不做任何写入。"""
    _submit(service, [{"key": "a"}, {"key": "b"}], "k-ro")
    from app.db import get_conn

    with get_conn() as conn:
        conn.execute("PRAGMA query_only = ON")
        conn.execute("BEGIN")
        try:
            plan = service.build_claim_plan(
                conn, worker_id=None, dag_id="k-ro", limit=10, now=service._now()
            )
        finally:
            conn.execute("ROLLBACK")
    assert [s.row["task_key"] for s in plan.selected] == ["a", "b"]

    # 状态未变：再次计划得到同样的选中序列，任务仍是 pending、无 attempt
    again = _build(service, worker_id=None, dag_id="k-ro", limit=10)
    assert [s.row["task_key"] for s in again.selected] == ["a", "b"]
    dag = service.get_dag("k-ro")
    assert dag["states"]["pending"] == 2


# ------------------------------------------------------------- 内核：计划应用

def test_apply_plan_generates_tokens_attempts_and_events(service):
    _submit(service, [{"key": "a"}, {"key": "b"}], "k-apply")
    plan = _build(service, worker_id="w-1", dag_id="k-apply", limit=10)
    claimed = _apply(service, plan)

    assert [c["task_key"] for c in claimed] == ["a", "b"]
    tokens = {c["lease_token"] for c in claimed}
    assert len(tokens) == 2 and all(tokens)  # 每个选中项一个唯一令牌
    for c in claimed:
        assert c["attempt"] == 1
        detail = service.get_task(c["task_id"])
        assert detail["state"] == "running"
        assert detail["lease"]["token"] == c["lease_token"]
        assert detail["lease"]["worker_id"] == "w-1"
        assert [a["state"] for a in detail["attempts"]] == ["active"]
    events = service.list_events(0, 50, "k-apply")["events"]
    claimed_events = [e for e in events if e["type"] == "task.claimed"]
    assert [e["task_id"] for e in claimed_events] == [c["task_id"] for c in claimed]
    assert all(
        e["from_state"] == "pending" and e["to_state"] == "running" and e["attempt"] == 1
        for e in claimed_events
    )


def test_apply_plan_rolls_back_everything_when_precondition_mismatch(service):
    """任一写入前置状态不匹配：整批回滚，不得部分领取。"""
    _submit(service, [{"key": "a"}, {"key": "b"}], "k-stale")
    plan = _build(service, worker_id=None, dag_id="k-stale", limit=2)
    assert [s.row["task_key"] for s in plan.selected] == ["a", "b"]

    # 计划构建后、b 被外部取消：b 的快照前置（state=pending, attempt=0）不再匹配
    b_id = next(s.row["id"] for s in plan.selected if s.row["task_key"] == "b")
    service.cancel_task(b_id)
    events_before = service.list_events(0, 100, "k-stale")["events"]

    from app.db import get_conn

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        # a 会先成功落位，随后 b 的前置校验失败 —— 整体必须回滚
        with pytest.raises(service.APIError) as exc:
            service.apply_claim_plan(conn, plan, service._iso(service._now()))
        conn.execute("ROLLBACK")

    assert exc.value.status_code == 409
    assert exc.value.code == "claim_plan_stale"
    assert exc.value.details["task_id"] == b_id

    # 不得部分领取：a 仍是 pending、无 attempt、无 task.claimed 事件
    a_id = next(s.row["id"] for s in plan.selected if s.row["task_key"] == "a")
    a = service.get_task(a_id)
    assert a["state"] == "pending" and a["attempt"] == 0 and a["attempts"] == []
    assert service.get_task(b_id)["state"] == "canceled"
    events_after = service.list_events(0, 100, "k-stale")["events"]
    assert [e["sequence"] for e in events_after] == [e["sequence"] for e in events_before]


def test_serialized_plan_shape_matches_plan_contents(service):
    """序列化是计划的唯一对外投影：claimed 无令牌，unselected 携带原因与说明。"""
    _submit(service, [{"key": "a"}, {"key": "b", "deps": ["a"]}], "k-ser")
    plan = _build(service, worker_id=None, dag_id="k-ser", limit=10)
    body = service.serialize_claim_plan(plan)

    assert body["simulated"] is True
    assert body["dag_id"] == "k-ser" and body["limit"] == 10
    assert body["evaluated_at"] == plan.evaluated_at
    assert body["count"] == 1
    (item,) = body["claimed"]
    assert item["task_key"] == "a" and item["lease_token"] is None
    assert item["lease_expires_at"] == service._iso(plan.selected[0].expires)
    (entry,) = body["unselected"]
    assert entry["task_key"] == "b"
    assert entry["reason"] == "dependency_not_succeeded"
    assert entry["message"]
    assert entry["blocked_by"] == [{"task_key": "a", "state": "running"}]


# ------------------------------------------------------------- 两接口一致性

def _submit_http(client, tasks, dag_id):
    r = client.post("/dags", json={"id": dag_id, "tasks": tasks})
    assert r.status_code == 201, r.text
    return r.json()


def _plan(client, **body):
    r = client.post("/tasks/claim-plan", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def _claim(client, **body):
    r = client.post("/tasks/claim", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def _assert_selection_parity(plan_body, claim_body):
    """同一请求下：计划选中序列与真实领取完全一致（除租约签发字段）。"""
    planned, actual = plan_body["claimed"], claim_body["claimed"]
    assert plan_body["count"] == claim_body["count"] == len(actual)
    assert [t["task_id"] for t in planned] == [t["task_id"] for t in actual]
    for p, r in zip(planned, actual):
        assert p["task_key"] == r["task_key"]
        assert p["dag_id"] == r["dag_id"]
        assert p["name"] == r["name"]
        assert p["payload"] == r["payload"]
        assert p["attempt"] == r["attempt"]
        assert p["lease_seconds"] == r["lease_seconds"]
        assert p["concurrency_group"] == r["concurrency_group"]
        assert p["requires"] == r["requires"]
        # 计划不签发租约；真实领取签发非空令牌
        assert p["lease_token"] is None and r["lease_token"]


def test_parity_dependencies_and_limit(client):
    _submit_http(
        client,
        [{"key": "a"}, {"key": "b", "deps": ["a"]}, {"key": "c"}, {"key": "d"}],
        "par-deps",
    )
    req = {"dag_id": "par-deps", "limit": 2}
    plan = _plan(client, **req)
    real = _claim(client, **req)
    _assert_selection_parity(plan, real)
    assert [t["task_key"] for t in real["claimed"]] == ["a", "c"]
    # 计划中的未选中原因与真实领取后的状态一致（b 被依赖挡住、d 因限额未选中）
    reasons = {u["task_key"]: u["reason"] for u in plan["unselected"]}
    assert reasons == {"b": "dependency_not_succeeded", "d": "limit_reached"}


def test_parity_concurrency_groups(client):
    _submit_http(
        client,
        [
            {"key": "g1", "concurrency_group": "g", "concurrency_limit": 1},
            {"key": "g2", "concurrency_group": "g", "concurrency_limit": 1},
            {"key": "u1"},
        ],
        "par-group",
    )
    req = {"dag_id": "par-group", "limit": 10}
    plan = _plan(client, **req)
    real = _claim(client, **req)
    _assert_selection_parity(plan, real)
    assert [t["task_key"] for t in real["claimed"]] == ["g1", "u1"]
    assert {u["task_key"]: u["reason"] for u in plan["unselected"]} == {
        "g2": "concurrency_group_full"
    }


def test_parity_capabilities(client):
    _submit_http(
        client,
        [
            {"key": "plain"},
            {"key": "gpu", "requires": ["gpu"]},
            {"key": "both", "requires": ["gpu", "io"]},
        ],
        "par-cap",
    )
    r = client.put("/workers/w1", json={"capabilities": ["gpu"], "ttl_seconds": 60})
    assert r.status_code == 200
    req = {"worker_id": "w1", "dag_id": "par-cap", "limit": 10}
    plan = _plan(client, **req)
    real = _claim(client, **req)
    _assert_selection_parity(plan, real)
    assert [t["task_key"] for t in real["claimed"]] == ["plain", "gpu"]
    assert {u["task_key"]: u["reason"] for u in plan["unselected"]} == {
        "both": "capability_mismatch"
    }


def test_parity_expired_lease_reclaim(client):
    _submit_http(client, [{"key": "a", "lease_seconds": 0.2}], "par-exp")
    first = _claim(client, dag_id="par-exp")
    time.sleep(0.25)
    req = {"dag_id": "par-exp"}
    plan = _plan(client, **req)
    real = _claim(client, **req)
    _assert_selection_parity(plan, real)
    assert real["claimed"][0]["attempt"] == 2
    assert real["claimed"][0]["task_id"] == first["claimed"][0]["task_id"]
    assert real["claimed"][0]["lease_token"] != first["claimed"][0]["lease_token"]


def test_parity_backoff_gate(client):
    """退避未到期时两接口都领不到；到期后选中序列一致。"""
    _submit_http(
        client,
        [
            {
                "key": "a",
                "max_retries": 1,
                "retry_backoff": {"initial_seconds": 0.3, "multiplier": 1, "max_seconds": 0.3},
            },
            {"key": "u"},
        ],
        "par-backoff",
    )
    t = _claim(client, dag_id="par-backoff", limit=10)["claimed"]
    a = next(x for x in t if x["task_key"] == "a")
    client.post(
        f"/tasks/{a['task_id']}/complete",
        json={"completion_id": "f1", "lease_token": a["lease_token"], "success": False},
    )
    # 退避未到期：只有 u 可领（上一轮已领走），两接口均为空
    req = {"dag_id": "par-backoff", "limit": 10}
    plan = _plan(client, **req)
    real = _claim(client, **req)
    _assert_selection_parity(plan, real)
    assert real["claimed"] == []
    assert {u["task_key"]: u["reason"] for u in plan["unselected"]} == {
        "a": "backoff_not_elapsed"
    }
    # 退避到期后：两接口一致地重领 a（attempt=2）
    time.sleep(0.35)
    plan2 = _plan(client, **req)
    real2 = _claim(client, **req)
    _assert_selection_parity(plan2, real2)
    assert [x["task_key"] for x in real2["claimed"]] == ["a"]
    assert real2["claimed"][0]["attempt"] == 2


def test_parity_409_bodies_are_identical(client):
    """worker_not_registered：两接口返回逐字节一致的错误体，且都不落任何写入。"""
    _submit_http(client, [{"key": "gpu", "requires": ["gpu"]}], "par-409")
    plan_r = client.post("/tasks/claim-plan", json={"dag_id": "par-409", "limit": 5})
    real_r = client.post("/tasks/claim", json={"dag_id": "par-409", "limit": 5})
    assert plan_r.status_code == real_r.status_code == 409
    assert plan_r.json() == real_r.json()
    assert plan_r.json()["error"]["code"] == "worker_not_registered"
    dag = client.get("/dags/par-409").json()
    assert dag["states"]["pending"] == 1
    events = client.get("/events", params={"dag_id": "par-409"}).json()["events"]
    assert all(e["type"] == "dag.created" for e in events)


def test_parity_404_bodies_are_identical(client):
    plan_r = client.post("/tasks/claim-plan", json={"dag_id": "nope"})
    real_r = client.post("/tasks/claim", json={"dag_id": "nope"})
    assert plan_r.status_code == real_r.status_code == 404
    assert plan_r.json() == real_r.json()
    assert plan_r.json()["error"]["code"] == "dag_not_found"


def test_claim_applies_exactly_the_planned_writes(client):
    """真实领取落库的令牌/attempt/事件与计划选中项一一对应。"""
    _submit_http(
        client,
        [{"key": "a"}, {"key": "b"}, {"key": "c", "deps": ["a"]}],
        "par-writes",
    )
    req = {"dag_id": "par-writes", "limit": 2}
    plan = _plan(client, **req)
    events_before = client.get("/events", params={"dag_id": "par-writes"}).json()["events"]

    real = _claim(client, **req)
    planned_ids = [t["task_id"] for t in plan["claimed"]]
    assert [t["task_id"] for t in real["claimed"]] == planned_ids

    events_after = client.get("/events", params={"dag_id": "par-writes"}).json()["events"]
    new_events = events_after[len(events_before):]
    assert [e["type"] for e in new_events] == ["task.claimed"] * len(planned_ids)
    assert [e["task_id"] for e in new_events] == planned_ids
    for t in real["claimed"]:
        detail = client.get(f"/tasks/{t['task_id']}").json()
        assert detail["state"] == "running"
        assert detail["attempts"][-1]["lease_token"] == t["lease_token"]
        assert detail["attempts"][-1]["state"] == "active"


def test_interleaved_plan_claim_parity_until_drained(client):
    """交替预演/领取直至无候选：每一步选中序列都一致。"""
    _submit_http(
        client,
        [{"key": "a"}, {"key": "b", "deps": ["a"]}, {"key": "c"}],
        "par-drain",
    )
    req = {"dag_id": "par-drain", "limit": 1}
    claimed_keys = []
    for _ in range(3):
        plan = _plan(client, **req)
        real = _claim(client, **req)
        _assert_selection_parity(plan, real)
        (t,) = real["claimed"]
        claimed_keys.append(t["task_key"])
        # 完成领到的任务以解锁后继
        r = client.post(
            f"/tasks/{t['task_id']}/complete",
            json={
                "completion_id": f"done-{t['task_key']}",
                "lease_token": t["lease_token"],
                "success": True,
            },
        )
        assert r.status_code == 200
    # rowid 顺序：a 先；a 完成后 b 解锁且排在 c 前
    assert claimed_keys == ["a", "b", "c"]
    assert _plan(client, **req)["claimed"] == []
    assert _claim(client, **req)["claimed"] == []


def test_claim_response_structure_is_unchanged(client):
    _submit_http(client, [{"key": "a", "payload": {"x": 1}}], "par-struct")
    body = _claim(client, dag_id="par-struct", worker_id="w")
    assert set(body) == {"claimed", "count"}
    assert set(body["claimed"][0]) == {
        "task_id",
        "dag_id",
        "task_key",
        "name",
        "payload",
        "attempt",
        "lease_token",
        "lease_expires_at",
        "lease_seconds",
        "concurrency_group",
        "requires",
    }
    assert body["claimed"][0]["requires"] == []
    assert body["claimed"][0]["concurrency_group"] is None


def test_plan_response_structure_is_unchanged(client):
    _submit_http(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], "par-struct2")
    body = _plan(client, dag_id="par-struct2", limit=10)
    assert set(body) == {
        "simulated",
        "worker_id",
        "dag_id",
        "limit",
        "evaluated_at",
        "claimed",
        "count",
        "unselected",
    }
    assert body["simulated"] is True
    assert set(body["claimed"][0]) == {
        "task_id",
        "dag_id",
        "task_key",
        "name",
        "payload",
        "attempt",
        "lease_token",
        "lease_expires_at",
        "lease_seconds",
        "concurrency_group",
        "requires",
    }
    (entry,) = body["unselected"]
    assert set(entry) == {
        "task_id",
        "dag_id",
        "task_key",
        "name",
        "reason",
        "message",
        "blocked_by",
    }
