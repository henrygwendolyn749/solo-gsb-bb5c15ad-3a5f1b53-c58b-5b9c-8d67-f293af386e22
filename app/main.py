"""FastAPI 入口：依赖感知的任务租约 API。

启动: uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import Response, JSONResponse

from . import service, templates
from .db import init_db
from .schemas import (
    ClaimIn,
    CompleteIn,
    DAGSubmitIn,
    HeartbeatIn,
    SubscriptionAckIn,
    SubscriptionIn,
    SubscriptionPollIn,
    TemplateCreateIn,
    TemplateInstantiateIn,
    TemplateVersionPublishIn,
    WorkerRegisterIn,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Dependency-Aware Task Lease API",
    version="1.3.0",
    description="提交 DAG、原子领取前置已完成的任务、基于租约令牌回报结果；支持工作器能力登记与 requires 能力路由；提供可复用 DAG 模板（版本化快照与参数化实例化）；提供只读的领取决策预演（POST /tasks/claim-plan）。",
    lifespan=lifespan,
)


@app.exception_handler(service.APIError)
async def api_error_handler(_request: Request, exc: service.APIError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message, "details": exc.details}},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/dags", status_code=201)
def submit_dag(body: DAGSubmitIn) -> dict:
    return service.submit_dag(body.model_dump())


@app.get("/dags/{dag_id}")
def get_dag(dag_id: str) -> dict:
    return service.get_dag(dag_id)


@app.get("/dags/{dag_id}/analysis")
def analyze_dag(dag_id: str) -> dict:
    return service.analyze_dag(dag_id)


# ------------------------------------------------------- 可复用 DAG 模板

@app.post(
    "/templates",
    status_code=201,
    summary="创建 DAG 模板（同时发布不可变版本 1）",
    responses={
        201: {"description": "模板已创建并发布版本 1，返回版本元数据与任务图快照"},
        409: {"description": "模板名已存在（template_exists）"},
        422: {"description": "任务图非法：重复键/缺失依赖/环/并发组矛盾，复用 DAG 提交校验"},
    },
)
def create_template(body: TemplateCreateIn) -> dict:
    return templates.create_template(body.model_dump())


@app.get("/templates", summary="模板列表")
def list_templates() -> dict:
    return templates.list_templates()


@app.get(
    "/templates/{name}",
    summary="模板详情（含全部已发布版本元数据）",
    responses={404: {"description": "模板不存在（template_not_found）"}},
)
def get_template(name: str) -> dict:
    return templates.get_template(name)


@app.post(
    "/templates/{name}/versions",
    status_code=201,
    summary="按名称发布新版本（保存不可变任务图快照）",
    responses={
        201: {"description": "新版本已发布（版本号在模板内递增），返回版本元数据与快照"},
        404: {"description": "模板不存在（template_not_found）"},
        422: {"description": "任务图非法：重复键/缺失依赖/环/并发组矛盾，复用 DAG 提交校验"},
    },
)
def publish_template_version(name: str, body: TemplateVersionPublishIn) -> dict:
    return templates.publish_template_version(name, body.model_dump())


@app.get(
    "/templates/{name}/versions/{version}",
    summary="查询指定已发布版本（含不可变任务图快照）",
    responses={
        200: {"description": "返回版本元数据与发布时保存的任务图快照"},
        404: {"description": "模板不存在（template_not_found）或版本不存在（template_version_not_found）"},
    },
)
def get_template_version(name: str, version: int) -> dict:
    return templates.get_template_version(name, version)


@app.post(
    "/templates/{name}/versions/{version}/instantiations",
    summary="用调用方参数实例化指定模板版本（幂等：同模板版本 + idempotency_key 返回同一 DAG）",
    responses={
        201: {"description": "已按参数替换 ${name} 占位符并创建新 DAG"},
        200: {"description": "幂等命中：返回首次创建的同一 DAG（同键同参重复调用）"},
        404: {"description": "模板不存在（template_not_found）或版本不存在（template_version_not_found）"},
        409: {"description": "同 idempotency_key 但参数不同（idempotency_conflict）；自定义 DAG id 已存在（dag_exists）或并发组上限冲突（concurrency_group_conflict）"},
        422: {"description": "缺少占位符参数（missing_template_parameter）或参数多余（unexpected_template_parameter），不落库"},
    },
)
def instantiate_template(name: str, version: int, body: TemplateInstantiateIn):
    dag, created = templates.instantiate_template(name, version, body.model_dump())
    # 首次创建 201；幂等重放 200（返回同一 DAG 视图）
    return JSONResponse(content=dag, status_code=201 if created else 200)


@app.post(
    "/tasks/claim",
    summary="原子领取前置已完成的任务",
    responses={
        200: {"description": "返回本次领取到的任务（依赖、退避、并发组与能力门控均满足）"},
        404: {"description": "指定的 dag_id 不存在（dag_not_found）"},
        409: {"description": "候选含 requires 任务但 worker_id 登记缺失或已过期（worker_not_registered），不领取任何任务"},
    },
)
def claim_tasks(body: ClaimIn) -> dict:
    return service.claim_tasks(body.worker_id, body.dag_id, body.limit)


@app.post(
    "/tasks/claim-plan",
    summary="只读预演领取决策（不写入任务/attempt/事件）",
    responses={
        200: {"description": "按真实领取的排序与门控模拟：返回将被选中的任务（无租约令牌）与未选中 pending 任务的首个阻塞原因"},
        404: {"description": "指定的 dag_id 不存在（dag_not_found）"},
        409: {"description": "与真实领取同语义：批量填满前遇到 requires 候选但 worker_id 登记缺失或已过期（worker_not_registered）"},
    },
)
def plan_claims(body: ClaimIn) -> dict:
    # 请求参数与 /tasks/claim 完全一致（ClaimIn）；整个计算在单个 SQLite 只读事务内完成
    return service.plan_claims(body.worker_id, body.dag_id, body.limit)


# ------------------------------------------------------- 工作器能力登记（能力路由）

@app.put(
    "/workers/{worker_id}",
    summary="登记或刷新工作器能力（去重标签 + 正数 ttl_seconds）",
    responses={
        200: {"description": "登记已保存/覆盖；重复 PUT 覆盖能力并刷新过期时刻，返回当前存活状态"},
        422: {"description": "capabilities 含非空字符串之外的元素，或 ttl_seconds 非正"},
    },
)
def register_worker(worker_id: str, body: WorkerRegisterIn) -> dict:
    return service.register_worker(worker_id, body.model_dump())


@app.get(
    "/workers/{worker_id}",
    summary="查询工作器登记与存活状态",
    responses={
        200: {"description": "返回能力标签、ttl、首次登记时刻、过期时刻与 alive"},
        404: {"description": "工作器从未登记（worker_not_found）"},
    },
)
def get_worker(worker_id: str) -> dict:
    return service.get_worker(worker_id)


@app.get("/tasks")
def list_tasks(dag_id: str | None = None, state: str | None = None) -> dict:
    return service.list_tasks(dag_id, state)


@app.get("/events")
def list_events(
    after: int = Query(0, ge=0, description="只返回 sequence 大于该值的事件"),
    limit: int = Query(50, ge=1, le=100, description="本页最多返回的事件数（1~100）"),
    dag_id: str | None = Query(None, description="可选：只返回该 DAG 的事件"),
) -> dict:
    return service.list_events(after, limit, dag_id)


# ------------------------------------------------------- 可靠事件订阅

@app.post(
    "/subscriptions",
    status_code=201,
    summary="创建事件订阅",
    responses={
        201: {"description": "订阅已创建并保存 committed_sequence；可选按 dag_id 过滤"},
    },
)
def create_subscription(body: SubscriptionIn) -> dict:
    return service.create_subscription(body.model_dump())


@app.get("/subscriptions", summary="订阅列表")
def list_subscriptions(dag_id: str | None = None) -> dict:
    return service.list_subscriptions(dag_id)


@app.get("/subscriptions/{subscription_id}", summary="订阅详情（含在途批次状态）")
def get_subscription(subscription_id: str) -> dict:
    return service.get_subscription(subscription_id)


@app.post(
    "/subscriptions/{subscription_id}/poll",
    summary="拉取（或在租约内原样回放）下一批事件",
    responses={
        200: {"description": "返回下一批事件与随机 delivery_token；租约未过期时重复 poll 原样回放同一批次"},
        404: {"description": "订阅不存在（subscription_not_found）"},
    },
)
def poll_subscription(subscription_id: str, body: SubscriptionPollIn):
    result = service.poll_subscription(subscription_id, body.model_dump())
    if isinstance(result, service.ReplayedResponse):
        # 租约内重复 poll：逐字节回放首次投递保存的响应体（同令牌、同事件集）
        return Response(
            content=result.body,
            status_code=result.status_code,
            media_type="application/json",
        )
    return result


@app.post(
    "/subscriptions/{subscription_id}/ack",
    summary="确认当前批次，把游标推进到该批最后一条 sequence",
    responses={
        200: {"description": "游标已推进到当前批次最后一条 sequence"},
        404: {"description": "订阅不存在（subscription_not_found）"},
        409: {"description": "无在途批次（no_pending_batch）或令牌旧/错误（invalid_delivery_token）"},
    },
)
def ack_subscription(subscription_id: str, body: SubscriptionAckIn) -> dict:
    return service.ack_subscription(subscription_id, body.model_dump())


@app.get("/tasks/{task_id}")
def get_task(task_id: str) -> dict:
    return service.get_task(task_id)


@app.post("/tasks/{task_id}/heartbeat")
def heartbeat(task_id: str, body: HeartbeatIn) -> dict:
    return service.heartbeat(task_id, body.model_dump())


@app.post("/tasks/{task_id}/complete")
def complete_task(task_id: str, body: CompleteIn):
    result = service.complete_task(task_id, body.model_dump())
    if isinstance(result, service.ReplayedResponse):
        # 幂等命中：逐字节回放首次有效回报保存的状态码与响应体
        return Response(
            content=result.body,
            status_code=result.status_code,
            media_type="application/json",
        )
    return result


@app.post(
    "/tasks/{task_id}/resume",
    summary="恢复最终失败的任务",
    responses={
        200: {"description": "已恢复为 pending；前置条件满足的被阻塞后继已在同事务内连锁解锁"},
        404: {"description": "任务不存在（task_not_found）"},
        409: {"description": "任务不是最终失败状态（task_not_failed）；重复恢复同样返回 409 且不新增事件"},
    },
)
def resume_task(task_id: str) -> dict:
    return service.resume_task(task_id)


@app.post(
    "/tasks/{task_id}/cancel",
    summary="取消任务",
    responses={
        200: {"description": "已置为 canceled；running 任务的当前 attempt 被终止、租约被清除，未结束后继在同事务内连锁取消"},
        404: {"description": "任务不存在（task_not_found）"},
        409: {"description": "任务不是 pending/running/blocked（task_not_cancelable）；重复取消同样返回 409 且不新增事件"},
    },
)
def cancel_task(task_id: str) -> dict:
    return service.cancel_task(task_id)
