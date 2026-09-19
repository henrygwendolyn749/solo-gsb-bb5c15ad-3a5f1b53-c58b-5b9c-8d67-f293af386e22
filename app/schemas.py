"""Pydantic 请求模型。"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field, model_validator


class RetryBackoffIn(BaseModel):
    """持久化指数退避策略：仅作用于显式失败后的重试，不作用于租约过期重领。"""

    initial_seconds: float = Field(..., gt=0, allow_inf_nan=False, description="首次失败后的重试延迟（秒），必须为正")
    multiplier: float = Field(..., ge=1, allow_inf_nan=False, description="退避倍数，>=1")
    max_seconds: float = Field(..., gt=0, allow_inf_nan=False, description="重试延迟上限（秒），不得小于 initial_seconds")

    @model_validator(mode="after")
    def _max_not_smaller_than_initial(self) -> "RetryBackoffIn":
        if self.max_seconds < self.initial_seconds:
            raise ValueError("max_seconds 必须不小于 initial_seconds")
        return self


class TaskIn(BaseModel):
    key: str = Field(..., min_length=1, description="DAG 内唯一任务键")
    name: str = ""
    payload: Any = None
    deps: list[str] = Field(default_factory=list, description="前置任务的 key 列表")
    max_retries: int = Field(0, ge=0, description="失败后的额外重试次数")
    lease_seconds: float | None = Field(None, gt=0, description="租约时长，缺省取服务端默认值")
    duration_seconds: float | None = Field(
        None, gt=0, allow_inf_nan=False, description="预计执行时长（秒），缺省为 1；用于只读分析"
    )
    retry_backoff: RetryBackoffIn | None = Field(
        None,
        description="可选指数退避策略；缺省（null）表示显式失败后立即重试，不延迟",
    )
    concurrency_group: str | None = Field(
        None,
        min_length=1,
        description="跨 DAG 并发组名；须与 concurrency_limit 同时提供，缺省（NULL）表示不限流",
    )
    concurrency_limit: int | None = Field(
        None,
        ge=1,
        description="同名组全局未过期 running 租约数上限（正整数）；须与 concurrency_group 同时提供",
    )
    requires: list[Annotated[str, Field(min_length=1)]] = Field(
        default_factory=list,
        description="能力要求标签列表：只有携带存活登记且覆盖全部标签的 worker_id 可领取；缺省/空数组表示不要求能力",
    )

    @model_validator(mode="after")
    def _group_fields_must_come_together(self) -> "TaskIn":
        # 组名与上限要么同时给出，要么都不给出：只给其一是 422
        if (self.concurrency_group is None) != (self.concurrency_limit is None):
            raise ValueError("concurrency_group 与 concurrency_limit 必须同时提供")
        return self


class DAGSubmitIn(BaseModel):
    name: str = "dag"
    tasks: list[TaskIn] = Field(..., min_length=1)
    id: str | None = Field(None, description="可选自定义 DAG id；已存在则返回 409")


class ClaimIn(BaseModel):
    worker_id: str | None = None
    dag_id: str | None = None
    limit: int = Field(1, ge=1, le=100)


class HeartbeatIn(BaseModel):
    lease_token: str
    extend_seconds: float | None = Field(None, gt=0)


class CompleteIn(BaseModel):
    completion_id: str = Field(
        ...,
        min_length=1,
        description="客户端生成的幂等键；同 task_id + completion_id 的等价回报只生效一次",
    )
    lease_token: str
    success: bool
    result: Any = None
    error: str | None = None


class SubscriptionIn(BaseModel):
    dag_id: str | None = Field(
        None,
        min_length=1,
        description="可选：只订阅该 DAG 的事件；缺省（null）订阅全库事件流",
    )
    committed_sequence: int = Field(
        0,
        ge=0,
        description="起始游标：只投递 sequence 大于该值的事件；默认 0（从头开始）",
    )


class SubscriptionPollIn(BaseModel):
    limit: int = Field(50, ge=1, le=100, description="本次最多投递的事件数（1～100）")
    lease_seconds: float = Field(
        ...,
        gt=0,
        allow_inf_nan=False,
        description="在途批次的确认租约时长（秒，必须为正）；租约内重复 poll 原样返回同一批次",
    )


class SubscriptionAckIn(BaseModel):
    delivery_token: str = Field(
        ...,
        min_length=1,
        description="poll 返回的当前批次随机令牌；仅当前令牌可把游标推进到该批最后一条 sequence",
    )


class WorkerRegisterIn(BaseModel):
    """工作器能力登记：去重的能力标签 + 正数 ttl_seconds；重复 PUT 覆盖并续期。"""

    capabilities: list[Annotated[str, Field(min_length=1)]] = Field(
        ...,
        description="能力标签列表（标签须为非空字符串；服务端按首次出现顺序去重持久化）",
    )
    ttl_seconds: float = Field(
        ...,
        gt=0,
        allow_inf_nan=False,
        description="登记存活时长（秒，必须为正）：过期前该 worker_id 的领取才视为携带存活登记；重复登记会刷新过期时刻",
    )


# ------------------------------------------------------- 可复用 DAG 模板

class TemplateCreateIn(BaseModel):
    """创建模板：名称 + 首版任务图（复用 DAG 提交校验，创建即发布不可变版本 1）。"""

    name: str = Field(..., min_length=1, description="模板名称（全局唯一；重复创建 409）")
    tasks: list[TaskIn] = Field(..., min_length=1, description="版本 1 的任务定义；payload 中可含 ${name} 占位符")


class TemplateVersionPublishIn(BaseModel):
    """按名称发布新版本：版本号由服务端在模板内递增，任务图快照不可变。"""

    tasks: list[TaskIn] = Field(
        ..., min_length=1, description="新版本的任务定义；payload 中可含 ${name} 占位符"
    )


class TemplateInstantiateIn(BaseModel):
    """实例化指定模板版本：必填幂等键 + 占位符参数（缺参/多参均 422）。"""

    idempotency_key: str = Field(
        ...,
        min_length=1,
        description="客户端生成的幂等键：同模板版本 + 同键返回首次创建的同一 DAG；同键参数不同 409",
    )
    parameters: dict[str, str | int | float | bool | None | list | dict] = Field(
        default_factory=dict,
        description="占位符实参：键须与版本 payload 中的 ${name} 集合一一对应，缺参或多参 422",
    )
    name: str | None = Field(None, min_length=1, description="可选：实例 DAG 名称；缺省用模板名")
    id: str | None = Field(None, description="可选自定义 DAG id；已存在则返回 409")
