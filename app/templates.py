"""可复用 DAG 模板：模板创建、按名称发布新版本、版本查询与参数化实例化。

- 模板按名称（`dag_templates.name`）组织；每次发布保存一个**不可变**的任务图
  快照（`dag_template_versions.snapshot_json`），版本号在模板内从 1 起递增。
- 实例化时用调用方参数替换任务 payload 中的 ``${name}`` 占位符：缺参或多参
  一律 422 拒绝，且与后续 DAG 创建、幂等记录写入同处一个 SQLite 事务——
  失败整体回滚，不落任何痕迹。
- 实例化必须携带 `idempotency_key`：同模板版本 + 同键返回首次创建的同一 DAG；
  同键但参数不同返回 409。

模板模块只复用 service 中的 DAG 校验（``_prepare_dag_tasks``）、事务内落库
（``_persist_dag``）与 DAG 响应结构（``get_dag``），不改变直接 POST /dags
的任何行为。
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any

from .db import get_conn
from .service import (
    APIError,
    _canonical,
    _dumps,
    _iso,
    _now,
    _persist_dag,
    _prepare_dag_tasks,
    get_dag,
)

# payload 中的 ${name} 占位符：name 须为非空的合法标识符（字母/数字/下划线，
# 首字符非数字）。非法形式（如 ${1a}、${}、${a-b}）不视为占位符，原样保留。
_PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


# --------------------------------------------------------------- 快照与占位符

def _snapshot_task(t: dict[str, Any]) -> dict[str, Any]:
    """发布时刻的规范化任务定义（快照）：只保留 DAG 提交支持的字段。

    依赖与 requires 同口径去重；缺省字段显式落为 None/空，保证同一定义重复
    发布得到字节一致的快照，且快照可直接作为 POST /dags 的任务定义使用。
    """
    return {
        "key": t["key"],
        "name": t.get("name") or "",
        "payload": t.get("payload"),
        "deps": list(dict.fromkeys(t.get("deps") or [])),
        "max_retries": int(t.get("max_retries") or 0),
        "lease_seconds": t.get("lease_seconds"),
        "duration_seconds": t.get("duration_seconds"),
        "retry_backoff": (
            dict(t["retry_backoff"]) if t.get("retry_backoff") is not None else None
        ),
        "concurrency_group": t.get("concurrency_group"),
        "concurrency_limit": t.get("concurrency_limit"),
        "requires": list(dict.fromkeys(t.get("requires") or [])),
    }


def _build_snapshot(raw_tasks: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    """复用 DAG 校验后构造不可变快照，并从任务 payload 中提取占位符参数名。

    图校验（重复键/缺失依赖/环/同批并发组矛盾）与直接 POST /dags 完全同口径，
    校验失败抛 422 APIError，不写库。
    """
    _prepare_dag_tasks([dict(t) for t in raw_tasks])
    snapshot = [_snapshot_task(t) for t in raw_tasks]
    names: set[str] = set()
    for t in snapshot:
        _collect_placeholders(t["payload"], names)
    return snapshot, sorted(names)


def _collect_placeholders(value: Any, names: set[str]) -> None:
    """递归遍历 payload（JSON 值）收集 ${name} 占位符名；只遍历值不遍历对象键。"""
    if isinstance(value, str):
        names.update(_PLACEHOLDER_RE.findall(value))
    elif isinstance(value, list):
        for item in value:
            _collect_placeholders(item, names)
    elif isinstance(value, dict):
        for item in value.values():
            _collect_placeholders(item, names)


def _stringify(value: Any) -> str:
    """占位符嵌入更长字符串时的替换文本：字符串原样，其他 JSON 值序列化嵌入。"""
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def _substitute(value: Any, params: dict[str, Any]) -> Any:
    """递归替换 payload 中的 ${name} 占位符。

    - 字符串整体恰为一个占位符（如 "${x}"）：替换后保留参数原类型（数字仍是数字、
      对象仍是对象），与直接在 POST /dags 中写出该 payload 等价；
    - 占位符嵌在更长字符串中（如 "s3://${bucket}/data"）：按文本替换，非字符串
      参数以紧凑 JSON 文本嵌入；
    - 含多个占位符或混合文本一律按字符串替换。
    """
    if isinstance(value, list):
        return [_substitute(item, params) for item in value]
    if isinstance(value, dict):
        return {k: _substitute(item, params) for k, item in value.items()}
    if isinstance(value, str):
        exact = _PLACEHOLDER_RE.fullmatch(value)
        if exact is not None:
            return params[exact.group(1)]
        if _PLACEHOLDER_RE.search(value):
            return _PLACEHOLDER_RE.sub(lambda m: _stringify(params[m.group(1)]), value)
    return value


# --------------------------------------------------------------- 创建 / 发布

def _publish_version(
    conn,
    name: str,
    snapshot: list[dict[str, Any]],
    parameters: list[str],
    now_iso: str,
) -> dict[str, Any]:
    """在调用方事务内为已锁定的模板追加下一个版本；返回版本元数据。"""
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS max_version FROM dag_template_versions WHERE template_name = ?",
        (name,),
    ).fetchone()
    version = int(row["max_version"]) + 1
    conn.execute(
        """INSERT INTO dag_template_versions
               (template_name, version, snapshot_json, parameters, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (name, version, json.dumps(snapshot, ensure_ascii=False), _dumps(parameters), now_iso),
    )
    conn.execute(
        "UPDATE dag_templates SET updated_at = ? WHERE name = ?", (now_iso, name)
    )
    return {
        "template_name": name,
        "version": version,
        "parameters": parameters,
        "task_count": len(snapshot),
        "created_at": now_iso,
    }


def create_template(body: dict[str, Any]) -> dict[str, Any]:
    """创建模板并立即发布不可变版本 1；模板名重复返回 409，图非法返回 422。"""
    name = body["name"]
    snapshot, parameters = _build_snapshot(body["tasks"])
    now_iso = _iso(_now())

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            try:
                conn.execute(
                    "INSERT INTO dag_templates (name, created_at, updated_at) VALUES (?, ?, ?)",
                    (name, now_iso, now_iso),
                )
            except Exception:
                raise APIError(409, "template_exists", f"模板 {name} 已存在")
            meta = _publish_version(conn, name, snapshot, parameters, now_iso)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {**meta, "snapshot": snapshot}


def publish_template_version(name: str, body: dict[str, Any]) -> dict[str, Any]:
    """按名称为已有模板发布新版本（版本号递增），快照不可变。

    模板不存在 → 404 template_not_found；图非法 → 422（不新增版本）。
    """
    snapshot, parameters = _build_snapshot(body["tasks"])
    now_iso = _iso(_now())

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not conn.execute(
                "SELECT 1 FROM dag_templates WHERE name = ?", (name,)
            ).fetchone():
                raise APIError(404, "template_not_found", f"模板 {name} 不存在")
            meta = _publish_version(conn, name, snapshot, parameters, now_iso)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {**meta, "snapshot": snapshot}


# --------------------------------------------------------------- 查询

def list_templates() -> dict[str, Any]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT t.name, t.created_at, t.updated_at,
                      COUNT(v.version) AS version_count,
                      MAX(v.version) AS latest_version
               FROM dag_templates t
               JOIN dag_template_versions v ON v.template_name = t.name
               GROUP BY t.name
               ORDER BY t.created_at, t.name"""
        ).fetchall()
        items = [
            {
                "name": r["name"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "version_count": r["version_count"],
                "latest_version": r["latest_version"],
            }
            for r in rows
        ]
    return {"templates": items, "count": len(items)}


def _load_template_row(conn, name: str):
    row = conn.execute("SELECT * FROM dag_templates WHERE name = ?", (name,)).fetchone()
    if row is None:
        raise APIError(404, "template_not_found", f"模板 {name} 不存在")
    return row


def get_template(name: str) -> dict[str, Any]:
    """模板详情：时间戳与全部已发布版本的元数据（不含快照正文）。"""
    with get_conn() as conn:
        tpl = _load_template_row(conn, name)
        versions = [
            {
                "template_name": name,
                "version": r["version"],
                "parameters": json.loads(r["parameters"]),
                "task_count": len(json.loads(r["snapshot_json"])),
                "created_at": r["created_at"],
            }
            for r in conn.execute(
                """SELECT version, parameters, snapshot_json, created_at
                   FROM dag_template_versions
                   WHERE template_name = ? ORDER BY version""",
                (name,),
            ).fetchall()
        ]
    return {
        "name": tpl["name"],
        "created_at": tpl["created_at"],
        "updated_at": tpl["updated_at"],
        "latest_version": versions[-1]["version"],
        "version_count": len(versions),
        "versions": versions,
    }


def get_template_version(name: str, version: int) -> dict[str, Any]:
    """查询单个已发布版本：元数据 + 发布时保存的不可变任务图快照。"""
    with get_conn() as conn:
        if not conn.execute(
            "SELECT 1 FROM dag_templates WHERE name = ?", (name,)
        ).fetchone():
            raise APIError(404, "template_not_found", f"模板 {name} 不存在")
        row = conn.execute(
            """SELECT version, snapshot_json, parameters, created_at
               FROM dag_template_versions
               WHERE template_name = ? AND version = ?""",
            (name, version),
        ).fetchone()
        if row is None:
            raise APIError(
                404,
                "template_version_not_found",
                f"模板 {name} 不存在版本 {version}",
                {"template_name": name, "version": version},
            )
        snapshot = json.loads(row["snapshot_json"])
    return {
        "template_name": name,
        "version": row["version"],
        "parameters": json.loads(row["parameters"]),
        "task_count": len(snapshot),
        "created_at": row["created_at"],
        "snapshot": snapshot,
    }


# --------------------------------------------------------------- 实例化

def instantiate_template(
    name: str,
    version: int,
    body: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """以调用方参数实例化指定模板版本，返回 (DAG 响应, 是否首次创建)。

    整个流程（参数校验、占位符替换、DAG 图校验/落库、幂等记录）在**单个
    SQLite 事务**内完成：

    - 模板/版本不存在 → 404；
    - 缺少占位符参数或传入未声明参数 → 422（缺参/多参均拒绝且不落库）；
    - 同 (模板, 版本, idempotency_key) 且参数等价 → 返回首次创建的同一 DAG；
    - 同键但参数（规范化后）不同 → 409 idempotency_conflict。
    """
    idempotency_key = body["idempotency_key"]
    params = body.get("parameters") or {}
    dag_id = body.get("id") or uuid.uuid4().hex
    dag_name = body.get("name")

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            tpl = conn.execute(
                "SELECT 1 FROM dag_templates WHERE name = ?", (name,)
            ).fetchone()
            if tpl is None:
                raise APIError(404, "template_not_found", f"模板 {name} 不存在")
            vrow = conn.execute(
                """SELECT snapshot_json, parameters FROM dag_template_versions
                   WHERE template_name = ? AND version = ?""",
                (name, version),
            ).fetchone()
            if vrow is None:
                raise APIError(
                    404,
                    "template_version_not_found",
                    f"模板 {name} 不存在版本 {version}",
                    {"template_name": name, "version": version},
                )

            # 幂等检查先于参数校验之外的一切写入；BEGIN IMMEDIATE 已取保留锁，
            # 并发同键请求在此被串行化。
            existing = conn.execute(
                """SELECT dag_id, parameters_json FROM dag_template_instantiations
                   WHERE template_name = ? AND version = ? AND idempotency_key = ?""",
                (name, version, idempotency_key),
            ).fetchone()

            snapshot = json.loads(vrow["snapshot_json"])
            required = json.loads(vrow["parameters"])

            # 缺参 / 多参：422，整事务回滚（即使幂等键已存在，坏请求也不返回首响）
            missing = sorted(p for p in required if p not in params)
            unexpected = sorted(p for p in params if p not in required)
            if missing:
                raise APIError(
                    422,
                    "missing_template_parameter",
                    "实例化参数缺少模板占位符所需的值",
                    {"missing": missing, "required": required},
                )
            if unexpected:
                raise APIError(
                    422,
                    "unexpected_template_parameter",
                    "实例化参数包含模板未声明的占位符",
                    {"unexpected": unexpected, "required": required},
                )

            fingerprint = _canonical(params)
            if existing is not None:
                if existing["parameters_json"] != fingerprint:
                    raise APIError(
                        409,
                        "idempotency_conflict",
                        "相同模板版本 + idempotency_key 已用于参数不同的实例化",
                        {
                            "template_name": name,
                            "version": version,
                            "idempotency_key": idempotency_key,
                        },
                    )
                # 同键同参：幂等命中，返回首次创建的同一 DAG（不新建、不产生事件）
                created = False
                result_dag_id = existing["dag_id"]
            else:
                # 占位符替换：快照不可变，替换发生在其副本上
                tasks = [
                    {**t, "payload": _substitute(t["payload"], params)} for t in snapshot
                ]
                # 复用现有 DAG 校验（替换后理论上仍是同一图结构，属于防御性同口径校验）
                keys, deps_map, tasks_by_key, declared = _prepare_dag_tasks(
                    [dict(t) for t in tasks]
                )
                now_iso = _iso(_now())
                # 与直接提交相同的落库路径（含跨 DAG 并发组冲突检测与 dag.created
                # 事件）；任何冲突抛错都会连同幂等记录一起回滚，不落痕迹
                _persist_dag(
                    conn,
                    dag_id,
                    dag_name or name,
                    keys,
                    deps_map,
                    tasks_by_key,
                    declared,
                    now_iso,
                )
                conn.execute(
                    """INSERT INTO dag_template_instantiations
                           (template_name, version, idempotency_key, dag_id,
                            parameters_json, created_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (name, version, idempotency_key, dag_id, fingerprint, now_iso),
                )
                created = True
                result_dag_id = dag_id

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # 返回事务提交后的一致 DAG 视图（与 POST /dags、GET /dags/{id} 同一结构）
    return get_dag(result_dag_id), created
