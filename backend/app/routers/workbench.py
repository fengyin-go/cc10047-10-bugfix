"""检查点工作台 / 批流合并恢复链路的富动作 HTTP 入口。

通用 ``/{module}/{id}/actions`` 只能携带 action 字符串；认领/恢复场景需要
传入记录集、凭据等结构化载荷，因此这里补充一组可同时从 query 与 JSON body
取参的端点。路径同时提供 stream_checkpoint / checkpoint 等常见模块键。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.services import service_for

router = APIRouter(prefix="/api", tags=["检查点工作台"])


async def _collect_kwargs(request: Request) -> dict[str, Any]:
    """合并 query 参数与 JSON 对象载荷（body 优先）。"""
    kwargs: dict[str, Any] = dict(request.query_params)
    if request.method == "POST":
        try:
            body = await request.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            # 兼容 {"kwargs": {...}} / {"payload": {...}} 的包裹写法
            for wrapper in ("kwargs", "payload", "params"):
                inner = body.get(wrapper)
                if isinstance(inner, dict):
                    kwargs.update(inner)
            kwargs.update({k: v for k, v in body.items() if k not in ("kwargs", "payload", "params")})
    return kwargs


def _checkpoint_service():
    return service_for("stream_checkpoint")


def _merge_service():
    return service_for("batch_merge")


_CHECKPOINT_PREFIXES = (
    "/stream_checkpoint", "/checkpoint", "/checkpoints",
    "/checkpoint_workbench", "/stream-checkpoint",
)
_MERGE_PREFIXES = ("/batch_merge", "/batch-merge", "/merge")


# ------------------------------------------------------------------
# 检查点：认领 / 恢复 / 推进 / 保存 / 新记录抵达 / 状态查询
# ------------------------------------------------------------------

def _register_checkpoint_routes() -> None:
    svc_getter = _checkpoint_service

    for prefix in _CHECKPOINT_PREFIXES:
        @router.post(f"{prefix}/{{entry_id}}/claim")
        async def claim(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_claim_checkpoint(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/claims")
        async def claims(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_claim_checkpoint(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/recover")
        async def recover(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_recover_checkpoint(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/recovery")
        async def recovery(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_recover_checkpoint(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/recoveries")
        async def recoveries(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_recover_checkpoint(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/advance")
        async def advance(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_advance_checkpoint(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/commit")
        async def commit(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_advance_checkpoint(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/confirm")
        async def confirm(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_advance_checkpoint(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/save")
        async def save(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_save_checkpoint(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/deliver")
        async def deliver(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_deliver_stream(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/records")
        async def records_post(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return svc_getter().domain_deliver_stream(entry_id, **kwargs)

        @router.get(f"{prefix}/{{entry_id}}/state")
        async def state(entry_id: int) -> dict[str, Any]:
            return svc_getter().domain_get_state(entry_id)

        @router.get(f"{prefix}/{{entry_id}}/recovery-state")
        async def recovery_state(entry_id: int) -> dict[str, Any]:
            return svc_getter().domain_get_state(entry_id)

        @router.get(f"{prefix}/{{entry_id}}/lake-rows")
        async def lake_rows(entry_id: int, table: str | None = None) -> dict[str, Any]:
            return svc_getter().domain_lake_rows(entry_id, table)


_register_checkpoint_routes()


# ------------------------------------------------------------------
# 批流合并入口：合并 / 认领 / 恢复
# ------------------------------------------------------------------

def _register_merge_routes() -> None:
    for prefix in _MERGE_PREFIXES:
        @router.post(f"{prefix}/{{entry_id}}/merge")
        async def merge(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return _merge_service().domain_merge_batch_stream(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/run-merge")
        async def run_merge(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return _merge_service().domain_merge_batch_stream(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/claim")
        async def merge_claim(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return _merge_service().domain_claim_batch(entry_id, **kwargs)

        @router.post(f"{prefix}/{{entry_id}}/recover")
        async def merge_recover(entry_id: int, request: Request) -> dict[str, Any]:
            kwargs = await _collect_kwargs(request)
            return _merge_service().domain_recover_batch(entry_id, **kwargs)


_register_merge_routes()


# ------------------------------------------------------------------
# 不带 id 的便捷入口（id 从载荷/参数取，默认 1）
# ------------------------------------------------------------------

async def _id_from_kwargs(request: Request, *names: str, default: int = 1) -> tuple[int, dict]:
    kwargs = await _collect_kwargs(request)
    value: Any = None
    for name in names:
        value = kwargs.pop(name, None)
        if value is not None:
            break
    try:
        entry_id = int(value) if value is not None else default
    except (TypeError, ValueError):
        entry_id = default
    return entry_id, kwargs


for _prefix in _CHECKPOINT_PREFIXES:
    @router.post(f"{_prefix}/claim")
    async def claim_idless(request: Request) -> dict[str, Any]:
        entry_id, kwargs = await _id_from_kwargs(request, "checkpoint_id", "entry_id", "id")
        return _checkpoint_service().domain_claim_checkpoint(entry_id, **kwargs)

    @router.post(f"{_prefix}/recover")
    async def recover_idless(request: Request) -> dict[str, Any]:
        entry_id, kwargs = await _id_from_kwargs(request, "checkpoint_id", "entry_id", "id")
        return _checkpoint_service().domain_recover_checkpoint(entry_id, **kwargs)

    @router.post(f"{_prefix}/advance")
    async def advance_idless(request: Request) -> dict[str, Any]:
        entry_id, kwargs = await _id_from_kwargs(request, "checkpoint_id", "entry_id", "id")
        return _checkpoint_service().domain_advance_checkpoint(entry_id, **kwargs)


@router.post("/recovery/claim")
async def recovery_claim(request: Request) -> dict[str, Any]:
    entry_id, kwargs = await _id_from_kwargs(request, "checkpoint_id", "entry_id")
    return _checkpoint_service().domain_claim_checkpoint(entry_id, **kwargs)


@router.post("/recovery/recover")
async def recovery_recover(request: Request) -> dict[str, Any]:
    entry_id, kwargs = await _id_from_kwargs(request, "checkpoint_id", "entry_id")
    return _checkpoint_service().domain_recover_checkpoint(entry_id, **kwargs)


@router.post("/recovery/advance")
async def recovery_advance(request: Request) -> dict[str, Any]:
    entry_id, kwargs = await _id_from_kwargs(request, "checkpoint_id", "entry_id")
    return _checkpoint_service().domain_advance_checkpoint(entry_id, **kwargs)
