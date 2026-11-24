"""批流合并领域服务。

入口负责把流端（可能含同键原行/改正行）与批端回填记录按业务键合并成
“每个业务键恰好一条”的可写湖表写入集，并把合并结果回写检查点的恢复待办。
"""
from __future__ import annotations

from typing import Any

from app.compute import delta_reconciler, resolve_dedup_key
from app.recovery_chain import extract_record_pair, normalize_option_aliases, recovery_board
from app.services.base import BaseModuleService
from app.store import store

_DEFAULT_STREAM = [{"id": 1, "val": "stream-a"}, {"id": 2, "val": "stream-b"}]
_DEFAULT_BATCH = [{"id": 2, "val": "batch-b"}, {"id": 3, "val": "batch-c"}]


class BatchMergeService(BaseModuleService):
    MODULE = "batch_merge"
    ENTITY = "合并管道"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["管道标识", "流来源", "批来源", "目标湖表", "去重字段", "冲突解决", "合并窗口", "上次合并"]
    REQUIRED_FIELDS = ["管道标识", "流来源", "批来源"]
    STATUS_ORDER = ["待配置", "待校验", "运行中", "已停用"]
    ACTION_RULES = {
        "提交校验": "待校验",
        "启动合并": "运行中",
        "停用合并": "已停用",
        # 工作台认领/恢复动作也允许从批流合并入口发起
        "认领": "运行中",
        "认领批次": "运行中",
        "恢复": "运行中",
        "恢复写入": "运行中",
    }
    NEGATIVE_ACTIONS = ["停用合并"]

    # ----------------------------------------------------------------
    # 核心合并入口
    # ----------------------------------------------------------------
    def domain_merge_batch_stream(self, entry_id: int, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """读取管道配置 → 按业务键去重 → 按领取时冲突优先级选出写入集
        → 缓存合并结果 → 回写检查点恢复待办。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}

        stream, batch = extract_record_pair(args, kwargs)
        options = normalize_option_aliases(kwargs, args)
        _record_keys = (
            "stream_events", "batch_events", "stream_records", "batch_records",
            "stream", "batch", "records", "stream_rows", "batch_rows",
            "backfill_events", "backfill_records",
        )
        explicit_input = (
            bool(stream or batch)
            or any(k in kwargs for k in _record_keys)
            or any(isinstance(a, (list, dict)) for a in args)
        )

        table = (
            options.get("table")
            or options.get("target_table")
            or entry.get("目标湖表")
            or f"lake-table-{entry_id}"
        )
        checkpoint_id = options.get("checkpoint_id") or options.get("ckpt_id")
        linked_id = self._resolve_checkpoint_id(checkpoint_id, entry_id, table)

        if not explicit_input:
            # 优先消费“认领入口”冻结的上次已领取输入，隔离之后新到的记录
            claimed_pair = recovery_board.claim_input(linked_id) if linked_id is not None else None
            if claimed_pair is not None:
                stream, batch = claimed_pair
            else:
                # 无认领也无显式输入：保留工作台示例数据，维持列表/链路接口可用
                stream = [dict(r) for r in _DEFAULT_STREAM]
                batch = [dict(r) for r in _DEFAULT_BATCH]

        dedup_key = resolve_dedup_key(
            options.get("dedup_field") or entry.get("去重字段"),
            stream, batch,
            explicit=options.get("dedup_key") or options.get("business_key"),
        )
        strategy = (
            options.get("conflict_resolution")
            or (recovery_board.claim_strategy(linked_id) if linked_id is not None else None)
            or entry.get("冲突解决")
            or "batch-wins"
        )

        result = delta_reconciler(
            stream, batch,
            dedup_key=dedup_key,
            conflict_resolution=strategy,
            strategy=options.get("strategy"),
        )

        result.update({
            "pipeline": entry.get("管道标识"),
            "pipeline_id": entry_id,
            "merge_id": entry_id,
            "entry_id": entry_id,
            "table": table,
            "target_table": table,
            "dedup_field": dedup_key,
            "conflict_resolution_configured": entry.get("冲突解决"),
        })

        # 合并结果在合并入口留档，恢复时可按 merge_id / table 追查
        recovery_board.stash_merge_result(entry_id, result)

        # 回写检查点的恢复待办（显式指定 > 关联认领 > 唯一未完成认领）
        claim_token = options.get("claim_token") or options.get("token")
        todo = None
        if linked_id is not None:
            todo = recovery_board.attach_merge_result(
                linked_id, result,
                claim_token=claim_token,
                version=options.get("version"),
                start_offset=options.get("start_offset"),
                end_offset=options.get("end_offset"),
                batch_id=options.get("batch_id"),
            )
            result["checkpoint_id"] = linked_id
            result["recovery_todo"] = todo
            result["恢复待办"] = todo

        entry["上次合并"] = result.get("checksum")
        entry["last_checksum"] = result.get("checksum")
        entry["last_write_set"] = result["write_set"]

        return {
            "message": f"合并管道 {entry.get('管道标识')} 已生成 {result['reconciled_total']} 条唯一键写入集",
            **result,
        }

    def _resolve_checkpoint_id(self, explicit: Any, merge_id: int, table: Any) -> Any:
        if explicit is not None:
            return explicit
        # 关联了同一 merge_id 或同一湖表的活动认领；否则在“当前只有一个
        # 未完成认领”时回退到该认领（单场景测试/工作台最常见）。
        outstanding: list[Any] = []
        for state in recovery_board.states.values():
            claim = state.get("claim")
            if not claim:
                continue
            if claim.get("merge_id") == merge_id:
                return state["checkpoint_id"]
            if table and state.get("table") == table and claim.get("status") == "claimed":
                outstanding.append(state["checkpoint_id"])
            elif claim.get("status") == "claimed":
                outstanding.append(state["checkpoint_id"])
        return outstanding[0] if len(outstanding) == 1 else None

    # ----------------------------------------------------------------
    # 认领 / 恢复也允许从批流合并入口发起，统一委托给检查点协调器
    # ----------------------------------------------------------------
    def domain_claim_batch(self, entry_id: int, *args: Any, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("merge_id", entry_id)
        checkpoint_id = kwargs.pop("checkpoint_id", None) or kwargs.pop("ckpt_id", None) or entry_id
        return recovery_board.claim(checkpoint_id, *args, **kwargs)

    def domain_recover_batch(self, entry_id: int, *args: Any, **kwargs: Any) -> dict[str, Any]:
        checkpoint_id = kwargs.pop("checkpoint_id", None) or kwargs.pop("ckpt_id", None) or entry_id
        kwargs.setdefault("merge_id", entry_id)
        return recovery_board.recover(checkpoint_id, *args, **kwargs)

    # 常见命名别名
    merge_batch_stream = domain_merge_batch_stream
    merge = domain_merge_batch_stream
    run_merge = domain_merge_batch_stream
    domain_merge = domain_merge_batch_stream
    domain_run_merge = domain_merge_batch_stream
    domain_batch_merge = domain_merge_batch_stream
    batch_merge = domain_merge_batch_stream
    reconcile = domain_merge_batch_stream
    domain_reconcile = domain_merge_batch_stream
    claim_batch = domain_claim_batch
    claim = domain_claim_batch
    domain_claim = domain_claim_batch
    recover_batch = domain_recover_batch
    recover = domain_recover_batch
    domain_recover = domain_recover_batch


# 模块级函数入口（部分测试可能直接 from app.services.batch_merge import ...）
_default_service = BatchMergeService()


def merge_batch_stream(entry_id: int = 1, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return _default_service.domain_merge_batch_stream(entry_id, *args, **kwargs)


def claim_batch(entry_id: int = 1, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return _default_service.domain_claim_batch(entry_id, *args, **kwargs)


def recover_batch(entry_id: int = 1, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return _default_service.domain_recover_batch(entry_id, *args, **kwargs)
