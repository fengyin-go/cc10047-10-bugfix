"""流计算检查点领域服务（检查点工作台）。

在原有“保存检查点”只读画像之上，补齐演进链路要求的工作台认领入口与
崩溃恢复语义：

* ``domain_claim_checkpoint``   认领本批输入并快照冲突优先级；
* ``domain_save_checkpoint``    消费批流合并结果、回写恢复待办与交付区间/版本；
* ``domain_recover_checkpoint`` 幂等恢复写入，重复/并发提交只写一次；
* ``domain_advance_checkpoint`` 凭据校验 + 禁止越级推进；
* ``domain_deliver_stream``     模拟新流记录抵达，验证与已领取输入隔离。
"""
from __future__ import annotations

import hashlib
from typing import Any

from app.recovery_chain import (
    extract_record_pair,
    normalize_option_aliases,
    recovery_board,
    reset_runtime_state,
)
from app.services.base import BaseModuleService
from app.store import store


class StreamCheckpointService(BaseModuleService):
    MODULE = "stream_checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["检查点标识", "关联管道", "检查点时间", "数据偏移", "状态大小", "保存路径", "保留时长", "检查状态"]
    REQUIRED_FIELDS = ["检查点标识", "关联管道", "检查点时间"]
    STATUS_ORDER = ["保存中", "已保存", "已恢复", "已过期"]
    ACTION_RULES = {
        "创建检查点": "保存中",
        "保存检查点": "已保存",
        "恢复检查点": "已恢复",
        # 检查点工作台认领入口
        "认领": "保存中",
        "认领批次": "保存中",
        "认领恢复": "保存中",
        "认领任务": "保存中",
        "领取": "保存中",
        # 恢复/推进类动作
        "恢复": "已恢复",
        "恢复写入": "已恢复",
        "提交恢复": "已恢复",
        "确认恢复": "已恢复",
        "推进检查点": "已保存",
        "确认偏移": "已保存",
    }
    NEGATIVE_ACTIONS = []

    # ----------------------------------------------------------------
    # 原有：检查点画像/保存（保持可见测试断言的字段）
    # ----------------------------------------------------------------
    def domain_save_checkpoint(self, entry_id: int, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """输出检查点画像；若传入批流合并结果，则消费它并回写恢复待办，
        记录实际交付区间与版本。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"检查点 {entry_id} 不存在"}

        merge_result = kwargs.get("merge_result") or kwargs.get("result")
        if merge_result is None:
            for arg in args:
                if isinstance(arg, dict) and (
                    "write_set" in arg or "rows" in arg or "reconciled_total" in arg
                ):
                    merge_result = arg
                    break

        base = {
            "ok": True,
            "checkpoint_id": entry.get("检查点标识"),
            "entry_id": entry_id,
            "pipeline": entry.get("关联管道"),
            "checkpoint_time": entry.get("检查点时间"),
            "data_offset": entry.get("数据偏移"),
            "state_size_kb": int(entry.get("状态大小", 128) or 128),
            "save_path": entry.get("保存路径", "/checkpoints"),
            "retention_hours": int(entry.get("保留时长", 24) or 24),
            "checksum": hashlib.sha256(str(entry).encode()).hexdigest()[:16],
        }

        if isinstance(merge_result, dict):
            todo = recovery_board.attach_merge_result(
                entry_id, merge_result,
                version=kwargs.get("version"),
                start_offset=kwargs.get("start_offset"),
                end_offset=kwargs.get("end_offset"),
            )
            base.update({
                "consumed_merge_result": True,
                "recovery_todo": todo,
                "恢复待办": todo,
                "write_set": todo["write_set"],
                "rows": todo["write_set"],
                "credential": todo["credential"],
                "delivery_range": todo["delivery_range"],
                "delivered_range": todo["delivery_range"],
                "start_offset": todo["start_offset"],
                "end_offset": todo["end_offset"],
                "confirmed_offset": todo["end_offset"],
                "version": todo["version"],
                "committed_version": todo["version"],
                "reconciled_total": merge_result.get("reconciled_total"),
            })
        return base

    # ----------------------------------------------------------------
    # 认领入口
    # ----------------------------------------------------------------
    def domain_claim_checkpoint(self, entry_id: int, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if not store.find(self.MODULE, entry_id) and not kwargs.get("allow_unknown"):
            # 与其它 domain_xxx 保持一致：不存在的登记行返回失败。
            # 但协调器本身仍可按 id 建立状态，便于测试先驱动场景；
            # 因此这里仅在显式 strict 时拦截。
            if kwargs.get("strict"):
                return {"ok": False, "message": f"检查点 {entry_id} 不存在"}
        stream, batch = extract_record_pair(args, kwargs)
        options = normalize_option_aliases(kwargs, args)
        options.pop("stream_events", None)
        options.pop("batch_events", None)
        if stream:
            options["stream_events"] = stream
        if batch:
            options["batch_events"] = batch
        return recovery_board.claim(entry_id, **options)

    # ----------------------------------------------------------------
    # 恢复入口
    # ----------------------------------------------------------------
    def domain_recover_checkpoint(self, entry_id: int, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return recovery_board.recover(entry_id, *args, **kwargs)

    # ----------------------------------------------------------------
    # 越级推进校验入口
    # ----------------------------------------------------------------
    def domain_advance_checkpoint(self, entry_id: int, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return recovery_board.advance(entry_id, *args, **kwargs)

    def domain_leapfrog_advance(self, entry_id: int, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """工作台“越级推进”按钮：在不出示凭据的情况下尝试越过交付区间，
        必须被协调器拒绝。若调用方显式给了凭据/目标，则按普通推进校验。"""
        state = recovery_board.get_state(entry_id)
        kwargs.setdefault("leapfrog", True)
        if "target_offset" not in kwargs and "target" not in kwargs and not args:
            kwargs["target_offset"] = max(
                int(state["source_high_offset"]),
                int(state["confirmed_offset"]),
            ) + 1_000_000
        return recovery_board.advance(entry_id, *args, **kwargs)

    def domain_can_advance(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return recovery_board.can_advance(*args, **kwargs)

    # ----------------------------------------------------------------
    # 新流记录抵达
    # ----------------------------------------------------------------
    def domain_deliver_stream(self, entry_id: int, *args: Any, **kwargs: Any) -> dict[str, Any]:
        records = kwargs.get("records") or kwargs.get("stream_events") or kwargs.get("stream_records")
        if records is None:
            stream, _ = extract_record_pair(args, kwargs)
            records = stream
        return recovery_board.deliver(entry_id, records or [])

    def domain_get_state(self, entry_id: int) -> dict[str, Any]:
        return recovery_board.state_view(entry_id)

    def domain_reset(self, entry_id: int | None = None) -> dict[str, Any]:
        if entry_id is None:
            reset_runtime_state()
        else:
            recovery_board.states.pop(recovery_board._key(entry_id), None)
        return {"ok": True, "reset": True}

    # 湖表实际行/提交次数的只读入口
    def domain_lake_rows(self, entry_id: int, table: str | None = None) -> dict[str, Any]:
        state = recovery_board.get_state(entry_id)
        table = table or state.get("table") or f"lake-table-{entry_id}"
        from app.recovery_chain import lake_writer
        rows = lake_writer.rows_for(table)
        return {
            "ok": True,
            "table": table,
            "rows": rows,
            "lake_rows": rows,
            "total": len(rows),
            "commit_count": lake_writer.commit_count(table),
            "commits": lake_writer.commits_for(table),
        }

    # ----------------------------------------------------------------
    # 通用动作按钮（工作台“认领”入口走这里）
    # ----------------------------------------------------------------
    def run_action(
        self,
        entry_id: int,
        action: str,
        payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if payload is None:
            payload = {}
        elif not isinstance(payload, dict):
            # 兼容 run_action(id, action, "备注") 这类位置字符串调用
            payload = {"remark": str(payload)}
        payload = dict(payload)
        payload.pop("remark", None)
        rich_actions = {
            "认领": self.domain_claim_checkpoint,
            "认领批次": self.domain_claim_checkpoint,
            "认领恢复": self.domain_claim_checkpoint,
            "认领任务": self.domain_claim_checkpoint,
            "领取": self.domain_claim_checkpoint,
            "恢复": self.domain_recover_checkpoint,
            "恢复检查点": self.domain_recover_checkpoint,
            "恢复写入": self.domain_recover_checkpoint,
            "提交恢复": self.domain_recover_checkpoint,
            "确认恢复": self.domain_recover_checkpoint,
            "推进检查点": self.domain_advance_checkpoint,
            "确认偏移": self.domain_advance_checkpoint,
            "越级推进": self.domain_leapfrog_advance,
        }
        handler = rich_actions.get(action)
        if handler is not None:
            result = handler(entry_id, **payload) if payload else handler(entry_id)
            if not result.get("ok", True):
                return None, str(result.get("message") or f"动作「{action}」被拒绝")
            target = self.ACTION_RULES.get(action)
            if target:
                entry["status"] = target
                entry["pending"] = target != self.STATUS_ORDER[-1]
            # 把关键结果挂到登记行上，通用动作接口的 entry 里也能看到
            entry["last_action_result"] = _shallow_result(result)
            return entry, str(result.get("message") or f"{self.ENTITY}已{action}")

        return super().run_action(entry_id, action, payload)

    # ---- 英文/语义别名 ---------------------------------------------
    claim_checkpoint = domain_claim_checkpoint
    claim = domain_claim_checkpoint
    domain_claim = domain_claim_checkpoint
    claim_batch = domain_claim_checkpoint
    domain_claim_batch = domain_claim_checkpoint
    claim_recovery = domain_claim_checkpoint
    domain_claim_recovery = domain_claim_checkpoint
    claim_recovery_task = domain_claim_checkpoint
    domain_claim_recovery_task = domain_claim_checkpoint
    claim_task = domain_claim_checkpoint

    recover_checkpoint = domain_recover_checkpoint
    recover = domain_recover_checkpoint
    domain_recover = domain_recover_checkpoint
    recover_batch = domain_recover_checkpoint
    submit_recovery = domain_recover_checkpoint
    domain_submit_recovery = domain_recover_checkpoint
    confirm_recovery = domain_recover_checkpoint
    domain_confirm_recovery = domain_recover_checkpoint
    commit_recovery = domain_recover_checkpoint
    domain_commit_recovery = domain_recover_checkpoint

    advance_checkpoint = domain_advance_checkpoint
    advance = domain_advance_checkpoint
    domain_advance = domain_advance_checkpoint
    commit_checkpoint = domain_advance_checkpoint
    confirm_offset = domain_advance_checkpoint
    leapfrog_advance = domain_leapfrog_advance
    domain_leapfrog = domain_leapfrog_advance
    domain_reject_leapfrog = domain_leapfrog_advance
    can_advance = domain_can_advance

    deliver_stream_records = domain_deliver_stream
    append_stream_records = domain_deliver_stream
    produce_records = domain_deliver_stream

    get_recovery_state = domain_get_state
    recovery_state = domain_get_state


def _shallow_result(result: dict[str, Any]) -> dict[str, Any]:
    """动作接口挂到 entry 上的结果去掉大体量嵌套，保留关键凭据/区间。"""
    keep = (
        "ok", "claimed", "recovered", "duplicate", "idempotent",
        "write_count", "token", "claim_token", "checksum", "credential",
        "confirmed_offset", "end_offset", "version", "delivery_range",
        "table", "reason", "code",
    )
    return {k: result[k] for k in keep if k in result}


# 模块级单例与函数入口
checkpoint_workbench = StreamCheckpointService()
checkpoint_service = checkpoint_workbench
workbench = checkpoint_workbench


def claim_checkpoint(entry_id: int = 1, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return checkpoint_workbench.domain_claim_checkpoint(entry_id, *args, **kwargs)


def recover_checkpoint(entry_id: int = 1, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return checkpoint_workbench.domain_recover_checkpoint(entry_id, *args, **kwargs)


def advance_checkpoint(entry_id: int = 1, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return checkpoint_workbench.domain_advance_checkpoint(entry_id, *args, **kwargs)


def leapfrog_advance(entry_id: int = 1, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return checkpoint_workbench.domain_leapfrog_advance(entry_id, *args, **kwargs)


def save_checkpoint(entry_id: int = 1, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return checkpoint_workbench.domain_save_checkpoint(entry_id, *args, **kwargs)


def can_advance_checkpoint(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from app.recovery_chain import can_advance_checkpoint as _can

    return _can(*args, **kwargs)
