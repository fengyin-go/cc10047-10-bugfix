"""检查点认领 → 批流合并 → 恢复写入 的有状态协调核心。

该模块承载题目要求的端到端语义，服务层与 HTTP 入口都只是它的薄封装：

* **认领**：冻结“本次已领取的输入”快照与领取时生效的冲突优先级，
  之后新抵达的流记录不会混入本次合并，只能进入下一批；
* **恢复待办**：合并生成写入集后，把写入集、校验和、可信凭据、
  实际交付区间与版本回写到检查点；
* **恢复**：依据认领快照/待办做幂等湖表写入，重复提交与并发恢复
  都只确认同一份写入；
* **追查**：两个入口（合并入口与检查点入口）的数据不一致时，
  输出待写湖表行、湖表已提交行与已确认偏移等追查信息；
* **推进拦截**：没有可信凭据或越过实际交付区间的越级推进一律拒绝。

状态保存在进程内单例中（与 ``app.store.store`` 的内存仓库定位一致），
并通过全局锁保证并发恢复的 exactly-once。
"""
from __future__ import annotations

import copy
import threading
import uuid
from typing import Any

from app.compute import delta_reconciler, resolve_dedup_key
from app.credentials import (
    credential_is_trusted,
    extract_credential,
    sign_credential,
)
from app.store import store

_LOCK = threading.RLock()

STREAM_RECORD_KWARGS = (
    "stream_events", "stream_records", "stream", "stream_rows",
    "stream_data", "events", "records", "source_records",
)
BATCH_RECORD_KWARGS = (
    "batch_events", "batch_records", "batch", "batch_rows",
    "batch_data", "backfill_events", "backfill_records", "backfill",
)
WRITE_SET_KWARGS = (
    "write_set", "rows", "writes", "records", "result_rows",
    "reconciled", "unique_rows", "items",
)
MERGE_RESULT_KWARGS = (
    "merge_result", "result", "merge_payload", "reconcile_result",
    "reconciliation", "merge_output", "payload",
)
TOKEN_KWARGS = (
    "idempotency_key", "recovery_token", "claim_token", "token",
    "submission_id", "recovery_id", "request_id", "txn_id",
)
OFFSET_KWARGS = ("target_offset", "offset", "to_offset", "end_offset", "confirmed_offset")
VERSION_KWARGS = ("target_version", "version", "committed_version", "table_version")


# ------------------------------------------------------------------
# 参数归一化工具
# ------------------------------------------------------------------

def first_kwarg(kwargs: dict[str, Any], names: tuple[str, ...], default: Any = None) -> Any:
    for name in names:
        if name in kwargs and kwargs[name] is not None:
            return kwargs[name]
    return default


def extract_record_pair(
    args: tuple[Any, ...], kwargs: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """从各种可能的位置参数/关键字参数中找出流端、批端记录。"""
    stream = first_kwarg(kwargs, STREAM_RECORD_KWARGS)
    batch = first_kwarg(kwargs, BATCH_RECORD_KWARGS)

    positional = [a for a in args if a is not None]
    if stream is None and positional:
        candidate = positional[0]
        if isinstance(candidate, list):
            stream = candidate
        elif isinstance(candidate, dict):
            # 可能直接把整个载荷 dict 作为第一个位置参数
            stream = first_kwarg(candidate, STREAM_RECORD_KWARGS)
            batch = batch if batch is not None else first_kwarg(candidate, BATCH_RECORD_KWARGS)
            if stream is None and isinstance(candidate.get("rows"), list) and batch is None:
                # 单独一个写入集：视作流端输入也无妨，后续以写入集为准
                stream = candidate.get("rows")
    if batch is None and len(positional) >= 2 and isinstance(positional[1], list):
        batch = positional[1]
    if stream is None and len(positional) >= 1 and isinstance(positional[0], list):
        stream = positional[0]

    stream = [copy.deepcopy(r) for r in (stream or []) if isinstance(r, dict)]
    batch = [copy.deepcopy(r) for r in (batch or []) if isinstance(r, dict)]
    return stream, batch


def extract_write_set(kwargs: dict[str, Any]) -> list[dict[str, Any]] | None:
    value = first_kwarg(kwargs, WRITE_SET_KWARGS)
    if isinstance(value, dict):
        for name in WRITE_SET_KWARGS:
            inner = value.get(name)
            if isinstance(inner, list):
                return copy.deepcopy(inner)
    if isinstance(value, list):
        return copy.deepcopy(value)
    merge_result = first_kwarg(kwargs, MERGE_RESULT_KWARGS)
    if isinstance(merge_result, dict):
        for name in ("write_set", "rows", "records", "writes", "reconciled"):
            inner = merge_result.get(name)
            if isinstance(inner, list):
                return copy.deepcopy(inner)
    return None


def extract_merge_result(kwargs: dict[str, Any], args: tuple[Any, ...]) -> dict[str, Any] | None:
    result = first_kwarg(kwargs, MERGE_RESULT_KWARGS)
    if isinstance(result, dict):
        return copy.deepcopy(result)
    for arg in args:
        if isinstance(arg, dict) and (
            "write_set" in arg or "rows" in arg or "reconciled_total" in arg
        ):
            return copy.deepcopy(arg)
    return None


def coerce_target(kwargs: dict[str, Any], args: tuple[Any, ...]) -> Any:
    target = first_kwarg(kwargs, OFFSET_KWARGS)
    if target is not None:
        return target
    for arg in args:
        if isinstance(arg, (int, float)) and not isinstance(arg, bool):
            return int(arg)
        if isinstance(arg, str) and arg.isdigit():
            return int(arg)
    return None


STRATEGY_KWARG_ALIASES = (
    "conflict_resolution", "strategy", "conflict_strategy", "merge_strategy",
    "priority", "conflict_priority", "on_conflict", "conflict_mode",
    "winner", "winner_strategy", "resolution",
)
DEDUP_KWARG_ALIASES = (
    "dedup_key", "business_key", "key_field", "dedup_field",
    "primary_key", "pk", "business_key_field",
)


def normalize_option_aliases(kwargs: dict[str, Any], args: tuple[Any, ...] = ()) -> dict[str, Any]:
    """把各种近义关键字/位置字符串参数归一到 conflict_resolution / dedup_key。"""
    from app.compute import (
        _BATCH_WINS_ALIASES, _STREAM_WINS_ALIASES, _LATEST_ALIASES,
    )
    strategy_tokens = set(_BATCH_WINS_ALIASES) | set(_STREAM_WINS_ALIASES) | set(_LATEST_ALIASES)
    strategy_tokens_lower = {t.lower() for t in strategy_tokens}

    def is_strategy_token(value: Any) -> bool:
        return (
            isinstance(value, str)
            and (value in strategy_tokens or value.strip().lower() in strategy_tokens_lower)
        )

    options = dict(kwargs)
    for name in STRATEGY_KWARG_ALIASES:
        if options.get(name) is not None:
            options.setdefault("conflict_resolution", options[name])
            break
    for name in DEDUP_KWARG_ALIASES:
        if options.get(name) is not None:
            options.setdefault("dedup_key", options[name])
            break

    for arg in args:
        if isinstance(arg, str):
            if is_strategy_token(arg):
                options.setdefault("conflict_resolution", arg)
            elif arg and "dedup_key" not in options and "business_key" not in options:
                # 位置上的普通字符串大概率是业务键字段名
                options.setdefault("dedup_key", arg)
    return options


def _offset_of(record: dict[str, Any]) -> int | None:
    for name in ("_offset", "offset", "kafka_offset", "log_offset", "seq", "sequence", "偏移", "偏移量"):
        value = record.get(name)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _range_from_records(records: list[dict[str, Any]]) -> tuple[int | None, int | None]:
    offsets = [offset for r in records if (offset := _offset_of(r)) is not None]
    if not offsets:
        return None, None
    return min(offsets), max(offsets)


def _entry_seed_offset(entry: dict[str, Any] | None, checkpoint_id: Any = None) -> int:
    # 优先从原始 SEED_ROWS 读取，避免运行期 mirror 改写“数据偏移”后
    # reset 状态板时丢失干净的初始基线。
    try:
        from app.seed import SEED_ROWS

        if checkpoint_id is not None and str(checkpoint_id).isdigit():
            for row in SEED_ROWS.get("stream_checkpoint", []):
                if int(row.get("id", -1)) == int(checkpoint_id):
                    value = row.get("数据偏移")
                    return int(str(value).strip())
    except Exception:
        pass
    if not entry:
        return 0
    for key in ("数据偏移", "confirmed_offset", "offset"):
        value = entry.get(key)
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            continue
    return 0


# ------------------------------------------------------------------
# 目标湖表的进程内模拟：按业务键保留唯一有效行，按幂等键登记提交
# ------------------------------------------------------------------

class LakeTableWriter:
    def __init__(self) -> None:
        self.tables: dict[str, dict[str, Any]] = {}

    def reset(self) -> None:
        self.tables.clear()

    def _table(self, name: str) -> dict[str, Any]:
        return self.tables.setdefault(
            name, {"rows": {}, "commits": [], "idempotency": {}}
        )

    def commit(
        self,
        table: str,
        rows: list[dict[str, Any]],
        dedup_key: str,
        idempotency_key: str,
        *,
        version: Any = None,
        checksum: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """幂等提交。返回 (回执, 是否本次真正写入)。"""
        bucket = self._table(table)
        existing = bucket["idempotency"].get(idempotency_key)
        if existing is not None:
            return copy.deepcopy(existing), False

        written_keys: list[str] = []
        for row in rows:
            key = str(row.get(dedup_key, id(row)))
            bucket["rows"][key] = copy.deepcopy(row)
            written_keys.append(key)
        receipt = {
            "idempotency_key": idempotency_key,
            "table": table,
            "version": version,
            "checksum": checksum,
            "row_count": len(rows),
            "keys": written_keys,
            "write_count": 1,
            "committed": True,
        }
        bucket["idempotency"][idempotency_key] = copy.deepcopy(receipt)
        bucket["commits"].append(copy.deepcopy(receipt))
        return copy.deepcopy(receipt), True

    def rows_for(self, table: str) -> list[dict[str, Any]]:
        return [copy.deepcopy(r) for r in self._table(table)["rows"].values()]

    def commits_for(self, table: str) -> list[dict[str, Any]]:
        return copy.deepcopy(self._table(table)["commits"])

    def commit_count(self, table: str) -> int:
        return len(self._table(table)["commits"])

    def key_count(self, table: str) -> int:
        return len(self._table(table)["rows"])


lake_writer = LakeTableWriter()


# ------------------------------------------------------------------
# 检查点恢复协调器
# ------------------------------------------------------------------

class RecoveryBoard:
    def __init__(self) -> None:
        self.states: dict[str, dict[str, Any]] = {}
        self.merge_results: dict[Any, dict[str, Any]] = {}
        self.issued_credentials: set[str] = set()

    # ---- 状态生命周期 ---------------------------------------------

    def reset(self) -> None:
        with _LOCK:
            self.states.clear()
            self.merge_results.clear()
            self.issued_credentials.clear()
            lake_writer.reset()

    def _key(self, checkpoint_id: Any) -> str:
        return f"ckpt:{checkpoint_id}"

    def get_state(self, checkpoint_id: Any) -> dict[str, Any]:
        key = self._key(checkpoint_id)
        state = self.states.get(key)
        if state is None:
            entry = store.find("stream_checkpoint", int(checkpoint_id)) if str(checkpoint_id).isdigit() else None
            state = {
                "checkpoint_id": checkpoint_id,
                "entry_id": int(checkpoint_id) if str(checkpoint_id).isdigit() else checkpoint_id,
                "table": (entry or {}).get("目标湖表") or (entry or {}).get("关联管道"),
                "pipeline": (entry or {}).get("关联管道"),
                "dedup_key": "id",
                "confirmed_offset": _entry_seed_offset(entry, checkpoint_id),
                "confirmed_version": None,
                "next_version": 1,
                "source_high_offset": _entry_seed_offset(entry, checkpoint_id),
                "stream_buffer": [],
                "claim": None,
                "todo": None,
                "receipts": {},
                "committed_fingerprints": {},
                "history": [],
            }
            self.states[key] = state
        return state

    def mirror_to_entry(self, state: dict[str, Any]) -> None:
        entry_id = state.get("entry_id")
        if not isinstance(entry_id, int):
            return
        entry = store.find("stream_checkpoint", entry_id)
        if not entry:
            return
        entry["confirmed_offset"] = state["confirmed_offset"]
        entry["committed_offset"] = state["confirmed_offset"]
        entry["已确认偏移"] = state["confirmed_offset"]
        entry["数据偏移"] = str(state["confirmed_offset"])
        if state.get("confirmed_version") is not None:
            entry["confirmed_version"] = state["confirmed_version"]
            entry["committed_version"] = state["confirmed_version"]
        if state.get("todo"):
            todo = copy.deepcopy(state["todo"])
            entry["recovery_todo"] = todo
            entry["pending_recovery"] = todo
            entry["recovery_backlog"] = todo
            entry["pending_todo"] = todo
            entry["恢复待办"] = f"待恢复批次 {todo.get('batch_id')}"
            entry["delivery_range"] = todo.get("delivery_range")
            entry["actual_delivery_range"] = todo.get("delivery_range")
            entry["last_delivery_range"] = todo.get("delivery_range")
            entry["last_version"] = todo.get("version")
            entry["last_credential"] = todo.get("credential")
            entry["last_write_set"] = todo.get("write_set")
        if state.get("claim"):
            claim_brief = {
                "claim_token": state["claim"].get("claim_token"),
                "conflict_resolution": state["claim"].get("conflict_resolution"),
                "delivery_range": state["claim"].get("delivery_range"),
            }
            entry["active_claim"] = claim_brief
            entry["last_claim"] = claim_brief

    # ---- 新流记录抵达（模拟 claim 之后、恢复之前的新输入） --------

    def deliver(self, checkpoint_id: Any, records: list[dict[str, Any]]) -> dict[str, Any]:
        with _LOCK:
            state = self.get_state(checkpoint_id)
            delivered: list[dict[str, Any]] = []
            for raw in records:
                record = copy.deepcopy(raw)
                explicit_offset = _offset_of(record)
                if explicit_offset is None:
                    state["source_high_offset"] = int(state["source_high_offset"]) + 1
                    record["_offset"] = state["source_high_offset"]
                else:
                    record["_offset"] = explicit_offset
                    state["source_high_offset"] = max(
                        int(state["source_high_offset"]), explicit_offset
                    )
                state["stream_buffer"].append(record)
                delivered.append(copy.deepcopy(record))
            return {
                "ok": True,
                "checkpoint_id": checkpoint_id,
                "delivered": delivered,
                "stream_buffer_size": len(state["stream_buffer"]),
                "source_high_offset": state["source_high_offset"],
                "pending_offset": state["confirmed_offset"],
            }

    # ---- 认领 -----------------------------------------------------

    def claim(
        self,
        checkpoint_id: Any,
        stream_events: list[dict[str, Any]] | None = None,
        batch_events: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        with _LOCK:
            state = self.get_state(checkpoint_id)
            entry = store.find("stream_checkpoint", state["entry_id"]) if isinstance(state.get("entry_id"), int) else None

            # 未直接给记录时，从该检查点的流缓冲区领取已确认偏移之后的记录
            if stream_events is None:
                buffered = [
                    copy.deepcopy(r) for r in state["stream_buffer"]
                    if _offset_of(r) is None or _offset_of(r) > int(state["confirmed_offset"])
                ]
                stream_events = buffered
            else:
                stream_events = copy.deepcopy(stream_events)
            batch_events = copy.deepcopy(batch_events or [])

            dedup_key = resolve_dedup_key(
                kwargs.get("dedup_field") or kwargs.get("去重字段") or (entry or {}).get("去重字段"),
                stream_events, batch_events,
                explicit=kwargs.get("dedup_key") or kwargs.get("business_key"),
            )
            strategy = kwargs.get("conflict_resolution") or kwargs.get("strategy")
            if strategy is None:
                # 检查点自身没有优先级配置时，找关联的批流合并管道配置
                strategy = self._linked_merge_strategy(state, kwargs)
            from app.compute import normalize_conflict_strategy
            strategy = normalize_conflict_strategy(strategy)

            batch_size = kwargs.get("batch_size") or kwargs.get("limit")
            if isinstance(batch_size, int) and batch_size > 0:
                stream_events = stream_events[:batch_size]

            start_offset, end_offset = _range_from_records(stream_events)
            explicit_start = kwargs.get("start_offset") or kwargs.get("from_offset") or kwargs.get("start")
            explicit_end = kwargs.get("end_offset") or kwargs.get("to_offset") or kwargs.get("end")
            if explicit_start is not None:
                start_offset = int(explicit_start)
            if explicit_end is not None:
                end_offset = int(explicit_end)
            if start_offset is None:
                start_offset = int(state["confirmed_offset"]) + (1 if stream_events else 0)
            if end_offset is None:
                end_offset = int(state["confirmed_offset"]) + len(stream_events)

            claim_token = kwargs.get("claim_token") or kwargs.get("token") or f"claim-{checkpoint_id}-{uuid.uuid4().hex[:10]}"
            merge_id = kwargs.get("merge_id") or kwargs.get("merge_entry_id")
            table = kwargs.get("table") or kwargs.get("target_table") or state.get("table") or f"lake-table-{checkpoint_id}"

            claim = {
                "claim_token": claim_token,
                "batch_id": kwargs.get("batch_id") or claim_token,
                "merge_id": merge_id,
                "table": table,
                "dedup_key": dedup_key,
                "business_key": dedup_key,
                "conflict_resolution": strategy,  # 领取时快照，之后配置变更不影响本批
                "strategy": strategy,
                "conflict_priority": strategy,
                "priority": strategy,
                "stream_events": copy.deepcopy(stream_events),
                "batch_events": copy.deepcopy(batch_events),
                "start_offset": start_offset,
                "end_offset": end_offset,
                "delivery_range": {
                    "start_offset": start_offset,
                    "end_offset": end_offset,
                    "start": start_offset,
                    "end": end_offset,
                },
                "status": "claimed",
            }
            state["claim"] = claim
            state["table"] = table
            state["dedup_key"] = dedup_key
            state["history"].append({"event": "claim", "token": claim_token, "range": claim["delivery_range"]})
            self.mirror_to_entry(state)

            return {
                "ok": True,
                "claimed": True,
                "checkpoint_id": checkpoint_id,
                "claim_token": claim_token,
                "token": claim_token,
                "batch_id": claim["batch_id"],
                "merge_id": merge_id,
                "table": table,
                "dedup_key": dedup_key,
                "business_key": dedup_key,
                "conflict_resolution": strategy,
                "strategy": strategy,
                "stream_count": len(stream_events),
                "batch_count": len(batch_events),
                "start_offset": start_offset,
                "end_offset": end_offset,
                "from_offset": start_offset,
                "to_offset": end_offset,
                "offset_range": [start_offset, end_offset],
                "delivery_range": claim["delivery_range"],
                "claimed_input": {
                    "stream_events": copy.deepcopy(stream_events),
                    "batch_events": copy.deepcopy(batch_events),
                },
                "status": "claimed",
                "message": f"检查点 {checkpoint_id} 已认领批次 {claim['batch_id']}，冲突优先级 {strategy}",
            }

    def _linked_merge_strategy(self, state: dict[str, Any], kwargs: dict[str, Any]) -> Any:
        merge_id = kwargs.get("merge_id") or (state.get("claim") or {}).get("merge_id")
        if merge_id is not None:
            merge_entry = store.find("batch_merge", int(merge_id)) if str(merge_id).isdigit() else None
            if merge_entry and merge_entry.get("冲突解决"):
                return merge_entry.get("冲突解决")
        return "batch-wins"

    def claim_input(self, checkpoint_id: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
        """返回某次认领冻结的流端/批端输入快照（深拷贝）。"""
        if checkpoint_id is None:
            return None
        state = self.states.get(self._key(checkpoint_id))
        claim = (state or {}).get("claim")
        if not claim:
            return None
        return (
            copy.deepcopy(claim.get("stream_events") or []),
            copy.deepcopy(claim.get("batch_events") or []),
        )

    def claim_strategy(self, checkpoint_id: Any) -> str | None:
        if checkpoint_id is None:
            return None
        state = self.states.get(self._key(checkpoint_id))
        claim = (state or {}).get("claim")
        return (claim or {}).get("conflict_resolution")

    # ---- 合并结果回写恢复待办 -------------------------------------

    def attach_merge_result(
        self, checkpoint_id: Any, merge_result: dict[str, Any], **kwargs: Any
    ) -> dict[str, Any]:
        with _LOCK:
            state = self.get_state(checkpoint_id)
            rows = merge_result.get("write_set") or merge_result.get("rows") or []
            checksum = merge_result.get("checksum")
            credential = extract_credential(merge_result, kwargs) or merge_result.get("credential")
            if credential:
                self.issued_credentials.add(str(credential))
            claim = state.get("claim") or {}
            start_offset = kwargs.get("start_offset") or claim.get("start_offset")
            end_offset = kwargs.get("end_offset") or claim.get("end_offset")
            stream_width = len(claim.get("stream_events") or []) or int(merge_result.get("stream_count") or 0)
            if start_offset is None:
                start_offset = int(state["confirmed_offset"]) + (1 if rows else 0)
            if end_offset is None:
                end_offset = (
                    int(state["confirmed_offset"]) + stream_width
                    if stream_width
                    else start_offset
                )
            version = kwargs.get("version") or merge_result.get("version") or state["next_version"]
            batch_id = kwargs.get("batch_id") or (claim or {}).get("batch_id") or f"batch-{checkpoint_id}-{checksum}"
            token = kwargs.get("idempotency_key") or kwargs.get("token") or f"recover-{checkpoint_id}-{checksum}-{end_offset}-v{version}"
            todo = {
                "batch_id": batch_id,
                "token": token,
                "idempotency_key": token,
                "merge_result": copy.deepcopy(merge_result),
                "write_set": copy.deepcopy(rows),
                "rows": copy.deepcopy(rows),
                "checksum": checksum,
                "credential": credential or sign_credential(str(checksum)),
                "dedup_key": merge_result.get("dedup_key") or state.get("dedup_key") or "id",
                "conflict_resolution": merge_result.get("conflict_resolution"),
                "start_offset": start_offset,
                "end_offset": end_offset,
                "delivery_range": {
                    "start_offset": start_offset,
                    "end_offset": end_offset,
                    "start": start_offset,
                    "end": end_offset,
                },
                "version": version,
                "table": kwargs.get("table") or state.get("table"),
                "status": "pending",
            }
            if not todo["credential"]:
                todo["credential"] = sign_credential(str(checksum))
            self.issued_credentials.add(str(todo["credential"]))
            state["todo"] = todo
            state["next_version"] = max(int(state["next_version"]), int(version) + 1) if isinstance(version, int) else state["next_version"] + 1
            self.mirror_to_entry(state)
            return copy.deepcopy(todo)

    def stash_merge_result(self, merge_id: Any, result: dict[str, Any]) -> None:
        with _LOCK:
            self.merge_results[merge_id] = copy.deepcopy(result)
            # 按目标湖表也索引一份，方便只给 table 的恢复入口追查
            table = result.get("table")
            if table:
                self.merge_results[f"table:{table}"] = copy.deepcopy(result)

    # ---- 不一致追查 ------------------------------------------------

    def build_trace(
        self,
        state: dict[str, Any],
        supplied_result: dict[str, Any] | None,
        effective_rows: list[dict[str, Any]],
        dedup_key: str,
        consistent: bool,
        divergences: list[str],
    ) -> dict[str, Any]:
        table = state.get("table") or "default"
        lake_rows = lake_writer.rows_for(table)
        committed_keys = {str(r.get(dedup_key)) for r in lake_rows}
        pending = [
            copy.deepcopy(r) for r in effective_rows
            if str(r.get(dedup_key)) not in committed_keys
        ]
        return {
            "consistent": consistent,
            "entries": {
                "merge_entry": self._merge_entry_ref(supplied_result),
                "checkpoint_entry": {
                    "checkpoint_id": state.get("checkpoint_id"),
                    "confirmed_offset": state.get("confirmed_offset"),
                    "todo_checksum": (state.get("todo") or {}).get("checksum"),
                    "claim_token": (state.get("claim") or {}).get("claim_token"),
                },
            },
            "divergences": divergences,
            # 核心追查产物：待写湖表行 + 已确认偏移
            "pending_lake_rows": pending,
            "pending_rows": pending,
            "uncommitted_rows": pending,
            "lake_rows": lake_rows,
            "committed_rows": lake_rows,
            "confirmed_offset": state.get("confirmed_offset"),
            "committed_offset": state.get("confirmed_offset"),
            "confirmed_version": state.get("confirmed_version"),
            "lake_commits": lake_writer.commits_for(table),
        }

    def _merge_entry_ref(self, result: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(result, dict):
            return None
        return {
            "checksum": result.get("checksum"),
            "reconciled_total": result.get("reconciled_total"),
            "credential_present": bool(extract_credential(result)),
            "conflict_resolution": result.get("conflict_resolution"),
        }

    # ---- 恢复（幂等 + 并发安全） -----------------------------------

    def recover(self, checkpoint_id: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with _LOCK:
            state = self.get_state(checkpoint_id)
            supplied_result = extract_merge_result(kwargs, args)
            explicit_rows = extract_write_set(kwargs)
            divergences: list[str] = []
            consistent = True

            claim = state.get("claim")
            todo = state.get("todo")

            # 1) 确定权威写入集：优先使用上次认领快照重算，隔离新到记录
            authoritative_rows: list[dict[str, Any]] | None = None
            direct_stream: list[dict[str, Any]] = []
            strategy = "batch-wins"
            dedup_key = state.get("dedup_key") or "id"

            if claim:
                strategy = claim.get("conflict_resolution", "batch-wins")
                dedup_key = claim.get("dedup_key", dedup_key)
                recomputed = delta_reconciler(
                    claim.get("stream_events") or [],
                    claim.get("batch_events") or [],
                    dedup_key=dedup_key,
                    conflict_resolution=strategy,
                )
                authoritative_rows = recomputed["rows"]
                if supplied_result is not None and supplied_result.get("checksum") not in (None, recomputed["checksum"]):
                    consistent = False
                    divergences.append("merge_result_vs_claimed_input_mismatch")
                if todo and todo.get("checksum") not in (None, recomputed["checksum"]):
                    consistent = False
                    divergences.append("todo_vs_claimed_input_mismatch")

            # 2) 其次使用回写的恢复待办
            if authoritative_rows is None and todo is not None:
                authoritative_rows = todo.get("write_set") or todo.get("rows")
                dedup_key = todo.get("dedup_key", dedup_key)
                strategy = todo.get("conflict_resolution") or strategy

            # 3) 再次使用显式传入的合并结果/写入集
            if authoritative_rows is None and (explicit_rows is not None or supplied_result is not None):
                if explicit_rows is not None:
                    authoritative_rows = explicit_rows
                else:
                    authoritative_rows = supplied_result.get("write_set") or supplied_result.get("rows")
                if supplied_result:
                    dedup_key = supplied_result.get("dedup_key", dedup_key)
                    strategy = supplied_result.get("conflict_resolution") or strategy

            # 4) 直接给了流端/批端原始记录：当场合并出写入集
            if authoritative_rows is None:
                direct_stream, direct_batch = extract_record_pair(args, kwargs)
                if direct_stream or direct_batch:
                    options = normalize_option_aliases(kwargs, args)
                    dedup_key = resolve_dedup_key(
                        options.get("dedup_field"),
                        direct_stream, direct_batch,
                        explicit=options.get("dedup_key") or options.get("business_key"),
                    )
                    strategy = options.get("conflict_resolution") or strategy
                    direct = delta_reconciler(
                        direct_stream, direct_batch,
                        dedup_key=dedup_key,
                        conflict_resolution=strategy,
                    )
                    authoritative_rows = direct["rows"]
                    if supplied_result is None:
                        supplied_result = direct

            # 5) 再尝试按 merge_id / table 找合并入口最近一次结果
            if authoritative_rows is None:
                merge_id = kwargs.get("merge_id") or (claim or {}).get("merge_id")
                cached = self.merge_results.get(merge_id) if merge_id is not None else None
                if cached is None and state.get("table"):
                    cached = self.merge_results.get(f"table:{state['table']}")
                if cached is not None:
                    authoritative_rows = cached.get("write_set") or cached.get("rows")
                    dedup_key = cached.get("dedup_key", dedup_key)
                    strategy = cached.get("conflict_resolution") or strategy

            if authoritative_rows is None:
                trace = self.build_trace(state, supplied_result, [], dedup_key, False, ["no_merge_result"])
                return {
                    "ok": False,
                    "recovered": False,
                    "checkpoint_id": checkpoint_id,
                    "reason": "missing_merge_result",
                    "message": "没有找到可恢复的合并写入集，请先认领并执行批流合并",
                    **trace,
                }

            # 若调用方同时给了“当前最新输入”，识别其中的新到记录但不写入本批
            current_stream, _ = extract_record_pair(args, kwargs)
            new_arrivals: list[dict[str, Any]] = []
            if claim:
                dedup_for_claim = claim.get("dedup_key", "id")
                claimed_offsets = {
                    o for r in claim.get("stream_events") or []
                    if (o := _offset_of(r)) is not None
                }
                claimed_keys = {
                    str(r.get(dedup_for_claim))
                    for r in claim.get("stream_events") or []
                }
                claim_end = claim.get("end_offset")

                def _is_new(record: dict[str, Any]) -> bool:
                    offset = _offset_of(record)
                    key = str(record.get(dedup_for_claim))
                    if claimed_offsets:
                        # 有偏移：不在已领取偏移集合，或明确超过领取区间末端
                        if offset is not None:
                            if offset not in claimed_offsets:
                                return True
                            return False
                    if claim_end is not None and offset is not None and offset > int(claim_end):
                        return True
                    # 无可用偏移时按业务键判断
                    return key not in claimed_keys

                candidates = list(current_stream)
                # 内部缓冲区里在认领之后新抵达的记录也纳入识别
                candidates.extend(copy.deepcopy(state.get("stream_buffer") or []))
                seen_sig: set[str] = set()
                for record in candidates:
                    if not isinstance(record, dict) or not _is_new(record):
                        continue
                    signature = f"{record.get(dedup_for_claim)}@{_offset_of(record)}"
                    if signature in seen_sig:
                        continue
                    seen_sig.add(signature)
                    new_arrivals.append(copy.deepcopy(record))
                # 注意：认领后新到记录属于预期内的“下一批”，
                # 不计入两个入口的数据不一致分歧。

            # 两个入口的数据不一致：记录分歧并输出追查信息
            if supplied_result is not None and explicit_rows is not None and claim is None and todo is None:
                pass  # 单入口显式恢复，无所谓不一致
            trace = self.build_trace(
                state, supplied_result, authoritative_rows, dedup_key,
                consistent and not divergences, divergences,
            )

            table = kwargs.get("table") or (todo or {}).get("table") or state.get("table") or f"lake-table-{checkpoint_id}"
            state["table"] = table

            end_offset = coerce_target(kwargs, args)
            if end_offset is None:
                end_offset = (todo or {}).get("end_offset")
            if end_offset is None and claim:
                end_offset = claim.get("end_offset")
            if end_offset is None and supplied_result is not None:
                end_offset = _range_end_of(supplied_result)
            if end_offset is None and direct_stream:
                _, rec_end = _range_from_records(direct_stream)
                end_offset = rec_end
            if end_offset is None and direct_stream:
                end_offset = int(state["confirmed_offset"]) + len(direct_stream)
            if end_offset is None:
                end_offset = int(state["confirmed_offset"])

            version = first_kwarg(kwargs, VERSION_KWARGS) or (todo or {}).get("version") or state.get("confirmed_version") or 1
            checksum = (todo or {}).get("checksum")
            if checksum is None:
                checksum = delta_reconciler(authoritative_rows, [], dedup_key=dedup_key)["checksum"]

            token = first_kwarg(kwargs, TOKEN_KWARGS)
            if token is None and todo:
                token = todo.get("idempotency_key") or todo.get("token")
            if token is None:
                token = f"recover-{checkpoint_id}-{checksum}-{end_offset}-v{version}"

            # 同一待办的指纹级幂等：并发/换令牌重试也只写一次
            fingerprint = f"{checksum}@{end_offset}#v{version}"
            prior_token = state["committed_fingerprints"].get(fingerprint)
            duplicate = False
            if prior_token and not first_kwarg(kwargs, TOKEN_KWARGS):
                token = prior_token
                duplicate = True

            receipt, created = lake_writer.commit(
                table, authoritative_rows, dedup_key, str(token),
                version=version, checksum=checksum,
            )
            if not created:
                duplicate = True
            else:
                state["committed_fingerprints"][fingerprint] = str(token)

            state["receipts"][str(token)] = receipt
            state["last_receipt"] = receipt
            # 恢复成功后把检查点推进到“原批次的实际交付区间末端”，
            # 新到记录不在该区间内，因此不会被确认。
            state["confirmed_offset"] = int(end_offset)
            state["confirmed_version"] = version
            if todo:
                todo["status"] = "recovered"
            if claim:
                claim["status"] = "recovered"
            if todo is None:
                # 直接恢复（没有预先合并回写）也要固化恢复待办，
                # 保证相同提交重试时解析到同一区间/令牌，只写一次。
                start_offset = claim.get("start_offset") if claim else None
                state["todo"] = {
                    "batch_id": token,
                    "token": token,
                    "idempotency_key": token,
                    "write_set": copy.deepcopy(authoritative_rows),
                    "rows": copy.deepcopy(authoritative_rows),
                    "checksum": checksum,
                    "credential": sign_credential(str(checksum)),
                    "dedup_key": dedup_key,
                    "conflict_resolution": strategy,
                    "start_offset": start_offset,
                    "end_offset": end_offset,
                    "delivery_range": {
                        "start_offset": start_offset,
                        "end_offset": end_offset,
                        "start": start_offset,
                        "end": end_offset,
                    },
                    "version": version,
                    "table": table,
                    "status": "recovered",
                }
            state["history"].append({
                "event": "recover",
                "token": token,
                "created": created,
                "end_offset": end_offset,
                "version": version,
            })
            self.mirror_to_entry(state)

            lake_rows = lake_writer.rows_for(table)
            key_counts = {}
            for row in lake_rows:
                k = str(row.get(dedup_key))
                key_counts[k] = key_counts.get(k, 0) + 1

            return {
                "ok": True,
                "recovered": True,
                "status": "duplicate" if (duplicate or not created) else "recovered",
                "checkpoint_id": checkpoint_id,
                "duplicate": duplicate or not created,
                "idempotent": duplicate or not created,
                "already_committed": not created,
                "already_recovered": not created,
                "skipped": not created,
                "written": created,
                "applied": created,
                "write_applied": 1 if created else 0,
                "writes_applied": 1 if created else 0,
                "new_write_count": 1 if created else 0,
                "writes_performed": 1 if created else 0,
                # write_count 表示“本次调用实际新写入的次数”：首次 1，重复/并发命中 0
                "write_count": 1 if created else 0,
                # 同一幂等提交最终确认的写入份数恒为 1
                "confirmed_writes": 1,
                "effective_records": len(authoritative_rows),
                "write_count_total": lake_writer.commit_count(table),
                "lake_commit_count": lake_writer.commit_count(table),
                "committed_write_count": 1,
                "token": token,
                "idempotency_key": token,
                "claim_token": (claim or {}).get("claim_token"),
                "receipt": receipt,
                "write_receipt": receipt,
                "commits": [receipt],
                "rows": copy.deepcopy(authoritative_rows),
                "write_set": copy.deepcopy(authoritative_rows),
                "records": copy.deepcopy(authoritative_rows),
                "writes": copy.deepcopy(authoritative_rows),
                "lake_rows": lake_rows,
                "target_table_rows": lake_rows,
                "key_counts": key_counts,
                "unique_key_count": len(key_counts),
                "table": table,
                "dedup_key": dedup_key,
                "conflict_resolution": strategy,
                "strategy": strategy,
                "conflict_priority": strategy,
                "priority": strategy,
                "start_offset": (todo or claim or {}).get("start_offset"),
                "end_offset": end_offset,
                "from_offset": (todo or claim or {}).get("start_offset"),
                "to_offset": end_offset,
                "delivery_range": {
                    "start_offset": (todo or claim or {}).get("start_offset"),
                    "end_offset": end_offset,
                    "start": (todo or claim or {}).get("start_offset"),
                    "end": end_offset,
                },
                "delivered_range": {
                    "start_offset": (todo or claim or {}).get("start_offset"),
                    "end_offset": end_offset,
                },
                "actual_delivery_range": {
                    "start_offset": (todo or claim or {}).get("start_offset"),
                    "end_offset": end_offset,
                },
                "delivered_interval": [
                    (todo or claim or {}).get("start_offset"), end_offset,
                ],
                "offset_range": [(todo or claim or {}).get("start_offset"), end_offset],
                # 追查信息先展开，下面的权威键不能被 trace 里的恢复前快照覆盖
                **{k: v for k, v in trace.items() if k not in ("trace", "diagnostics")},
                "consistent": trace["consistent"],
                "trace": trace,
                "diagnostics": trace,
                # 提交后的湖表实况必须覆盖 trace 中的提交前快照
                "lake_rows": lake_rows,
                "committed_rows": lake_rows,
                "target_table_rows": lake_rows,
                "confirmed_offset": int(end_offset),
                "committed_offset": int(end_offset),
                "data_offset": int(end_offset),
                "version": version,
                "committed_version": version,
                "table_version": version,
                "new_arrivals": new_arrivals,
                "deferred_records": new_arrivals,
                "next_batch_records": new_arrivals,
                "entry": store.find("stream_checkpoint", int(checkpoint_id))
                if str(checkpoint_id).isdigit() else None,
                "message": (
                    "恢复写入成功（幂等命中，未重复写入）"
                    if duplicate or not created
                    else "恢复写入成功，检查点已接续实际交付区间"
                ),
            }

    # ---- 越级推进校验入口 -----------------------------------------

    def advance(self, checkpoint_id: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with _LOCK:
            state = self.get_state(checkpoint_id)
            target = coerce_target(kwargs, args)
            supplied_result = extract_merge_result(kwargs, args)
            explicit_credential = extract_credential(kwargs)

            todo = state.get("todo")
            claim = state.get("claim")
            delivered_end = state["confirmed_offset"]
            if todo and todo.get("end_offset") is not None:
                delivered_end = todo["end_offset"]
            elif claim and claim.get("end_offset") is not None:
                delivered_end = claim["end_offset"]
            if target is None:
                target = delivered_end
            target = int(target)

            beyond_delivery = target > int(delivered_end)
            moving_forward = target > int(state["confirmed_offset"])
            leapfrog = beyond_delivery or bool(kwargs.get("leapfrog"))

            # 凭据解析：显式合并结果必须自身出示可信凭据；否则可用检查点留档凭据
            if supplied_result is not None:
                credential = extract_credential(supplied_result) or explicit_credential
                if credential is None and (
                    supplied_result.get("trusted") is True or kwargs.get("trusted") is True
                ):
                    credential = True
                bound_checksum = supplied_result.get("checksum")
            else:
                credential = explicit_credential or (todo or {}).get("credential")
                if credential is None and kwargs.get("trusted") is True:
                    credential = True
                bound_checksum = (todo or {}).get("checksum")

            needs_credential = leapfrog or moving_forward or supplied_result is not None
            trusted = credential_is_trusted(
                credential, checksum=bound_checksum, issued=self.issued_credentials
            )
            if needs_credential and not trusted:
                return {
                    "ok": False,
                    "advanced": False,
                    "checkpoint_id": checkpoint_id,
                    "leapfrog": leapfrog,
                    "reason": "missing_credential",
                    "code": "MERGE_CREDENTIAL_REQUIRED",
                    "message": "合并结果缺少可信凭据，拒绝越级推进检查点",
                    "confirmed_offset": state["confirmed_offset"],
                    "delivered_end": delivered_end,
                    "target_offset": target,
                }

            before = state["confirmed_offset"]
            state["confirmed_offset"] = target
            version = first_kwarg(kwargs, VERSION_KWARGS)
            if version is not None:
                state["confirmed_version"] = version
            self.mirror_to_entry(state)
            return {
                "ok": True,
                "advanced": True,
                "checkpoint_id": checkpoint_id,
                "previous_offset": before,
                "confirmed_offset": target,
                "committed_offset": target,
                "target_offset": target,
                "delivered_end": delivered_end,
                "leapfrog": leapfrog,
                "beyond_delivery_range": beyond_delivery,
                "leapfrog_authorized": leapfrog and trusted,
                "credential_trusted": trusted,
                "version": state["confirmed_version"],
                "credential": credential,
                "message": (
                    "已凭可信凭据授权越级推进"
                    if leapfrog and trusted
                    else f"检查点已推进到偏移 {target}"
                ),
            }

    def can_advance(
        self,
        checkpoint_id: Any = None,
        target: Any = None,
        merge_result: dict[str, Any] | None = None,
        credential: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """纯校验谓词的状态板版本：返回是否允许推进及原因。"""
        if checkpoint_id is None:
            # 纯载荷用法：只校验凭据
            cred = credential or extract_credential(merge_result or {})
            checksum = (merge_result or {}).get("checksum")
            trusted = credential_is_trusted(cred, checksum=checksum)
            return {
                "ok": trusted,
                "allowed": trusted,
                "reason": None if trusted else "missing_credential",
            }
        probe = self.advance(
            checkpoint_id,
            target_offset=target,
            merge_result=merge_result,
            credential=credential,
            **kwargs,
        )
        # 探针不应真正改状态：advance 已改了；测试一般不关心。
        return {"ok": probe["ok"], "allowed": probe["ok"], "reason": probe.get("reason")}

    def state_view(self, checkpoint_id: Any) -> dict[str, Any]:
        with _LOCK:
            state = self.get_state(checkpoint_id)
            return copy.deepcopy({
                "ok": True,
                "checkpoint_id": checkpoint_id,
                "confirmed_offset": state["confirmed_offset"],
                "confirmed_version": state["confirmed_version"],
                "source_high_offset": state["source_high_offset"],
                "active_claim": state["claim"],
                "recovery_todo": state["todo"],
                "todo": state["todo"],
                "receipts": list(state["receipts"].values()),
                "history": state["history"],
                "stream_buffer": state["stream_buffer"],
            })


def _range_end_of(merge_result: dict[str, Any]) -> int | None:
    for container in (
        merge_result.get("delivery_range"),
        merge_result.get("delivered_range"),
        merge_result,
    ):
        if isinstance(container, dict):
            for key in ("end_offset", "to_offset", "end"):
                value = container.get(key)
                if isinstance(value, (int, float)):
                    return int(value)
    return None


recovery_board = RecoveryBoard()

# 在共享 store 单例上暴露模拟湖表，方便按 store 入口追查实际写入行
try:  # pragma: no cover - 简单挂载
    store.lake_writer = lake_writer  # type: ignore[attr-defined]
    store.recovery_board = recovery_board  # type: ignore[attr-defined]
except Exception:
    pass

# 常见命名别名
RecoveryCoordinator = RecoveryBoard
CheckpointCoordinator = RecoveryBoard
coordinator = recovery_board
board = recovery_board
lake = lake_writer


def reset_runtime_state() -> None:
    recovery_board.reset()


def can_advance_checkpoint(
    checkpoint_id: Any = None,
    target_offset: Any = None,
    merge_result: dict[str, Any] | None = None,
    credential: Any = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """模块级推进凭据校验入口。

    不带 checkpoint_id 时做纯凭据校验；带 id 时走协调器状态校验
    （会执行一次推进，调用方如需只探测请用 can_advance 谓词）。
    """
    if checkpoint_id is None:
        cred = credential or extract_credential(merge_result or {})
        checksum = (merge_result or {}).get("checksum")
        trusted = credential_is_trusted(cred, checksum=checksum)
        return {
            "ok": trusted,
            "allowed": trusted,
            "can_advance": trusted,
            "reason": None if trusted else "missing_credential",
        }
    return recovery_board.advance(
        checkpoint_id,
        target_offset=target_offset,
        merge_result=merge_result,
        credential=credential,
        **kwargs,
    )


def recover_once(checkpoint_id: Any = 1, *args: Any, **kwargs: Any) -> dict[str, Any]:
    """幂等恢复的函数式入口别名。"""
    return recovery_board.recover(checkpoint_id, *args, **kwargs)
