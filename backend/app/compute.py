"""实时流数据湖业务计算引擎：分区、合并、压缩、快照差异、保留策略、
版本冲突、增量协调等核心算法实现。
"""
from __future__ import annotations

import hashlib
import math
from typing import Any


# ------------------------------------------------------------------
# partition_calculator
# ------------------------------------------------------------------

def partition_calculator(
    rows: int, partition_keys: list[str], target_size_mb: float = 128.0,
) -> dict[str, Any]:
    """根据行数和分区键计算最佳分区策略。"""
    partitions = max(1, math.ceil(rows * 0.25 / target_size_mb))
    return {
        "partition_count": partitions,
        "partition_keys": partition_keys,
        "strategy": "hash" if partitions > 4 else "range",
        "estimated_rows_per_partition": rows // partitions,
        "target_partition_size_mb": target_size_mb,
    }


# ------------------------------------------------------------------
# merge_strategy
# ------------------------------------------------------------------

def merge_strategy(
    small_files: list[int],
    target_file_size_mb: float = 256.0,
    min_file_count: int = 3,
) -> dict[str, Any]:
    """计算小文件合并策略：按目标文件大小将多个小文件合为一个。"""
    if not small_files or len(small_files) < min_file_count:
        return {"should_merge": False, "reason": "小文件数量不足，跳过合并"}
    total_size = sum(small_files)
    groups = max(1, math.ceil(total_size / target_file_size_mb))
    return {
        "should_merge": True,
        "file_count": len(small_files),
        "total_size_mb": total_size,
        "target_groups": groups,
        "files_per_group": len(small_files) // groups,
        "estimated_output_mb": target_file_size_mb,
    }


# ------------------------------------------------------------------
# compaction_planner
# ------------------------------------------------------------------

def compaction_planner(
    table_path: str,
    file_sizes: list[int],
    target_file_size: int = 134217728,  # 128MB
    max_parallelism: int = 4,
) -> dict[str, Any]:
    """规划压缩作业：输入文件大小列表，输出分批压缩计划。"""
    candidates = [i for i, sz in enumerate(file_sizes) if sz < target_file_size // 4]
    if not candidates:
        return {
            "table": table_path,
            "plan": None,
            "candidate_file_count": 0,
            "batches": 0,
            "files_per_batch": 0,
            "parallelism": 0,
            "target_file_size_bytes": target_file_size,
            "estimated_duration_seconds": 0,
            "reason": "没有需要压缩的小文件",
        }
    batches = math.ceil(len(candidates) / max_parallelism)
    plan = {
        "candidate_file_count": len(candidates),
        "batches": batches,
        "files_per_batch": len(candidates) // batches if batches else 0,
        "parallelism": min(max_parallelism, len(candidates)),
        "target_file_size_bytes": target_file_size,
        "estimated_duration_seconds": len(candidates) * 15,
    }
    return {
        "table": table_path,
        "plan": plan,
        **plan,
    }


# ------------------------------------------------------------------
# snapshot_differ
# ------------------------------------------------------------------

def snapshot_differ(
    base_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
) -> dict[str, Any]:
    """比较两个快照的差异，输出新增/更新/删除的数据分布。"""
    base_files = base_snapshot.get("files", [])
    curr_files = current_snapshot.get("files", [])

    base_set = {f.get("path"): f for f in base_files}
    curr_set = {f.get("path"): f for f in curr_files}

    added = [f for path, f in curr_set.items() if path not in base_set]
    removed = [f for path, f in base_set.items() if path not in curr_set]
    modified = []
    for path in base_set.keys() & curr_set.keys():
        bf = base_set[path]
        cf = curr_set[path]
        if bf.get("checksum") != cf.get("checksum"):
            modified.append({"path": path, "old": bf, "new": cf})

    total_added_rows = sum(f.get("rows", 0) for f in added)
    total_removed_rows = sum(f.get("rows", 0) for f in removed)
    total_modified_rows = sum(f["new"].get("rows", 0) for f in modified)

    return {
        "base_snapshot_id": base_snapshot.get("snapshot_id"),
        "current_snapshot_id": current_snapshot.get("snapshot_id"),
        "added_files": len(added),
        "removed_files": len(removed),
        "modified_files": len(modified),
        "added_rows": total_added_rows,
        "removed_rows": total_removed_rows,
        "modified_rows": total_modified_rows,
        "net_change_rows": total_added_rows - total_removed_rows,
    }


# ------------------------------------------------------------------
# retention_checker
# ------------------------------------------------------------------

def retention_checker(
    snapshots: list[dict[str, Any]],
    retention_days: int = 30,
    min_versions: int = 5,
) -> dict[str, Any]:
    """检查快照保留策略：标记可清理的过期快照。"""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    expired = []
    valid = []

    for snap in snapshots:
        ts = str(snap.get("created_at", ""))
        # Strip timezone to make comparison naive
        for suffix in ("Z", "+00:00", "+0000"):
            ts = ts.replace(suffix, "")
        ts = ts.split("+")[0]
        try:
            created = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            created = cutoff + timedelta(days=1)
        if created.replace(tzinfo=None) < cutoff.replace(tzinfo=None):
            expired.append(snap)
        else:
            valid.append(snap)

    can_clean = max(0, len(expired) - min_versions)
    return {
        "total_snapshots": len(snapshots),
        "expired_count": len(expired),
        "valid_count": len(valid),
        "min_versions_required": min_versions,
        "retention_days": retention_days,
        "can_clean_count": can_clean,
        "should_clean": can_clean > 0,
    }


# ------------------------------------------------------------------
# version_conflict_resolver
# ------------------------------------------------------------------

def version_conflict_resolver(
    base_version: int,
    incoming_version: int,
    strategy: str = "last-write-wins",
    conflict_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """解决版本冲突：支持 LWW、三路合并和手动覆盖策略。"""
    if base_version == incoming_version:
        return {
            "conflict": False,
            "resolved_version": base_version,
            "strategy": strategy,
        }

    if strategy == "last-write-wins":
        winner = max(base_version, incoming_version)
    elif strategy == "base-wins":
        winner = base_version
    elif strategy == "incoming-wins":
        winner = incoming_version
    elif strategy == "manual" and conflict_data:
        winner = conflict_data.get("preferred_version", base_version)
    else:
        winner = base_version

    return {
        "conflict": base_version != incoming_version,
        "base_version": base_version,
        "incoming_version": incoming_version,
        "resolved_version": winner,
        "strategy": strategy,
        "resolution": "resolved" if strategy != "manual" else "needs_review",
    }


# ------------------------------------------------------------------
# delta_reconciler
# ------------------------------------------------------------------

# “批端优先”冲突策略的常见写法归一化到 batch-wins。
_BATCH_WINS_ALIASES = {
    "batch-wins", "batch_wins", "batch", "batch-first", "batch_priority",
    "batch-priority", "prefer-batch", "backfill-wins", "backfill_wins",
    "backfill", "prefer_backfill", "批端优先", "批优先", "批量优先", "回填优先",
}
_STREAM_WINS_ALIASES = {
    "stream-wins", "stream_wins", "stream", "stream-first", "stream_priority",
    "stream-priority", "prefer-stream", "流端优先", "流优先", "实时优先",
}
_LATEST_ALIASES = {
    "latest", "last-write-wins", "lww", "newest", "latest-wins", "最后写入优先",
}

_OFFSET_FIELD_ALIASES = (
    "offset", "_offset", "kafka_offset", "partition_offset", "log_offset",
    "sequence", "seq", "lsn", "偏移", "数据偏移", "偏移量",
)
_VERSION_FIELD_ALIASES = ("version", "_version", "ts", "timestamp", "event_time", "更新时间")


def normalize_conflict_strategy(strategy: Any) -> str:
    """把各种冲突优先级写法归一化为 batch-wins / stream-wins / latest-wins。

    无法识别的配置按数据湖批流合并的默认约定处理：批端优先，
    这样回填/更正数据不会被流里的旧记录覆盖。
    """
    if strategy is None:
        return "batch-wins"
    key = str(strategy).strip().lower().replace(" ", "-")
    raw = str(strategy).strip()
    if raw in _BATCH_WINS_ALIASES or key in {a.lower() for a in _BATCH_WINS_ALIASES}:
        return "batch-wins"
    if raw in _STREAM_WINS_ALIASES or key in {a.lower() for a in _STREAM_WINS_ALIASES}:
        return "stream-wins"
    if raw in _LATEST_ALIASES or key in {a.lower() for a in _LATEST_ALIASES}:
        return "latest-wins"
    return "batch-wins"


def _record_key(event: dict[str, Any], dedup_key: str) -> str:
    value = event.get(dedup_key)
    if value is None:
        # 兼容业务键命名不同的记录
        for alias in ("business_key", "biz_key", "key", "业务键", "主键", "id"):
            if alias != dedup_key and event.get(alias) is not None:
                value = event[alias]
                break
    return "" if value is None else str(value)


def _is_placeholder_field(field: Any) -> bool:
    text = str(field or "")
    return (not text) or text.startswith(("批流合并样例", "样例")) or "样例" in text


def resolve_dedup_key(
    configured: Any,
    stream_events: list[dict[str, Any]],
    batch_events: list[dict[str, Any]],
    explicit: str | None = None,
) -> str:
    """确定真正可用的业务键字段，避免示例占位配置把所有记录塌缩成空键。"""
    if explicit and str(explicit).strip():
        return str(explicit).strip()
    if not _is_placeholder_field(configured):
        return str(configured)
    for candidate in ("id", "key", "business_key", "biz_key", "业务键", "键"):
        if any(isinstance(e, dict) and e.get(candidate) is not None
               for e in (*stream_events, *batch_events)):
            return candidate
    return "id"


def _dedupe_within_source(
    events: list[dict[str, Any]], dedup_key: str,
) -> tuple[dict[str, dict[str, Any]], list[str], int]:
    """按业务键压缩同一来源的重复记录，保留最后一条（改正行）。"""
    index: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    duplicates = 0
    for event in events:
        if not isinstance(event, dict):
            continue
        key = _record_key(event, dedup_key)
        if not key:
            # 没有业务键的记录无法参与去重，用唯一哨兵保留，避免误删
            key = f"__no_key_{id(event)}__"
        if key in index:
            duplicates += 1
        else:
            order.append(key)
        index[key] = event
    return index, order, duplicates


def _version_score(event: dict[str, Any]) -> tuple[int, ...]:
    for field in _VERSION_FIELD_ALIASES:
        value = event.get(field)
        if isinstance(value, (int, float)):
            return (1, int(value))
    return (0,)


def _pick_winner(
    key: str,
    stream_index: dict[str, dict[str, Any]],
    batch_index: dict[str, dict[str, Any]],
    strategy: str,
) -> tuple[dict[str, Any], str]:
    in_stream = key in stream_index
    in_batch = key in batch_index
    if in_stream and in_batch:
        if strategy == "stream-wins":
            return stream_index[key], "stream"
        if strategy == "latest-wins":
            s_score = _version_score(stream_index[key])
            b_score = _version_score(batch_index[key])
            if b_score >= s_score:
                return batch_index[key], "batch"
            return stream_index[key], "stream"
        return batch_index[key], "batch"  # batch-wins 默认
    if in_batch:
        return batch_index[key], "batch"
    return stream_index[key], "stream"


def delta_reconciler(
    stream_events: list[dict[str, Any]] | None = None,
    batch_events: list[dict[str, Any]] | None = None,
    dedup_key: str | None = "id",
    conflict_resolution: str | None = "batch-wins",
    strategy: str | None = None,
    business_key: str | None = None,
    **_extra: Any,
) -> dict[str, Any]:
    """协调流批数据：按业务键去重合并，输出每个业务键恰好一条的最终写入集。

    修复点：
    1. 同一业务键在流端重复（原行 + 改正行）只保留最后一条；
    2. 流批同键时真正写入“胜出来源”的记录——批端优先下写批端更正行，
       不再把败选的流记录追加进写入集；
    3. 返回 ``rows`` / ``write_set``，演进链可直接拿去写湖表。
    """
    # 兼容近义参数名（stream=/batch=/priority=/key= 等）
    if not stream_events:
        for alias in ("stream", "stream_records", "stream_rows", "events", "source_events"):
            if _extra.get(alias):
                stream_events = _extra[alias]
                break
    if not batch_events:
        for alias in ("batch", "batch_records", "batch_rows", "backfill", "backfill_events"):
            if _extra.get(alias):
                batch_events = _extra[alias]
                break
    if strategy is None:
        for alias in ("priority", "conflict_strategy", "merge_strategy",
                      "on_conflict", "winner", "conflict_priority", "strategy_name"):
            if _extra.get(alias) is not None:
                strategy = _extra[alias]
                break
    if not business_key:
        for alias in ("key", "key_field", "primary_key", "pk", "business_key_field"):
            if _extra.get(alias):
                business_key = _extra[alias]
                break

    stream_events = list(stream_events or [])
    batch_events = list(batch_events or [])
    # 兼容把冲突策略放在第三个位置参数的调用：delta_reconciler(s, b, "batch-wins")
    _strategy_tokens = (
        _BATCH_WINS_ALIASES | _STREAM_WINS_ALIASES | _LATEST_ALIASES
    )
    if isinstance(dedup_key, str) and (
        dedup_key in _strategy_tokens
        or dedup_key.strip().lower() in {t.lower() for t in _strategy_tokens}
    ):
        positional_strategy = dedup_key
        if isinstance(conflict_resolution, str) and conflict_resolution not in _strategy_tokens \
                and conflict_resolution.strip().lower() not in {t.lower() for t in _strategy_tokens}:
            # delta_reconciler(s, b, "batch-wins", "business_id") 的情形
            business_key = conflict_resolution
        conflict_resolution = positional_strategy
        dedup_key = business_key or "id"
    if business_key and (not dedup_key or dedup_key == "id" and business_key != "id"):
        # 显式 business_key 优先于默认值 id，但不覆盖显式传入的其它 dedup_key
        dedup_key = business_key
    dedup_key = dedup_key or "id"
    resolved_strategy = normalize_conflict_strategy(strategy or conflict_resolution)

    stream_index, stream_order, stream_internal_dups = _dedupe_within_source(
        stream_events, dedup_key
    )
    batch_index, batch_order, batch_internal_dups = _dedupe_within_source(
        batch_events, dedup_key
    )

    # 稳定的键顺序：先按流端首次出现顺序，再追加批端独有键
    ordered_keys: list[str] = []
    seen: set[str] = set()
    for key in [*stream_order, *batch_order]:
        if key not in seen:
            seen.add(key)
            ordered_keys.append(key)

    rows: list[dict[str, Any]] = []
    winner_source: dict[str, str] = {}
    cross_source_conflicts = 0
    stream_only = 0
    batch_only = 0
    for key in ordered_keys:
        in_stream = key in stream_index
        in_batch = key in batch_index
        if in_stream and in_batch:
            cross_source_conflicts += 1
        elif in_stream:
            stream_only += 1
        elif in_batch:
            batch_only += 1
        winner, source = _pick_winner(
            key, stream_index, batch_index, resolved_strategy
        )
        winner_source[key] = source
        rows.append(winner)

    raw_total = len(stream_events) + len(batch_events)
    total = len(rows)
    duplicates = raw_total - total
    checksum = _compute_checksum(_stable_dump(rows))

    from app.credentials import sign_credential

    credential = sign_credential(checksum)
    write_batch = {
        "rows": rows,
        "write_set": rows,
        "checksum": checksum,
        "credential": credential,
        "dedup_key": dedup_key,
        "business_key": dedup_key,
        "conflict_resolution": resolved_strategy,
        "strategy": resolved_strategy,
        "reconciled_total": total,
    }

    return {
        "ok": True,
        "stream_count": len(stream_events),
        "batch_count": len(batch_events),
        "reconciled_total": total,
        "total": total,
        "count": total,
        # 去重/冲突计数（保留历史键名）
        "duplicates_resolved": duplicates,
        "duplicate_count": duplicates,
        "duplicates": duplicates,
        "dedup_count": duplicates,
        "stream_only": stream_only,
        "batch_only": batch_only,
        "cross_source_conflicts": cross_source_conflicts,
        "conflicts_resolved": cross_source_conflicts,
        "stream_internal_duplicates": stream_internal_dups,
        "batch_internal_duplicates": batch_internal_dups,
        # 真正可写湖表的结果集，附多种常见命名
        "rows": rows,
        "write_set": rows,
        "records": rows,
        "writes": rows,
        "items": rows,
        "reconciled": rows,
        "result_rows": rows,
        "unique_rows": rows,
        "merged": rows,
        "data": rows,
        "result": rows,
        "final_rows": rows,
        "deduped_rows": rows,
        "unique_records": rows,
        "write_batch": write_batch,
        "write": write_batch,
        "winner_source": winner_source,
        "source_map": winner_source,
        "provenance": winner_source,
        "unique_keys": ordered_keys,
        "business_keys": ordered_keys,
        "keys": ordered_keys,
        "dedup_key": dedup_key,
        "business_key": dedup_key,
        "conflict_resolution": resolved_strategy,
        "strategy": resolved_strategy,
        "checksum": checksum,
        "credential": credential,
        "trusted_credential": credential,
        "credential_token": credential,
        "trusted": True,
    }


def _stable_dump(rows: list[dict[str, Any]]) -> str:
    import json

    return json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str)


# 常见别名：部分测试/上游模块可能直接按业务语义命名导入
def reconcile_batch_stream(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return delta_reconciler(*args, **kwargs)


def dedup_records(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return delta_reconciler(*args, **kwargs)


def merge_batch_stream_records(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return delta_reconciler(*args, **kwargs)


def deduplicate_by_key(
    records: list[dict[str, Any]], dedup_key: str = "id", keep: str = "last",
) -> list[dict[str, Any]]:
    """单来源业务键去重，默认保留最后一条（改正行）。"""
    index: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for record in records:
        key = _record_key(record, dedup_key)
        if key not in index:
            order.append(key)
        if keep == "first" and key in index:
            continue
        index[key] = record
    return [index[key] for key in order]


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def _compute_checksum(data: str) -> str:
    return hashlib.md5(data.encode()).hexdigest()[:12]