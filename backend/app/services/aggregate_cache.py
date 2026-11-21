"""聚合缓存领域服务：缓存预热、命中统计、淘汰策略与跨模块缓存一致性管理。

聚合缓存同时承担“批次账本”的职责：每次立方构建/预热都会写入一个批次，
并显式记录完整性。指标工厂只消费完整批次，结果里保留批次号以便追溯。
"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.concurrent import run_once
from app.services.cube_ref import (
    cube_ref_label,
    cube_ref_matches,
    parse_cube_id,
    result_summary_refs_cube,
)
from app.store import store

# 批次里边可以明确表达“完整/不完整”的字段
_COMPLETE_TRUE = {"完整", "齐全", "已完成", "已齐全", "完整批次", "complete", "completed", "full", "ready", "ok", "true", "1"}
_COMPLETE_FALSE = {"部分", "半批", "不完整", "待补齐", "构建中", "partial", "incomplete", "building", "processing", "false", "0"}
# 明确不能再用于计算的缓存状态；草稿配置若已带明细仍按可读批次处理。
_INACTIVE_STATUS = {
    "淘汰中", "已停用", "已淘汰", "失效", "已失效", "已过期", "过期", "停用", "异常",
    "failed", "error", "evicted", "expired", "disabled",
}


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().casefold() in {"1", "true", "yes", "y", "是", "完整", "齐全", "completed"}


def _first_int(entry: dict[str, Any], fields: tuple[str, ...]) -> int | None:
    for field in fields:
        value = entry.get(field)
        if value is None or isinstance(value, bool):
            continue
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            continue
    return None


def _parse_timestamp(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return time.mktime(time.strptime(text, fmt))
        except (ValueError, OverflowError):
            continue
    return 0.0


def batch_data(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """读取批次明细，兼容 cached_data / result_data / data 三种历史写法。"""
    for field in ("cached_data", "result_data", "data", "明细", "聚合明细"):
        value = entry.get(field)
        if isinstance(value, list):
            return value
    return []


def batch_identifier(entry: dict[str, Any]) -> str:
    """批次号；历史条目没有显式批次号时按缓存条目 id 派生，保证结果可追溯。"""
    for field in ("batch_id", "批次号", "批次", "batch", "batch_code"):
        value = entry.get(field)
        if value not in (None, ""):
            return str(value)
    try:
        return f"AGGR-{int(entry.get('id')):04d}"
    except (TypeError, ValueError):
        digest = hashlib.md5(str(batch_data(entry)).encode()).hexdigest()[:8]
        return f"AGGR-LEGACY-{digest}"


def batch_completeness(entry: dict[str, Any]) -> tuple[bool, str]:
    """判断批次是否完整可用，返回 (是否完整, 原因码)。"""
    data = batch_data(entry)
    if not data:
        return False, "NO_DATA"

    status = str(entry.get("status", "") or "").strip()
    if status and status.casefold() in _INACTIVE_STATUS:
        return False, "NOT_ACTIVE"

    for field in ("complete", "is_complete", "batch_complete", "已完整", "完整"):
        if field in entry and not _to_bool(entry.get(field)):
            return False, "MARKED_INCOMPLETE"

    for field in ("partial", "incomplete", "半批", "不完整"):
        if _to_bool(entry.get(field)):
            return False, "MARKED_PARTIAL"

    for field in ("完整性", "批次状态", "batch_status", "state"):
        text = str(entry.get(field, "") or "").strip()
        if not text:
            continue
        folded = text.casefold()
        if folded in _COMPLETE_TRUE:
            continue
        if folded in _COMPLETE_FALSE or any(
            marker in folded for marker in ("部分", "半批", "不完整", "partial", "incomplete", "building")
        ):
            return False, "MARKED_PARTIAL"

    expected = _first_int(entry, ("expected_rows", "预期行数", "总行数", "应到行数", "expected_count", "target_rows"))
    actual = _first_int(entry, ("row_count", "实际行数", "loaded_rows", "已到行数", "batch_rows"))
    if actual is None:
        actual = len(data)
    if expected is not None and actual < expected:
        return False, "PARTIAL_ROWS"

    expected_groups = _first_int(entry, ("expected_groups", "预期分组数", "total_groups", "分组总数"))
    if expected_groups is not None and len(data) < expected_groups:
        return False, "PARTIAL_GROUPS"

    return True, "COMPLETE"


def _split_ref(value: Any) -> list[str]:
    text = str(value or "")
    return [part.strip() for part in text.replace(";", ",").replace("|", ",").split(",") if part.strip()]


def _entry_dims(entry: dict[str, Any]) -> list[str]:
    for field in ("维度列表", "dims", "dimensions"):
        value = entry.get(field)
        if value:
            return [d.strip() for d in str(value).split(",") if d.strip()]
    return []


def _entry_measures(entry: dict[str, Any]) -> list[str]:
    for field in ("度量列表", "measures", "metrics"):
        value = entry.get(field)
        if value:
            return [m.strip() for m in str(value).split(",") if m.strip()]
    return []


def batch_group_matches(entry: dict[str, Any], dims: list[str] | None, measures: list[str] | None) -> bool:
    """批次声明的维度/度量必须与请求完全一致；历史批次未声明元数据时向后兼容。"""
    entry_dims = _entry_dims(entry)
    entry_measures = _entry_measures(entry)
    if entry_dims and dims and set(entry_dims) != set(dims):
        return False
    if entry_measures and measures and set(entry_measures) != set(measures):
        return False
    return True


def _batch_sort_key(entry: dict[str, Any]) -> tuple[float, int]:
    stamp = 0.0
    for field in ("published_at", "创建时间", "确认时间", "batch_time", "时间戳"):
        stamp = _parse_timestamp(entry.get(field))
        if stamp:
            break
    try:
        identifier = int(entry.get("id", 0))
    except (TypeError, ValueError):
        identifier = 0
    return stamp, identifier


def batch_summary(entry: dict[str, Any], *, reason: str | None = None) -> dict[str, Any]:
    data = batch_data(entry)
    complete, completeness = batch_completeness(entry)
    dims = _entry_dims(entry)
    measures = _entry_measures(entry)
    try:
        cube_id = parse_cube_id(entry.get("cube_id", entry.get("目标立方")))
    except Exception:  # pragma: no cover - parse_cube_id 不抛异常
        cube_id = None
    return {
        "batch_id": batch_identifier(entry),
        "批次号": batch_identifier(entry),
        "cache_id": entry.get("id"),
        "entry_id": entry.get("id"),
        "cube_id": cube_id,
        "目标立方": entry.get("目标立方") or (cube_ref_label(cube_id) if cube_id is not None else ""),
        "dimensions": dims,
        "measures": measures,
        "维度列表": ",".join(dims),
        "度量列表": ",".join(measures),
        "rows": data,
        "data": data,
        "row_count": len(data),
        "complete": complete,
        "completeness": completeness,
        "reason": reason or (None if complete else completeness),
        "created_at": entry.get("published_at") or entry.get("创建时间"),
        "status": entry.get("status"),
        "source": entry.get("source") or entry.get("来源"),
        "entry": entry,
    }


class AggregateCacheService(BaseModuleService):
    MODULE = "aggregate_cache"
    ENTITY = "缓存配置"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["配置编号", "目标立方", "缓存粒度", "TTL秒数", "预热策略", "命中阈值", "淘汰算法", "缓存大小"]
    REQUIRED_FIELDS = ["配置编号", "目标立方"]
    STATUS_ORDER = ["草稿", "生效中", "淘汰中", "已停用"]
    ACTION_RULES = {"启效缓存": "生效中", "强制淘汰": "淘汰中", "停用缓存": "已停用"}
    NEGATIVE_ACTIONS = ["强制淘汰", "停用缓存"]

    # ---- 批次账本 ----

    def batch_is_complete(self, entry: dict[str, Any]) -> bool:
        return batch_completeness(entry)[0]

    def list_complete_batches(
        self,
        cube_id: int | str,
        dims: list[str] | None = None,
        measures: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """返回指定立方可用于计算的完整批次（按时间/id 升序）。"""
        numeric_id = parse_cube_id(cube_id)
        if numeric_id is None:
            return []
        batches: list[dict[str, Any]] = []
        for entry in store.rows(self.MODULE):
            if not cube_ref_matches(entry.get("目标立方"), numeric_id) and not cube_ref_matches(
                entry.get("cube_id"), numeric_id
            ):
                continue
            if not batch_group_matches(entry, dims, measures):
                continue
            complete, reason = batch_completeness(entry)
            if not complete:
                continue
            batches.append(batch_summary(entry, reason=reason))
        batches.sort(key=lambda item: _batch_sort_key(item.get("entry") or {}))
        return batches

    # 兼容不同调用方的命名
    get_complete_batches = list_complete_batches
    complete_batches = list_complete_batches
    list_usable_batches = list_complete_batches

    def latest_complete_batch(
        self,
        cube_id: int | str,
        dims: list[str] | None = None,
        measures: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """取最近一个完整批次；不完整的新批次不会遮住更早的完整批次。"""
        batches = self.list_complete_batches(cube_id, dims, measures)
        return batches[-1] if batches else None

    get_latest_complete_batch = latest_complete_batch
    select_complete_batch = latest_complete_batch
    resolve_complete_batch = latest_complete_batch

    def incomplete_batches(
        self, cube_id: int | str, dims: list[str] | None = None, measures: list[str] | None = None
    ) -> list[dict[str, Any]]:
        numeric_id = parse_cube_id(cube_id)
        if numeric_id is None:
            return []
        result: list[dict[str, Any]] = []
        for entry in store.rows(self.MODULE):
            if not cube_ref_matches(entry.get("目标立方"), numeric_id):
                continue
            if not batch_group_matches(entry, dims, measures):
                continue
            complete, reason = batch_completeness(entry)
            if not complete:
                result.append(batch_summary(entry, reason=reason))
        return result

    def get_batch(
        self, batch_id: str, cube_id: int | str | None = None
    ) -> dict[str, Any] | None:
        """按批次号取批次；可选按立方完整标识限定。"""
        numeric_id = parse_cube_id(cube_id) if cube_id is not None else None
        for entry in store.rows(self.MODULE):
            if batch_identifier(entry) != str(batch_id):
                continue
            if numeric_id is not None and not cube_ref_matches(entry.get("目标立方"), numeric_id):
                continue
            return batch_summary(entry)
        return None

    find_batch = get_batch
    get_batch_by_id = get_batch

    def batch_is_complete_by_id(self, batch_id: str, cube_id: int | str | None = None) -> bool:
        batch = self.get_batch(batch_id, cube_id)
        return bool(batch and batch.get("complete"))

    def publish_batch(
        self,
        cube_id: int | str,
        dims: list[str],
        measures: list[str],
        data: list[dict[str, Any]],
        *,
        batch_id: str | None = None,
        complete: bool = True,
        expected_rows: int | None = None,
        source: str = "manual",
    ) -> dict[str, Any]:
        """登记一个聚合批次；``complete=False`` 用于半批/未齐批次。"""
        numeric_id = parse_cube_id(cube_id)
        if numeric_id is None:
            return {"ok": False, "message": "立方标识无效", "batch_id": None}
        rows = store.rows(self.MODULE)
        cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        identifier = batch_id or f"AGGR-C{numeric_id:04d}-B{cache_id:04d}"
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        entry = {
            "id": cache_id,
            "配置编号": f"AGGR-{identifier}",
            "目标立方": cube_ref_label(numeric_id),
            "cube_id": numeric_id,
            "缓存粒度": f"{len(dims)}维 x {len(measures)}度",
            "TTL秒数": "3600",
            "预热策略": "LAZY",
            "命中阈值": "0.8",
            "淘汰算法": "LRU",
            "缓存大小": str(len(str(data))),
            "status": "生效中" if complete else "部分生效",
            "pending": not complete,
            "abnormal": not complete,
            "cached_data": data,
            "batch_id": identifier,
            "批次号": identifier,
            "维度列表": ",".join(dims),
            "度量列表": ",".join(measures),
            "row_count": len(data),
            "expected_rows": expected_rows if expected_rows is not None else len(data),
            "complete": complete,
            "完整性": "完整" if complete else "部分",
            "published_at": now,
            "创建时间": now,
            "source": source,
        }
        rows.append(entry)
        return {"ok": True, **batch_summary(entry)}

    def check_cache_consistency(self, cube_id: int) -> dict[str, Any]:
        """检查缓存与源 olap_cube 数据的一致性，过期则淘汰。"""
        cube = store.find("olap_cube", cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        cache_rows = store.rows(self.MODULE)
        stale_count = 0

        for cache_entry in cache_rows:
            if cache_entry.get("status") != "生效中":
                continue
            # 必须按完整立方标识匹配，cube_id=1 不能命中 olap_cube:11
            if not cube_ref_matches(cache_entry.get("目标立方"), cube_id):
                continue

            # 检查缓存是否过期
            ttl = int(cache_entry.get("TTL秒数", "3600"))
            cube_rows = cube.get("数据行数", "0")
            cache_size = cache_entry.get("缓存大小", "0")

            # 如果立方行数变化超过20%，标记为过期
            try:
                cube_total = int(cube_rows) if cube_rows.isdigit() else 0
                cache_total = int(cache_size) if cache_size.isdigit() else 0
            except (ValueError, TypeError):
                cube_total = 0
                cache_total = 0

            if cube_total > 0 and abs(cube_total - cache_total) / cube_total > 0.2:
                cache_entry["status"] = "淘汰中"
                cache_entry["pending"] = True
                stale_count += 1

        return {
            "ok": True,
            "cube_id": cube_id,
            "stale_entries": stale_count,
            "cube_row_count": cube.get("数据行数"),
        }

    def prewarm_cube_cache(self, cube_id: int) -> dict[str, Any]:
        """预热立方缓存：读取 olap_cube 和 semantic_model，写入 aggregate_cache 和 result_cache。

        同一组维度/度量的并发预热只实际执行一次，其他调用复用同一份结果。
        """
        cube = store.find("olap_cube", cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        dims = [d.strip() for d in str(cube.get("维度列表", "id")).split(",") if d.strip()]
        meas = [m.strip() for m in str(cube.get("度量列表", "value")).split(",") if m.strip()]

        key = ("aggregate_prewarm", int(cube_id), tuple(dims), tuple(meas))
        result, deduplicated = run_once(
            key, lambda: self._prewarm_cube_cache(cube_id, dims, meas)
        )
        payload = dict(result)
        payload["deduplicated"] = deduplicated
        return payload

    def _prewarm_cube_cache(
        self, cube_id: int, dims: list[str], meas: list[str]
    ) -> dict[str, Any]:
        cube = store.find("olap_cube", cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        # 生成预热数据
        import random
        prewarmed = []
        for i in range(random.randint(20, 100)):
            row = {}
            for d in dims:
                row[d] = f"{d}_prewarm_{random.randint(1, 10)}"
            for m in meas:
                row[m] = random.randint(100, 50000)
            prewarmed.append(row)

        # 写入 aggregate_cache（完整批次）
        rows = store.rows(self.MODULE)
        cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        data_str = str(prewarmed)
        batch_id = f"AGGR-PW{cube_id:04d}-B{cache_id:04d}"
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        cache = {
            "id": cache_id,
            "配置编号": f"AGGR-PW{cube_id:04d}",
            "目标立方": cube_ref_label(cube_id),
            "cube_id": cube_id,
            "缓存粒度": f"{len(dims)}维 x {len(meas)}度",
            "TTL秒数": "7200",
            "预热策略": "EAGER",
            "命中阈值": "0.85",
            "淘汰算法": "LRU",
            "缓存大小": str(len(data_str)),
            "status": "生效中",
            "pending": True,
            "abnormal": False,
            "cached_data": prewarmed,
            "batch_id": batch_id,
            "批次号": batch_id,
            "维度列表": ",".join(dims),
            "度量列表": ",".join(meas),
            "row_count": len(prewarmed),
            "expected_rows": len(prewarmed),
            "complete": True,
            "完整性": "完整",
            "published_at": now,
            "创建时间": now,
            "source": "prewarm",
        }
        rows.append(cache)

        # 也写入 result_cache
        result_rows = store.rows("result_cache")
        result_id = max((int(r.get("id", 0)) for r in result_rows), default=0) + 1
        result = {
            "id": result_id,
            "条目编号": f"RESU-PW{cube_id:04d}",
            "查询摘要": f"prewarm_cube_{cube_id}",
            "结果哈希": hashlib.md5(data_str.encode()).hexdigest()[:12],
            "缓存命中": "0",
            "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 7200)),
            "存储大小": str(len(data_str)),
            "创建时间": now,
            "最后命中": now,
            "status": "有效",
            "pending": True,
            "abnormal": False,
            "cube_id": cube_id,
            "batch_id": batch_id,
            "批次号": batch_id,
            "result_data": prewarmed,
        }
        result_rows.append(result)

        return {
            "ok": True,
            "cube_id": cube_id,
            "batch_id": batch_id,
            "prewarmed_rows": len(prewarmed),
            "row_count": len(prewarmed),
            "cache_entry_id": cache_id,
            "result_cache_id": result_id,
            "deduplicated": False,
        }

    def evict_stale_cache(self, cube_id: int) -> dict[str, Any]:
        """淘汰与指定立方相关的过期 aggregate_cache 和 result_cache 条目。"""
        evicted = []

        # 淘汰 aggregate_cache
        agg_rows = store.rows(self.MODULE)
        for entry in agg_rows:
            if not cube_ref_matches(entry.get("目标立方"), cube_id):
                continue
            if entry.get("status") in ("生效中", "草稿"):
                entry["status"] = "淘汰中"
                entry["abnormal"] = True
                evicted.append({
                    "type": "aggregate_cache",
                    "id": entry["id"],
                    "配置编号": entry.get("配置编号"),
                    "batch_id": batch_identifier(entry),
                })

        # 淘汰 result_cache（只命中立方自身的构建/预热缓存，指标结果另行治理）
        result_rows = store.rows("result_cache")
        for entry in result_rows:
            if result_summary_refs_cube(entry.get("查询摘要"), cube_id) and entry.get("status") == "有效":
                entry["status"] = "已过期"
                evicted.append({
                    "type": "result_cache",
                    "id": entry["id"],
                    "条目编号": entry.get("条目编号"),
                })

        # 检查数据新鲜度并通知
        _check_freshness_after_eviction(cube_id, len(evicted))

        # 写入审计
        _audit_cache_eviction(cube_id, len(evicted))

        return {
            "ok": True,
            "cube_id": cube_id,
            "evicted_count": len(evicted),
            "entries": evicted[:10],
        }


def _check_freshness_after_eviction(cube_id: int, evicted_count: int) -> None:
    fresh_rows = store.rows("freshness_mon")
    for fr in fresh_rows:
        cube_ref = fr.get("目标数据集", "")
        if cube_ref_matches(cube_ref, cube_id) or f"cube_{cube_id}" in str(cube_ref):
            if evicted_count > 0:
                fr["status"] = "已告警"
                fr["触发次数"] = str(int(fr.get("触发次数", "0")) + 1)
                fr["最后检查"] = time.strftime("%Y-%m-%d %H:%M:%S")


def _audit_cache_eviction(cube_id: int, count: int) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"CACHE_EVICT cube_id={cube_id}",
        "执行耗时": "100ms",
        "扫描行数": str(count),
        "返回行数": str(count),
        "数据来源": "aggregate_cache",
        "操作用户": "cache_evictor",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    rows.append(audit)
