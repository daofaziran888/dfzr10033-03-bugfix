"""多维立方领域服务：立方构建、聚合计算、缓存预热与分区策略。

每次构建/刷新都会产出一个“聚合批次”：聚合缓存明细 + 完整性标记 + 批次号。
同一立方同一组维度/度量在刷新期间被并发提交时，只真正计算一次。
"""
from __future__ import annotations

import hashlib
import random
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.concurrent import run_once
from app.services.cube_ref import (
    cube_ref_label,
    cube_ref_matches,
    metric_identifier_candidates,
    parse_cube_id,
    result_summary_refs_cube,
)
from app.store import store


class OlapCubeService(BaseModuleService):
    MODULE = "olap_cube"
    ENTITY = "OLAP立方"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["立方编号", "立方名称", "维度列表", "度量列表", "聚合类型", "分区策略", "刷新周期", "数据行数"]
    REQUIRED_FIELDS = ["立方编号", "立方名称", "维度列表"]
    STATUS_ORDER = ["草稿", "构建中", "可用", "构建失败"]
    ACTION_RULES = {"启动构建": "构建中", "标记可用": "可用", "标记失败": "构建失败"}
    NEGATIVE_ACTIONS = []

    def build_cube(
        self,
        cube_id: int | str,
        data_sources: list[str] | None = None,
        *,
        expected_rows: int | None = None,
        complete: bool = True,
    ) -> dict[str, Any]:
        """构建多维立方、聚合计算并写入 aggregate_cache、result_cache、query_audit。

        ``complete=False`` / ``expected_rows`` 用于表达刷新期间只到位了一部分数据：
        该批次会以“部分”登记，指标计算不会把它当作完整依据。
        """
        numeric_id = parse_cube_id(cube_id)
        if numeric_id is None:
            return {"ok": False, "message": "立方标识无效"}
        cube = store.find(self.MODULE, numeric_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        dims = [d.strip() for d in str(cube.get("维度列表", "")).split(",") if d.strip()]
        meas = [m.strip() for m in str(cube.get("度量列表", "")).split(",") if m.strip()]

        if not dims or not meas:
            return {"ok": False, "message": "维度和度量定义不能为空"}

        # 同一组维度/度量的并发构建只算一次
        key = ("cube_build", numeric_id, tuple(dims), tuple(meas))
        result, deduplicated = run_once(
            key,
            lambda: self._build_cube_locked(
                numeric_id, cube, dims, meas, data_sources,
                expected_rows=expected_rows, complete=complete,
            ),
        )
        payload = dict(result)
        payload["deduplicated"] = deduplicated
        return payload

    def _build_cube_locked(
        self,
        cube_id: int,
        cube: dict[str, Any],
        dims: list[str],
        meas: list[str],
        data_sources: list[str] | None,
        *,
        expected_rows: int | None,
        complete: bool,
    ) -> dict[str, Any]:
        # 从语义模型获取字段映射
        models = _get_models_for_cube(dims)
        field_map = {}
        for m in models:
            aliases = m.get("字段别名", "").split(",")
            for a in aliases:
                a = a.strip()
                if a:
                    field_map[a] = a

        # 从数据源获取原始数据
        sources = data_sources or _get_registered_sources()
        raw_data = _fetch_source_data(sources, dims)

        # 执行聚合计算
        t0 = time.time()
        aggregated = _compute_aggregates(raw_data, dims, meas, cube.get("聚合类型", "SUM"))
        elapsed_ms = (time.time() - t0) * 1000

        now = time.strftime("%Y-%m-%d %H:%M:%S")
        batch_id = f"AGGR-C{cube_id:04d}-{uuid.uuid4().hex[:8]}"

        batch_expected = expected_rows if expected_rows is not None else len(aggregated)
        batch_complete = complete and len(aggregated) >= int(batch_expected)

        # 写入多维立方：半批不把立方标成“可用”，等下一批齐全再确认
        cube["数据行数"] = str(len(aggregated))
        cube["status"] = "可用" if batch_complete else "构建中"
        cube["pending"] = not batch_complete
        cube["最后批次"] = batch_id
        cube["最后刷新"] = now

        # 写入聚合缓存（完整或部分批次）
        cache_id = _write_aggregate_cache(
            cube_id, dims, meas, aggregated,
            batch_id=batch_id, complete=batch_complete, expected_rows=batch_expected,
        )

        # 写入结果缓存
        result_cache_id = _write_result_cache(cube_id, aggregated, batch_id=batch_id)

        # 写入审计
        _write_cube_audit(cube_id, elapsed_ms, len(aggregated), batch_id)

        # 只有完整批次才通知指标工厂有可重算的数据
        if batch_complete:
            _update_metrics(cube_id, meas, aggregated)

        return {
            "ok": True,
            "cube_id": cube_id,
            "batch_id": batch_id,
            "batch_complete": batch_complete,
            "expected_rows": batch_expected,
            "aggregate_cache_id": cache_id,
            "result_cache_id": result_cache_id,
            "dims": len(dims),
            "measures": len(meas),
            "dimensions": dims,
            "measures_list": meas,
            "aggregated_rows": len(aggregated),
            "elapsed_ms": elapsed_ms,
            "sample": aggregated[:5],
            "deduplicated": False,
        }

    def refresh_cube(self, cube_id: int | str) -> dict[str, Any]:
        """刷新立方：淘汰旧 cache、重建、刷新依赖集市。"""
        numeric_id = parse_cube_id(cube_id)
        if numeric_id is None:
            return {"ok": False, "message": "立方标识无效"}
        cube = store.find(self.MODULE, numeric_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        # 淘汰旧缓存（只淘汰本立方，不碰立方 11 或指标结果）
        _evict_cube_cache(numeric_id)

        # 重新构建（并发提交的同一组维度/度量由 run_once 合并）
        result = self.build_cube(numeric_id)

        # 更新数据集市（如果存在依赖）
        _refresh_dependent_marts(numeric_id)

        return result

    def estimate_partition_cost(
        self, cube_id: int, partition_count: int, partition_rows: int
    ) -> dict[str, Any]:
        """估算分区构建代价，写入 cost_govern。"""
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        total_rows = partition_count * partition_rows
        estimated_memory = total_rows * 0.5  # MB
        estimated_time = total_rows / 10000  # seconds

        # 写入成本治理
        cost_rows = store.rows("cost_govern")
        cost_id = max((int(r.get("id", 0)) for r in cost_rows), default=0) + 1
        cost = {
            "id": cost_id,
            "策略编号": f"COST-C{cube_id:04d}",
            "适用角色": "olap_cube_builder",
            "查询上限": str(total_rows),
            "并发上限": str(partition_count),
            "月度配额": str(total_rows * 8),
            "告警阈值": str(int(estimated_memory * 1.5)),
            "限流动作": "monitor",
            "生效时段": time.strftime("%Y-%m-%d"),
            "status": "已生效" if estimated_memory < 1000 else "已告警",
            "pending": estimated_memory >= 1000,
            "abnormal": estimated_memory >= 2000,
        }
        cost_rows.append(cost)

        return {
            "ok": True,
            "estimated_rows": total_rows,
            "estimated_memory_mb": estimated_memory,
            "estimated_time_s": estimated_time,
            "cost_entry_id": cost_id,
        }


# ---------- 跨模块辅助函数 ----------

def _get_models_for_cube(dims: list[str]) -> list[dict[str, Any]]:
    models = store.rows("semantic_model")
    return [m for m in models if m.get("status") == "已投产"]


def _get_registered_sources() -> list[str]:
    sources = store.rows("source_registry")
    return [s.get("源编号", f"src_{i}") for i, s in enumerate(sources) if s.get("status") == "已注册"]


def _fetch_source_data(sources: list[str], dims: list[str]) -> list[dict[str, Any]]:
    """模拟从数据源读取原始数据。"""
    data = []
    for i in range(random.randint(20, 200)):
        row = {"_source": sources[i % len(sources)] if sources else "local"}
        for dim in dims:
            row[dim] = f"{dim}_val_{random.randint(1, 20)}"
        row["_value"] = random.randint(1, 10000)
        data.append(row)
    return data


def _compute_aggregates(
    raw: list[dict[str, Any]],
    dims: list[str],
    measures: list[str],
    agg_type: str,
) -> list[dict[str, Any]]:
    """按维度分组执行聚合计算。"""
    groups: dict[tuple, list[int]] = {}
    for row in raw:
        key = tuple(row.get(d, "?") for d in dims)
        groups.setdefault(key, []).append(row.get("_value", 0))

    result = []
    for key, values in groups.items():
        agg_row = {}
        for i, d in enumerate(dims):
            agg_row[d] = key[i]
        if agg_type == "SUM":
            agg_row["aggregated_value"] = sum(values)
        elif agg_type == "AVG":
            agg_row["aggregated_value"] = sum(values) / len(values)
        elif agg_type == "COUNT":
            agg_row["aggregated_value"] = len(values)
        elif agg_type == "MAX":
            agg_row["aggregated_value"] = max(values)
        elif agg_type == "MIN":
            agg_row["aggregated_value"] = min(values)
        else:
            agg_row["aggregated_value"] = sum(values)
        agg_row["group_count"] = len(values)
        result.append(agg_row)
    return result


def _write_aggregate_cache(
    cube_id: int,
    dims: list[str],
    measures: list[str],
    data: list[dict[str, Any]],
    *,
    batch_id: str | None = None,
    complete: bool = True,
    expected_rows: int | None = None,
) -> int:
    rows = store.rows("aggregate_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    data_str = str(data)
    identifier = batch_id or f"AGGR-C{cube_id:04d}-B{cache_id:04d}"
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    cache = {
        "id": cache_id,
        "配置编号": f"AGGR-C{cube_id:04d}",
        "目标立方": cube_ref_label(cube_id),
        "cube_id": cube_id,
        "缓存粒度": f"{len(dims)}维 x {len(measures)}度",
        "TTL秒数": "3600",
        "预热策略": "LAZY",
        "命中阈值": "0.8",
        "淘汰算法": "LRU",
        "缓存大小": str(len(data_str)),
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
        "source": "cube_build",
    }
    rows.append(cache)
    return cache_id


def _write_result_cache(
    cube_id: int, data: list[dict[str, Any]], *, batch_id: str | None = None
) -> int:
    rows = store.rows("result_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    data_str = str(data)
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    cache = {
        "id": cache_id,
        "条目编号": f"RESU-C{cube_id:04d}-B{cache_id:04d}",
        "查询摘要": f"cube_build_{cube_id}",
        "结果哈希": hashlib.md5(data_str.encode()).hexdigest()[:12],
        "缓存命中": str(len(data)),
        "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 7200)),
        "存储大小": str(len(data_str)),
        "创建时间": now,
        "最后命中": now,
        "status": "有效",
        "pending": True,
        "abnormal": False,
        "result_type": "cube",
        "cube_id": cube_id,
        "batch_id": batch_id,
        "批次号": batch_id,
        "result_data": data,
    }
    rows.append(cache)
    return cache_id


def _write_cube_audit(cube_id: int, elapsed_ms: float, row_count: int, batch_id: str | None = None) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    status = "正常" if elapsed_ms < 10000 else "慢查询"
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"CUBE_BUILD cube_id={cube_id} batch={batch_id or 'NONE'}",
        "执行耗时": f"{elapsed_ms:.0f}ms",
        "扫描行数": str(row_count),
        "返回行数": str(row_count),
        "数据来源": "olap_cube",
        "操作用户": "cube_builder",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "pending": False,
        "abnormal": elapsed_ms > 30000,
        "cube_id": cube_id,
        "batch_id": batch_id,
    }
    rows.append(audit)


def _metric_depends_on_cube(metric: dict[str, Any], cube_id: int) -> bool:
    """指标是否依赖指定立方；支持 1 / olap_cube:1 / 1,2 等写法，按完整编号比较。"""
    for field in ("依赖立方", "cube_id", "数据来源"):
        value = metric.get(field)
        if value in (None, ""):
            continue
        parts = [p.strip() for p in str(value).replace(";", ",").split(",") if p.strip()]
        for part in parts:
            if cube_ref_matches(part, cube_id):
                return True
    return False


def _update_metrics(cube_id: int, measures: list[str], data: list[dict[str, Any]]) -> None:
    """立方刷新后只更新真正依赖它的指标（不碰立方 11 的指标）。

    这里只登记“有新批次可供重算”，不直接把指标标成“已产出”：批次只是原料，
    指标值必须由指标工厂按批次计算后写入结果缓存，才算一次成功产出。
    """
    metrics = store.rows("metric_factory")
    for m in metrics:
        if not _metric_depends_on_cube(m, cube_id):
            continue
        m["依赖度量"] = ",".join(measures)
        m["待重算"] = True
        m["pending"] = True
        if m.get("status") not in ("已产出",):
            m["status"] = "投产中"


def _evict_cube_cache(cube_id: int) -> None:
    """淘汰与立方相关的所有缓存条目（严格按完整立方标识）。

    聚合缓存里的旧批次只标记为“已被新刷新取代”，不直接判死：当新批次尚未
    齐全时，指标计算仍可回退到最近一个完整批次，避免无谓的产空中断。
    """
    # 淘汰 aggregate_cache
    agg = store.rows("aggregate_cache")
    for a in agg:
        if cube_ref_matches(a.get("目标立方"), cube_id):
            a["superseded"] = True
            a["pending"] = True

    # 淘汰 result_cache：只淘汰该立方自己的构建/预热结果与聚合明细副本，
    # 不淘汰指标结果（指标结果由指标工厂按批次治理）。
    result = store.rows("result_cache")
    for r in result:
        if result_summary_refs_cube(r.get("查询摘要"), cube_id):
            r["status"] = "已淘汰"


def _refresh_dependent_marts(cube_id: int) -> None:
    """刷新依赖此立方的结果集市。"""
    marts = store.rows("result_mart")
    for mart in marts:
        source = str(mart.get("数据来源", ""))
        if any(cube_ref_matches(part, cube_id) for part in source.split(",")):
            mart["status"] = "刷新中"
            mart["pending"] = True
