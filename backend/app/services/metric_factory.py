"""指标工厂领域服务：指标计算定义、聚合窗口、从立方到指标的产出管线。

取数链路（修复后）：

1. 指标 -> 依赖立方，按完整立方标识在 ``aggregate_cache`` 里找 **完整批次**；
2. 只用该批次的聚合明细按指标公式计算，绝不混入其它立方（立方 1 不会读到
   立方 11 的明细），也不会把半批金额计入；
3. 结果写入 ``result_cache`` 时带上批次号、维度、度量、公式与确认时间，形成
   可追溯的指标计算结果；
4. 报表重算只从结果缓存读取当时确认的数值，并兼容历史指标标识与计算口径。
"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any, Iterable

from app.services.aggregate_cache import (
    AggregateCacheService,
    batch_completeness,
    batch_data,
    batch_identifier,
    batch_summary,
)
from app.services.base import BaseModuleService
from app.services.concurrent import run_once
from app.services.cube_ref import (
    cube_ref_matches,
    metric_identifier_candidates,
    metric_ref_matches,
    parse_cube_id,
)
from app.store import store


class MetricFactoryService(BaseModuleService):
    MODULE = "metric_factory"
    ENTITY = "计算指标"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["指标编号", "指标名称", "数据来源", "计算公式", "聚合窗口", "精度模式", "依赖立方", "最后产出"]
    REQUIRED_FIELDS = ["指标编号", "指标名称", "数据来源"]
    STATUS_ORDER = ["草稿", "投产中", "已产出", "产出异常"]
    ACTION_RULES = {"计算指标": "已产出", "提交投产": "投产中", "确认产出": "已产出", "标记异常": "产出异常"}
    NEGATIVE_ACTIONS = ["标记异常"]
    COMPUTE_ACTIONS = ("计算指标", "确认产出")

    def __init__(self) -> None:
        self._aggregate_cache = AggregateCacheService()

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """“计算指标/确认产出”必须真的走一次批次取数。

        没有完整聚合依据时保持异常状态，不允许只改状态就记成一次成功产出。
        """
        if action not in self.COMPUTE_ACTIONS:
            return super().run_action(entry_id, action)

        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        result = self.compute_metric(entry_id)
        if not result["ok"]:
            entry["_action_ok"] = False
            return entry, f"{self.ENTITY}计算失败：{result['message']}（{result['reason']}）"
        return entry, (
            f"{self.ENTITY}已按批次 {result['batch_id']} 产出，"
            f"指标值 {_format_number(float(result['value']))}"
        )

    # ------------------------------------------------------------------
    # 指标计算
    # ------------------------------------------------------------------

    def compute_metric(
        self,
        metric_id: int | str,
        cube_id: int | str | None = None,
        *,
        dims: list[str] | None = None,
        measures: list[str] | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """计算指标，从指定立方的完整聚合批次取数并写入可追溯的结果缓存。

        ``cube_id`` 省略时从指标的“依赖立方”/“数据来源”推导。
        """
        metric = self.find_metric(metric_id)
        if metric is None:
            return {"ok": False, "message": "指标不存在", "reason": "METRIC_NOT_FOUND"}

        if cube_id is None:
            cube_id = _metric_cube_reference(metric)
        numeric_cube = parse_cube_id(cube_id)
        if numeric_cube is None:
            return {"ok": False, "message": "依赖立方标识无效", "reason": "INVALID_CUBE"}
        cube = store.find("olap_cube", numeric_cube)

        dims = dims or _split_list(cube.get("维度列表") if cube else metric.get("依赖维度"))
        measures = measures or _split_list(cube.get("度量列表") if cube else metric.get("依赖度量"))
        if cube is None and not (
            self._aggregate_cache.list_complete_batches(numeric_cube, dims, measures)
            or self._aggregate_cache.incomplete_batches(numeric_cube, dims, measures)
        ):
            return {"ok": False, "message": "依赖立方不存在", "reason": "CUBE_NOT_FOUND"}
        formula = str(metric.get("计算公式") or "SUM")

        key = (
            "metric_compute",
            int(metric["id"]),
            numeric_cube,
            tuple(dims),
            tuple(measures),
            batch_id,
            formula,
        )
        result, deduplicated = run_once(
            key,
            lambda: self._compute_metric_locked(
                metric=metric,
                cube=cube or {"id": numeric_cube},
                cube_id=numeric_cube,
                dims=dims,
                measures=measures,
                formula=formula,
                batch_id=batch_id,
            ),
        )
        payload = dict(result)
        payload["deduplicated"] = deduplicated
        return payload

    # 兼容不同调用方的命名
    calculate_metric = compute_metric
    run_metric = compute_metric
    compute = compute_metric

    def _compute_metric_locked(
        self,
        *,
        metric: dict[str, Any],
        cube: dict[str, Any],
        cube_id: int,
        dims: list[str],
        measures: list[str],
        formula: str,
        batch_id: str | None,
    ) -> dict[str, Any]:
        metric_id = int(metric["id"])
        t0 = time.time()

        if batch_id:
            batch = self._batch_for_cube(cube_id, batch_id)
            if batch is None:
                return self._fail(
                    metric,
                    cube_id,
                    reason="BATCH_NOT_FOUND",
                    detail=f"立方 {cube_id} 下不存在批次 {batch_id}",
                    elapsed_ms=(time.time() - t0) * 1000,
                )
            complete, reason = batch_completeness(batch.get("entry") or {})
            if not complete:
                return self._fail(
                    metric,
                    cube_id,
                    reason=f"INCOMPLETE_BATCH:{reason}",
                    detail=f"批次 {batch_id} 不完整，不能作为计算依据",
                    elapsed_ms=(time.time() - t0) * 1000,
                    batch=batch,
                )
            selected = batch
        else:
            selected = self._aggregate_cache.latest_complete_batch(cube_id, dims, measures)
            if selected is None:
                # 没有与立方当前维度/度量完全一致声明的批次时，回退到该立方
                # 最近一个“没有声明冲突元数据”的完整批次（历史批次常常缺少维度元数据）。
                for candidate in reversed(self._aggregate_cache.list_complete_batches(cube_id)):
                    if candidate.get("measures") and measures and set(candidate["measures"]) != set(measures):
                        continue
                    if candidate.get("dimensions") and dims and set(candidate["dimensions"]) != set(dims):
                        continue
                    selected = candidate
                    break
            if selected is None:
                partials = self._aggregate_cache.incomplete_batches(cube_id, dims, measures)
                reason = "NO_COMPLETE_BATCH"
                if partials:
                    reason = f"NO_COMPLETE_BATCH:仅存在不完整批次（{', '.join(str(p['batch_id']) for p in partials[:3])}）"
                return self._fail(
                    metric,
                    cube_id,
                    reason=reason,
                    detail="聚合缓存没有可用于计算的完整批次",
                    elapsed_ms=(time.time() - t0) * 1000,
                    partial_batches=partials,
                )

        # 以批次实际声明的维度/度量为准记录口径
        batch_dims = selected.get("dimensions") or dims
        batch_measures = selected.get("measures") or measures

        data = batch_data(selected.get("entry") or {})
        value = _compute_metric_value(formula, data)
        if value is None:
            # 没有完整聚合依据时拒绝产出，不再用随机金额冒充一次成功产出。
            return self._fail(
                metric,
                cube_id,
                reason="NO_USABLE_AGGREGATE",
                detail=f"批次 {selected.get('batch_id')} 没有与公式 {formula} 匹配的聚合值",
                elapsed_ms=(time.time() - t0) * 1000,
                batch=selected,
            )

        elapsed_ms = (time.time() - t0) * 1000
        produced_at = time.strftime("%Y-%m-%d %H:%M:%S")
        batch_ref = str(selected.get("batch_id"))

        # 先清理同一指标/立方的历史“无批次”有效结果，避免重算后残留。
        expired = _expire_untraceable_metric_results(metric_id, cube_id)

        # 更新指标
        metric["最后产出"] = produced_at
        metric["status"] = "已产出"
        metric["pending"] = False
        metric["abnormal"] = False
        metric["最后批次"] = batch_ref
        metric["指标值"] = _format_number(value)
        metric["计算口径"] = formula

        # 写入结果缓存（带批次，可追溯）
        cache_id = _cache_metric_result(
            metric=metric,
            cube_id=cube_id,
            value=value,
            batch=selected,
            dims=batch_dims,
            measures=batch_measures,
            formula=formula,
            produced_at=produced_at,
        )

        # 写入审计
        _audit_metric(
            metric_id=metric_id,
            cube_id=cube_id,
            batch_id=batch_ref,
            elapsed_ms=elapsed_ms,
            value=value,
        )

        return {
            "ok": True,
            "metric_id": metric_id,
            "指标编号": metric.get("指标编号"),
            "cube_id": cube_id,
            "value": value,
            "batch_id": batch_ref,
            "batch_ids": [batch_ref],
            "formula": formula,
            "dimensions": batch_dims,
            "measures": batch_measures,
            "row_count": len(data),
            "result_cache_id": cache_id,
            "produced_at": produced_at,
            "elapsed_ms": elapsed_ms,
            "traceable": True,
            "expired_untraceable_results": expired,
            "message": f"指标 {metric.get('指标编号') or metric_id} 已按批次 {batch_ref} 产出",
        }

    def _batch_for_cube(self, cube_id: int, batch_id: str) -> dict[str, Any] | None:
        for entry in store.rows("aggregate_cache"):
            if not cube_ref_matches(entry.get("目标立方"), cube_id):
                continue
            if batch_identifier(entry) == str(batch_id):
                return batch_summary(entry)
        return None

    def _fail(
        self,
        metric: dict[str, Any],
        cube_id: int,
        *,
        reason: str,
        detail: str,
        elapsed_ms: float,
        batch: dict[str, Any] | None = None,
        partial_batches: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        metric_id = int(metric["id"])
        metric["status"] = "产出异常"
        metric["pending"] = True
        metric["abnormal"] = True
        metric["最后产出异常"] = time.strftime("%Y-%m-%d %H:%M:%S")
        metric["产出异常原因"] = detail

        # 没有完整依据时，不能留下“有效但没有批次”的旧结果。
        expired = _expire_untraceable_metric_results(metric_id, cube_id)
        _audit_metric(
            metric_id=metric_id,
            cube_id=cube_id,
            batch_id=(batch or {}).get("batch_id"),
            elapsed_ms=elapsed_ms,
            value=None,
            status="产出异常",
            message=reason,
        )
        return {
            "ok": False,
            "metric_id": metric_id,
            "cube_id": cube_id,
            "value": None,
            "batch_id": (batch or {}).get("batch_id"),
            "reason": reason,
            "message": detail,
            "partial_batches": partial_batches or [],
            "expired_untraceable_results": expired,
            "traceable": False,
        }

    # ------------------------------------------------------------------
    # 指标标识解析（兼容历史报表）
    # ------------------------------------------------------------------

    def find_metric(self, reference: Any) -> dict[str, Any] | None:
        """按 id、指标编号、metric_<id>、历史别名等完整标识查找指标。"""
        text = str(reference if reference is not None else "").strip()
        if not text:
            return None
        rows = store.rows(self.MODULE)

        # 1) 精确标识匹配
        for metric in rows:
            if text in metric_identifier_candidates(metric):
                return metric
        # 2) 历史编号里的完整数字段（METR-0001 -> 1），不做子串匹配
        parsed = parse_cube_id(text)
        if parsed is not None:
            for metric in rows:
                try:
                    if int(metric.get("id")) == parsed:
                        return metric
                except (TypeError, ValueError):
                    continue
            for metric in rows:
                if parse_cube_id(metric.get("指标编号")) == parsed:
                    return metric
        return None

    # 兼容命名
    resolve_metric = find_metric
    get_metric = find_metric

    # ------------------------------------------------------------------
    # 报表重算：只读结果缓存里“当时确认”的数值
    # ------------------------------------------------------------------

    def recompute_report(
        self,
        report: dict[str, Any] | list[dict[str, Any]] | None = None,
        *,
        metric_ids: Iterable[Any] | None = None,
        cube_id: int | str | None = None,
    ) -> dict[str, Any]:
        """重算报表：从 result_cache 读取当时确认的指标数值。

        - ``report`` 可以是历史报表（列表或含 ``items``/``metrics`` 的对象），
          每行可带 ``指标标识``/``指标编号``/``metric_id``/``计算口径``/``batch_id``；
        - 不传报表时按 ``metric_ids`` 或全部指标生成新报表；
        - 半批金额不计入已确认合计；无确认结果的指标明确给出原因与可引用批次。
        """
        numeric_cube = parse_cube_id(cube_id) if cube_id is not None else None
        raw_lines = _normalize_report_lines(report)

        if raw_lines is None:
            lines = [{"metric": metric, "line": {}} for metric in self._metrics_for_report(metric_ids)]
        else:
            lines = [
                {"metric": _resolve_report_metric(line), "line": line}
                for line in raw_lines
            ]

        entries: list[dict[str, Any]] = []
        confirmed_total = 0.0
        unresolved: list[dict[str, Any]] = []

        for line in lines:
            metric = line.get("metric")
            requested_batch = _line_value(line.get("line"), "batch_id", "批次号", "批次", "batch")
            requested_cube = _line_value(line.get("line"), "cube_id", "依赖立方", "立方")
            line_cube = parse_cube_id(requested_cube) if requested_cube is not None else numeric_cube
            if line_cube is None and metric is not None:
                line_cube = parse_cube_id(_metric_cube_reference(metric))
            if metric is None:
                entry = {
                    "指标标识": _line_value(line.get("line"), "指标标识", "指标编号", "metric_id", "metric_code"),
                    "status": "尚无结果",
                    "reason": "METRIC_NOT_FOUND",
                    "value": None,
                    "batch_id": None,
                }
                entries.append(entry)
                unresolved.append(entry)
                continue

            identifier = metric.get("指标编号") or metric.get("id")
            confirmed = self.get_confirmed_metric_result(
                metric, cube_id=line_cube, batch_id=requested_batch
            )
            partials = self._aggregate_cache.incomplete_batches(
                line_cube, _split_list(metric.get("依赖维度")), _split_list(metric.get("依赖度量"))
            ) if line_cube is not None else []

            if confirmed is None:
                entry = {
                    "指标标识": identifier,
                    "metric_id": metric.get("id"),
                    "指标名称": metric.get("指标名称"),
                    "计算口径": _line_value(line.get("line"), "计算口径", "formula") or metric.get("计算口径") or metric.get("计算公式"),
                    "status": "尚无结果",
                    "reason": "NO_CONFIRMED_RESULT",
                    "detail": "结果缓存中没有该指标已确认且带批次的数值",
                    "value": None,
                    "batch_id": requested_batch,
                    "partial_batches": [
                        {"batch_id": p["batch_id"], "reason": p["completeness"], "rows": p["row_count"]}
                        for p in partials
                    ],
                }
                entries.append(entry)
                unresolved.append(entry)
                continue

            value = confirmed["value"]
            entry = {
                "指标标识": identifier,
                "metric_id": metric.get("id"),
                "指标名称": metric.get("指标名称"),
                "计算口径": confirmed.get("formula") or metric.get("计算公式"),
                "status": "已确认",
                "value": value,
                "batch_id": confirmed.get("batch_id"),
                "batch_ids": confirmed.get("batch_ids", [confirmed.get("batch_id")]),
                "result_cache_id": confirmed.get("cache_id"),
                "confirmed_at": confirmed.get("confirmed_at"),
                "dimensions": confirmed.get("dimensions", []),
                "measures": confirmed.get("measures", []),
                "cube_id": confirmed.get("cube_id"),
                "reason": None,
                # 半批金额不计入已确认合计：这里的 value 仅来自完整批次。
                "partial_batches": [
                    {"batch_id": p["batch_id"], "reason": p["completeness"], "rows": p["row_count"]}
                    for p in partials
                ],
            }
            entries.append(entry)
            confirmed_total += float(value)

        return {
            "ok": True,
            "report": entries,
            "items": entries,
            "confirmed_total": confirmed_total,
            "confirmed_count": len(entries) - len(unresolved),
            "unresolved": unresolved,
            "unresolved_count": len(unresolved),
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    # 兼容命名
    rebuild_report = recompute_report
    recalculate_report = recompute_report
    recompute_metric_report = recompute_report
    generate_report = recompute_report

    def get_confirmed_metric_result(
        self,
        metric: dict[str, Any] | int | str,
        *,
        cube_id: int | str | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any] | None:
        """读取指标在结果缓存中已确认、且带批次的数值。"""
        if not isinstance(metric, dict):
            metric = self.find_metric(metric)
        if metric is None:
            return None

        candidates = metric_identifier_candidates(metric)
        formula = str(metric.get("计算口径") or metric.get("计算公式") or "")
        if formula:
            candidates.add(formula)
        numeric_cube = parse_cube_id(cube_id) if cube_id is not None else None

        matches: list[dict[str, Any]] = []
        for entry in store.rows("result_cache"):
            if entry.get("status") != "有效":
                continue
            data = _result_data(entry)
            identifier = (
                entry.get("指标标识")
                or entry.get("指标编号")
                or entry.get("metric_code")
                or entry.get("metric_id")
                or _first_present(data, ("指标标识", "指标编号", "metric_code"))
            )
            if identifier is None:
                # 历史条目没有显式指标标识时，回退到查询摘要 metric_<id>
                summary = str(entry.get("查询摘要", ""))
                identifier = summary if summary.startswith("metric") else None
            if identifier is None or not metric_ref_matches(identifier, candidates):
                continue

            entry_cube = parse_cube_id(entry.get("cube_id", _first_present(data, ("cube_id", "立方编号"))))
            if numeric_cube is not None and entry_cube is not None and entry_cube != numeric_cube:
                continue
            entry_batch = entry.get("batch_id") or entry.get("批次号") or _first_present(data, ("batch_id", "批次号"))
            if not entry_batch:
                # 没有批次的结果不可追溯，不能进入报表。
                continue
            if batch_id and str(entry_batch) != str(batch_id):
                continue

            value = entry.get("指标值")
            if value is None:
                value = _first_present(data, ("value", "指标值", "计算结果"))
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                continue

            matches.append({
                "cache_id": entry.get("id"),
                "value": numeric_value,
                "batch_id": str(entry_batch),
                "batch_ids": entry.get("batch_ids") or [str(entry_batch)],
                "formula": entry.get("计算口径") or _first_present(data, ("formula", "计算口径")),
                "dimensions": entry.get("dimensions") or _first_present(data, ("dimensions", "维度列表")) or [],
                "measures": entry.get("measures") or _first_present(data, ("measures", "度量列表")) or [],
                "cube_id": entry_cube,
                "confirmed_at": entry.get("确认时间") or entry.get("创建时间"),
                "result_hash": entry.get("结果哈希"),
                "created_index": int(entry.get("id", 0)),
            })

        if not matches:
            return None
        matches.sort(key=lambda item: (item.get("confirmed_at") or "", item["created_index"]))
        return matches[-1]

    get_metric_result = get_confirmed_metric_result
    get_confirmed_result = get_confirmed_metric_result

    def _metrics_for_report(self, metric_ids: Iterable[Any] | None) -> list[dict[str, Any]]:
        if metric_ids is None:
            return list(store.rows(self.MODULE))
        metrics: list[dict[str, Any]] = []
        for reference in metric_ids:
            metric = self.find_metric(reference)
            if metric is not None:
                metrics.append(metric)
        return metrics


# ----------------------------------------------------------------------
# 公式计算
# ----------------------------------------------------------------------

def _format_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _split_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _metric_cube_reference(metric: dict[str, Any]) -> Any:
    for field in ("依赖立方", "cube_id", "数据来源"):
        value = metric.get(field)
        if value not in (None, ""):
            return value
    return None


def _extract_field(formula: str) -> str | None:
    """从 SUM(revenue) / AVG(value) 之类的公式里取出度量字段。"""
    start = formula.find("(")
    end = formula.find(")")
    if start == -1:
        return None
    inner = formula[start + 1:end if end != -1 else len(formula)].strip()
    if not inner or inner == "*":
        return None
    return inner.split(",")[0].strip().strip("`\"'")


def _pick_value(row: dict[str, Any], field: str | None) -> Any:
    if field:
        for key in (field, f"aggregated_{field}", f"{field}_value"):
            if key in row:
                return row[key]
    for key in ("aggregated_value", "value", "指标值", "metric_value", "amount", "金额"):
        if key in row:
            return row[key]
    return None


def _compute_metric_value(formula: str, data: list[dict[str, Any]]) -> float | None:
    """按公式计算；没有可用数据时返回 None（绝不伪造随机金额）。"""
    if not data:
        return None
    field = _extract_field(formula)
    values = []
    for row in data:
        raw = _pick_value(row, field)
        if isinstance(raw, bool):
            continue
        if isinstance(raw, (int, float)):
            values.append(float(raw))
        elif isinstance(raw, str):
            try:
                values.append(float(raw.strip()))
            except ValueError:
                continue
    if not values:
        return None
    upper = formula.upper()
    if "AVG" in upper or "平均" in formula:
        return sum(values) / len(values)
    if "COUNT" in upper or "计数" in formula:
        return float(len(values))
    if "MAX" in upper:
        return float(max(values))
    if "MIN" in upper:
        return float(min(values))
    # SUM / 合计 / 默认口径
    return float(sum(values))


# ----------------------------------------------------------------------
# 结果缓存读写
# ----------------------------------------------------------------------

def _result_data(entry: dict[str, Any]) -> list[dict[str, Any]]:
    data = entry.get("result_data", entry.get("cached_data", []))
    return data if isinstance(data, list) else []


def _first_present(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> Any:
    for row in rows:
        if not isinstance(row, dict):
            continue
        for field in fields:
            if row.get(field) is not None:
                return row[field]
    return None


def _expire_untraceable_metric_results(metric_id: int, cube_id: int) -> int:
    """把同一指标/立方下没有批次号的历史有效结果标记过期。"""
    expired = 0
    for entry in store.rows("result_cache"):
        if entry.get("status") != "有效":
            continue
        if entry.get("result_type") != "metric" and not str(entry.get("查询摘要", "")).startswith("metric_"):
            continue
        data = _result_data(entry)
        entry_metric = entry.get("metric_id") or _first_present(data, ("metric_id",))
        if entry_metric is None:
            summary = str(entry.get("查询摘要", ""))
            if summary.startswith("metric_"):
                entry_metric = summary[len("metric_"):]
        if str(entry_metric or "") != str(metric_id):
            continue
        entry_cube = parse_cube_id(entry.get("cube_id") or _first_present(data, ("cube_id",)))
        if entry_cube is not None and entry_cube != cube_id:
            continue
        batch = entry.get("batch_id") or entry.get("批次号") or _first_present(data, ("batch_id", "批次号"))
        if not batch:
            entry["status"] = "已过期"
            entry["pending"] = False
            entry["过期原因"] = "缺少批次依据"
            expired += 1
    return expired


def _cache_metric_result(
    *,
    metric: dict[str, Any],
    cube_id: int,
    value: float,
    batch: dict[str, Any],
    dims: list[str],
    measures: list[str],
    formula: str,
    produced_at: str,
) -> int:
    rows = store.rows("result_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    metric_id = int(metric["id"])
    batch_id = str(batch.get("batch_id"))
    trace = {
        "result_type": "metric",
        "metric_id": metric_id,
        "指标标识": metric.get("指标编号") or str(metric_id),
        "指标编号": metric.get("指标编号"),
        "value": value,
        "batch_id": batch_id,
        "batch_ids": [batch_id],
        "cube_id": cube_id,
        "dimensions": dims,
        "维度列表": ",".join(dims),
        "measures": measures,
        "度量列表": ",".join(measures),
        "formula": formula,
        "计算口径": formula,
        "computed_at": produced_at,
    }
    cache = {
        "id": cache_id,
        "条目编号": f"RESU-M{metric_id:04d}-B{cache_id:04d}",
        "查询摘要": f"metric_{metric_id}",
        "结果哈希": hashlib.md5(f"{metric_id}:{cube_id}:{batch_id}:{value}".encode()).hexdigest()[:12],
        "缓存命中": "1",
        "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 3600)),
        "存储大小": str(len(str(trace))),
        "创建时间": produced_at,
        "确认时间": produced_at,
        "最后命中": produced_at,
        "status": "有效",
        "pending": False,
        "abnormal": False,
        "result_type": "metric",
        "metric_id": metric_id,
        "指标标识": metric.get("指标编号") or str(metric_id),
        "指标编号": metric.get("指标编号"),
        "指标值": value,
        "batch_id": batch_id,
        "批次号": batch_id,
        "batch_ids": [batch_id],
        "cube_id": cube_id,
        "dimensions": dims,
        "维度列表": ",".join(dims),
        "measures": measures,
        "度量列表": ",".join(measures),
        "formula": formula,
        "计算口径": formula,
        "source_row_count": batch.get("row_count"),
        "result_data": [trace],
    }
    rows.append(cache)
    return cache_id


def _audit_metric(
    *,
    metric_id: int,
    cube_id: int,
    batch_id: str | None,
    elapsed_ms: float,
    value: float | None,
    status: str = "正常",
    message: str | None = None,
) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    value_text = "N/A" if value is None else f"{value:.2f}"
    batch_text = batch_id or "NONE"
    suffix = f" reason={message}" if message else ""
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"METRIC_COMPUTE metric_id={metric_id} cube_id={cube_id} batch={batch_text} value={value_text}{suffix}",
        "执行耗时": f"{elapsed_ms:.0f}ms",
        "扫描行数": "1" if value is not None else "0",
        "返回行数": "1" if value is not None else "0",
        "数据来源": "metric_factory",
        "操作用户": "metric_engine",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "pending": False,
        "abnormal": status != "正常" or value is None,
        "batch_id": batch_id,
        "cube_id": cube_id,
    }
    rows.append(audit)


# ----------------------------------------------------------------------
# 历史报表归一化 / 指标解析
# ----------------------------------------------------------------------

def _normalize_report_lines(report: Any) -> list[dict[str, Any]] | None:
    if report is None:
        return None
    if isinstance(report, list):
        return [line if isinstance(line, dict) else {"指标标识": line} for line in report]
    if isinstance(report, dict):
        for field in ("items", "metrics", "lines", "report", "rows"):
            value = report.get(field)
            if isinstance(value, list):
                return [line if isinstance(line, dict) else {"指标标识": line} for line in value]
        # 单行报表：把报表对象本身当一行（其中的指标标识/计算口径字段即该行的口径）
        return [report]
    return [{"指标标识": report}]


def _line_value(line: dict[str, Any] | None, *fields: str) -> Any:
    if not isinstance(line, dict):
        return None
    for field in fields:
        if line.get(field) is not None:
            return line[field]
    return None


def _resolve_report_metric(line: dict[str, Any]) -> dict[str, Any] | None:
    """兼容历史报表里的指标标识和计算口径。"""
    service = _metric_service()
    for field in ("指标标识", "指标编号", "metric_id", "metric_code", "metric", "指标"):
        reference = line.get(field)
        if reference is None or isinstance(reference, (dict, list)):
            continue
        metric = service.find_metric(reference)
        if metric is not None:
            return metric
    # 没有标识时按计算口径（公式）兜底匹配；多个指标同口径时取第一个
    formula = _line_value(line, "计算口径", "formula", "公式")
    if formula:
        for metric in store.rows("metric_factory"):
            if str(metric.get("计算公式") or "") == str(formula) or str(
                metric.get("计算口径") or ""
            ) == str(formula):
                return metric
    return None


def _metric_service() -> MetricFactoryService:
    global _METRIC_SERVICE
    if _METRIC_SERVICE is None:
        _METRIC_SERVICE = MetricFactoryService()
    return _METRIC_SERVICE


_METRIC_SERVICE: MetricFactoryService | None = None
