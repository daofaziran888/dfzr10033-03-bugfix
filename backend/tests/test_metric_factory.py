"""指标工厂取数链路：完整立方标识、批次完整性、可追溯结果与报表重算。"""
from __future__ import annotations

import threading
import time

import pytest
from fastapi.testclient import TestClient

import app.services.olap_cube as olap_module
from app.main import app
from app.services.aggregate_cache import AggregateCacheService
from app.services.metric_factory import MetricFactoryService
from app.services.olap_cube import OlapCubeService
from app.store import store

client = TestClient(app)


@pytest.fixture(autouse=True)
def isolated_store():
    """每个测试从种子数据重新开始，清空取数链路涉及的模块。"""
    from app.seed import SEED_ROWS

    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    for module in ("olap_cube", "metric_factory", "aggregate_cache", "result_cache", "query_audit"):
        store._tables[module] = []
    yield store


def _add_cube(cube_id: int, name: str = "立方") -> dict:
    rows = store.rows("olap_cube")
    cube = {
        "id": cube_id,
        "立方编号": f"OLAP-{cube_id:04d}",
        "立方名称": name,
        "维度列表": "region",
        "度量列表": "revenue",
        "聚合类型": "SUM",
        "分区策略": "dt",
        "刷新周期": "3600",
        "数据行数": "1",
        "status": "可用",
        "pending": False,
        "abnormal": False,
    }
    rows.append(cube)
    return cube


def _add_metric(metric_id: int, cube_id: int, formula: str = "SUM(revenue)") -> dict:
    rows = store.rows("metric_factory")
    metric = {
        "id": metric_id,
        "指标编号": f"METR-{metric_id:04d}",
        "指标名称": f"指标{metric_id}",
        "数据来源": f"olap_cube:{cube_id}",
        "计算公式": formula,
        "聚合窗口": "daily",
        "精度模式": "exact",
        "依赖立方": str(cube_id),
        "最后产出": "2026-11-01",
        "status": "草稿",
        "pending": True,
        "abnormal": False,
    }
    rows.append(metric)
    return metric


@pytest.fixture()
def two_cubes():
    _add_cube(1, "立方一")
    _add_cube(11, "立方十一")
    agg = AggregateCacheService()
    agg.publish_batch(1, ["region"], ["revenue"],
                      [{"region": "east", "aggregated_value": 12}], batch_id="BATCH-1")
    agg.publish_batch(11, ["region"], ["revenue"],
                      [{"region": "east", "aggregated_value": 30}], batch_id="BATCH-11")
    return agg


class TestExactCubeMatching:
    def test_cube_1_does_not_read_cube_11(self, two_cubes):
        metric = _add_metric(1, 1)
        result = MetricFactoryService().compute_metric(metric["id"], 1)
        assert result["ok"] is True
        assert result["value"] == 12.0, "立方 1 只能读到自己的 12，不能把立方 11 的 30 一起加进来"
        assert result["batch_id"] == "BATCH-1"

    def test_cube_11_reads_its_own_batch(self, two_cubes):
        metric = _add_metric(1, 11)
        result = MetricFactoryService().compute_metric(metric["id"], 11)
        assert result["ok"] is True
        assert result["value"] == 30.0
        assert result["batch_id"] == "BATCH-11"

    def test_consistency_check_does_not_touch_cube_11(self, two_cubes):
        agg = AggregateCacheService()
        # 立方 1 行数与缓存大小差异大，立方 11 的批次不能被牵连
        before = {e["batch_id"]: e["status"] for e in store.rows("aggregate_cache") if e.get("batch_id")}
        result = agg.check_cache_consistency(1)
        assert result["ok"] is True
        after = {e["batch_id"]: e["status"] for e in store.rows("aggregate_cache") if e.get("batch_id")}
        assert after["BATCH-11"] == before["BATCH-11"]

    def test_refresh_cube_1_keeps_cube_11_batches(self, two_cubes, monkeypatch):
        monkeypatch.setattr(olap_module, "_compute_aggregates",
                            lambda *a, **k: [{"region": "east", "aggregated_value": 12}])
        OlapCubeService().refresh_cube(1)
        cube_11_batches = [
            e for e in store.rows("aggregate_cache")
            if e.get("batch_id") == "BATCH-11"
        ]
        assert cube_11_batches and cube_11_batches[0]["status"] == "生效中"


class TestCompleteness:
    def test_partial_batch_is_not_used(self, two_cubes):
        agg = AggregateCacheService()
        agg.publish_batch(
            1, ["region"], ["revenue"],
            [{"region": "west", "aggregated_value": 999}],
            batch_id="BATCH-1-PARTIAL", complete=False, expected_rows=2,
        )
        metric = _add_metric(1, 1)
        result = MetricFactoryService().compute_metric(metric["id"], 1)
        # 半批不会盖住更早的完整批次
        assert result["batch_id"] == "BATCH-1"
        assert result["value"] == 12.0

    def test_no_complete_batch_does_not_produce_random_value(self):
        _add_cube(1)
        agg = AggregateCacheService()
        agg.publish_batch(1, ["region"], ["revenue"],
                          [{"region": "east", "aggregated_value": 500}],
                          batch_id="PARTIAL", complete=False, expected_rows=2)
        metric = _add_metric(1, 1)
        result = MetricFactoryService().compute_metric(metric["id"], 1)
        assert result["ok"] is False
        assert result["value"] is None
        assert metric["status"] == "产出异常"
        # 不产生任何有效的结果缓存
        assert not [
            e for e in store.rows("result_cache")
            if e.get("result_type") == "metric" and e.get("status") == "有效"
        ]

    def test_empty_aggregate_is_failure_not_random(self, two_cubes):
        agg = AggregateCacheService()
        agg.publish_batch(1, ["region"], ["revenue"], [], batch_id="EMPTY", complete=True)
        metric = _add_metric(1, 1)
        result = MetricFactoryService().compute_metric(metric["id"], 1, batch_id="EMPTY")
        assert result["ok"] is False
        assert result["value"] is None

    def test_explicit_partial_batch_request_rejected(self, two_cubes):
        agg = AggregateCacheService()
        agg.publish_batch(1, ["region"], ["revenue"],
                          [{"region": "west", "aggregated_value": 7}],
                          batch_id="HALF", complete=False, expected_rows=2)
        metric = _add_metric(1, 1)
        result = MetricFactoryService().compute_metric(metric["id"], 1, batch_id="HALF")
        assert result["ok"] is False
        assert "INCOMPLETE_BATCH" in result["reason"]


class TestConcurrency:
    def test_concurrent_builds_same_dims_run_once(self, monkeypatch):
        _add_cube(1)
        real = olap_module._compute_aggregates
        calls = {"n": 0}

        def slow(*args, **kwargs):
            calls["n"] += 1
            time.sleep(0.15)
            return real(*args, **kwargs)

        monkeypatch.setattr(olap_module, "_compute_aggregates", slow)
        service = OlapCubeService()
        before = len(store.rows("aggregate_cache"))
        results: list[dict] = []
        threads = [threading.Thread(target=lambda: results.append(service.build_cube(1))) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(results) == 6
        assert len({r["batch_id"] for r in results}) == 1, "同一组维度/度量只应产出一个批次"
        assert len(store.rows("aggregate_cache")) - before == 1
        assert sum(1 for r in results if not r["deduplicated"]) == 1

    def test_metric_compute_dedup_during_refresh(self, two_cubes):
        _add_metric(1, 1)
        service = MetricFactoryService()
        results: list[dict] = []
        threads = [threading.Thread(target=lambda: results.append(service.compute_metric(1, 1)))
                   for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert {r["value"] for r in results} == {12.0}
        assert {r["batch_id"] for r in results} == {"BATCH-1"}


class TestTraceability:
    def test_result_cache_records_batch_and_metric(self, two_cubes):
        metric = _add_metric(1, 1)
        service = MetricFactoryService()
        result = service.compute_metric(metric["id"], 1)

        entry = store.find("result_cache", result["result_cache_id"])
        assert entry["batch_id"] == "BATCH-1"
        assert entry["metric_id"] == 1
        assert entry["cube_id"] == 1
        assert entry["formula"] == "SUM(revenue)"
        assert entry["result_data"][0]["value"] == 12.0
        assert entry["result_data"][0]["batch_id"] == "BATCH-1"

        audits = [a for a in store.rows("query_audit") if a.get("batch_id") == "BATCH-1"]
        assert audits, "审计记录里也应留下批次号"

    def test_recompute_does_not_leave_batchless_valid_result(self, two_cubes):
        metric = _add_metric(1, 1)
        # 预置一条历史“无批次”有效结果，重算后必须被清理
        store.rows("result_cache").append({
            "id": 90, "条目编号": "RESU-LEGACY", "查询摘要": "metric_1",
            "status": "有效", "result_data": [{"metric_id": 1, "value": 42.0}],
        })
        service = MetricFactoryService()
        result = service.compute_metric(metric["id"], 1)
        assert result["ok"] is True
        assert result["expired_untraceable_results"] == 1
        legacy = store.find("result_cache", 90)
        assert legacy["status"] != "有效", "无批次的历史结果不能继续留在有效集合里"
        assert service.recompute_report(metric_ids=[metric["id"]])["confirmed_total"] == 12.0


class TestReportRecompute:
    def test_report_uses_confirmed_cache_value(self, two_cubes):
        _add_metric(1, 1)
        service = MetricFactoryService()
        service.compute_metric(1, 1)

        # 立方 11 新增批次不会影响报表里指标 1 的确认值
        AggregateCacheService().publish_batch(
            11, ["region"], ["revenue"], [{"region": "east", "aggregated_value": 30}],
            batch_id="BATCH-11-B",
        )
        report = service.recompute_report([{"指标标识": "METR-0001", "batch_id": "BATCH-1"}])
        assert report["confirmed_total"] == 12.0
        assert report["report"][0]["batch_id"] == "BATCH-1"

    def test_partial_batch_excluded_from_confirmed_total(self, two_cubes):
        _add_metric(1, 1)
        service = MetricFactoryService()
        service.compute_metric(1, 1)
        AggregateCacheService().publish_batch(
            1, ["region"], ["revenue"], [{"region": "west", "aggregated_value": 1000}],
            batch_id="BATCH-1-NEW", complete=False, expected_rows=2,
        )
        report = service.recompute_report(metric_ids=[1])
        assert report["confirmed_total"] == 12.0
        partials = report["report"][0]["partial_batches"]
        assert any(p["batch_id"] == "BATCH-1-NEW" for p in partials)

    def test_report_without_result_states_reason(self, two_cubes):
        _add_metric(1, 1)
        report = MetricFactoryService().recompute_report(metric_ids=[1])
        line = report["report"][0]
        assert line["status"] == "尚无结果"
        assert line["reason"] == "NO_CONFIRMED_RESULT"
        assert line["value"] is None
        assert report["confirmed_total"] == 0.0

    def test_historical_metric_identifier_and_formula(self, two_cubes):
        _add_metric(1, 1)
        MetricFactoryService().compute_metric(1, 1)
        # 历史报表：只有旧的指标编号，或只有计算口径
        by_code = MetricFactoryService().recompute_report([{"指标标识": "METR-0001"}])
        assert by_code["confirmed_total"] == 12.0
        by_formula = MetricFactoryService().recompute_report([{"计算口径": "SUM(revenue)"}])
        assert by_formula["confirmed_total"] == 12.0
        by_numeric = MetricFactoryService().recompute_report(metric_ids=["metric_1"])
        assert by_numeric["confirmed_total"] == 12.0

    def test_unknown_metric_line_is_unresolved(self, two_cubes):
        report = MetricFactoryService().recompute_report([{"指标标识": "METR-0099"}])
        assert report["unresolved_count"] == 1
        assert report["report"][0]["status"] == "尚无结果"
        assert report["confirmed_total"] == 0.0


class TestMetricAction:
    def test_action_requires_real_batch(self, two_cubes):
        metric = _add_metric(1, 1)
        service = MetricFactoryService()
        entry, message = service.run_action(metric["id"], "计算指标")
        assert entry["status"] == "已产出"
        assert "BATCH-1" in message
        assert entry["指标值"] == "12"

    def test_action_fails_without_complete_batch(self):
        _add_cube(1)
        metric = _add_metric(1, 1)
        service = MetricFactoryService()
        entry, _ = service.run_action(metric["id"], "计算指标")
        assert entry["status"] == "产出异常"
        assert entry["_action_ok"] is False
        assert not [
            e for e in store.rows("result_cache")
            if e.get("result_type") == "metric" and e.get("status") == "有效"
        ]

    def test_api_action_reports_failure(self):
        _add_cube(1)
        _add_metric(1, 1)
        resp = client.post("/api/metric_factory/1/actions", json={"action": "计算指标"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is False
        assert "计算失败" in body["message"]
        assert body["entry"]["status"] == "产出异常"


class TestCubeRefreshOverlap:
    def test_overlapping_refresh_with_partial_batch_keeps_confirmed_value(self, two_cubes):
        """两次刷新重叠、第一批不齐时：仍按最近完整批次出数，半批金额不进台账。"""
        _add_metric(1, 1)
        service = MetricFactoryService()
        service.compute_metric(1, 1)

        agg = AggregateCacheService()
        agg.publish_batch(
            1, ["region"], ["revenue"],
            [{"region": "west", "aggregated_value": 4000}],
            batch_id="REFRESH-A-PARTIAL", complete=False, expected_rows=3,
        )

        # 报表仍引用已确认的完整批次
        report = service.recompute_report([{"指标标识": "METR-0001"}])
        assert report["confirmed_total"] == 12.0
        assert report["report"][0]["batch_id"] == "BATCH-1"

        # 新一批完整数据到来后，重算才能更新口径
        agg.publish_batch(
            1, ["region"], ["revenue"],
            [{"region": "west", "aggregated_value": 5},
             {"region": "east", "aggregated_value": 8}],
            batch_id="REFRESH-B", complete=True,
        )
        recomputed = service.compute_metric(1, 1)
        assert recomputed["batch_id"] == "REFRESH-B"
        assert recomputed["value"] == 13.0
        fresh_report = service.recompute_report([{"指标标识": "METR-0001"}])
        assert fresh_report["confirmed_total"] == 13.0
        assert fresh_report["report"][0]["batch_id"] == "REFRESH-B"


class TestApi:
    def test_metric_pipeline_end_to_end(self, two_cubes):
        _add_metric(1, 1)
        computed = client.post("/api/pipelines/metric-compute", params={"metric_id": "METR-0001"})
        assert computed.status_code == 200
        body = computed.json()
        assert body["ok"] is True and body["value"] == 12.0 and body["batch_id"] == "BATCH-1"

        report = client.post("/api/pipelines/metric-report", params={"metric_ids": "METR-0001"})
        assert report.status_code == 200
        assert report.json()["confirmed_total"] == 12.0

        batches = client.post("/api/pipelines/aggregate-batches", params={"cube_id": "1"})
        assert batches.status_code == 200
        assert batches.json()["usable_batch_id"] == "BATCH-1"

    def test_historical_report_recompute_endpoint(self, two_cubes):
        _add_metric(1, 1)
        MetricFactoryService().compute_metric(1, 1)
        resp = client.post(
            "/api/pipelines/metric-report/recompute",
            json=[{"指标标识": "METR-0001", "计算口径": "SUM(revenue)"}],
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["confirmed_total"] == 12.0
        assert body["report"][0]["batch_id"] == "BATCH-1"
