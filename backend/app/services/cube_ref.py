"""立方/批次标识解析工具。

聚合缓存、指标计算、立方刷新都按“完整标识”比对立方，不能再使用
``str(cube_id) in target`` 这种子串判断：立方 1 与立方 11 会互相串读。
"""
from __future__ import annotations

import re
from typing import Any

_TRAILING_ID = re.compile(r"(\d+)\s*$")


def cube_ref_label(cube_id: int) -> str:
    """标准的立方引用写法。"""
    return f"olap_cube:{int(cube_id)}"


def parse_cube_id(value: Any) -> int | None:
    """从立方标识中取出编号。

    支持 ``1``、``olap_cube:1``、``cube_1``、``OLAP-0001`` 等写法；
    只认结尾的完整数字段，``olap_cube:11`` 解析为 11，绝不会落到 1。
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    match = _TRAILING_ID.search(text)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except ValueError:  # pragma: no cover - 正则已经保证是数字
        return None


def cube_ref_matches(value: Any, cube_id: int) -> bool:
    """判断一个立方引用是否精确指向 ``cube_id``。

    支持逗号/分号分隔的多引用；逐段完整解析，绝不用子串包含判断。
    """
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value == int(cube_id)
    parts = [part.strip() for part in re.split(r"[,;|]", str(value)) if part.strip()]
    for part in parts:
        parsed = parse_cube_id(part)
        if parsed is not None and parsed == int(cube_id):
            return True
    return False


def result_summary_refs_cube(summary: Any, cube_id: int) -> bool:
    """只匹配立方自身的构建/预热结果缓存摘要。

    指标结果摘要（``metric_1``）不是立方缓存，刷新立方 1 时不能把它一起
    淘汰；同时 ``cube_build_11`` 也不能因为包含字符 ``1`` 而被误删。
    """
    text = str(summary or "")
    patterns = (
        rf"(?:^|[^0-9])cube_build_{int(cube_id)}(?:$|[^0-9])",
        rf"(?:^|[^0-9])prewarm_cube_{int(cube_id)}(?:$|[^0-9])",
    )
    return any(re.search(pattern, text) for pattern in patterns)


def metric_identifier_candidates(metric: dict[str, Any] | None, metric_id: int | None = None) -> set[str]:
    """一个指标可能出现的全部标识，兼容历史报表里的写法。"""
    candidates: set[str] = set()
    if metric_id is not None:
        candidates.add(str(metric_id))
        candidates.add(f"metric_{metric_id}")
    if metric:
        for field in ("指标编号", "指标标识", "指标ID", "metric_code", "metric_id"):
            value = metric.get(field)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                candidates.add(text)
                candidates.add(f"metric_{text}")
        try:
            candidates.add(str(int(metric.get("id"))))
        except (TypeError, ValueError):
            pass
    return candidates


def metric_ref_matches(value: Any, candidates: set[str]) -> bool:
    """按完整标识匹配指标，避免指标 1/11 互相命中。"""
    text = str(value or "").strip()
    if not text:
        return False
    if text in candidates:
        return True
    parsed = parse_cube_id(text)
    if parsed is None:
        return False
    return str(parsed) in candidates
