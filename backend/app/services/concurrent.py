"""同一次计算的并发合并工具。

同一组标识（立方 + 维度 + 度量，或指标 + 立方 + 批次）的并发请求只让第一个
调用真正执行，其余调用等待并复用它的结果，避免刷新期间重复产出缓存。
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Hashable, TypeVar

T = TypeVar("T")

_REGISTRY: dict[Hashable, "_Inflight"] = {}
_REGISTRY_LOCK = threading.Lock()


class _Inflight:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: Any = None
        self.error: BaseException | None = None


def run_once(key: Hashable, handler: Callable[[], T], timeout: float = 60.0) -> tuple[T, bool]:
    """执行 ``handler``；相同 key 的并发调用合并成一次。

    返回值 ``(result, deduplicated)``：第一个调用 ``deduplicated=False``，
    等待中的并发调用 ``deduplicated=True`` 并拿到同一份结果。
    """
    with _REGISTRY_LOCK:
        call = _REGISTRY.get(key)
        owner = call is None
        if owner:
            call = _Inflight()
            _REGISTRY[key] = call

    assert call is not None
    if not owner:
        call.event.wait(timeout)
        if call.error is not None:
            raise call.error
        return call.result, True

    try:
        result = handler()
        call.result = result
        return result, False
    except BaseException as exc:  # noqa: BLE001 - 原样传给等待者
        call.error = exc
        raise
    finally:
        call.event.set()
        with _REGISTRY_LOCK:
            _REGISTRY.pop(key, None)
