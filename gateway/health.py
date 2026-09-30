"""上游健康状态机：closed -> open -> half-open -> closed。

并发语义：
- 所有状态迁移都在同一把 asyncio.Lock 内完成，计数不会错乱；
- open 期间请求直接被拒绝（快速失败）；
- 冷却结束后进入 half-open，只允许有限个探测请求并发通过，
  其余请求仍被拒绝，避免惊群；
- half-open 探测成功即关闭并清零计数；失败则重新打开并重置冷却，
  不会出现状态回跳。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum

from .models import FailureKind, GatewayConfig


class State(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class _UpstreamHealth:
    state: State = State.CLOSED
    consecutive_failures: int = 0
    opened_at: float = 0.0
    half_open_inflight: int = 0
    last_failure: FailureKind | None = None


class HealthTracker:
    def __init__(self, config: GatewayConfig, clock=time.monotonic) -> None:
        self._config = config
        self._clock = clock
        self._lock = asyncio.Lock()
        self._states: dict[str, _UpstreamHealth] = {}

    def _get(self, upstream: str) -> _UpstreamHealth:
        return self._states.setdefault(upstream, _UpstreamHealth())

    async def try_acquire(self, upstream: str) -> bool:
        """请求前调用：该上游当前是否允许接收一个请求。

        half-open 状态下占用一个探测名额，请求结束后必须调用
        record_success / record_failure 释放。
        """
        async with self._lock:
            h = self._get(upstream)
            if h.state is State.CLOSED:
                return True
            if h.state is State.OPEN:
                if self._clock() - h.opened_at >= self._config.cooldown_s:
                    h.state = State.HALF_OPEN
                    h.half_open_inflight = 0
                else:
                    return False
            # half-open
            if h.half_open_inflight >= self._config.half_open_max_probe:
                return False
            h.half_open_inflight += 1
            return True

    async def record_success(self, upstream: str) -> None:
        async with self._lock:
            h = self._get(upstream)
            if h.state is State.HALF_OPEN:
                h.half_open_inflight = max(0, h.half_open_inflight - 1)
            h.state = State.CLOSED
            h.consecutive_failures = 0
            h.last_failure = None

    async def record_failure(self, upstream: str, kind: FailureKind) -> None:
        async with self._lock:
            h = self._get(upstream)
            if h.state is State.HALF_OPEN:
                h.half_open_inflight = max(0, h.half_open_inflight - 1)
                # 探测失败：直接重新打开，重置冷却窗口
                h.state = State.OPEN
                h.opened_at = self._clock()
                h.consecutive_failures += 1
                h.last_failure = kind
                return
            h.consecutive_failures += 1
            h.last_failure = kind
            if h.consecutive_failures >= self._config.failure_threshold:
                h.state = State.OPEN
                h.opened_at = self._clock()

    async def is_available(self, upstream: str) -> bool:
        """只读查询（用于状态展示），不改变状态。"""
        async with self._lock:
            h = self._get(upstream)
            if h.state is State.OPEN and (
                self._clock() - h.opened_at >= self._config.cooldown_s
            ):
                return True  # 下次 try_acquire 会转入 half-open
            return h.state is not State.OPEN

    async def snapshot(self) -> dict[str, dict]:
        async with self._lock:
            return {
                name: {
                    "state": h.state.value,
                    "consecutive_failures": h.consecutive_failures,
                    "last_failure": h.last_failure.value if h.last_failure else None,
                }
                for name, h in self._states.items()
            }
