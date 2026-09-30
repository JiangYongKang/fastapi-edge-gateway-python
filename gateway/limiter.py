"""限流：每条路由一个令牌桶，协程安全，单调时钟。"""
from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """令牌桶：容量 = max(1, qps)（即可承受 1 秒突发），按 qps 速率补充。"""

    def __init__(self, clock=time.monotonic) -> None:
        self._clock = clock
        self._lock = asyncio.Lock()
        # key -> (tokens, last_refill)
        self._buckets: dict[str, list[float]] = {}

    async def allow(self, key: str, qps: float) -> bool:
        if qps <= 0:
            return False
        async with self._lock:
            now = self._clock()
            capacity = max(1.0, qps)
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = [capacity, now]
                self._buckets[key] = bucket
            tokens, last = bucket
            tokens = min(capacity, tokens + (now - last) * qps)
            bucket[0], bucket[1] = tokens, now
            if tokens >= 1.0:
                bucket[0] = tokens - 1.0
                return True
            return False

    async def reset(self, key: str) -> None:
        async with self._lock:
            self._buckets.pop(key, None)
