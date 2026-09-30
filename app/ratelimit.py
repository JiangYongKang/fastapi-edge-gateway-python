"""限流：按 (规则, 客户端) 维度的令牌桶，并发下扣减原子。

语义：

* 每个 key 一个桶，``capacity`` 为突发上限，按 ``refill_per_sec`` 匀速补充。
* :meth:`RateLimiter.allow` **从不等待**：令牌够就扣 1 个返回 ``True``，
  不够立即返回 ``False``（由上层快速失败为 429），请求不会被挂起。
* 所有扣减在同一把锁内"先补充再判断再扣减"，并发请求不会超发。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass
class _TokenBucket:
    capacity: float
    refill_per_sec: float
    tokens: float
    last_refill: float

    def refill(self, now: float) -> None:
        elapsed = now - self.last_refill
        if elapsed > 0:
            self.tokens = min(
                self.capacity, self.tokens + elapsed * self.refill_per_sec
            )
            self.last_refill = now


class RateLimiter:
    """按 key 懒加载令牌桶；令牌不足时立即拒绝（不等待）。"""

    def __init__(self, default_capacity: int, default_refill_per_sec: float) -> None:
        if default_capacity <= 0 or default_refill_per_sec <= 0:
            raise ValueError("限流容量与填充速率必须为正数")
        self._default_capacity = float(default_capacity)
        self._default_refill = float(default_refill_per_sec)
        self._buckets: dict[str, _TokenBucket] = {}
        # 计算都在临界区内同步完成，asyncio 下不会交错，无需每桶一把锁。
        self._lock = asyncio.Lock()

    async def configure(
        self, key: str, capacity: int, refill_per_sec: float, now: float
    ) -> None:
        """为某 key 设置独立限流参数（路由级覆盖）。

        已存在的桶保留剩余令牌（但不超过新容量），不会因改配置突然放大流量。
        """
        if capacity <= 0 or refill_per_sec <= 0:
            raise ValueError("限流容量与填充速率必须为正数")
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                self._buckets[key] = _TokenBucket(
                    capacity=float(capacity),
                    refill_per_sec=float(refill_per_sec),
                    tokens=float(capacity),
                    last_refill=now,
                )
            else:
                bucket.refill(now)
                bucket.capacity = float(capacity)
                bucket.refill_per_sec = float(refill_per_sec)
                bucket.tokens = min(bucket.tokens, float(capacity))

    async def remove(self, key: str) -> None:
        async with self._lock:
            self._buckets.pop(key, None)

    async def allow(
        self,
        key: str,
        now: float,
        capacity: int | None = None,
        refill_per_sec: float | None = None,
    ) -> bool:
        """非阻塞取 1 个令牌；可为该 key 指定独立的容量与填充速率。"""
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                cap = float(capacity) if capacity is not None else self._default_capacity
                refill = (
                    float(refill_per_sec)
                    if refill_per_sec is not None
                    else self._default_refill
                )
                bucket = _TokenBucket(
                    capacity=cap,
                    refill_per_sec=refill,
                    tokens=cap,
                    last_refill=now,
                )
                self._buckets[key] = bucket
            elif capacity is not None and refill_per_sec is not None:
                # 路由参数变化时平滑生效：保留不超过新容量的剩余令牌。
                if (
                    bucket.capacity != float(capacity)
                    or bucket.refill_per_sec != float(refill_per_sec)
                ):
                    bucket.refill(now)
                    bucket.capacity = float(capacity)
                    bucket.refill_per_sec = float(refill_per_sec)
                    bucket.tokens = min(bucket.tokens, float(capacity))
            bucket.refill(now)
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return True
            return False

    async def available(
        self,
        key: str,
        now: float,
        capacity: int | None = None,
        refill_per_sec: float | None = None,
    ) -> float:
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                return float(capacity) if capacity is not None else self._default_capacity
            bucket.refill(now)
            return bucket.tokens
