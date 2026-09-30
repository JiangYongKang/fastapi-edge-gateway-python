"""限流：突发上限、匀速补充、不同 key 隔离、并发原子扣减、快速拒绝。"""

import asyncio

import pytest

from app.ratelimit import RateLimiter


async def test_burst_capacity_then_fast_reject():
    rl = RateLimiter(default_capacity=5, default_refill_per_sec=10)
    decisions = await asyncio.gather(*[rl.allow("k", 0.0) for _ in range(7)])
    assert decisions == [True] * 5 + [False] * 2


async def test_refill_is_time_based():
    rl = RateLimiter(default_capacity=5, default_refill_per_sec=10)
    for _ in range(5):
        assert await rl.allow("k", 0.0)
    assert await rl.allow("k", 0.0) is False
    # 0.1 秒补 1 个令牌
    assert await rl.allow("k", 0.1) is True
    assert await rl.allow("k", 0.1) is False
    # 0.5 秒补 5 个，回到满桶
    for _ in range(5):
        assert await rl.allow("k", 0.6)
    assert await rl.allow("k", 0.6) is False


async def test_keys_are_isolated():
    rl = RateLimiter(default_capacity=1, default_refill_per_sec=0.0001)
    assert await rl.allow("r1:a", 0.0)
    assert await rl.allow("r1:b", 0.0)
    assert await rl.allow("r2:a", 0.0)
    assert await rl.allow("r1:a", 0.0) is False


async def test_concurrent_allow_never_oversells():
    """100 个并发请求抢 10 个令牌，恰好 10 个通过。"""
    rl = RateLimiter(default_capacity=10, default_refill_per_sec=0.0001)
    decisions = await asyncio.gather(*[rl.allow("hot", 0.0) for _ in range(100)])
    assert sum(decisions) == 10


async def test_route_override_applies_on_first_and_later_use():
    rl = RateLimiter(default_capacity=100, default_refill_per_sec=100)
    # 首次使用即按路由覆盖参数：容量只有 2
    decisions = await asyncio.gather(
        *[rl.allow("tight", 0.0, capacity=2, refill_per_sec=1) for _ in range(3)]
    )
    assert decisions == [True, True, False]
    # 改大容量后，剩余令牌受新容量约束但不会突然超发
    # 大容量在更晚时刻生效：此时已补 1 个令牌，可再通过 1 次
    assert await rl.allow("tight", 2.0, capacity=50, refill_per_sec=1) is True
    assert await rl.allow("tight", 2.0, capacity=50, refill_per_sec=1) is True
    assert await rl.allow("tight", 2.0, capacity=50, refill_per_sec=1) is False


async def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        RateLimiter(default_capacity=0, default_refill_per_sec=1)
    rl = RateLimiter(default_capacity=1, default_refill_per_sec=1)
    with pytest.raises(ValueError):
        await rl.configure("k", 0, 1, now=0.0)
