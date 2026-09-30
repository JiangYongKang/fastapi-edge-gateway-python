"""健康注册中心：摘除、半开探测、恢复、并发计数、三类失败原因可区分。"""

import asyncio

import pytest

from app.health import FailureKind, HealthRegistry, HealthState, validate_base_url
from app.routing import UpstreamDef


def registry(**kw) -> HealthRegistry:
    defaults = {"threshold": 3, "recover_after": 0.3, "probe_max_inflight": 1}
    defaults.update(kw)
    return HealthRegistry(**defaults)


async def _trip(reg: HealthRegistry, uid: str = "u1", kind=FailureKind.TIMEOUT, t=0.0):
    state = None
    for _ in range(3):
        state = await reg.report_failure(uid, kind, t)
    return state


async def test_trip_after_threshold_and_reject_while_open():
    reg = registry()
    await reg.register(UpstreamDef("u1", "http://127.0.0.1:9001"))
    state = await _trip(reg)
    assert state is HealthState.UNHEALTHY
    assert await reg.acquire("u1", now=0.1) is False
    status = await reg.status("u1")
    assert status.consecutive_failures == 3
    assert status.last_error == "timeout"


async def test_stale_success_does_not_restore():
    reg = registry()
    await reg.register(UpstreamDef("u1", "http://127.0.0.1:9001"))
    await _trip(reg)
    assert await reg.report_success("u1", now=0.1) is False
    assert (await reg.status("u1")).state is HealthState.UNHEALTHY


async def test_half_open_single_probe_then_recover():
    reg = registry()
    await reg.register(UpstreamDef("u1", "http://127.0.0.1:9001"))
    await _trip(reg, t=0.0)
    # 冷却到期：只有一个探测名额
    assert await reg.acquire("u1", now=0.31) is True
    assert await reg.acquire("u1", now=0.32) is False
    await reg.release("u1")
    # release 后可以再领一个名额（模拟探测请求已结束）
    assert await reg.acquire("u1", now=0.33) is True
    assert await reg.report_success("u1", now=0.34) is True
    assert (await reg.status("u1")).state is HealthState.HEALTHY
    assert await reg.acquire("u1", now=0.35) is True


async def test_half_open_failed_probe_reopens_with_fresh_cooldown():
    reg = registry(recover_after=0.5)
    await reg.register(UpstreamDef("u1", "http://127.0.0.1:9001"))
    await _trip(reg, t=0.0)
    assert await reg.acquire("u1", now=0.51) is True
    state = await reg.report_failure("u1", FailureKind.CONNECT_ERROR, now=0.52)
    assert state is HealthState.UNHEALTHY
    # 冷却已重新计时：0.7 < 0.52+0.5 仍拒绝
    assert await reg.acquire("u1", now=0.7) is False
    assert await reg.acquire("u1", now=1.03) is True


async def test_concurrent_failures_no_counter_corruption():
    """200 个并发失败上报：状态机迁移恰好一次，计数不错乱、不回跳。"""
    reg = registry()
    await reg.register(UpstreamDef("u1", "http://127.0.0.1:9001"))

    async def fail():
        await reg.report_failure("u1", FailureKind.UPSTREAM_5XX, now=0.0)

    await asyncio.gather(*[fail() for _ in range(200)])
    status = await reg.status("u1")
    assert status.state is HealthState.UNHEALTHY
    assert status.consecutive_failures == 3
    assert status.last_error == "upstream_5xx: HTTP 5xx" or status.last_error.startswith(
        "upstream_5xx"
    )


async def test_concurrent_health_jitter_settles():
    """成功与失败并发抖动：阈值 1 时最终态只能是 healthy/unhealthy 之一，
    且 healthy 时计数必须为 0，绝不出现 half_open 残留计数。"""
    reg = registry(threshold=1, probe_max_inflight=2)
    await reg.register(UpstreamDef("u1", "http://127.0.0.1:9001"))

    async def jitter(i):
        if i % 2 == 0:
            await reg.report_failure("u1", FailureKind.CONNECT_ERROR, now=0.0)
        else:
            await reg.report_success("u1", now=0.0)

    await asyncio.gather(*[jitter(i) for i in range(100)])
    status = await reg.status("u1")
    assert status.state in (HealthState.HEALTHY, HealthState.UNHEALTHY)
    if status.state is HealthState.HEALTHY:
        assert status.consecutive_failures == 0


async def test_three_failure_kinds_are_distinguishable():
    reg = registry(threshold=10)
    await reg.register(UpstreamDef("u1", "http://127.0.0.1:9001"))
    await reg.report_failure("u1", FailureKind.UPSTREAM_5XX, 0.0, "HTTP 503")
    assert (await reg.status("u1")).last_error == "upstream_5xx: HTTP 503"
    await reg.report_failure("u1", FailureKind.CONNECT_ERROR, 0.0, "refused")
    assert (await reg.status("u1")).last_error == "connect_error: refused"
    await reg.report_failure("u1", FailureKind.TIMEOUT, 0.0, "read timed out")
    assert (await reg.status("u1")).last_error == "timeout: read timed out"


async def test_select_round_robin_skips_unhealthy_and_probes_later():
    reg = registry()
    for uid in ("u1", "u2"):
        await reg.register(UpstreamDef(uid, f"http://127.0.0.1:90{uid[1:]}"))
    picked = {(await reg.select(("u1", "u2"), 0.0))[0] for _ in range(4)}
    assert picked == {"u1", "u2"}

    # 摘掉 u1 后只选 u2
    await _trip(reg, "u1", FailureKind.CONNECT_ERROR, t=0.0)
    for _ in range(3):
        choice = await reg.select(("u1", "u2"), 0.1)
        assert choice[0] == "u2"
    # 冷却到期，u1 可被作为半开探测选中
    choice = await reg.select(("u1", "u2"), 1.0)
    assert choice is not None
    assert choice[0] in {"u1", "u2"}


async def test_register_reset_clears_state_and_bad_url_rejected():
    reg = registry()
    await reg.register(UpstreamDef("u1", "http://127.0.0.1:9001"))
    await _trip(reg)
    await reg.register(UpstreamDef("u1", "http://127.0.0.1:9002"))
    assert (await reg.status("u1")).state is HealthState.HEALTHY
    with pytest.raises(ValueError):
        validate_base_url("u1", "ftp://127.0.0.1:9")
    with pytest.raises(ValueError):
        validate_base_url("u1", "http://")
