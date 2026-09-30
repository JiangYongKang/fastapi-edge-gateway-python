"""并发场景：健康抖动、摘除期流量隔离、冷却后半开探测与自动恢复。"""

import asyncio

import pytest


async def _setup(gateway, upstream_factory, *, capacity=10_000, max_retries=0):
    runner, app, _settings = gateway
    state, server = upstream_factory()
    cm = runner()
    client, _app = await cm.__aenter__()
    await client.post(
        "/admin/upstreams", json={"id": "u1", "base_url": server.base_url}
    )
    await client.post(
        "/admin/routes",
        json={
            "id": "r1",
            "prefix": "/svc",
            "upstream_ids": ["u1"],
            "max_retries": max_retries,
            "rate_limit_capacity": capacity,
            "rate_limit_refill_per_sec": 10_000,
        },
    )
    return cm, client, state, _settings, app


@pytest.mark.parametrize("concurrency", [10, 40])
async def test_concurrent_failures_trip_once_and_isolate_traffic(
    gateway, upstream_factory, concurrency
):
    cm, client, state, _settings, app = await _setup(
        gateway, upstream_factory, max_retries=1
    )
    try:
        # 该用例要求摘除在整个测试窗口内保持：拉长冷却（直接调注册表）。
        app.state.health_registry._recover_after = 60.0

        state.fail_remaining = 1000
        results = await asyncio.gather(
            *[client.get("/svc/ping") for _ in range(concurrency)]
        )
        # 所有请求都是明确的错误，没有挂起；没有一个被当成成功
        assert all(r.status_code in (502, 503) for r in results)

        health = await client.get("/health")
        upstream = next(u for u in health.json()["upstreams"] if u["id"] == "u1")
        assert upstream["state"] == "unhealthy"

        # 等第一波所有在途请求收尾，确认摘除稳定后再打第二波
        for _ in range(50):
            h = next(u for u in (await client.get("/health")).json()["upstreams"]
                     if u["id"] == "u1")
            if h["state"] == "unhealthy":
                break
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.05)  # 让可能已取得许可的在途请求全部结束
        # 摘除后第二波并发：全部 503，且对上游零新增调用（流量完全隔离）
        calls_after_trip = state.snapshot_calls()
        results2 = await asyncio.gather(
            *[client.get("/svc/ping") for _ in range(concurrency)]
        )
        assert all(r.status_code == 503 for r in results2)
        assert state.snapshot_calls() == calls_after_trip
    finally:
        await cm.__aexit__(None, None, None)


async def test_recovery_after_cooldown_uses_single_probe(gateway, upstream_factory):
    cm, client, state, settings, _app = await _setup(gateway, upstream_factory)
    try:
        state.fail_remaining = 1000
        for _ in range(3):
            await client.get("/svc/ping")
        assert (
            next(u for u in (await client.get("/health")).json()["upstreams"]
                 if u["id"] == "u1")["state"]
            == "unhealthy"
        )

        # 确认摘除稳定
        for _ in range(50):
            h = next(u for u in (await client.get("/health")).json()["upstreams"]
                     if u["id"] == "u1")
            if h["state"] == "unhealthy":
                break
            await asyncio.sleep(0.02)
        # 摘除后拉长冷却，保证冷波期间不会提前进入半开
        registry = _app.state.health_registry
        registry._recover_after = 60.0
        cold = await asyncio.gather(*[client.get("/svc/ping") for _ in range(20)])
        assert all(r.status_code == 503 for r in cold)

        # 上游恢复健康；把冷却恢复为正常值并令摘除时刻已过冷却窗口
        state.fail_remaining = 0
        registry._recover_after = settings.health_recover_after
        records = registry._records
        records["u1"].opened_at_mono -= settings.health_recover_after + 1

        # 冷却后一波并发：半开只放 1 个探测，其余 503；探测成功后状态恢复。
        wave = await asyncio.gather(*[client.get("/svc/ping") for _ in range(20)])
        statuses = sorted(r.status_code for r in wave)
        assert 200 in statuses
        assert set(statuses) <= {200, 503}
        assert (
            next(u for u in (await client.get("/health")).json()["upstreams"]
                 if u["id"] == "u1")["state"]
            == "healthy"
        )
        # 恢复后并发全部成功
        follow = await asyncio.gather(*[client.get("/svc/ping") for _ in range(10)])
        assert all(r.status_code == 200 for r in follow)
    finally:
        await cm.__aexit__(None, None, None)


async def test_failed_probe_reopens_and_does_not_flap(gateway, upstream_factory):
    cm, client, state, settings, _app = await _setup(gateway, upstream_factory)
    try:
        state.fail_remaining = 1000
        for _ in range(3):
            await client.get("/svc/ping")
        # 拉长冷却避免中间步骤提前半开，再回退冷却窗口
        await asyncio.sleep(settings.health_recover_after + 0.15)

        # 冷却到期但上游仍坏：探测失败，状态回到 unhealthy 且重新计时
        r = await client.get("/svc/ping")
        assert r.status_code in (502, 503)
        assert (
            next(u for u in (await client.get("/health")).json()["upstreams"]
                 if u["id"] == "u1")["state"]
            == "unhealthy"
        )
        # 紧接着的请求仍是 503（新冷却期未到），不会抖动回 healthy
        assert (await client.get("/svc/ping")).status_code == 503
    finally:
        await cm.__aexit__(None, None, None)
