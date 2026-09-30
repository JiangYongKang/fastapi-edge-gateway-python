"""HTTP 层限流：超限 429、原因可区分、不同客户端隔离、路由级参数覆盖。"""

import pytest


@pytest.fixture
async def env(gateway, upstream_factory):
    runner, _app, _settings = gateway
    _state, server = upstream_factory()
    cm = runner()
    client, _app = await cm.__aenter__()
    try:
        await client.post(
            "/admin/upstreams",
            json={"id": "u1", "base_url": server.base_url},
        )
        await client.post(
            "/admin/routes",
            json={
                "id": "r1",
                "prefix": "/svc",
                "upstream_ids": ["u1"],
                "rate_limit_capacity": 3,
                "rate_limit_refill_per_sec": 0.0001,  # 测试期内基本不补充
            },
        )
        yield client
    finally:
        await cm.__aexit__(None, None, None)


async def test_burst_then_429_fast_fail(env):
    statuses = [
        (await env.get("/svc/p", headers={"X-Forwarded-For": "10.0.0.1"})).status_code
        for _ in range(5)
    ]
    assert statuses == [200, 200, 200, 429, 429]
    rejected = await env.get("/svc/p", headers={"X-Forwarded-For": "10.0.0.1"})
    assert rejected.headers["x-gateway-reason"] == "rate_limited"
    assert rejected.json()["reason"] == "rate_limited"


async def test_rate_limit_is_per_client(env):
    # 客户端 A 用完 3 个令牌
    for _ in range(3):
        assert (
            await env.get("/svc/p", headers={"X-Forwarded-For": "10.0.0.1"})
        ).status_code == 200
    assert (
        await env.get("/svc/p", headers={"X-Forwarded-For": "10.0.0.1"})
    ).status_code == 429
    # 客户端 B 独立计数，仍可通过
    assert (
        await env.get("/svc/p", headers={"X-Forwarded-For": "10.0.0.2"})
    ).status_code == 200


async def test_route_override_changes_limit_at_runtime(env):
    # 更新成容量 1：新客户端立刻只能通过 1 个
    r = await env.put(
        "/admin/routes/r1",
        json={
            "id": "r1",
            "prefix": "/svc",
            "upstream_ids": ["u1"],
            "rate_limit_capacity": 1,
            "rate_limit_refill_per_sec": 0.0001,
        },
    )
    assert r.status_code == 200
    headers = {"X-Forwarded-For": "10.0.0.9"}
    assert (await env.get("/svc/p", headers=headers)).status_code == 200
    assert (await env.get("/svc/p", headers=headers)).status_code == 429
