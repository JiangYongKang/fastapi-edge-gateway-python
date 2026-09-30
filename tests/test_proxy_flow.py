"""转发核心：成功路径、5xx 区分与重试、超时、连接失败、非幂等不重试。"""

import asyncio

import pytest


async def _start_env(gateway, upstream_factory, upstreams=("u1",), route_kw=None):
    runner, _app, settings = gateway
    states, servers = {}, {}
    for uid in upstreams:
        state, server = upstream_factory()
        states[uid], servers[uid] = state, server
    cm = runner()
    client, _app = await cm.__aenter__()
    for uid in upstreams:
        r = await client.post(
            "/admin/upstreams",
            json={"id": uid, "base_url": servers[uid].base_url},
        )
        assert r.status_code == 201
    payload = {"id": "r1", "prefix": "/svc", "upstream_ids": list(upstreams)}
    if route_kw:
        payload.update(route_kw)
    r = await client.post("/admin/routes", json=payload)
    assert r.status_code == 201
    return cm, client, states, servers, settings


@pytest.fixture
async def single(gateway, upstream_factory):
    cm, client, states, servers, settings = await _start_env(
        gateway, upstream_factory, ("u1",)
    )
    yield client, states["u1"], servers["u1"], settings
    await cm.__aexit__(None, None, None)


async def test_forward_success_preserves_path_query_body_and_headers(single):
    client, _state, _server, _ = single
    r = await client.post(
        "/svc/orders/42?verbose=1",
        json={"item": "book"},
        headers={"X-Trace": "abc"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["path"] == "/orders/42"
    assert body["method"] == "POST"
    assert body["query"] == "verbose=1"
    assert body["body"] == '{"item":"book"}'
    assert body["headers"]["x-trace"] == "abc"
    assert body["headers"]["x-gateway-upstream"] == "u1"
    assert r.headers["x-gateway-route"] == "r1"
    assert r.headers["x-gateway-upstream"] == "u1"


async def test_upstream_5xx_is_retried_for_safe_get_then_distinct_502(single):
    client, state, _server, _ = single
    state.fail_remaining = 1  # 第一次 500，第二次成功
    r = await client.get("/svc/ping")
    assert r.status_code == 200
    assert state.snapshot_calls() == 2

    state.fail_remaining = 5  # 一直失败：2 次尝试后明确返回 upstream_5xx
    r = await client.get("/svc/ping")
    assert r.status_code == 502
    assert r.headers["x-gateway-reason"] == "upstream_5xx"
    assert r.json()["reason"] == "upstream_5xx"


async def test_non_idempotent_request_is_never_retried(single):
    client, state, server, _ = single
    for method, kwargs in (
        ("POST", {"json": {"x": 1}}),
        ("PUT", {"json": {"x": 1}}),
        ("PATCH", {"json": {"x": 1}}),
        ("DELETE", {}),
    ):
        # 每个方法前重新指向同一真实地址以重置健康计数，并要求该次上游失败
        await client.put(
            "/admin/upstreams/u1",
            json={"id": "u1", "base_url": server.base_url},
        )
        state.fail_remaining = 5
        before = state.snapshot_calls()
        r = await getattr(client, method.lower())("/svc/resource", **kwargs)
        assert r.status_code == 502, method
        # 无论如何只打了一次，副作用不会被重复执行
        assert state.snapshot_calls() - before == 1, method
        assert r.headers["x-gateway-reason"] == "upstream_5xx"


async def test_upstream_timeout_fails_fast_with_distinct_reason(single):
    client, state, _server, settings = single
    state.slow = True
    started = asyncio.get_event_loop().time()
    r = await client.get("/svc/slow")
    elapsed = asyncio.get_event_loop().time() - started
    assert r.status_code == 504
    assert r.headers["x-gateway-reason"] == "upstream_timeout"
    assert elapsed < settings.request_timeout * 2 + 1.0
    assert elapsed >= settings.request_timeout  # 确实等了超时而非立即返回


async def test_connect_error_is_distinct_and_no_leftover_hang(
    gateway, upstream_factory, dead_endpoint
):
    cm, client, _states, _servers, _ = await _start_env(
        gateway, upstream_factory, ("u1",)
    )
    try:
        await client.put(
            "/admin/upstreams/u1",
            json={"id": "u1", "base_url": dead_endpoint},
        )
        started = asyncio.get_event_loop().time()
        r = await client.get("/svc/x")
        elapsed = asyncio.get_event_loop().time() - started
        assert r.status_code == 502
        assert r.headers["x-gateway-reason"] == "upstream_connect_error"
        assert elapsed < 2.0  # 连接被拒绝应快速失败，不挂起
    finally:
        await cm.__aexit__(None, None, None)


async def test_route_level_timeout_override(single):
    client, state, _server, _ = single
    # 把该路由超时改成 0.05s，慢上游应快速返回 504
    r = await client.put(
        "/admin/routes/r1",
        json={
            "id": "r1",
            "prefix": "/svc",
            "upstream_ids": ["u1"],
            "timeout": 0.05,
        },
    )
    assert r.status_code == 200
    state.slow = True
    r = await client.get("/svc/slow")
    assert r.status_code == 504
    assert r.headers["x-gateway-reason"] == "upstream_timeout"


async def test_all_upstreams_down_returns_503(gateway, upstream_factory, dead_endpoint):
    cm, client, _states, _servers, _ = await _start_env(
        gateway, upstream_factory, ("u1", "u2")
    )
    try:
        await client.put(
            "/admin/routes/r1",
            json={
                "id": "r1",
                "prefix": "/svc",
                "upstream_ids": ["u1", "u2"],
                "max_retries": 0,
            },
        )
        for uid in ("u1", "u2"):
            await client.put(
                f"/admin/upstreams/{uid}",
                json={"id": uid, "base_url": dead_endpoint},
            )
        last = None
        for _ in range(20):
            last = await client.get("/svc/x")
            if last.status_code == 503:
                break
        assert last.status_code == 503
        assert last.headers["x-gateway-reason"] == "no_healthy_upstream"
        health = await client.get("/health")
        assert all(u["state"] == "unhealthy" for u in health.json()["upstreams"])
    finally:
        await cm.__aexit__(None, None, None)
