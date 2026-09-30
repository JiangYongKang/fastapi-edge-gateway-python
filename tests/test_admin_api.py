"""管理 API：上游/路由的运行时增删改查、冲突报错、引用校验、立即生效。"""

import pytest


async def _setup_gateway(gateway, upstream_factory):
    runner, _app, _settings = gateway
    state1, srv1 = upstream_factory()
    state2, srv2 = upstream_factory()
    cm = runner()
    client, _app = await cm.__aenter__()
    try:
        r = await client.post(
            "/admin/upstreams", json={"id": "u1", "base_url": srv1.base_url}
        )
        assert r.status_code == 201
        r = await client.post(
            "/admin/upstreams", json={"id": "u2", "base_url": srv2.base_url}
        )
        assert r.status_code == 201
        r = await client.post(
            "/admin/routes",
            json={"id": "r1", "prefix": "/svc", "upstream_ids": ["u1"]},
        )
        assert r.status_code == 201
        return cm, client, state1, state2, srv1, srv2
    except Exception:
        await cm.__aexit__(None, None, None)
        raise


@pytest.fixture
async def env(gateway, upstream_factory):
    cm, client, state1, state2, _srv1, _srv2 = await _setup_gateway(
        gateway, upstream_factory
    )
    yield client, state1, state2, _srv1, _srv2
    await cm.__aexit__(None, None, None)


async def test_duplicate_upstream_and_bad_url_rejected(env):
    client, *_ = env
    r = await client.post(
        "/admin/upstreams", json={"id": "u1", "base_url": "http://127.0.0.1:1"}
    )
    assert r.status_code == 409
    assert r.json()["reason"] == "routing_error"
    r = await client.post(
        "/admin/upstreams", json={"id": "bad", "base_url": "ftp://x"}
    )
    assert r.status_code == 400
    assert r.headers["x-gateway-reason"] == "invalid_config"


async def test_route_requires_existing_upstream(env):
    client, *_ = env
    r = await client.post(
        "/admin/routes",
        json={"id": "rx", "prefix": "/x", "upstream_ids": ["ghost"]},
    )
    assert r.status_code == 409
    assert "不存在的上游" in r.json()["detail"]


async def test_duplicate_prefix_conflict_rejected(env):
    client, *_ = env
    r = await client.post(
        "/admin/routes",
        json={"id": "r2", "prefix": "/svc/", "upstream_ids": ["u1"]},
    )
    assert r.status_code == 409
    assert "冲突" in r.json()["detail"]


async def test_runtime_change_takes_effect_without_restart(env):
    client, state1, state2, _, _srv2 = env
    # 初始路由指向 u1
    r = await client.get("/svc/ping")
    assert r.status_code == 200
    assert state1.snapshot_calls() == 1
    assert state2.snapshot_calls() == 0

    # 运行时把上游切到 u2：立即生效
    r = await client.put(
        "/admin/routes/r1", json={"id": "r1", "prefix": "/svc", "upstream_ids": ["u2"]}
    )
    assert r.status_code == 200
    r = await client.get("/svc/ping")
    assert r.status_code == 200
    assert state2.snapshot_calls() == 1
    assert state1.snapshot_calls() == 1

    # 运行时再改前缀
    r = await client.put(
        "/admin/routes/r1", json={"id": "r1", "prefix": "/new", "upstream_ids": ["u2"]}
    )
    assert r.status_code == 200
    assert (await client.get("/svc/ping")).status_code == 404
    r = await client.get("/new/ping")
    assert r.status_code == 200 and state2.snapshot_calls() == 2

    # 删除路由立即生效
    assert (await client.delete("/admin/routes/r1")).status_code == 204
    assert (await client.get("/new/ping")).status_code == 404


async def test_cannot_delete_referenced_upstream(env):
    client, *_ = env
    r = await client.delete("/admin/upstreams/u1")
    assert r.status_code == 409
    assert "仍被路由" in r.json()["detail"]
    await client.delete("/admin/routes/r1")
    r = await client.delete("/admin/upstreams/u1")
    assert r.status_code == 204


async def test_updating_upstream_url_resets_health(env):
    client, state1, _, _, srv2 = env
    state1.fail_remaining = 3
    for _ in range(2):  # 两次 GET 各重试一次 => 4 次失败，足够摘除
        await client.get("/svc/ping")
    health = await client.get("/health")
    assert next(
        u for u in health.json()["upstreams"] if u["id"] == "u1"
    )["state"] == "unhealthy"

    # PUT 一个（重新指向正常的）新地址，健康状态应重置为 healthy
    r = await client.put(
        "/admin/upstreams/u1", json={"id": "u1", "base_url": srv2.base_url}
    )
    assert r.status_code == 200
    health = await client.get("/health")
    assert next(
        u for u in health.json()["upstreams"] if u["id"] == "u1"
    )["state"] == "healthy"


async def test_listing_endpoints(env):
    client, *_ = env
    routes = await client.get("/admin/routes")
    assert [r["id"] for r in routes.json()["routes"]] == ["r1"]
    ups = await client.get("/admin/upstreams")
    assert {u["id"] for u in ups.json()["upstreams"]} == {"u1", "u2"}
