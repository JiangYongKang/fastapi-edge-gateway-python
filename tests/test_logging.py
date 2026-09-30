"""日志可解释性：命中规则、选中上游、失败/限流原因都要出现在日志里。"""

import logging

import pytest


@pytest.fixture
async def env(gateway, upstream_factory):
    runner, _app, _settings = gateway
    state, server = upstream_factory()
    cm = runner()
    client, _app = await cm.__aenter__()
    try:
        await client.post(
            "/admin/upstreams", json={"id": "uA", "base_url": server.base_url}
        )
        await client.post(
            "/admin/routes",
            json={
                "id": "routeA",
                "prefix": "/svc",
                "upstream_ids": ["uA"],
                "rate_limit_capacity": 100,
                "rate_limit_refill_per_sec": 1000,
                "max_retries": 0,
            },
        )
        yield client, state
    finally:
        await cm.__aexit__(None, None, None)


class _Collector(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(self.format(record))


async def test_logs_show_route_upstream_and_success(env):
    client, _state = env
    collector = _Collector()
    collector.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger("gateway")
    root.addHandler(collector)
    try:
        r = await client.get("/svc/hello")
        assert r.status_code == 200
        text = "\n".join(collector.lines)
        assert "命中路由 rule=routeA" in text
        assert "upstream=uA" in text
        assert "转发 GET" in text and "/hello" in text
        assert "成功 status=200" in text
    finally:
        root.removeHandler(collector)


async def test_logs_show_distinct_failure_reasons(env):
    client, state = env
    collector = _Collector()
    collector.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger("gateway")
    root.addHandler(collector)
    try:
        state.fail_remaining = 5
        r = await client.get("/svc/boom")
        assert r.status_code == 502
        text = "\n".join(collector.lines)
        assert "reason=upstream_5xx" in text
        assert "rule=routeA" in text and "upstream=uA" in text

        # 继续打满阈值后，无可用上游的原因也要有日志
        collector.lines.clear()
        for _ in range(3):
            await client.get("/svc/boom")
        text = "\n".join(collector.lines)
        assert "reason=no_healthy_upstream" in text
        assert "摘除熔断" in text
    finally:
        root.removeHandler(collector)


async def test_logs_show_rate_limited_reason(env):
    client, _state = env
    collector = _Collector()
    collector.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger("gateway")
    root.addHandler(collector)
    try:
        await client.put(
            "/admin/routes/routeA",
            json={
                "id": "routeA",
                "prefix": "/svc",
                "upstream_ids": ["uA"],
                "rate_limit_capacity": 1,
                "rate_limit_refill_per_sec": 0.0001,
                "max_retries": 0,
            },
        )
        assert (await client.get("/svc/a")).status_code == 200
        rejected = await client.get("/svc/a")
        assert rejected.status_code == 429
        text = "\n".join(collector.lines)
        assert "限流拒绝 reason=rate_limited" in text
        assert "route=routeA" in text
    finally:
        root.removeHandler(collector)
