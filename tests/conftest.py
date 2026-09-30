"""测试基建：用 MockTransport 模拟上游，用 ASGITransport 打网关。"""
from __future__ import annotations

import httpx
import pytest

from gateway.health import HealthTracker
from gateway.limiter import RateLimiter
from gateway.models import GatewayConfig, Route, Upstream
from gateway.proxy import Gateway, create_app
from gateway.routing import RouteTable


class UpstreamMocker:
    """按 host 分发处理函数，模拟多个本地上游。"""

    def __init__(self) -> None:
        self.handlers: dict[str, callable] = {}
        self.requests: list[httpx.Request] = []

    def set(self, base_url: str, handler) -> None:
        host = httpx.URL(base_url).netloc.decode()
        self.handlers[host] = handler

    def count(self, base_url: str) -> int:
        host = httpx.URL(base_url).netloc.decode()
        return sum(1 for r in self.requests if r.url.netloc.decode() == host)

    def transport(self) -> httpx.MockTransport:
        def dispatch(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            handler = self.handlers.get(request.url.netloc.decode())
            if handler is None:
                return httpx.Response(599, text="no mock")
            result = handler(request)
            if isinstance(result, Exception):
                raise result
            return result
        return httpx.MockTransport(dispatch)


@pytest.fixture
def mocker() -> UpstreamMocker:
    return UpstreamMocker()


@pytest.fixture
async def gw_client(mocker):
    """工厂：传入路由/上游，返回 (gateway, 打网关的 client)。"""
    created = []

    async def factory(routes, upstreams, config=None):
        table = RouteTable()
        health = HealthTracker(config or GatewayConfig(
            failure_threshold=3, cooldown_s=0.2, half_open_max_probe=1))
        limiter = RateLimiter()
        http = httpx.AsyncClient(transport=mocker.transport())
        gw = Gateway(table, health, limiter, http)
        for u in upstreams:
            await table.add_upstream(u)
        for r in routes:
            await table.add_route(r)
        app = create_app(gw)
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw")
        created.append((client, http))
        return gw, client

    yield factory
    for client, http in created:
        await client.aclose()
        await http.aclose()
