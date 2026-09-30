"""反向代理核心：路由匹配 -> 限流 -> 选健康上游 -> 转发（超时/有限重试）。

可预期性保证：
- 只有幂等方法（GET/HEAD/OPTIONS/PUT/DELETE）会重试，POST/PATCH 等
  有副作用的请求只尝试一次，不会重复打到上游；
- 每次尝试都有独立超时，超时/超限立即失败，不挂起、不泄漏连接
  （响应体完整读取后连接归还连接池）；
- 每次尝试都写一条结构化日志：命中规则、选中上游、结果与失败原因。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque

import httpx
from fastapi import FastAPI, Request, Response

from .health import HealthTracker
from .limiter import RateLimiter
from .models import AttemptLog, FailureKind, RejectReason
from .routing import RouteTable

logger = logging.getLogger("gateway")

IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})

HOP_BY_HOP_HEADERS = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
})


class Gateway:
    def __init__(
        self,
        routes: RouteTable,
        health: HealthTracker,
        limiter: RateLimiter,
        client: httpx.AsyncClient,
        log_buffer_size: int = 1000,
    ) -> None:
        self.routes = routes
        self.health = health
        self.limiter = limiter
        self.client = client
        self._rr_counters: dict[str, int] = {}
        self._rr_lock = asyncio.Lock()
        self.attempts: deque[AttemptLog] = deque(maxlen=log_buffer_size)

    # ---- 主入口 ----------------------------------------------------------

    async def handle(self, request: Request) -> Response:
        path = request.url.path
        matched = await self.routes.match(path)
        if matched is None:
            return self._reject(request, RejectReason.NO_ROUTE, 404)
        route, upstreams = matched

        if route.rate_limit_qps is not None:
            if not await self.limiter.allow(route.prefix, route.rate_limit_qps):
                return self._reject(request, RejectReason.RATE_LIMITED, 429)

        idempotent = request.method in IDEMPOTENT_METHODS
        max_attempts = 1 + (route.max_retries if idempotent else 0)
        body = await request.body()
        headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in HOP_BY_HOP_HEADERS
        }

        tried: set[str] = set()
        last_failure: FailureKind | None = None
        for attempt in range(1, max_attempts + 1):
            upstream = await self._pick_upstream(route.prefix, upstreams, tried)
            if upstream is None:
                # 没有可用上游：若之前已尝试过则按最后失败原因失败，否则 503
                if last_failure is not None:
                    break
                return self._reject(request, RejectReason.NO_HEALTHY_UPSTREAM, 503,
                                    route=route)
            tried.add(upstream.name)
            outcome, status, content, resp_headers, elapsed = await self._forward(
                request, route, upstream.base_url, headers, body
            )
            self._log(route.prefix, upstream.name, outcome, status, elapsed, attempt)

            if outcome == "ok":
                await self.health.record_success(upstream.name)
                return Response(
                    content=content,
                    status_code=status,
                    headers={
                        k: v for k, v in resp_headers.items()
                        if k.lower() not in HOP_BY_HOP_HEADERS
                    },
                )

            kind = FailureKind(outcome)
            last_failure = kind
            await self.health.record_failure(upstream.name, kind)
            # 4xx 不算失败、不重试（_forward 里已归为 ok），这里只剩可重试错误
            if not idempotent:
                break
            # 还有未试过的上游就继续，否则也用尽重试次数后退出

        assert last_failure is not None
        if last_failure is FailureKind.TIMEOUT:
            return self._reject(request, RejectReason.TIMEOUT, 504, route=route)
        return self._reject(request, RejectReason.UPSTREAM_ERROR, 502, route=route)

    # ---- 内部 ------------------------------------------------------------

    async def _pick_upstream(self, prefix, upstreams, tried):
        """轮询选一个健康上游；优先选本轮没试过的。

        只对最终选中的上游调用 try_acquire，避免半开探测名额泄漏。
        """
        async with self._rr_lock:
            idx = self._rr_counters.get(prefix, 0)
            self._rr_counters[prefix] = idx + 1
        untried = [u for u in upstreams if u.name not in tried]
        # 先在没试过的上游里轮询选择；都不健康再考虑已试过的
        for pool in (untried, list(upstreams)):
            n = len(pool)
            if n == 0:
                continue
            for i in range(n):
                u = pool[(idx + i) % n]
                if await self.health.try_acquire(u.name):
                    return u
        return None

    async def _forward(self, request, route, base_url, headers, body):
        """返回 (outcome, status, content, headers, elapsed_ms)。"""
        url = base_url + request.url.path
        if request.url.query:
            url += "?" + request.url.query
        start = time.monotonic()
        try:
            resp = await self.client.request(
                request.method, url, headers=headers, content=body,
                timeout=httpx.Timeout(route.timeout_s),
            )
        except httpx.TimeoutException:
            return (FailureKind.TIMEOUT.value, None, None, None,
                    (time.monotonic() - start) * 1000)
        except httpx.TransportError:
            return (FailureKind.CONNECT_ERROR.value, None, None, None,
                    (time.monotonic() - start) * 1000)
        elapsed = (time.monotonic() - start) * 1000
        if resp.status_code >= 500:
            return (FailureKind.UPSTREAM_5XX.value, resp.status_code,
                    None, None, elapsed)
        # 2xx/3xx/4xx：上游是活的，原样透传
        return ("ok", resp.status_code, resp.content, resp.headers, elapsed)

    def _log(self, prefix, upstream, outcome, status, elapsed_ms, attempt):
        entry = AttemptLog(prefix, upstream, outcome, status,
                           round(elapsed_ms, 2), attempt)
        self.attempts.append(entry)
        log = logger.info if outcome == "ok" else logger.warning
        log("route=%s upstream=%s outcome=%s status=%s elapsed_ms=%.2f attempt=%d",
            prefix, upstream, outcome, status, elapsed_ms, attempt)

    def _reject(self, request, reason: RejectReason, status: int, route=None):
        prefix = route.prefix if route else None
        self._log(prefix or "-", "-", reason.value, status, 0.0, 0)
        return Response(
            content=json.dumps({
                "error": reason.value,
                "path": request.url.path,
                "route": prefix,
            }),
            status_code=status,
            media_type="application/json",
            headers={"X-Gateway-Reason": reason.value},
        )


def create_app(gateway: Gateway, lifespan=None) -> FastAPI:
    from .admin import build_admin_router

    app = FastAPI(title="edge-gateway", lifespan=lifespan)
    app.state.gateway = gateway
    app.include_router(build_admin_router(gateway))

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE",
                                            "PATCH", "HEAD", "OPTIONS"])
    async def proxy_entry(request: Request) -> Response:
        return await gateway.handle(request)

    return app
