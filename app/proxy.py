"""代理核心：限流检查 -> 选上游 -> 带超时转发 -> 健康反馈与重试。

失败原因码（响应头 ``X-Gateway-Reason`` 与日志中一致，可区分）：

* ``no_route``           没有任何规则命中（404）
* ``rate_limited``       令牌桶耗尽（429）
* ``no_healthy_upstream``规则下所有上游均不可用（503）
* ``upstream_timeout``   上游读/总超时（504）
* ``upstream_connect_error`` 与上游建连失败（502）
* ``upstream_5xx``       上游返回 5xx（透传其状态码）

重试纪律：只有 GET/HEAD/OPTIONS 这类无副作用的安全方法才重试；
POST/PUT/PATCH/DELETE 等绝不重试，同一次请求不会被重复打到上游。
每次转发都受独立超时约束，连接由共享的异步客户端统一管理并在关闭时释放，
不挂起、不泄漏。
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx
from fastapi import Request
from fastapi.responses import Response

from app.config import Settings
from app.health import FailureKind, HealthRegistry
from app.routing import MatchedRoute, UpstreamDef

logger = logging.getLogger("gateway.proxy")

# RFC 9110 定义的安全（只读、无副作用）方法。
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

_REASON_STATUS = {
    "no_route": 404,
    "rate_limited": 429,
    "no_healthy_upstream": 503,
    "upstream_timeout": 504,
    "upstream_connect_error": 502,
    "upstream_5xx": 502,
}


class GatewayError(Exception):
    """网关自身错误，携带可解释的原因码与 HTTP 状态码。"""

    def __init__(self, reason: str, status_code: int, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.status_code = status_code
        self.detail = detail


class UpstreamCallError(Exception):
    """单次上游调用失败：区分 5xx / 连接失败 / 超时。"""

    def __init__(self, kind: FailureKind, detail: str, status_code: int | None = None):
        super().__init__(detail)
        self.kind = kind
        self.detail = detail
        self.upstream_status = status_code


class ProxyOrchestrator:
    """串起路由、限流、健康、HTTP 转发与重试。"""

    def __init__(
        self,
        routing,
        health: HealthRegistry,
        rate_limiter,
        settings: Settings,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._routing = routing
        self._health = health
        self._limiter = rate_limiter
        self._settings = settings
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                settings.request_timeout, connect=settings.connect_timeout
            ),
            follow_redirects=False,
        )
        self._now = time.monotonic

    def set_clock(self, now_provider) -> None:
        """注入时钟（测试用）。"""
        self._now = now_provider

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    # ---- 主流程 -------------------------------------------------------

    async def forward(
        self,
        request: Request,
        matched: MatchedRoute,
        client_ip: str,
    ) -> Response:
        rule = matched.rule
        now = self._now()

        # 1) 限流：按 规则 + 客户端 维度，非阻塞、快速失败。
        rl_key = f"{rule.id}:{client_ip}"
        if not await self._limiter.allow(
            rl_key,
            now,
            capacity=rule.rate_limit_capacity,
            refill_per_sec=rule.rate_limit_refill_per_sec,
        ):
            available = await self._limiter.available(
                rl_key,
                now,
                capacity=rule.rate_limit_capacity,
                refill_per_sec=rule.rate_limit_refill_per_sec,
            )
            logger.warning(
                "route=%s client=%s 限流拒绝 reason=rate_limited available=%.2f",
                rule.id, client_ip, available,
            )
            raise GatewayError(
                "rate_limited",
                _REASON_STATUS["rate_limited"],
                f"规则 {rule.id} 对客户端 {client_ip} 的令牌已耗尽",
            )

        # 2) 决定尝试次数：只有安全方法允许重试。
        max_retries = (
            rule.max_retries
            if rule.max_retries is not None
            else self._settings.max_retries
        )
        method = request.method.upper()
        retriable = method in _SAFE_METHODS
        attempts_budget = (1 + max_retries) if retriable else 1
        if not retriable and max_retries > 0:
            logger.info(
                "route=%s method=%s 非幂等请求，禁止重试", rule.id, method
            )

        body = await request.body()
        last_error: UpstreamCallError | None = None
        tried: list[str] = []

        # 3) 选上游并转发（可能多次尝试，但每次都用不同的许可/上游选择）。
        for attempt in range(attempts_budget):
            now = self._now()
            picked = await self._health.select(rule.upstream_ids, now)
            if picked is None:
                logger.error(
                    "route=%s 无可用上游 reason=no_healthy_upstream tried=%s",
                    rule.id, tried,
                )
                raise GatewayError(
                    "no_healthy_upstream",
                    _REASON_STATUS["no_healthy_upstream"],
                    f"规则 {rule.id} 下的上游均不可用",
                )
            upstream_id, upstream = picked
            tried.append(upstream_id)
            try:
                response = await self._call_upstream(
                    request, matched, body, upstream_id, upstream, attempt
                )
            except UpstreamCallError as exc:
                last_error = exc
                now = self._now()
                state = await self._health.report_failure(
                    upstream_id, exc.kind, now, exc.detail
                )
                logger.warning(
                    "route=%s upstream=%s attempt=%d/%d 失败 reason=%s state=%s detail=%s",
                    rule.id, upstream_id, attempt + 1, attempts_budget,
                    exc.kind.value, state.value, exc.detail,
                )
                await self._health.release(upstream_id)
                will_retry = retriable and attempt + 1 < attempts_budget
                if will_retry and self._settings.retry_backoff > 0:
                    await asyncio.sleep(self._settings.retry_backoff)
                if not will_retry:
                    break
                continue
            else:
                await self._health.report_success(upstream_id, self._now())
                await self._health.release(upstream_id)
                logger.info(
                    "route=%s upstream=%s attempt=%d 成功 status=%d",
                    rule.id, upstream_id, attempt + 1, response.status_code,
                )
                return response

        # 4) 所有尝试用尽：按最后一次的失败原因给出可区分的快速失败。
        assert last_error is not None
        if last_error.kind is FailureKind.UPSTREAM_5XX:
            reason = "upstream_5xx"
        elif last_error.kind is FailureKind.TIMEOUT:
            reason = "upstream_timeout"
        else:
            reason = "upstream_connect_error"
        raise GatewayError(
            reason,
            _REASON_STATUS[reason],
            f"上游 {tried[-1]} 返回 {last_error.kind.value}: {last_error.detail}",
        )

    # ---- 单次上游调用 -------------------------------------------------

    async def _call_upstream(
        self,
        request: Request,
        matched: MatchedRoute,
        body: bytes,
        upstream_id: str,
        upstream: UpstreamDef,
        attempt: int,
    ) -> Response:
        rule = matched.rule
        timeout = (
            rule.timeout
            if rule.timeout is not None
            else self._settings.request_timeout
        )
        target_path = matched.remaining_path or "/"
        url = f"{upstream.base_url}{target_path}"
        if request.url.query:
            url = f"{url}?{request.url.query}"

        # 转发请求头，剔除逐跳头与 host，避免把网关自身的长度/编码带给上游。
        headers = self._build_request_headers(request, upstream_id)

        logger.info(
            "route=%s upstream=%s attempt=%d 转发 %s %s timeout=%.2f",
            rule.id, upstream_id, attempt + 1, request.method, url, timeout,
        )
        try:
            upstream_response = await self._client.request(
                request.method,
                url,
                headers=headers,
                content=body,
                timeout=httpx.Timeout(timeout, connect=self._settings.connect_timeout),
            )
        except httpx.TimeoutException as exc:
            raise UpstreamCallError(FailureKind.TIMEOUT, str(exc)) from exc
        except httpx.ConnectError as exc:
            raise UpstreamCallError(FailureKind.CONNECT_ERROR, str(exc)) from exc
        except httpx.TransportError as exc:
            # 其它传输层问题（如连接被重置）归入连接类失败，但保留原文。
            raise UpstreamCallError(FailureKind.CONNECT_ERROR, str(exc)) from exc

        # 读取响应体后立即归还连接到连接池，避免连接泄漏。
        content = upstream_response.content
        if 500 <= upstream_response.status_code <= 599:
            raise UpstreamCallError(
                FailureKind.UPSTREAM_5XX,
                f"HTTP {upstream_response.status_code}",
                status_code=upstream_response.status_code,
            )

        return Response(
            content=content,
            status_code=upstream_response.status_code,
            headers=self._build_response_headers(upstream_response, rule.id, upstream_id),
            media_type=upstream_response.headers.get("content-type"),
        )

    @staticmethod
    def _build_request_headers(request: Request, upstream_id: str) -> dict[str, str]:
        hop_by_hop = {
            "host", "connection", "keep-alive", "proxy-authenticate",
            "proxy-authorization", "te", "trailers", "transfer-encoding",
            "upgrade",
        }
        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in hop_by_hop
        }
        headers["X-Forwarded-For"] = request.client.host if request.client else "-"
        headers["X-Gateway-Upstream"] = upstream_id
        return headers

    @staticmethod
    def _build_response_headers(
        upstream_response: httpx.Response, rule_id: str, upstream_id: str
    ) -> dict[str, str]:
        excluded = {
            "content-length", "content-encoding", "transfer-encoding",
            "connection",
        }
        headers = {
            k: v for k, v in upstream_response.headers.items() if k.lower() not in excluded
        }
        headers["X-Gateway-Route"] = rule_id
        headers["X-Gateway-Upstream"] = upstream_id
        return headers
