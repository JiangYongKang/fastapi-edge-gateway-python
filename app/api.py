"""HTTP 入口：管理 API、全方法转发入口、状态查询与统一错误处理。

路由分为两类（管理接口先注册，因此优先于兜底转发规则）：

* ``/admin/...`` 路由/上游的运行时增删改查；
* ``/{full_path}`` 兜底转发，按路由表最长前缀匹配后交给代理编排器。

所有错误都返回统一 JSON：``{"reason": ..., "detail": ...}`` 并带
``X-Gateway-Reason`` 响应头，原因可区分、可解释。
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import Settings
from app.health import HealthRegistry, validate_base_url
from app.proxy import GatewayError, ProxyOrchestrator
from app.ratelimit import RateLimiter
from app.routing import RouteRule, RoutingError, RoutingTable, UpstreamDef

logger = logging.getLogger("gateway")

_ADMIN_PREFIX = "/admin"


# ----------------------------------------------------------------------
# 请求/响应模型
# ----------------------------------------------------------------------


class UpstreamIn(BaseModel):
    id: str = Field(min_length=1)
    base_url: str = Field(min_length=1)


class RouteIn(BaseModel):
    id: str = Field(min_length=1)
    prefix: str
    upstream_ids: list[str] = Field(min_length=1)
    timeout: float | None = None
    max_retries: int | None = None
    rate_limit_capacity: int | None = None
    rate_limit_refill_per_sec: float | None = None


def _configure_logging() -> None:
    gateway_logger = logging.getLogger("gateway")
    if not gateway_logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-7s %(name)s | %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        gateway_logger.addHandler(handler)
        gateway_logger.setLevel(logging.INFO)
        gateway_logger.propagate = False


def _error_body(reason: str, detail: str) -> dict:
    return {"reason": reason, "detail": detail}


def create_app(settings: Settings | None = None) -> FastAPI:
    """应用工厂：装配路由表、限流器、健康注册中心、代理编排器。"""
    settings = settings or Settings.from_env()
    _configure_logging()

    routing = RoutingTable()
    rate_limiter = RateLimiter(
        settings.rate_limit_capacity, settings.rate_limit_refill_per_sec
    )
    health = HealthRegistry(
        threshold=settings.health_threshold,
        recover_after=settings.health_recover_after,
        probe_max_inflight=settings.health_probe_max_inflight,
    )
    orchestrator = ProxyOrchestrator(routing, health, rate_limiter, settings)


    # 上游目录是管理面的唯一写入源，同时镜像注册进健康中心。
    catalog_lock = asyncio.Lock()
    catalog: dict[str, UpstreamDef] = {}

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if settings.initial_routes_file:
            await _load_initial_file(settings.initial_routes_file)
        try:
            yield
        finally:
            await orchestrator.aclose()

    app = FastAPI(title="local-edge-gateway", lifespan=lifespan)

    # ---- 文件加载 -----------------------------------------------------

    async def _load_initial_file(path: str) -> None:
        def _read() -> dict:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)

        payload = await asyncio.to_thread(_read)
        async with catalog_lock:
            for item in payload.get("upstreams", []):
                definition = UpstreamDef(
                    id=item["id"],
                    base_url=validate_base_url(item["id"], item["base_url"]),
                )
                catalog[definition.id] = definition
                await health.register(definition)
            for item in payload.get("routes", []):
                rule = _build_rule(RouteIn(**item))
                _assert_upstreams_exist(rule, catalog)
                routing.add(rule)
        logger.info(
            "从 %s 加载 %d 个上游、%d 条路由",
            path, len(payload.get("upstreams", [])), len(payload.get("routes", [])),
        )

    # ---- 管理面：上游 -------------------------------------------------

    @app.get("/admin/upstreams")
    async def list_upstreams() -> dict:
        statuses = {s.upstream_id: s.state.value for s in await health.all_status()}
        return {
            "upstreams": [
                {
                    "id": uid,
                    "base_url": definition.base_url,
                    "state": statuses.get(uid, "unknown"),
                }
                for uid, definition in sorted(catalog.items())
            ]
        }

    @app.post("/admin/upstreams", status_code=201)
    async def create_upstream(payload: UpstreamIn) -> dict:
        async with catalog_lock:
            if payload.id in catalog:
                raise RoutingError(
                    f"上游 id 已存在: {payload.id!r}；如需修改请用 PUT"
                )
            definition = UpstreamDef(
                id=payload.id,
                base_url=validate_base_url(payload.id, payload.base_url),
            )
            catalog[definition.id] = definition
            await health.register(definition)
        logger.info("管理面：新增上游 %s -> %s", definition.id, definition.base_url)
        return {"id": definition.id, "base_url": definition.base_url}

    @app.put("/admin/upstreams/{upstream_id}")
    async def replace_upstream(upstream_id: str, payload: UpstreamIn) -> dict:
        if payload.id != upstream_id:
            raise RoutingError("路径中的上游 id 与请求体不一致")
        async with catalog_lock:
            definition = UpstreamDef(
                id=upstream_id,
                base_url=validate_base_url(upstream_id, payload.base_url),
            )
            catalog[upstream_id] = definition
            await health.register(definition)  # 覆盖目标视为新上游，状态重置
        logger.info("管理面：更新上游 %s -> %s（健康状态已重置）", upstream_id, definition.base_url)
        return {"id": definition.id, "base_url": definition.base_url}

    @app.delete("/admin/upstreams/{upstream_id}", status_code=204)
    async def delete_upstream(upstream_id: str) -> None:
        async with catalog_lock:
            if upstream_id not in catalog:
                raise RoutingError(f"上游不存在: {upstream_id!r}", status_code=404)
            referenced = [
                rule.id for rule in routing.list_rules() if upstream_id in rule.upstream_ids
            ]
            if referenced:
                raise RoutingError(
                    f"上游 {upstream_id!r} 仍被路由 {referenced} 引用，请先改路由"
                )
            del catalog[upstream_id]
            await health.unregister(upstream_id)
        logger.info("管理面：删除上游 %s", upstream_id)

    # ---- 管理面：路由 -------------------------------------------------

    @app.get("/admin/routes")
    async def list_routes() -> dict:
        return {"routes": [_rule_to_dict(rule) for rule in routing.list_rules()]}

    @app.post("/admin/routes", status_code=201)
    async def create_route(payload: RouteIn) -> dict:
        rule = _build_rule(payload)
        async with catalog_lock:
            _assert_upstreams_exist(rule, catalog)
            routing.add(rule)
        logger.info("管理面：新增路由 %s prefix=%s upstreams=%s",
                    rule.id, rule.prefix, rule.upstream_ids)
        return _rule_to_dict(rule)

    @app.put("/admin/routes/{rule_id}")
    async def replace_route(rule_id: str, payload: RouteIn) -> dict:
        if payload.id != rule_id:
            raise RoutingError("路径中的规则 id 与请求体不一致")
        rule = _build_rule(payload)
        async with catalog_lock:
            _assert_upstreams_exist(rule, catalog)
            routing.update(rule)
        logger.info("管理面：更新路由 %s prefix=%s upstreams=%s",
                    rule.id, rule.prefix, rule.upstream_ids)
        return _rule_to_dict(rule)

    @app.delete("/admin/routes/{rule_id}", status_code=204)
    async def delete_route(rule_id: str) -> None:
        routing.delete(rule_id)
        logger.info("管理面：删除路由 %s", rule_id)

    # ---- 状态查询 -----------------------------------------------------

    @app.get("/health")
    async def health_overview() -> dict:
        statuses = await health.all_status()
        return {
            "upstreams": [
                {
                    "id": s.upstream_id,
                    "state": s.state.value,
                    "consecutive_failures": s.consecutive_failures,
                    "opened_at": s.opened_at,
                    "last_error": s.last_error,
                }
                for s in statuses
            ]
        }

    @app.get("/")
    async def root() -> dict:
        return {
            "service": "local-edge-gateway",
            "admin": "/admin/routes, /admin/upstreams",
            "health": "/health",
        }

    # ---- 兜底转发（必须最后注册，避免吞掉 /admin 与 /health） ----------

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def proxy_entry(request: Request, full_path: str):
        path = "/" + full_path
        matched = routing.match(path)
        if matched is None:
            logger.info("未命中任何路由: %s %s", request.method, path)
            raise GatewayError("no_route", 404, f"没有路由匹配 {path}")
        client_ip = _client_ip(request)
        logger.info(
            "命中路由 rule=%s prefix=%s path=%s client=%s",
            matched.rule.id, matched.rule.prefix, path, client_ip,
        )
        return await orchestrator.forward(request, matched, client_ip)

    # ---- 统一错误处理 -------------------------------------------------

    @app.exception_handler(GatewayError)
    async def _on_gateway_error(_request: Request, exc: GatewayError):
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_body(exc.reason, exc.detail),
            headers={"X-Gateway-Reason": exc.reason},
        )

    @app.exception_handler(RoutingError)
    async def _on_routing_error(_request: Request, exc: RoutingError):
        # 含"不存在"语义的用 404，冲突/重复/引用校验用 409。
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_body("routing_error", str(exc)),
            headers={"X-Gateway-Reason": "routing_error"},
        )

    @app.exception_handler(ValueError)
    async def _on_value_error(_request: Request, exc: ValueError):
        return JSONResponse(
            status_code=400,
            content=_error_body("invalid_config", str(exc)),
            headers={"X-Gateway-Reason": "invalid_config"},
        )

    @app.exception_handler(RequestValidationError)
    async def _on_validation_error(_request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content=_error_body("invalid_payload", json.loads(exc.json())),
            headers={"X-Gateway-Reason": "invalid_payload"},
        )

    app.state.health_registry = health
    app.state.routing_table = routing
    app.state.settings = settings
    return app


# ----------------------------------------------------------------------
# 辅助函数
# ----------------------------------------------------------------------


def _build_rule(payload: RouteIn) -> RouteRule:
    return RouteRule(
        id=payload.id,
        prefix=payload.prefix,
        upstream_ids=tuple(payload.upstream_ids),
        timeout=payload.timeout,
        max_retries=payload.max_retries,
        rate_limit_capacity=payload.rate_limit_capacity,
        rate_limit_refill_per_sec=payload.rate_limit_refill_per_sec,
    )


def _assert_upstreams_exist(rule: RouteRule, catalog: dict[str, UpstreamDef]) -> None:
    missing = [uid for uid in rule.upstream_ids if uid not in catalog]
    if missing:
        raise RoutingError(f"规则 {rule.id!r} 引用了不存在的上游: {missing}")


def _rule_to_dict(rule: RouteRule) -> dict:
    return {
        "id": rule.id,
        "prefix": rule.prefix,
        "upstream_ids": list(rule.upstream_ids),
        "timeout": rule.timeout,
        "max_retries": rule.max_retries,
        "rate_limit_capacity": rule.rate_limit_capacity,
        "rate_limit_refill_per_sec": rule.rate_limit_refill_per_sec,
    }


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"
