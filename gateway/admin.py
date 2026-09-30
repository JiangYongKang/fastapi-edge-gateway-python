"""管理 API：运行时增删改路由与上游，查看健康状态与最近尝试日志。

所有变更立即生效，无需重启进程；冲突返回 409 而不是静默覆盖。
"""
from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .models import Route, Upstream
from .routing import (
    RouteConflictError,
    RouteNotFoundError,
    RouteTable,
    UpstreamNotFoundError,
)

ADMIN_PREFIX = "/_gateway"


class UpstreamIn(BaseModel):
    name: str
    base_url: str = Field(pattern=r"^https?://")


class RouteIn(BaseModel):
    prefix: str
    upstreams: list[str]
    rate_limit_qps: float | None = None
    timeout_s: float = 5.0
    max_retries: int = 1


def build_admin_router(gateway) -> APIRouter:
    routes: RouteTable = gateway.routes
    router = APIRouter(prefix=ADMIN_PREFIX, tags=["admin"])

    @router.get("/state")
    async def state():
        return {
            "table": await routes.snapshot(),
            "health": await gateway.health.snapshot(),
        }

    @router.get("/attempts")
    async def attempts():
        return [asdict(a) for a in list(gateway.attempts)]

    @router.post("/upstreams", status_code=201)
    async def add_upstream(body: UpstreamIn):
        try:
            await routes.add_upstream(Upstream(body.name, body.base_url.rstrip("/")))
        except RouteConflictError as e:
            raise HTTPException(409, str(e))
        return {"ok": True}

    @router.delete("/upstreams/{name}")
    async def remove_upstream(name: str):
        try:
            await routes.remove_upstream(name)
        except UpstreamNotFoundError:
            raise HTTPException(404, f"上游不存在: {name}")
        except RouteConflictError as e:
            raise HTTPException(409, str(e))
        return {"ok": True}

    @router.post("/routes", status_code=201)
    async def add_route(body: RouteIn):
        try:
            await routes.add_route(Route(**body.model_dump()))
        except RouteConflictError as e:
            raise HTTPException(409, str(e))
        except UpstreamNotFoundError as e:
            raise HTTPException(422, str(e))
        except ValueError as e:
            raise HTTPException(422, str(e))
        return {"ok": True}

    @router.put("/routes")
    async def update_route(body: RouteIn):
        try:
            await routes.update_route(Route(**body.model_dump()))
        except RouteNotFoundError:
            raise HTTPException(404, f"路由不存在: {body.prefix}")
        except UpstreamNotFoundError as e:
            raise HTTPException(422, str(e))
        return {"ok": True}

    @router.delete("/routes")
    async def remove_route(prefix: str):
        try:
            await routes.remove_route(prefix)
        except RouteNotFoundError:
            raise HTTPException(404, f"路由不存在: {prefix}")
        return {"ok": True}

    return router
