"""路由表：运行时增删改，最长前缀匹配，冲突显式报错。"""
from __future__ import annotations

import asyncio

from .models import Route, Upstream


class RouteConflictError(ValueError):
    """重复或冲突的路由规则。"""


class RouteNotFoundError(KeyError):
    pass


class UpstreamNotFoundError(KeyError):
    pass


def _normalize_prefix(prefix: str) -> str:
    if not prefix.startswith("/"):
        raise ValueError(f"prefix 必须以 / 开头: {prefix!r}")
    if len(prefix) > 1 and prefix.endswith("/"):
        prefix = prefix.rstrip("/")
    if not prefix:
        raise ValueError("prefix 不能为空")
    return prefix


class RouteTable:
    """协程安全的路由表。

    - 前缀规范化后必须唯一，重复 add 报 RouteConflictError；
    - 匹配采用最长前缀优先，结果确定；
    - 路由引用的上游必须已存在。
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._routes: dict[str, Route] = {}
        self._upstreams: dict[str, Upstream] = {}

    async def add_upstream(self, upstream: Upstream) -> None:
        async with self._lock:
            if upstream.name in self._upstreams:
                raise RouteConflictError(f"上游已存在: {upstream.name}")
            self._upstreams[upstream.name] = upstream

    async def remove_upstream(self, name: str) -> None:
        async with self._lock:
            if name not in self._upstreams:
                raise UpstreamNotFoundError(name)
            users = [p for p, r in self._routes.items() if name in r.upstreams]
            if users:
                raise RouteConflictError(
                    f"上游 {name} 仍被路由引用: {users}"
                )
            del self._upstreams[name]

    async def add_route(self, route: Route) -> None:
        route.prefix = _normalize_prefix(route.prefix)
        async with self._lock:
            if route.prefix in self._routes:
                raise RouteConflictError(f"路由前缀已存在: {route.prefix}")
            self._check_upstreams(route)
            self._routes[route.prefix] = route

    async def update_route(self, route: Route) -> None:
        route.prefix = _normalize_prefix(route.prefix)
        async with self._lock:
            if route.prefix not in self._routes:
                raise RouteNotFoundError(route.prefix)
            self._check_upstreams(route)
            self._routes[route.prefix] = route

    async def remove_route(self, prefix: str) -> None:
        prefix = _normalize_prefix(prefix)
        async with self._lock:
            if prefix not in self._routes:
                raise RouteNotFoundError(prefix)
            del self._routes[prefix]

    async def match(self, path: str) -> tuple[Route, list[Upstream]] | None:
        """最长前缀匹配；前缀边界必须在路径段上（/api 不匹配 /apix）。"""
        async with self._lock:
            best: Route | None = None
            for prefix, route in self._routes.items():
                if path == prefix or path.startswith(prefix + "/") or prefix == "/":
                    if best is None or len(prefix) > len(best.prefix):
                        best = route
            if best is None:
                return None
            ups = [self._upstreams[n] for n in best.upstreams if n in self._upstreams]
            return best, ups

    async def snapshot(self) -> dict:
        async with self._lock:
            return {
                "routes": {p: vars(r) for p, r in self._routes.items()},
                "upstreams": {n: vars(u) for n, u in self._upstreams.items()},
            }

    def _check_upstreams(self, route: Route) -> None:
        if not route.upstreams:
            raise RouteConflictError(f"路由 {route.prefix} 至少需要一个上游")
        missing = [n for n in route.upstreams if n not in self._upstreams]
        if missing:
            raise UpstreamNotFoundError(
                f"路由 {route.prefix} 引用了不存在的上游: {missing}"
            )
