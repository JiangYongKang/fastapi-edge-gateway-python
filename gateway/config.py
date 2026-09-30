"""网关配置：环境变量覆盖，带默认值。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from .models import GatewayConfig, Route, Upstream


@dataclass
class Settings:
    host: str = "127.0.0.1"
    port: int = 8000
    gateway: GatewayConfig = field(default_factory=GatewayConfig)
    # 初始路由/上游（运行时可通过管理 API 修改）
    initial_upstreams: list[Upstream] = field(default_factory=list)
    initial_routes: list[Route] = field(default_factory=list)


def load_settings() -> Settings:
    """从环境变量加载；GATEWAY_ROUTES 格式:
    prefix=up1,up2;qps;timeout;retries 多条用空格分隔，例如：
    GATEWAY_UPSTREAMS="u1=http://127.0.0.1:9001 u2=http://127.0.0.1:9002"
    GATEWAY_ROUTES="/api=u1,u2;10;2.0;1 /web=u1"
    """
    s = Settings()
    s.host = os.environ.get("GATEWAY_HOST", s.host)
    s.port = int(os.environ.get("GATEWAY_PORT", s.port))
    s.gateway.failure_threshold = int(
        os.environ.get("GATEWAY_FAILURE_THRESHOLD", s.gateway.failure_threshold))
    s.gateway.cooldown_s = float(
        os.environ.get("GATEWAY_COOLDOWN_S", s.gateway.cooldown_s))
    s.gateway.half_open_max_probe = int(
        os.environ.get("GATEWAY_HALF_OPEN_MAX_PROBE", s.gateway.half_open_max_probe))

    for item in os.environ.get("GATEWAY_UPSTREAMS", "").split():
        name, _, url = item.partition("=")
        if name and url:
            s.initial_upstreams.append(Upstream(name, url.rstrip("/")))
    for item in os.environ.get("GATEWAY_ROUTES", "").split():
        prefix, _, spec = item.partition("=")
        parts = spec.split(";")
        ups = [u for u in parts[0].split(",") if u]
        qps = float(parts[1]) if len(parts) > 1 and parts[1] else None
        timeout = float(parts[2]) if len(parts) > 2 and parts[2] else 5.0
        retries = int(parts[3]) if len(parts) > 3 and parts[3] else 1
        if prefix and ups:
            s.initial_routes.append(Route(prefix, ups, qps, timeout, retries))
    return s
