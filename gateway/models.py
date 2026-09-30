"""数据模型：路由规则、上游、网关配置。只放类型定义。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class FailureKind(str, Enum):
    """上游失败原因，用于日志与指标区分。"""

    CONNECT_ERROR = "connect_error"
    TIMEOUT = "timeout"
    UPSTREAM_5XX = "upstream_5xx"


class RejectReason(str, Enum):
    """网关快速失败原因。"""

    NO_ROUTE = "no_route"
    NO_HEALTHY_UPSTREAM = "no_healthy_upstream"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    UPSTREAM_ERROR = "upstream_error"


@dataclass(frozen=True)
class Upstream:
    """一个本地上游服务实例。"""

    name: str
    base_url: str  # 例如 "http://127.0.0.1:9001"


@dataclass
class Route:
    """一条路由规则：path 前缀 -> 一组上游。"""

    prefix: str
    upstreams: list[str]  # 上游名字列表
    rate_limit_qps: float | None = None  # None 表示不限流
    timeout_s: float = 5.0
    max_retries: int = 1  # 仅对幂等方法生效


@dataclass
class GatewayConfig:
    """健康检查与重试的全局配置。"""

    failure_threshold: int = 3  # 连续失败多少次后摘除
    cooldown_s: float = 5.0  # 摘除后多久进入半开
    half_open_max_probe: int = 1  # 半开时允许同时探测的请求数


@dataclass
class AttemptLog:
    """单次转发尝试的记录（写入日志）。"""

    route_prefix: str
    upstream: str
    outcome: str  # "ok" / FailureKind / RejectReason
    status_code: int | None
    elapsed_ms: float
    attempt: int
