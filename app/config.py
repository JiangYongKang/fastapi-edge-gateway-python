"""网关配置：从环境变量读取，全部带默认值。

环境变量（均有默认值，未设置时用默认值）：

- GATEWAY_REQUEST_TIMEOUT      单次上游请求总超时（秒），默认 5.0
- GATEWAY_CONNECT_TIMEOUT      建连超时（秒），默认 1.0
- GATEWAY_HEALTH_THRESHOLD     连续失败多少次后摘除上游，默认 3
- GATEWAY_HEALTH_RECOVER_AFTER 摘除后冷却多久进入半开（秒），默认 10.0
- GATEWAY_PROBE_MAX_INFLIGHT   半开期间允许同时探测的请求数，默认 1
- GATEWAY_RL_CAPACITY          每路由令牌桶容量（突发上限），默认 20
- GATEWAY_RL_REFILL            每路由每秒补充令牌数，默认 10.0
- GATEWAY_MAX_RETRIES          安全请求的最大重试次数（0 = 不重试），默认 1
- GATEWAY_RETRY_BACKOFF        重试前固定退避（秒），默认 0.05
- GATEWAY_INITIAL_ROUTES_FILE  启动时加载的初始路由 JSON 文件，默认无
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 不是合法数字: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"环境变量 {name} 不能为负数: {value}")
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 不是合法整数: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"环境变量 {name} 不能为负数: {value}")
    return value


@dataclass(frozen=True)
class Settings:
    """网关运行配置。"""

    request_timeout: float = 5.0
    connect_timeout: float = 1.0
    health_threshold: int = 3
    health_recover_after: float = 10.0
    health_probe_max_inflight: int = 1
    rate_limit_capacity: int = 20
    rate_limit_refill_per_sec: float = 10.0
    max_retries: int = 1
    retry_backoff: float = 0.05
    initial_routes_file: str | None = None

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            request_timeout=_env_float("GATEWAY_REQUEST_TIMEOUT", 5.0),
            connect_timeout=_env_float("GATEWAY_CONNECT_TIMEOUT", 1.0),
            health_threshold=_env_int("GATEWAY_HEALTH_THRESHOLD", 3),
            health_recover_after=_env_float("GATEWAY_HEALTH_RECOVER_AFTER", 10.0),
            health_probe_max_inflight=max(
                1, _env_int("GATEWAY_PROBE_MAX_INFLIGHT", 1)
            ),
            rate_limit_capacity=_env_int("GATEWAY_RL_CAPACITY", 20),
            rate_limit_refill_per_sec=_env_float("GATEWAY_RL_REFILL", 10.0),
            max_retries=_env_int("GATEWAY_MAX_RETRIES", 1),
            retry_backoff=_env_float("GATEWAY_RETRY_BACKOFF", 0.05),
            initial_routes_file=os.environ.get("GATEWAY_INITIAL_ROUTES_FILE"),
        )
