"""上游健康管理：失败计数、熔断摘除、冷却后半开探测、自动恢复。

状态机（每次迁移都在同一把 asyncio 锁内完成，事件循环里不会交错）：

    HEALTHY --连续失败达到阈值--> UNHEALTHY --冷却到期且有请求来探测--> HALF_OPEN
    HALF_OPEN --探测成功--> HEALTHY
    HALF_OPEN --探测失败--> UNHEALTHY（重新计时冷却）

设计要点：

* 半开期间最多 ``probe_max_inflight`` 个探测请求在途，其余请求直接被拒，
  不会把整个流量灌给待验证的上游。
* 冷却到期是惰性判定：在 ``acquire`` 时拿当前时间比较，不依赖后台任务，
  行为可解释、可测试。
* 摘除期间才返回的"陈旧在途请求"不允许把状态改回健康，也不刷新冷却计时，
  杜绝并发下的状态回跳。
* ``5xx`` / 连接失败 / 超时三类失败都会计入失败数，但分别记录原因码，
  日志与状态快照里可区分。
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse

from app.routing import UpstreamDef

logger = logging.getLogger("gateway.health")


class HealthState(str, Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    HALF_OPEN = "half_open"


class FailureKind(str, Enum):
    """可区分的失败原因。"""

    UPSTREAM_5XX = "upstream_5xx"
    CONNECT_ERROR = "connect_error"
    TIMEOUT = "timeout"


@dataclass(frozen=True)
class UpstreamStatus:
    """对外暴露的某上游状态快照。"""

    upstream_id: str
    state: HealthState
    consecutive_failures: int
    opened_at: float | None
    last_error: str | None


@dataclass
class _Record:
    upstream: UpstreamDef
    state: HealthState = HealthState.HEALTHY
    failures: int = 0
    opened_at_mono: float | None = None
    opened_at_wall: float | None = None
    probes_inflight: int = 0
    last_error: str | None = None


def validate_base_url(upstream_id: str, base_url: str) -> str:
    """校验上游地址，返回去掉结尾 / 的规范化地址。"""
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"上游 {upstream_id!r} 的地址必须是 http(s) URL: {base_url!r}")
    if not parsed.netloc:
        raise ValueError(f"上游 {upstream_id!r} 的地址缺少主机: {base_url!r}")
    return base_url.rstrip("/")


class HealthRegistry:
    """所有上游的健康状态机。"""

    def __init__(
        self,
        threshold: int = 3,
        recover_after: float = 10.0,
        probe_max_inflight: int = 1,
    ) -> None:
        if threshold < 1:
            raise ValueError("health threshold 必须 >= 1")
        if recover_after < 0:
            raise ValueError("recover_after 不能为负")
        self._threshold = threshold
        self._recover_after = recover_after
        self._probe_max = max(1, probe_max_inflight)
        self._records: dict[str, _Record] = {}
        self._lock = asyncio.Lock()
        self._rr = itertools.count()

    # ---- 上游生命周期 -------------------------------------------------

    async def register(self, upstream: UpstreamDef) -> None:
        """注册或覆盖一个上游；覆盖视为新目标，健康状态重置。"""
        base_url = validate_base_url(upstream.id, upstream.base_url)
        normalized = UpstreamDef(id=upstream.id, base_url=base_url)
        async with self._lock:
            existed = upstream.id in self._records
            self._records[upstream.id] = _Record(upstream=normalized)
        if existed:
            logger.warning("upstream %s 被覆盖，健康状态已重置", upstream.id)

    async def unregister(self, upstream_id: str) -> None:
        async with self._lock:
            self._records.pop(upstream_id, None)

    async def get_definition(self, upstream_id: str) -> UpstreamDef:
        async with self._lock:
            if upstream_id not in self._records:
                raise KeyError(f"上游未注册: {upstream_id!r}")
            return self._records[upstream_id].upstream

    # ---- 请求许可 -----------------------------------------------------

    async def acquire(self, upstream_id: str, now: float) -> bool:
        """尝试取得该上游的请求许可（健康放行 / 冷却拒绝 / 半开探测名额）。"""
        async with self._lock:
            record = self._records.get(upstream_id)
            if record is None:
                return False
            if record.state is HealthState.HEALTHY:
                return True
            if record.state is HealthState.UNHEALTHY:
                assert record.opened_at_mono is not None
                if now - record.opened_at_mono >= self._recover_after:
                    record.state = HealthState.HALF_OPEN
                    record.probes_inflight = 1
                    logger.info(
                        "upstream %s 冷却到期，进入 half_open 放行探测请求",
                        upstream_id,
                    )
                    return True
                return False
            # HALF_OPEN：仅放有限的探测名额
            if record.probes_inflight < self._probe_max:
                record.probes_inflight += 1
                return True
            return False

    async def release(self, upstream_id: str) -> None:
        """归还半开探测名额（健康状态下的普通请求不占名额）。"""
        async with self._lock:
            record = self._records.get(upstream_id)
            if record is not None and record.state is HealthState.HALF_OPEN:
                record.probes_inflight = max(0, record.probes_inflight - 1)

    # ---- 结果反馈 -----------------------------------------------------

    async def report_success(self, upstream_id: str, now: float) -> bool:
        """报告一次成功；返回反馈后该上游是否处于健康态。"""
        async with self._lock:
            record = self._records.get(upstream_id)
            if record is None:
                return False
            if record.state is HealthState.HALF_OPEN:
                record.state = HealthState.HEALTHY
                record.failures = 0
                record.opened_at_mono = None
                record.opened_at_wall = None
                record.probes_inflight = 0
                record.last_error = None
                logger.info("upstream %s 探测成功，恢复为 healthy", upstream_id)
            elif record.state is HealthState.HEALTHY:
                # 健康期的成功把连续失败计数清零。
                record.failures = 0
                record.last_error = None
            # UNHEALTHY 下的陈旧成功：忽略，不允许状态回跳。
            return record.state is HealthState.HEALTHY

    async def report_failure(
        self,
        upstream_id: str,
        kind: FailureKind,
        now: float,
        detail: str = "",
    ) -> HealthState:
        """报告一次失败并推进状态机，返回反馈后的状态。"""
        async with self._lock:
            record = self._records.get(upstream_id)
            if record is None:
                return HealthState.UNHEALTHY
            record.last_error = f"{kind.value}: {detail}" if detail else kind.value
            if record.state is HealthState.HEALTHY:
                record.failures += 1
                if record.failures >= self._threshold:
                    record.state = HealthState.UNHEALTHY
                    record.opened_at_mono = now
                    record.opened_at_wall = time.time()
                    logger.warning(
                        "upstream %s 连续失败 %d 次，摘除熔断（最后原因=%s）",
                        upstream_id,
                        record.failures,
                        kind.value,
                    )
            elif record.state is HealthState.HALF_OPEN:
                # 探测失败：重新进入摘除并重新计时，不发生回跳。
                record.state = HealthState.UNHEALTHY
                record.opened_at_mono = now
                record.opened_at_wall = time.time()
                record.probes_inflight = 0
                logger.warning(
                    "upstream %s 半开探测失败（原因=%s），重新摘除",
                    upstream_id,
                    kind.value,
                )
            # UNHEALTHY 下的陈旧失败：只记录原因，不刷新冷却计时。
            return record.state

    # ---- 查询与选择 ---------------------------------------------------

    async def status(self, upstream_id: str) -> UpstreamStatus:
        async with self._lock:
            if upstream_id not in self._records:
                raise KeyError(f"上游未注册: {upstream_id!r}")
            return self._snapshot(self._records[upstream_id])

    async def all_status(self) -> list[UpstreamStatus]:
        async with self._lock:
            return [
                self._snapshot(self._records[uid])
                for uid in sorted(self._records)
            ]

    async def select(
        self, upstream_ids: tuple[str, ...], now: float
    ) -> tuple[str, UpstreamDef] | None:
        """在给定上游集合里轮询挑选一个当前可用的。

        健康的上游直接轮询；全部摘除时，冷却到期的上游可作为半开探测
        被选中；没有任何可许可的上游时返回 None。
        """
        if not upstream_ids:
            return None
        start = next(self._rr) % len(upstream_ids)
        for step in range(len(upstream_ids)):
            uid = upstream_ids[(start + step) % len(upstream_ids)]
            if await self.acquire(uid, now):
                async with self._lock:
                    record = self._records.get(uid)
                    if record is None:
                        continue
                    return uid, record.upstream
        return None

    @staticmethod
    def _snapshot(record: _Record) -> UpstreamStatus:
        return UpstreamStatus(
            upstream_id=record.upstream.id,
            state=record.state,
            consecutive_failures=record.failures,
            opened_at=record.opened_at_wall,
            last_error=record.last_error,
        )
