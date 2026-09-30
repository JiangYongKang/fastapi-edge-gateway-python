"""共享测试夹具：真实的本地 uvicorn 上游 + 进程内 ASGI 网关客户端。

上游行为可通过线程安全的 UpstreamState 动态控制，用于模拟
5xx 抖动、慢响应（超时）、连接拒绝、请求计数等。
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import threading
import time
from dataclasses import dataclass, field

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api import create_app
from app.config import Settings


@dataclass
class UpstreamState:
    """单个上游的可动态切换行为。"""

    fail_remaining: int = 0          # >0 时接下来 N 次请求返回 500
    slow: bool = False               # True 时挂住请求，用于触发超时
    calls: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def should_fail(self) -> bool:
        with self.lock:
            if self.fail_remaining > 0:
                self.fail_remaining -= 1
                return True
            return False

    def record_call(self) -> None:
        with self.lock:
            self.calls += 1

    def snapshot_calls(self) -> int:
        with self.lock:
            return self.calls


def _build_upstream_app(state: UpstreamState) -> FastAPI:
    app = FastAPI()

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    )
    async def echo(request: Request, full_path: str):
        state.record_call()
        if state.should_fail():
            return JSONResponse(status_code=500, content={"err": "forced"})
        if state.slow:
            # 远超网关超时；sleep 是可取消的，取消后立刻释放，不泄漏协程。
            await asyncio.sleep(30)
        body = await request.body()
        return {
            "path": "/" + full_path,
            "method": request.method,
            "query": str(request.url.query),
            "body": body.decode() if body else "",
            "headers": {k.lower(): v for k, v in request.headers.items()},
        }

    return app


class _ServerThread:
    def __init__(self, app: FastAPI):
        self._server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
        )
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        while not self._server.started:
            time.sleep(0.005)
        self.port = self._server.servers[0].sockets[0].getsockname()[1]

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5)


def _dead_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture
def fast_settings() -> Settings:
    return Settings(
        request_timeout=0.5,
        connect_timeout=0.2,
        health_threshold=3,
        health_recover_after=0.3,
        health_probe_max_inflight=1,
        rate_limit_capacity=5,
        rate_limit_refill_per_sec=1000.0,
        max_retries=1,
        retry_backoff=0.01,
    )


@pytest.fixture
def upstream_factory():
    servers: list[_ServerThread] = []
    states: list[UpstreamState] = []

    def start() -> tuple[UpstreamState, _ServerThread]:
        state = UpstreamState()
        server = _ServerThread(_build_upstream_app(state))
        servers.append(server)
        states.append(state)
        return state, server

    yield start

    for server in servers:
        server.stop()


@pytest.fixture
def gateway(fast_settings):
    """装配网关并返回 (client, app, helper)；测试结束时清理客户端与上游连接。"""
    app = create_app(fast_settings)

    @contextlib.asynccontextmanager
    async def _runner():
        transport = httpx.ASGITransport(app=app)
        async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=transport, base_url="http://gw"
        ) as client:
            yield client, app

    return _runner, app, fast_settings


@pytest.fixture
def dead_endpoint() -> str:
    """返回一个确定没有进程监听的本地地址（连接会被立即拒绝）。"""
    port = _dead_port()
    return f"http://127.0.0.1:{port}"
