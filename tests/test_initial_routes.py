"""启动时从 JSON 文件加载初始路由与上游。"""

import json
from pathlib import Path

import httpx
import pytest

from app.api import create_app
from app.config import Settings


@pytest.mark.asyncio
async def test_loads_upstreams_and_routes_from_file(upstream_factory, tmp_path):
    _state, server = upstream_factory()
    config_path: Path = tmp_path / "routes.json"
    config_path.write_text(
        json.dumps(
            {
                "upstreams": [
                    {"id": "u1", "base_url": server.base_url},
                ],
                "routes": [
                    {
                        "id": "r1",
                        "prefix": "/init",
                        "upstream_ids": ["u1"],
                        "max_retries": 0,
                    }
                ],
            }
        )
    )
    settings = Settings(
        request_timeout=0.5,
        connect_timeout=0.2,
        rate_limit_capacity=10,
        rate_limit_refill_per_sec=100,
        initial_routes_file=str(config_path),
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app), httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gw"
    ) as client:
        r = await client.get("/init/ping")
        assert r.status_code == 200
        assert r.json()["path"] == "/ping"
        rules = await client.get("/admin/routes")
        assert rules.json()["routes"][0]["id"] == "r1"
        health = await client.get("/health")
        assert health.json()["upstreams"][0]["id"] == "u1"


@pytest.mark.asyncio
async def test_missing_upstream_reference_in_file_fails_startup(
    upstream_factory, tmp_path
):
    config_path: Path = tmp_path / "bad.json"
    config_path.write_text(
        json.dumps(
            {
                "upstreams": [],
                "routes": [
                    {"id": "r1", "prefix": "/x", "upstream_ids": ["ghost"]}
                ],
            }
        )
    )
    settings = Settings(initial_routes_file=str(config_path))
    app = create_app(settings)
    with pytest.raises(Exception, match="不存在的上游"):
        async with app.router.lifespan_context(app):
            pass
