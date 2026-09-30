"""并发健康抖动、熔断与自动恢复。"""
import asyncio

import httpx

from gateway.models import GatewayConfig, Route, Upstream

U1 = Upstream("u1", "http://127.0.0.1:9001")
U2 = Upstream("u2", "http://127.0.0.1:9002")


async def test_concurrent_flapping_counts_consistent(gw_client, mocker):
    """上游时好时坏并发抖动：失败计数单调、状态机不混乱。"""
    state = {"fail": True}
    def flaky(req):
        # 每次调用切换，模拟抖动
        state["fail"] = not state["fail"]
        if state["fail"]:
            return httpx.Response(500, text="boom")
        return httpx.Response(200, text="ok")
    mocker.set(U1.base_url, flaky)
    cfg = GatewayConfig(failure_threshold=1000, cooldown_s=60.0)
    gw, client = await gw_client(
        [Route("/api", ["u1"], max_retries=0)], [U1], config=cfg)

    await asyncio.gather(*[client.get("/api/x") for _ in range(200)])
    snap = await gw.health.snapshot()
    h = snap["u1"]
    upstream_hits = mocker.count(U1.base_url)
    ok = sum(1 for a in gw.attempts if a.outcome == "ok")
    bad = sum(1 for a in gw.attempts if a.outcome == "upstream_5xx")
    # 计数守恒：成功 + 失败 == 打到上游的总次数
    assert ok + bad == upstream_hits
    assert h["consecutive_failures"] <= upstream_hits
    assert h["state"] in ("closed", "open")


async def test_circuit_opens_and_recovers(gw_client, mocker):
    """连续失败熔断 -> 冷却后半开探测 -> 恢复后自动纳入。"""
    behavior = {"mode": "down"}
    def handler(req):
        if behavior["mode"] == "down":
            return httpx.ConnectError("refused")
        return httpx.Response(200, text="recovered")
    mocker.set(U1.base_url, handler)
    cfg = GatewayConfig(failure_threshold=2, cooldown_s=0.1, half_open_max_probe=1)
    gw, client = await gw_client(
        [Route("/api", ["u1"], max_retries=0)], [U1], config=cfg)

    assert (await client.get("/api/a")).status_code == 502
    assert (await client.get("/api/b")).status_code == 502
    # 已熔断：快速 503，不再打上游
    assert (await client.get("/api/c")).status_code == 503
    assert mocker.count(U1.base_url) == 2
    assert (await gw.health.snapshot())["u1"]["state"] == "open"

    # 上游恢复 + 冷却结束 -> 半开探测成功 -> 自动纳入
    behavior["mode"] = "up"
    await asyncio.sleep(0.15)
    resp = await client.get("/api/d")
    assert resp.status_code == 200 and resp.text == "recovered"
    snap = (await gw.health.snapshot())["u1"]
    assert snap["state"] == "closed" and snap["consecutive_failures"] == 0


async def test_half_open_probe_failure_reopens(gw_client, mocker):
    """半开探测仍失败 -> 重新熔断，不出现状态回跳。"""
    mocker.set(U1.base_url, lambda req: httpx.ConnectError("still down"))
    cfg = GatewayConfig(failure_threshold=1, cooldown_s=0.05, half_open_max_probe=1)
    gw, client = await gw_client(
        [Route("/api", ["u1"], max_retries=0)], [U1], config=cfg)

    assert (await client.get("/api/a")).status_code == 502  # 熔断
    await asyncio.sleep(0.06)
    assert (await client.get("/api/b")).status_code == 502  # 半开探测失败
    assert (await gw.health.snapshot())["u1"]["state"] == "open"
    # 立即再请求：冷却重置，仍 503
    assert (await client.get("/api/c")).status_code == 503


async def test_concurrent_requests_no_duplicate_probe(gw_client, mocker):
    """熔断后并发涌入：半开只放行有限探测，其余快速失败。"""
    gate = asyncio.Event()
    def slow(req):
        # 半开探测会阻塞在 MockTransport 外，这里直接成功即可
        return httpx.Response(200, text="ok")
    mocker.set(U1.base_url, lambda req: httpx.ConnectError("x"))
    cfg = GatewayConfig(failure_threshold=1, cooldown_s=0.05, half_open_max_probe=1)
    gw, client = await gw_client(
        [Route("/api", ["u1"], max_retries=0)], [U1], config=cfg)
    assert (await client.get("/api/a")).status_code == 502  # 熔断
    await asyncio.sleep(0.06)
    # 占住唯一的半开探测名额
    assert await gw.health.try_acquire("u1")
    results = await asyncio.gather(*[client.get("/api/b") for _ in range(10)])
    assert all(r.status_code == 503 for r in results)
    await gw.health.record_success("u1")
    mocker.set(U1.base_url, slow)
    assert (await client.get("/api/c")).status_code == 200
