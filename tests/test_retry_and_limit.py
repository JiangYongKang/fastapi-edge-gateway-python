"""重试策略（仅幂等）与限流。"""
import asyncio

import httpx

from gateway.models import Route, Upstream

U1 = Upstream("u1", "http://127.0.0.1:9001")
U2 = Upstream("u2", "http://127.0.0.1:9002")


async def test_get_retried_on_second_upstream(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.ConnectError("down"))
    mocker.set(U2.base_url, lambda req: httpx.Response(200, text="ok"))
    gw, client = await gw_client(
        [Route("/api", ["u1", "u2"], max_retries=1)], [U1, U2])
    # 固定先打到 u1（轮询计数清零）：失败后要重试到 u2
    gw._rr_counters.clear()
    resp = await client.get("/api/x")
    assert resp.status_code == 200 and resp.text == "ok"
    assert mocker.count(U1.base_url) == 1
    assert mocker.count(U2.base_url) == 1
    outcomes = [a.outcome for a in gw.attempts]
    assert outcomes == ["connect_error", "ok"]


async def test_post_never_retried(gw_client, mocker):
    """非幂等请求不重试：上游失败一次就直接失败，不会重复打。"""
    mocker.set(U1.base_url, lambda req: httpx.Response(500, text="boom"))
    mocker.set(U2.base_url, lambda req: httpx.Response(200, text="ok"))
    gw, client = await gw_client(
        [Route("/api", ["u1", "u2"], max_retries=3)], [U1, U2])
    resp = await client.post("/api/pay", json={"amount": 1})
    assert resp.status_code == 502
    total = mocker.count(U1.base_url) + mocker.count(U2.base_url)
    assert total == 1, f"POST 被打了 {total} 次"


async def test_retry_exhausted_reports_last_reason(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.ReadTimeout("slow"))
    gw, client = await gw_client(
        [Route("/api", ["u1"], timeout_s=0.1, max_retries=2)], [U1])
    resp = await client.get("/api/x")
    assert resp.status_code == 504
    assert resp.headers["X-Gateway-Reason"] == "timeout"
    timeouts = [a for a in gw.attempts if a.outcome == "timeout" and a.attempt >= 1]
    assert len(timeouts) == 3  # 1 + 2 次重试


async def test_rate_limit_rejects(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.Response(200, text="ok"))
    gw, client = await gw_client(
        [Route("/api", ["u1"], rate_limit_qps=5.0)], [U1])
    results = await asyncio.gather(*[client.get("/api/x") for _ in range(20)])
    ok = [r for r in results if r.status_code == 200]
    limited = [r for r in results if r.status_code == 429]
    assert len(ok) == 5 and len(limited) == 15
    assert all(r.headers["X-Gateway-Reason"] == "rate_limited" for r in limited)
    # 限流拒绝也写日志
    assert any(a.outcome == "rate_limited" for a in gw.attempts)


async def test_no_healthy_upstream_503(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.ConnectError("down"))
    gw, client = await gw_client(
        [Route("/api", ["u1"], max_retries=0)],
        [U1],
        config=__import__("gateway.models", fromlist=["GatewayConfig"]).GatewayConfig(
            failure_threshold=2, cooldown_s=60.0),
    )
    assert (await client.get("/api/a")).status_code == 502
    assert (await client.get("/api/b")).status_code == 502
    # 连续 2 次失败后熔断，第三个请求直接 503，不再打上游
    resp = await client.get("/api/c")
    assert resp.status_code == 503
    assert resp.headers["X-Gateway-Reason"] == "no_healthy_upstream"
    assert mocker.count(U1.base_url) == 2
