"""基本转发、路由匹配、失败原因区分。"""
import httpx

from gateway.models import Route, Upstream

U1 = Upstream("u1", "http://127.0.0.1:9001")
U2 = Upstream("u2", "http://127.0.0.1:9002")


async def test_forward_ok_and_log(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.Response(200, json={"hi": 1}))
    gw, client = await gw_client([Route("/api", ["u1"])], [U1])

    resp = await client.get("/api/users?x=1")
    assert resp.status_code == 200 and resp.json() == {"hi": 1}
    # 上游收到的路径与查询串完整
    assert mocker.requests[0].url.path == "/api/users"
    assert mocker.requests[0].url.query == b"x=1"
    # 尝试日志能看出命中规则与上游
    attempt = gw.attempts[-1]
    assert attempt.route_prefix == "/api" and attempt.upstream == "u1"
    assert attempt.outcome == "ok"


async def test_longest_prefix_match(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.Response(200, text="v1"))
    mocker.set(U2.base_url, lambda req: httpx.Response(200, text="v2"))
    _, client = await gw_client(
        [Route("/api", ["u1"]), Route("/api/v2", ["u2"])], [U1, U2])
    assert (await client.get("/api/a")).text == "v1"
    assert (await client.get("/api/v2/b")).text == "v2"


async def test_no_route_404(gw_client):
    gw, client = await gw_client([], [])
    resp = await client.get("/nope")
    assert resp.status_code == 404
    assert resp.headers["X-Gateway-Reason"] == "no_route"


async def test_upstream_5xx_distinguished(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.Response(500, text="boom"))
    gw, client = await gw_client([Route("/api", ["u1"], max_retries=0)], [U1])
    resp = await client.get("/api/x")
    assert resp.status_code == 502
    assert resp.headers["X-Gateway-Reason"] == "upstream_error"
    outcomes = [a.outcome for a in gw.attempts]
    assert "upstream_5xx" in outcomes and outcomes[-1] == "upstream_error"


async def test_connect_error_distinguished(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.ConnectError("refused"))
    gw, client = await gw_client([Route("/api", ["u1"], max_retries=0)], [U1])
    resp = await client.get("/api/x")
    assert resp.status_code == 502
    outcomes = [a.outcome for a in gw.attempts]
    assert "connect_error" in outcomes and outcomes[-1] == "upstream_error"


async def test_timeout_504(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.ReadTimeout("slow"))
    gw, client = await gw_client(
        [Route("/api", ["u1"], timeout_s=0.1, max_retries=0)], [U1])
    resp = await client.get("/api/x")
    assert resp.status_code == 504
    assert resp.headers["X-Gateway-Reason"] == "timeout"
    outcomes = [a.outcome for a in gw.attempts]
    assert "timeout" in outcomes and outcomes[-1] == "timeout"
