"""管理 API：运行时增删改，冲突显式报错，立即生效。"""
import httpx

from gateway.models import Route, Upstream

U1 = Upstream("u1", "http://127.0.0.1:9001")
U2 = Upstream("u2", "http://127.0.0.1:9002")


async def test_add_route_at_runtime_takes_effect(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.Response(200, text="hi"))
    _, client = await gw_client([], [])
    assert (await client.get("/api/x")).status_code == 404  # 还没路由

    assert (await client.post("/_gateway/upstreams", json={
        "name": "u1", "base_url": U1.base_url})).status_code == 201
    assert (await client.post("/_gateway/routes", json={
        "prefix": "/api", "upstreams": ["u1"]})).status_code == 201
    # 不重启，立即生效
    resp = await client.get("/api/x")
    assert resp.status_code == 200 and resp.text == "hi"


async def test_duplicate_route_conflict_409(gw_client, mocker):
    _, client = await gw_client([Route("/api", ["u1"])], [U1])
    resp = await client.post("/_gateway/routes", json={
        "prefix": "/api", "upstreams": ["u1"]})
    assert resp.status_code == 409
    # 尾斜杠规范化后仍算重复
    resp = await client.post("/_gateway/routes", json={
        "prefix": "/api/", "upstreams": ["u1"]})
    assert resp.status_code == 409


async def test_route_with_unknown_upstream_422(gw_client):
    _, client = await gw_client([], [])
    resp = await client.post("/_gateway/routes", json={
        "prefix": "/x", "upstreams": ["ghost"]})
    assert resp.status_code == 422


async def test_update_and_remove_route(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.Response(200, text="one"))
    mocker.set(U2.base_url, lambda req: httpx.Response(200, text="two"))
    _, client = await gw_client([Route("/api", ["u1"])], [U1, U2])
    assert (await client.get("/api/x")).text == "one"

    resp = await client.put("/_gateway/routes", json={
        "prefix": "/api", "upstreams": ["u2"]})
    assert resp.status_code == 200
    assert (await client.get("/api/x")).text == "two"  # 立即切换

    assert (await client.delete("/_gateway/routes",
                                params={"prefix": "/api"})).status_code == 200
    assert (await client.get("/api/x")).status_code == 404
    # 再删一次 -> 404
    assert (await client.delete("/_gateway/routes",
                                params={"prefix": "/api"})).status_code == 404


async def test_remove_upstream_still_referenced_409(gw_client):
    _, client = await gw_client([Route("/api", ["u1"])], [U1])
    resp = await client.delete("/_gateway/upstreams/u1")
    assert resp.status_code == 409


async def test_state_and_attempts_endpoints(gw_client, mocker):
    mocker.set(U1.base_url, lambda req: httpx.Response(200, text="ok"))
    _, client = await gw_client([Route("/api", ["u1"])], [U1])
    await client.get("/api/x")
    state = (await client.get("/_gateway/state")).json()
    assert "/api" in state["table"]["routes"]
    assert state["health"]["u1"]["state"] == "closed"
    attempts = (await client.get("/_gateway/attempts")).json()
    assert attempts[-1]["route_prefix"] == "/api"
    assert attempts[-1]["upstream"] == "u1"
    assert attempts[-1]["outcome"] == "ok"
