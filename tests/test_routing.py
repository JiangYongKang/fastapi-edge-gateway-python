"""路由表：前缀规范化、最长前缀匹配、重复/冲突显式报错。"""

import threading

import pytest

from app.routing import (
    RouteRule,
    RoutingError,
    RoutingTable,
    normalize_prefix,
)


def rule(rule_id: str, prefix: str, upstreams=("u1",)) -> RouteRule:
    return RouteRule(id=rule_id, prefix=prefix, upstream_ids=upstreams)


def test_normalize_prefix_strips_trailing_slash():
    assert normalize_prefix("/") == "/"
    assert normalize_prefix("/api/") == "/api"


@pytest.mark.parametrize(
    "bad",
    ["api", "/a b", "/a?x=1", "/a#frag", "/a//b", ""],
)
def test_normalize_prefix_rejects_invalid(bad):
    with pytest.raises(RoutingError):
        normalize_prefix(bad)


def test_longest_prefix_wins_and_segment_boundary():
    table = RoutingTable()
    table.add(rule("root", "/"))
    table.add(rule("api", "/api"))
    table.add(rule("v2", "/api/v2"))

    assert table.match("/api/v2/x").rule.id == "v2"
    assert table.match("/api/v1/x").rule.id == "api"
    assert table.match("/other").rule.id == "root"
    # 段边界：/apixyz 不能被 /api 命中
    assert table.match("/apixyz").rule.id == "root"
    # remaining_path 计算
    assert table.match("/api").remaining_path == ""
    assert table.match("/api/v2").remaining_path == ""
    assert table.match("/api/v2/x/y").remaining_path == "/x/y"


def test_duplicate_id_rejected():
    table = RoutingTable()
    table.add(rule("r1", "/a"))
    with pytest.raises(RoutingError, match="已存在"):
        table.add(rule("r1", "/b"))


def test_same_prefix_conflict_rejected():
    table = RoutingTable()
    table.add(rule("r1", "/api/"))  # 规范化后 /api
    with pytest.raises(RoutingError, match="冲突"):
        table.add(rule("r2", "/api"))


def test_update_must_exist_and_must_not_conflict():
    table = RoutingTable()
    table.add(rule("r1", "/a"))
    table.add(rule("r2", "/b"))
    with pytest.raises(RoutingError, match="不存在"):
        table.update(rule("r9", "/x"))
    with pytest.raises(RoutingError, match="冲突"):
        table.update(rule("r1", "/b"))
    table.update(rule("r1", "/a2"))
    assert table.match("/a2/x").rule.id == "r1"
    assert table.match("/a/x") is None


def test_delete_requires_existing_rule():
    table = RoutingTable()
    with pytest.raises(RoutingError, match="不存在"):
        table.delete("ghost")
    table.add(rule("r1", "/a"))
    table.delete("r1")
    assert table.match("/a") is None


def test_validation_requires_upstreams_and_unique_ids():
    table = RoutingTable()
    with pytest.raises(RoutingError, match="至少需要一个上游"):
        table.add(rule("r1", "/a", upstreams=()))
    with pytest.raises(RoutingError, match="重复"):
        table.add(RouteRule(id="r2", prefix="/a", upstream_ids=("u", "u")))


def test_concurrent_updates_never_silently_overwrite():
    """多线程并发加不同前缀/同 id：要么成功存在一条，要么明确报错。"""
    table = RoutingTable()
    table.add(rule("shared", "/init"))
    errors: list[Exception] = []

    def worker(i: int) -> None:
        try:
            if i % 2 == 0:
                table.update(rule("shared", f"/even-{i}"))
            else:
                table.add(rule(f"odd-{i}", f"/odd-{i}"))
        except RoutingError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 共享规则最终恰好一条，前缀唯一，没有被静默覆盖成多份
    ids = [r.id for r in table.list_rules()]
    assert ids.count("shared") == 1
    prefixes = [r.prefix for r in table.list_rules()]
    assert len(prefixes) == len(set(prefixes))
