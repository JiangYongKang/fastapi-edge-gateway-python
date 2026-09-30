"""路由规则：前缀匹配、运行时增删改、冲突显式报错。

确定性约束：

* 前缀一律规范化（必须以 ``/`` 开头、去掉结尾的 ``/``，根路径除外）。
* 同一个规则 id 只允许存在一条；重复添加报 :class:`RoutingError`。
* 同一个规范化前缀只允许属于一条规则；重复/冲突报错并给出冲突双方 id，
  绝不会静默覆盖。
* 增删改在锁内完成并整体替换快照，匹配读快照，无需重启进程即生效。
* 匹配取最长的、处于路径段边界上的前缀，结果唯一确定。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass


class RoutingError(Exception):
    """路由配置错误（路径非法、冲突、规则不存在等）。

    ``status_code`` 显式给出对应的 HTTP 状态：找不到实体用 404，
    重复/冲突/引用非法用 409，绝不靠错误消息文本猜测。
    """

    def __init__(self, message: str, status_code: int = 409) -> None:
        super().__init__(message)
        self.status_code = status_code


def normalize_prefix(prefix: str) -> str:
    """把用户给出的前缀规范化成路由表内部使用的形式。"""
    if not isinstance(prefix, str) or not prefix:
        raise RoutingError("前缀必须是非空字符串")
    if not prefix.startswith("/"):
        raise RoutingError(f"前缀必须以 / 开头: {prefix!r}")
    if any(ch.isspace() for ch in prefix):
        raise RoutingError(f"前缀不能包含空白字符: {prefix!r}")
    if "?" in prefix or "#" in prefix:
        raise RoutingError(f"前缀不能包含查询串或片段: {prefix!r}")
    if "//" in prefix:
        raise RoutingError(f"前缀不能包含连续的 /: {prefix!r}")
    if len(prefix) > 1 and prefix.endswith("/"):
        prefix = prefix.rstrip("/")
    return prefix


def is_path_prefix(prefix: str, path: str) -> bool:
    """段边界匹配：``/api`` 命中 ``/api``、``/api/x``，但不命中 ``/apixyz``。"""
    if not path.startswith(prefix):
        return False
    if prefix == "/" or len(path) == len(prefix):
        return True
    return path[len(prefix)] == "/"


@dataclass(frozen=True)
class UpstreamDef:
    """单个上游：id + 基础地址。"""

    id: str
    base_url: str


@dataclass(frozen=True)
class RouteRule:
    """一条路由规则：路径前缀 -> 一组上游 + 该规则可覆盖的策略。"""

    id: str
    prefix: str
    upstream_ids: tuple[str, ...]
    timeout: float | None = None
    max_retries: int | None = None
    rate_limit_capacity: int | None = None
    rate_limit_refill_per_sec: float | None = None

    def validated(self) -> RouteRule:
        """校验字段并返回前缀规范化后的新规则。"""
        if not isinstance(self.id, str) or not self.id.strip():
            raise RoutingError("规则 id 不能为空")
        if " " in self.id:
            raise RoutingError(f"规则 id 不能包含空白: {self.id!r}")
        prefix = normalize_prefix(self.prefix)
        if not self.upstream_ids:
            raise RoutingError(f"规则 {self.id!r} 至少需要一个上游")
        if len(set(self.upstream_ids)) != len(self.upstream_ids):
            raise RoutingError(f"规则 {self.id!r} 的上游列表存在重复 id")
        if self.timeout is not None and self.timeout <= 0:
            raise RoutingError(f"规则 {self.id!r} 的 timeout 必须为正数")
        if self.max_retries is not None and self.max_retries < 0:
            raise RoutingError(f"规则 {self.id!r} 的 max_retries 不能为负")
        if self.rate_limit_capacity is not None and self.rate_limit_capacity <= 0:
            raise RoutingError(f"规则 {self.id!r} 的限流容量必须为正数")
        if (
            self.rate_limit_refill_per_sec is not None
            and self.rate_limit_refill_per_sec <= 0
        ):
            raise RoutingError(f"规则 {self.id!r} 的令牌填充速率必须为正数")
        return RouteRule(
            id=self.id,
            prefix=prefix,
            upstream_ids=tuple(self.upstream_ids),
            timeout=self.timeout,
            max_retries=self.max_retries,
            rate_limit_capacity=self.rate_limit_capacity,
            rate_limit_refill_per_sec=self.rate_limit_refill_per_sec,
        )


@dataclass(frozen=True)
class MatchedRoute:
    """前缀匹配结果：命中的规则 + 去前缀后的剩余路径（含前导 /，根为 ""）。"""

    rule: RouteRule
    remaining_path: str


class RoutingTable:
    """线程安全的路由表，整体快照替换，匹配结果确定。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_id: dict[str, RouteRule] = {}
        # 不变快照：按前缀长度降序，匹配时顺序扫描即得"最长前缀"。
        self._snapshot: tuple[RouteRule, ...] = ()

    def _rebuild(self) -> None:
        self._snapshot = tuple(
            sorted(self._by_id.values(), key=lambda r: len(r.prefix), reverse=True)
        )

    def add(self, rule: RouteRule) -> None:
        rule = rule.validated()
        with self._lock:
            if rule.id in self._by_id:
                raise RoutingError(
                    f"规则 id 已存在: {rule.id!r}；如需修改请调用更新接口"
                )
            for existing in self._by_id.values():
                if existing.prefix == rule.prefix:
                    raise RoutingError(
                        f"前缀 {rule.prefix!r} 已被规则 {existing.id!r} 占用，"
                        f"规则 {rule.id!r} 与其冲突"
                    )
            self._by_id[rule.id] = rule
            self._rebuild()

    def update(self, rule: RouteRule) -> None:
        rule = rule.validated()
        with self._lock:
            if rule.id not in self._by_id:
                raise RoutingError(f"规则不存在，无法更新: {rule.id!r}", status_code=404)
            for existing in self._by_id.values():
                if existing.id == rule.id:
                    continue
                if existing.prefix == rule.prefix:
                    raise RoutingError(
                        f"前缀 {rule.prefix!r} 已被规则 {existing.id!r} 占用，"
                        f"规则 {rule.id!r} 更新后与其冲突"
                    )
            self._by_id[rule.id] = rule
            self._rebuild()

    def delete(self, rule_id: str) -> None:
        with self._lock:
            if rule_id not in self._by_id:
                raise RoutingError(f"规则不存在，无法删除: {rule_id!r}", status_code=404)
            del self._by_id[rule_id]
            self._rebuild()

    def get(self, rule_id: str) -> RouteRule:
        with self._lock:
            if rule_id not in self._by_id:
                raise RoutingError(f"规则不存在: {rule_id!r}", status_code=404)
            return self._by_id[rule_id]

    def list_rules(self) -> list[RouteRule]:
        with self._lock:
            return list(self._by_id.values())

    def match(self, path: str) -> MatchedRoute | None:
        snapshot = self._snapshot  # 引用原子替换，无需持锁
        for rule in snapshot:
            if is_path_prefix(rule.prefix, path):
                remaining = path[len(rule.prefix):]
                return MatchedRoute(rule=rule, remaining_path=remaining)
        return None
