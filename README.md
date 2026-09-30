# FastAPI 边缘网关（本地反向代理）

把进入的请求按前缀规则转发到本地上游服务，提供运行时路由管理、
上游健康熔断、限流、超时与有限重试。所有行为在并发下可预期、可解释。

## 架构

```
请求 -> 路由匹配(最长前缀) -> 限流(令牌桶) -> 选健康上游(轮询)
     -> 转发(超时) -> 成功: 透传响应
                    -> 失败: 记录失败原因 -> 幂等请求有限重试(换上游)
                                          -> 非幂等请求立即失败
```

代码结构：

| 文件 | 职责 |
|---|---|
| `gateway/models.py` | 数据模型：路由、上游、失败原因枚举 |
| `gateway/routing.py` | 路由表：运行时增删改、最长前缀匹配、冲突报错 |
| `gateway/health.py` | 健康状态机：closed → open → half-open |
| `gateway/limiter.py` | 令牌桶限流（每条路由一个桶） |
| `gateway/proxy.py` | 转发核心：选上游、超时、重试、结构化日志 |
| `gateway/admin.py` | 管理 API（`/_gateway/*`） |
| `gateway/config.py` | 环境变量配置加载 |

## 路由规则

- 以前缀匹配，**最长前缀优先**，且边界必须在路径段上
  （`/api` 不匹配 `/apix`），结果确定。
- 前缀会规范化（去尾斜杠）；**重复或冲突的规则返回 409，绝不静默覆盖**。
- 路由引用的上游必须先存在，否则 422；仍被引用的上游不能删除（409）。
- 通过管理 API 修改后**立即生效，无需重启**。

### 管理 API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/_gateway/upstreams` | 新增上游 `{"name","base_url"}` |
| DELETE | `/_gateway/upstreams/{name}` | 删除上游（被引用则 409） |
| POST | `/_gateway/routes` | 新增路由 |
| PUT | `/_gateway/routes` | 更新已有路由 |
| DELETE | `/_gateway/routes?prefix=/api` | 删除路由 |
| GET | `/_gateway/state` | 路由表 + 各上游健康状态 |
| GET | `/_gateway/attempts` | 最近的转发尝试记录 |

路由字段：`prefix`、`upstreams`（名字列表）、`rate_limit_qps`
（可空=不限流）、`timeout_s`、`max_retries`。

## 健康检查与熔断

被动健康检查（基于真实流量结果）：

- 连续失败达到 `failure_threshold`（默认 3）→ 上游被**摘除**（open），
  请求直接快速失败，不再打到该上游；
- 冷却 `cooldown_s`（默认 5s）后进入**半开**，只放行
  `half_open_max_probe`（默认 1）个探测请求，其余仍快速失败，避免惊群；
- 探测成功 → 关闭熔断、计数清零，自动重新纳入；探测失败 → 重新熔断
  并重置冷却窗口，**不会状态回跳**。

失败原因被明确区分（日志与 `/_gateway/attempts` 中可见）：

- `connect_error`：连接失败（拒绝、重置等）
- `timeout`：连接/读取超时
- `upstream_5xx`：上游返回 5xx（4xx 视为上游存活，原样透传）

所有计数与状态迁移在同一把锁内完成，并发下不会错乱。

## 超时、限流与重试

- **超时**：每次尝试独立计时（`timeout_s`），超时立即失败并归还连接，
  不挂起、不泄漏。
- **限流**：每条路由一个令牌桶（容量 = max(1, qps)，可承受 1 秒突发），
  超限立即 429。
- **重试**：仅幂等方法（GET/HEAD/OPTIONS/PUT/DELETE）重试，
  上限 `max_retries`，且优先换没试过的上游；**POST/PATCH 等有副作用的
  请求绝不重试**，不会重复打到上游。

快速失败响应带 `X-Gateway-Reason` 头，原因可区分：

| 原因 | 状态码 | 含义 |
|---|---|---|
| `no_route` | 404 | 没有匹配的路由 |
| `rate_limited` | 429 | 被限流 |
| `no_healthy_upstream` | 503 | 所有上游都在熔断 |
| `timeout` | 504 | 重试耗尽，最后失败原因是超时 |
| `upstream_error` | 502 | 重试耗尽，最后失败原因是连接错误/5xx |

## 日志

每次尝试一条结构化日志（logger 名 `gateway`），能看出命中规则、
选中上游、结果与失败原因：

```
route=/api upstream=u1 outcome=connect_error status=None elapsed_ms=1.0 attempt=1
route=/api upstream=u2 outcome=ok status=200 elapsed_ms=2.0 attempt=2
```

## 配置项（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `GATEWAY_HOST` / `GATEWAY_PORT` | `127.0.0.1` / `8000` | 监听地址 |
| `GATEWAY_FAILURE_THRESHOLD` | `3` | 连续失败多少次熔断 |
| `GATEWAY_COOLDOWN_S` | `5.0` | 熔断冷却秒数 |
| `GATEWAY_HALF_OPEN_MAX_PROBE` | `1` | 半开时并发探测数 |
| `GATEWAY_UPSTREAMS` | 空 | `name=url` 空格分隔 |
| `GATEWAY_ROUTES` | 空 | `prefix=up1,up2;qps;timeout;retries` 空格分隔 |

## 本地验证

```bash
# 安装依赖
uv sync

# 跑测试（并发健康抖动、超时、非幂等不重试、限流拒绝等 21 个用例）
uv run pytest

# 起两个演示上游 + 网关
uv run python -c "from fastapi import FastAPI; import uvicorn; app=FastAPI();
app.get('/{p:path}')(lambda p: {'up':'u1'}); uvicorn.run(app, port=9001)" &
uv run python -c "from fastapi import FastAPI; import uvicorn; app=FastAPI();
app.get('/{p:path}')(lambda p: {'up':'u2'}); uvicorn.run(app, port=9002)" &
GATEWAY_UPSTREAMS="u1=http://127.0.0.1:9001 u2=http://127.0.0.1:9002" \
GATEWAY_ROUTES="/api=u1,u2;100;1.0;1" \
uv run python main.py &

# 转发（轮询到 u1/u2）
curl localhost:8000/api/hello
# 运行时加路由，立即生效
curl -X POST localhost:8000/_gateway/upstreams \
  -H 'content-type: application/json' \
  -d '{"name":"u3","base_url":"http://127.0.0.1:9003"}'
# 查看健康状态与尝试日志
curl localhost:8000/_gateway/state
curl localhost:8000/_gateway/attempts
# 杀掉一个上游，观察熔断（503 no_healthy_upstream）与自动恢复
```
