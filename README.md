# 本地反向代理网关（FastAPI）

把进入网关的 HTTP 请求按**前缀规则**转发到**本地**上游服务，并对上游的
健康状态、超时、限流、重试做统一管理。所有行为都追求两件事：

1. **可预期**：匹配、摘除、限流、重试在并发下有确定规则，不会静默覆盖或状态回跳；
2. **可解释**：每条错误都带原因码（响应体 `reason` + 响应头 `X-Gateway-Reason`），
   日志里能看到命中了哪条规则、选了哪个上游、因为什么失败。

## 架构与模块

```
main.py                  进程入口（uvicorn 启动 create_app）
app/api.py               HTTP 入口：/admin 管理面 + 兜底转发 + 统一错误处理
app/routing.py           路由表：前缀规范化、最长前缀匹配、冲突报错、原子快照
app/health.py            健康状态机：失败计数、摘除、半开探测、自动恢复
app/ratelimit.py         令牌桶限流：按 规则×客户端 维度，非阻塞
app/proxy.py             代理编排：限流 → 选上游 → 超时转发 → 健康反馈 → 安全重试
app/config.py            环境变量配置
tests/                   单元 + 真实本地 uvicorn 上游的集成/并发测试
routes.example.json      启动时加载的初始路由示例
```

一次请求的处理链路：

```
请求 → 最长前缀匹配路由 → 令牌桶限流（不足立刻 429）
     → 在健康上游里轮询选一个（全摘除时允许半开探测）
     → 带超时转发（读取完整响应后连接归还连接池）
     → 上报健康结果；安全方法且失败时才在预算内换上游重试
```

## 一、路由规则

### 前缀匹配

* 前缀必须以 `/` 开头；内部会规范化：去掉结尾的 `/`（`/api/` 与 `/api` 等价），
  拒绝空串、空白字符、`?`/`#`、连续 `/` 等非法写法。
* 命中规则取**最长的、处于路径段边界上的前缀**：
  `/api/v2` 优先于 `/api`；`/api` 命中 `/api` 和 `/api/x`，但**不**命中 `/apixyz`。
* 转发时去掉前缀，剩余路径拼到上游地址后，并保留原始 query string。
  例如规则 `/svc → http://127.0.0.1:9001`，请求
  `GET /svc/orders/42?verbose=1` 会转发成 `GET http://127.0.0.1:9001/orders/42?verbose=1`。

### 运行时增删改，冲突显式报错

通过 `/admin/*` 接口即可在**不重启进程**的情况下增删改上游和路由：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/admin/upstreams` | 列出全部上游及当前健康状态 |
| POST | `/admin/upstreams` | 新增上游；id 重复返回 `409 routing_error` |
| PUT | `/admin/upstreams/{id}` | 修改上游地址；视为新目标，健康计数重置 |
| DELETE | `/admin/upstreams/{id}` | 删除；仍被路由引用时返回 `409` |
| GET | `/admin/routes` | 列出全部路由 |
| POST | `/admin/routes` | 新增路由 |
| PUT | `/admin/routes/{id}` | 更新路由（前缀、上游、策略都可改） |
| DELETE | `/admin/routes/{id}` | 删除路由 |
| GET | `/health` | 全部上游的健康状态/连续失败数/摘除时间/最后错误 |

规则对象字段：

```json
{
  "id": "orders-api",
  "prefix": "/api/orders",
  "upstream_ids": ["orders"],
  "timeout": 2.0,
  "max_retries": 2,
  "rate_limit_capacity": 50,
  "rate_limit_refill_per_sec": 20
}
```

冲突与重复**绝不静默覆盖**，一律 `409 routing_error` 并在消息里点明双方：

* 规则 id 重复（请用 PUT 修改）；
* 同一规范化前缀被两条规则占用（消息形如：`前缀 '/api' 已被规则 'r1' 占用，规则 'r2' 与其冲突`）；
* 路由引用了不存在的上游；
* 删除仍被引用的上游。

增删改在锁内完成并整体替换不可变快照，转发读快照引用，因此改完对新请求**立即生效**。

## 二、上游健康检查与熔断

状态机（状态迁移全部在同一把 asyncio 锁内完成）：

```
HEALTHY ──连续失败达到阈值──▶ UNHEALTHY ──冷却到期且有请求──▶ HALF_OPEN
   ▲                                                           │
   └──────────────────── 探测成功 ─────────────────────────────┘
HALF_OPEN ──探测失败──▶ UNHEALTHY（重新计时冷却，不回跳到 healthy）
```

* **计数**：连续失败达到 `GATEWAY_HEALTH_THRESHOLD`（默认 3）次即摘除；
  成功一次清零。200 个并发失败上报也只会精确迁移一次，计数不会错乱。
* **摘除**：摘除期间该上游不接流量，请求若没有其它可用上游会快速返回
  `503 no_healthy_upstream`。
* **冷却与半开**：摘除 `GATEWAY_HEALTH_RECOVER_AFTER`（默认 10 秒）后，
  下一波请求里**最多** `GATEWAY_PROBE_MAX_INFLIGHT`（默认 1）个请求作为
  探测放行；探测成功恢复为 healthy，探测失败重新摘除并重新计时。
* **状态不回跳**：摘除期间才返回的"陈旧在途请求"的成功/失败不会改变状态，
  也不会刷新冷却计时。
* **三类失败可区分**：`upstream_5xx`（5xx 响应）、`connect_error`
  （连接失败/连接重置）、`timeout`（建连或读取超时）；三者都计入连续失败，
  但 `/health` 的 `last_error`、日志与错误响应里原因码不同。
* 冷却判定是惰性的（取当前时间比较），**没有后台探活线程**，行为可复现。

多个上游时在健康集合内**轮询（round-robin）**选择；全部摘除时冷却到期者
可被选为半开探测。

## 三、超时、限流与重试

### 超时（快速失败，不挂起、不泄漏）

* `GATEWAY_REQUEST_TIMEOUT`（默认 5s）为单次上游调用总超时，
  `GATEWAY_CONNECT_TIMEOUT`（默认 1s）为建连超时；单条路由可用 `timeout` 覆盖。
* 超时返回 `504 upstream_timeout`，连接被拒/重置返回 `502 upstream_connect_error`。
* 网关读取完整响应体后立即把连接归还 httpx 连接池；应用关闭时统一 `aclose()`，
  慢上游的请求在超时后被取消，不会泄漏协程或连接。

### 限流（令牌桶）

* 维度为 **规则 id × 客户端 IP**（客户端 IP 取 `X-Forwarded-For` 首项）。
* `rate_limit_capacity` 是突发容量，`rate_limit_refill_per_sec` 是每秒补充令牌数；
  路由字段可覆盖全局默认（`GATEWAY_RL_CAPACITY` / `GATEWAY_RL_REFILL`）。
* 取令牌**从不等待**：不足立刻返回 `429 rate_limited`；
  扣减在锁内"补充→判断→扣减"，并发请求不会超发。

### 重试（只有安全请求，有上限）

* 仅 **GET / HEAD / OPTIONS** 这类无副作用的安全方法允许重试；
  POST / PUT / PATCH / DELETE 等**绝不重试**，同一次请求不会被重复打到上游，
  日志会打印 `非幂等请求，禁止重试`。
* 重试次数 `GATEWAY_MAX_RETRIES`（默认 1，即最多 2 次尝试），路由可用
  `max_retries` 覆盖；重试前有固定退避 `GATEWAY_RETRY_BACKOFF`。
* 每次重试都重新走"选上游"，因此会尽量换到另一个健康上游。

### 错误原因码一览

| reason | 状态码 | 含义 |
| --- | --- | --- |
| `no_route` | 404 | 没有规则命中该路径 |
| `rate_limited` | 429 | 该客户端在该规则上令牌耗尽 |
| `no_healthy_upstream` | 503 | 规则下所有上游都被摘除 |
| `upstream_timeout` | 504 | 上游建连/读取超时 |
| `upstream_connect_error` | 502 | 连不上上游 / 连接被重置 |
| `upstream_5xx` | 502 | 上游返回 5xx（已用尽重试） |
| `routing_error` | 404/409 | 规则不存在 / 重复冲突 / 非法引用 |
| `invalid_config` | 400 | 上游地址非法等配置错误 |
| `invalid_payload` | 422 | 请求体不满足模型 |

成功响应带 `X-Gateway-Route` 与 `X-Gateway-Upstream`，便于核对命中规则与上游。

## 四、配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `GATEWAY_REQUEST_TIMEOUT` | `5.0` | 单次上游请求总超时（秒） |
| `GATEWAY_CONNECT_TIMEOUT` | `1.0` | 建连超时（秒） |
| `GATEWAY_HEALTH_THRESHOLD` | `3` | 连续失败多少次摘除 |
| `GATEWAY_HEALTH_RECOVER_AFTER` | `10.0` | 摘除后冷却多久允许半开探测（秒） |
| `GATEWAY_PROBE_MAX_INFLIGHT` | `1` | 半开期间允许同时探测的请求数 |
| `GATEWAY_RL_CAPACITY` | `20` | 每规则×客户端令牌桶容量 |
| `GATEWAY_RL_REFILL` | `10.0` | 每秒补充令牌数 |
| `GATEWAY_MAX_RETRIES` | `1` | 安全请求最大重试次数（0=不重试） |
| `GATEWAY_RETRY_BACKOFF` | `0.05` | 重试前固定退避（秒） |
| `GATEWAY_INITIAL_ROUTES_FILE` | 无 | 启动时加载的初始路由 JSON（格式见 `routes.example.json`） |

## 五、本地验证方法

依赖通过 [uv](https://docs.astral.sh/uv/) 管理（Python ≥ 3.14）：

```bash
uv sync                                  # 安装依赖（含 pytest/pytest-asyncio）
uv run pytest -q                         # 运行全部测试（55 个）
uv run pytest -q -k concurrency          # 只跑并发健康抖动相关用例
```

手工端到端验证（三个终端）：

```bash
# 1) 起一个本地"上游"（任何 HTTP 服务都行，这里用 python 自带模块演示）
python -m http.server 9001

# 2) 起网关
GATEWAY_REQUEST_TIMEOUT=2 GATEWAY_HEALTH_RECOVER_AFTER=5 \
  GATEWAY_INITIAL_ROUTES_FILE=routes.example.json \
  uv run python main.py                  # 默认监听 127.0.0.1:8080

# 3) 配置与验证（示例中上游是 9001/9002，先注册一个真实存在的）
curl -s localhost:8080/admin/upstreams
curl -s -X POST localhost:8080/admin/upstreams \
  -H 'content-type: application/json' \
  -d '{"id":"demo","base_url":"http://127.0.0.1:9001"}'
curl -s -X POST localhost:8080/admin/routes \
  -H 'content-type: application/json' \
  -d '{"id":"demo-r","prefix":"/files","upstream_ids":["demo"],"max_retries":0}'

curl -i localhost:8080/files/            # 应返回 9001 的目录列表，
                                         # 响应头含 X-Gateway-Route/X-Gateway-Upstream
curl -i localhost:8080/nope              # 404 no_route
curl -s localhost:8080/health            # 查看上游健康状态
```

观察失败原因可区分性：把上游指向一个没有进程监听的端口（连接失败）、
一个 sleep 很久的接口（超时）、一个返回 500 的接口（5xx），
分别得到 `upstream_connect_error` / `upstream_timeout` / `upstream_5xx`；
连续失败 3 次后 `/health` 显示 `unhealthy`，请求变为 `503 no_healthy_upstream`，
冷却后再发请求可看到半开探测与自动恢复。

## 六、日志

所有网关日志在 `gateway` logger 下输出到 stderr，关键行示例：

```
命中路由 rule=orders-api prefix=/api/orders path=/api/orders/42 client=127.0.0.1
route=orders-api upstream=orders attempt=1 转发 GET http://127.0.0.1:9001/orders/42 timeout=2.00
route=orders-api upstream=orders attempt=1/2 失败 reason=timeout state=healthy detail=...
upstream orders 连续失败 3 次，摘除熔断（最后原因=timeout）
route=orders-api 无可用上游 reason=no_healthy_upstream tried=['orders']
route=orders-api client=10.0.0.3 限流拒绝 reason=rate_limited available=0.00
upstream orders 冷却到期，进入 half_open 放行探测请求
upstream orders 探测成功，恢复为 healthy
```

据此可直接回答三个问题：**命中哪条规则、选了哪个上游（尝试了几次）、为什么失败**。
