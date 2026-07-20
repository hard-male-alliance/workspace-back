# 前端诊断接入契约

版本：v1（2026-07-21）

`POST /api/v1/diagnostics` 用于接收浏览器侧、尽力而为（best-effort）的错误、Web
Vitals 与网络性能信号。它不是审计、计费或授权事实：服务端始终从已认证请求重建
`ActorScope`，不会信任客户端提交的身份、service、severity 或环境字段。

## 请求边界

- `Content-Type` 必须为 `application/json`。
- 原始请求体在 JSON 解析前限制为 65,536 bytes；每批 1–50 条，部署配置可以进一步收紧。
- 同一批的 `client_event_id` 必须唯一；格式为 1–128 位字母、数字、`.`、`_`、`:` 或 `-`。
- `occurred_at` 必须是带时区 RFC 3339 时间，默认只接受过去 24 小时至未来 5 分钟。
- `route` 必须是后端产品 router 已发布、去掉 `/api/v1` 前缀后的权威模板，例如
  `/resumes/{resume_id}`；`/` 也合法。不得发送实际资源 ID、未知 SPA 路径或任意 URL；
  后端路由目录演进时，前端采集器应同步使用新发布的模板。
- `release` 必须匹配 `^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$`：以 ASCII 字母或数字开头，只允许 ASCII 字母、数字、`.`、`_`、`+`、`-`，最长 64 字符。
- error 事件的 `error_code` 必须匹配 `^[a-z][a-z0-9_.-]{0,127}$`；可选的 `stack_fingerprint` 必须是 16–64 位小写十六进制文本。两者都只能表示稳定分类，不能编码消息、路径或用户数据。
- performance 的 `value` 与 network 的 `duration_ms` 必须是非负有限数；服务端不接受 `NaN` 或无穷值。
- 未声明字段一律拒绝。不要发送 message、stack、DOM、表单、cookie、header、localStorage、token、用户文本、URL 或源码片段。

请求采用严格判别联合（discriminated union）：

```json
{
  "events": [
    {
      "event_type": "error",
      "client_event_id": "diag-018f1",
      "occurred_at": "2026-07-21T09:30:00Z",
      "route": "/resumes/{resume_id}",
      "release": "web-2026.07.21.1",
      "error_code": "ui.resume.render_failed",
      "stack_fingerprint": "0123456789abcdef0123456789abcdef"
    },
    {
      "event_type": "performance",
      "client_event_id": "diag-018f2",
      "occurred_at": "2026-07-21T09:30:01Z",
      "route": "/resumes/{resume_id}",
      "release": "web-2026.07.21.1",
      "metric_name": "largest_contentful_paint",
      "value": 1250.4,
      "unit": "ms"
    },
    {
      "event_type": "network",
      "client_event_id": "diag-018f3",
      "occurred_at": "2026-07-21T09:30:02Z",
      "route": "/resumes/{resume_id}",
      "release": "web-2026.07.21.1",
      "operation": "fetch",
      "duration_ms": 420.2,
      "status_code": 503
    }
  ]
}
```

性能指标只允许：

| `metric_name` | 请求单位 | 持久化单位 |
|---|---|---|
| `cumulative_layout_shift` | `1` | `1` |
| `first_contentful_paint` | `ms` | `s` |
| `interaction_to_next_paint` | `ms` | `s` |
| `largest_contentful_paint` | `ms` | `s` |
| `time_to_first_byte` | `ms` | `s` |

网络 `status_code` 允许 `0`（没有 HTTP 响应）或 `100..599`；`operation` 只允许
`asset`、`fetch`、`navigation`。

## 响应与重试

成功准入返回 `202`：

```json
{"accepted": 3, "dropped": 0}
```

`accepted` 表示事件已进入后端的有界异步管线，不承诺已经刷入 PostgreSQL；数据库故障、
进程终止或背压都可能使诊断丢失。仅 PostgreSQL 模式以
`(workspace_id, resource_owner_id, actor_id, client_event_id)` 持久去重；同一 ActorScope 内
同一事件的网络重试必须复用 ID，不同事件不得复用。该约束由仅作用于
`source=frontend AND client_event_id IS NOT NULL` 的部分唯一索引（partial unique index）执行，
因此不同 ActorScope 可以合法复用同一个客户端 ID。
`database.mode=memory` 只有进程内、非持久的 demo writer，不提供跨重启去重或持久化。
`observability.enabled=false` 时路由仍存在；未触发其他准入错误的有效批次通常返回 `202`，
但事件会计入 `dropped` 而不会持久化，调用方不能把 HTTP 成功等同于已启用采集。

- `400`：`Content-Length` 不是合法的非负整数；修正请求生成或代理层。
- `413`：请求体或部署允许的批次过大；拆分后再试。
- `415`：Content-Type 错误；改为 JSON，不要降级为任意文本。
- `422`：字段、单位或时间窗口无效；修正采集器，不要盲目重试。
- `429`：ActorScope token bucket 已耗尽；遵循整数秒 `Retry-After`，加入随机抖动。
- `5xx`：可有限退避重试；页面卸载时允许直接丢弃，绝不能阻塞产品操作。

推荐在前端基础设施层维护一个有大小和生存期上限的内存批次，使用 `fetch` 的
`keepalive` 或正确声明 `application/json` 的 `Blob` 配合 `sendBeacon`。诊断失败不得
触发新的同类诊断事件，避免递归风暴。

## 准入自监控与查询

对完成 schema 校验的准入尝试，后端以固定 `operation=ingest` 和受限 `outcome` 记录四组自监控
指标：`aiws.diagnostics.ingest.payload.size`（bytes histogram）、
`aiws.diagnostics.ingest.batch.count`（batch counter）、
`aiws.diagnostics.ingest.duration`（seconds histogram），以及按 `accepted`、`dropped`、
`rate_limited` 结果计数的 `aiws.diagnostics.ingest.event.count`。这些指标不使用 route、release
或 actor ID 作为 metric attributes；400/413/415/422 等 schema/协议拒绝仍由统一 HTTP 指标与
server span 体现，而不伪装成已经进入准入阶段的事件计数。

自监控指标和客户端诊断都走同一有界、尽力而为管线，所以它们可用于研究吞吐和丢弃趋势，
不能作为审计计数。Dashboard 的 `diagnostics`/`events` 读取有界 log/span 以及
`source=frontend` 的 metric；CLI `frontend` 进一步固定过滤 `service=frontend.browser`，因此
Web Vitals performance 数值和 network duration/outcome/status-class 与浏览器错误可以在同一
最近事件上下文中查看。当前实现不是按 route/release 聚合的完整 Frontend 专页。

## 部署边界

公网入口必须先经过身份代理；浏览器不得发送 `X-AIWS-*`、`X-Mock-*` 或 Dashboard
凭证。仓库的 Nginx 示例为该精确路径设置 64 KiB、每直接对端 IP 的速率限制、短 upstream
timeout，并清除所有私有断言。生产环境还应通过明确的 first-party CORS allowlist、TLS、
网络隔离和正常的成员授权限制来源。

应用内 ActorScope token bucket 是单进程、单 worker 的尽力而为保护：每个事件消耗一个
token，进程重启会重置状态，`max_actor_buckets` 的 LRU 淘汰也会让被淘汰 scope 重新开始。
多 worker/Pod 的总准入能力会随实例数增加；需要全局预算时必须在入口或共享限流服务补充，
不能把本地 bucket 当作分布式配额。

后端只持久化稳定事件名、受限属性、服务端身份和关联 ID；自由文本 message/stack 只可在
受控本地日志中出现，不进入 `observability.telemetry_records`。更完整的威胁模型、SRE
信号语义与演进依据见 [ADR-0003](decisions/0003-observability-platform.md)。
