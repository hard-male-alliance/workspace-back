# ADR-0003：可观测性平台、诊断入口与 Dashboard 产品化

- 状态：Accepted
- 日期：2026-07-21
- 范围：`backend`、`dashboard` 与 `dbctl` 的可观测性契约；包含日志、指标、追踪关联、前端诊断、Dashboard/CLI 和 PostgreSQL 读写模型
- 决策类型：跨应用架构、数据治理、安全与运维

## 背景

系统已有 `observability.telemetry_records`、有界批量 telemetry sink、Dashboard 只读视图以及独立的 `backend`、`dashboard`、`dbctl` 可执行应用，但仍存在以下结构性问题：

1. 日志主要被送入数据库，没有形成清晰可见、可配置的 STDOUT、STDERR 与文件输出；运行时故障也缺少稳定事件语义。
2. 已有 telemetry 是最小闭环，不足以系统性解释流量、延迟、错误、饱和度、依赖、发布和遥测管线自身的状态。
3. Dashboard 虽已成为独立应用，但查询、领域读模型、CLI、API、GUI 和可视化适配器还需要一套长期边界，避免产品继续增长后重新耦合为“分布式单体”。
4. 从 PostgreSQL 拉取大量原始样本再由 Python 聚合，会浪费数据库、网络、内存和 GUI 渲染预算；简单平均下采样还可能抹掉尖峰。
5. 前端诊断 API 将接收由浏览器乃至攻击者完全控制的数据，若按普通后端日志处理，会引入敏感信息泄漏、日志注入、重放、存储放大和高基数拒绝服务。
6. CLI 将查询参数暴露为日常必填项，没有把安全默认值、上下文发现和常用运维问题建模成面向用户的视图。

Google SRE 将延迟、流量、错误和饱和度定义为需要优先监控的“四个黄金信号”（Four Golden Signals），并区分适合告警的指标与适合根因分析的日志；监控还应覆盖预期变更和监控系统自身。[Google SRE：Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/)、[Google SRE Workbook：Monitoring](https://sre.google/workbook/monitoring/)

本 ADR 不要求立即引入独立 Prometheus、OpenTelemetry Collector 或分布式追踪存储。当前部署仍以 PostgreSQL 为持久化事实来源，但内部语义必须可映射到 OpenTelemetry，以保留未来替换或增加 exporter 的能力。

## 决策驱动原则

1. **业务正确性高于遥测完整性。** 遥测失败、阻塞或容量耗尽不得使业务事务失败。
2. **一次发出，一个事件，多路消费。** 业务代码只产生一个规范化事件，分流属于基础设施组合职责。
3. **指标告警、日志解释、追踪关联。** 三者共享相关标识，但不互相冒充。
4. **低基数优先。** 指标维度在进入存储前执行白名单；高基数信息只进入受限日志或追踪字段。
5. **先服务用户问题，再选择图表和 SQL。** Dashboard 与 CLI 围绕稳定 use case，而不是围绕表、参数或绘图库组织。
6. **数据库先聚合，客户端再呈现。** 返回行数由时间窗口、图表宽度和查询预算共同约束。
7. **不维持实验性旧接口的长期兼容。** 允许删除旧 DTO、视图、工具封装和重复索引；但数据库迁移必须保存既有有效数据，并经过 expand–backfill–validate–contract。
8. **可观测性也需要可观测。** 队列积压、丢弃、刷新延迟、sink 故障、查询超时和存储余量均是一等指标。

## 决策

### 1. 信号目录与打桩边界

目标信号目录按以下互斥维度管理；它同时承担演进边界，不表示表中每一项已经打桩。当前
as-built 范围是完整 HTTP 流生命周期与四黄金信号、WebSocket 连接终态、worker-local HTTP/WS
活跃量、业务后台任务结果、请求采样的 supervisor/telemetry queue 饱和度、遥测管线累计损失
快照，以及前端诊断和其准入自监控。DB pool、主机资源、发布/配置变更等仍属于路线图：

| 信号域 | 必需观测 | 允许的主要分组维度 | 禁止作为指标维度 |
|---|---|---|---|
| HTTP 流量 | 请求增量、活动请求数、入/出字节 | 服务、方法、路由模板、状态类别、结果 | 原始 URL、query、用户 ID、request/trace ID |
| HTTP 延迟 | 成功与失败分别记录的 duration histogram | 服务、方法、路由模板、结果 | 异常消息、请求正文 |
| 错误 | 错误增量、超时、策略/SLO 违约 | 服务、操作、错误类型、状态类别 | 完整 stack、用户文本 |
| 饱和度 | DB pool used/idle/pending、队列深度与最老年龄、CPU、RSS、线程、文件描述符、磁盘余量 | 服务、资源类型、pool/queue 的稳定名称 | 文件路径、进程命令行 |
| 数据库 | 操作时长 histogram、连接等待/超时、事务回滚、锁等待 | 系统、操作、归一化 query summary | 完整 SQL、DSN、绑定参数 |
| 后台任务 | 入队、开始、完成、失败、重试、队列年龄 | job type、provider、outcome | 输入正文、prompt |
| 变更 | service version、部署环境、配置版本、migration revision、feature flag 版本 | 服务、环境、变更类别 | secret、完整配置 |
| 遥测管线 | accepted/dropped、queue depth、flush duration、batch size、export failure、shutdown loss | sink、drop reason、outcome | 原始失败 payload |

HTTP、数据库与进程字段优先采用 OpenTelemetry semantic conventions；HTTP 路由必须是低基数模板，不是实际路径。当前 HTTP metric/span 使用 `http.request.method`、`http.response.status_code`、`http.route`、`url.scheme`，不再以项目私有的 `method`/`status_code` 代替标准字段。[OpenTelemetry HTTP Metrics](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/)、[OpenTelemetry HTTP Spans](https://opentelemetry.io/docs/specs/semconv/http/http-spans/)、[OpenTelemetry Database Metrics](https://opentelemetry.io/docs/specs/semconv/db/database-metrics/)

HTTP 由最外层 ASGI middleware 观察到最后一个 `http.response.body` 成功发送，完成 span 和 duration
因此覆盖完整 SSE/StreamingResponse 生命周期，而不是只覆盖 response start/TTFB。客户端
`http.disconnect`、Starlette `ClientDisconnect` 或发送端断开归为 499；非断开型流生成异常即使
已经发出 200 response start，终态仍强制归为 500。WebSocket 在握手接受时增加活跃连接数，终态
记录 connection count、duration、server error 与 span，并持久化受控 `close_code`：已接受连接的
1000/1001 是 `success`，未处理服务端异常或 1011–1014 是 `server_error`，其余是
`client_error`。

延迟使用直方图（Histogram）并展示 p50、p95、p99；不得仅记录均值。失败请求的延迟与成功请求分开。百分位数不能跨 bucket 再取平均，长期聚合必须合并原始样本或可合并的 histogram bucket/count/sum。

SLO 与错误预算放在 Overview 首屏。未来告警层采用多窗口多燃烧率（Multi-window Multi-burn-rate），而不是将固定错误率阈值等同于 SLO；具体窗口和预算消耗比例作为策略配置并通过回放验证。当前 as-built Dashboard 只有单窗口、steady-traffic 错误预算估算与健康分级，没有 page/ticket 告警引擎。[Google SRE Workbook：Alerting on SLOs](https://sre.google/workbook/alerting-on-slos/)

### 2. 规范化数据模型

信号写模型属于 `backend.domain`，Dashboard 只拥有自己的查询读模型；二者通过数据库稳定视图集成，而不是共享一个“万能 telemetry DTO”。`workspace_shared` 仅保留真正跨应用且无业务语义的身份/租户值对象。原始持久化采用一张 append-only 的事件信封表，聚合读模型采用独立 rollup/视图；不按日志等级、服务或指标名拆表。

规范化事件信封至少表达：

| 字段组 | 字段与不变量 |
|---|---|
| 身份与来源 | `event_id`、`source`；前端重试保持 `client_event_id`，数据库以 `(workspace_id, resource_owner_id, actor_id, client_event_id)` 在完整 ActorScope 内去重 |
| 时间 | `occurred_at` 表示来源声称的发生时间；`observed_at` 表示服务端收到时间；二者均为 UTC `timestamptz` |
| 类型 | `kind` 为受限枚举 `metric`、`log`、`span`；`name` 为版本化稳定事件名 |
| 资源 | `service`、`service_version`、`service_instance_id`、`deployment_environment` |
| 租户 | `workspace_id`、`resource_owner_id`；`actor_id` 只作受控审计维度，不作 metric label |
| 严重度 | `severity_number`、`severity_text`；比较使用规范化数字，展示保留原始文本 |
| 数值 | `metric_type`、`value`、`unit`；metric 必须有合法有限数值；v2 的 histogram 行是原始观测点，未来 rollup 才使用可合并 bucket/count/sum |
| 关联 | `request_id`、`trace_id`、`span_id`、`parent_span_id`；相关 ID 不是身份凭据 |
| 内容 | 数据库 v2 不保存自由文本 message/stack；异常只保存受控 `error_code` 或 fingerprint |
| 扩展 | `attributes JSONB` 仅容纳经过 schema/白名单校验的稀疏属性；高频过滤字段必须提升为强类型列 |

该信封映射 OpenTelemetry 日志模型中的 `Timestamp`、`ObservedTimestamp`、Severity、Resource、Instrumentation Scope 和 Trace/Span correlation；OpenTelemetry `Body` 仅进入受控流/文件输出，数据库用稳定 `name` 取代自由文本正文。severity 采用 1–24 的规范化范围。[OpenTelemetry Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/)、[OpenTelemetry Service Resource](https://opentelemetry.io/docs/specs/semconv/resource/service/)

OpenTelemetry 部分 semantic conventions 仍可能处于 Development 或 Mixed 状态，因此项目固定已验证的约定版本，并通过 adapter 完成映射；Domain 与数据库 schema 不直接依赖某个 SDK 类。

数据表职责如下：

- `observability.telemetry_records`：规范化、仅追加的原始事件；只允许受限更新用于迁移或保留期维护。
- `observability.dashboard_signals` 稳定安全视图：只暴露脱敏列和受控 attributes，不把底表授权给呈现层。
- 时间 rollup：当前不预建；只有真实执行计划超出预算时，才保存 counter、gauge 和 histogram 的可合并聚合。rollup 不是另一套业务事实。
- 诊断事件仍使用同一信封，以 `source=frontend` 强类型字段标识来源，不创建一张字段重复的“前端日志表”。只有生命周期、权限或查询负载经测量后确实不同，才允许物理分区或冷热表。
- migration `20260721_0006` 的前端幂等约束是覆盖完整 ActorScope 与 `client_event_id` 的部分唯一索引，只对 `source=frontend AND client_event_id IS NOT NULL` 生效；不同 scope 之间不互相碰撞。

### 3. 日志 fan-out 与交付语义

日志路由是 `severity -> set[sink_id]` 的配置化映射。输出 sink 类型为 `stdout`、`stderr`、`file`，数据库 fan-out 由独立开关控制；可声明多个具名文件 sink。默认策略为：

| Severity | STDOUT | STDERR | 轮转 JSONL 文件 | PostgreSQL |
|---|---:|---:|---:|---:|
| DEBUG | ✓ |  | 配置开启时 ✓ | ✓ |
| INFO | ✓ |  | 配置开启时 ✓ | ✓ |
| WARNING |  | ✓ | 配置开启时 ✓ | ✓ |
| ERROR |  | ✓ | 配置开启时 ✓ | ✓ |
| CRITICAL |  | ✓ | 配置开启时 ✓ | ✓ |

“✓”表示该等级被入口阈值接纳后会尝试投递，不表示跨 sink 原子提交。各 sink 独立失败；系统提供的是有界、best-effort 交付，不承诺 exactly-once，也不为了让数据库和文件一致而建立分布式事务。“独立”是结构约束：每条 route 拥有自己的有界队列和单 sink worker，listener 拒绝挂载多个 handler，因此一个阻塞 sink 不能造成跨 route 队头阻塞（head-of-line blocking）。配置同时拒绝第二条 stdout/stderr route 和重复文件路径，确保“一个 sink 一个 owner”不是仅靠运维约定维持。

业务代码只调用一次 logger。composition root 将同一规范化记录送入 handler；不得在调用点分别写 DB、文件和 stream。Python handler 的 level 只有下限语义，因此 stdout/stderr 的互斥区间必须使用精确等级/范围 filter，否则 ERROR 会重复进入低等级 handler。[Python 3.14 Logging Cookbook：Custom handling of levels](https://docs.python.org/3.14/howto/logging-cookbook.html)

运行规则：

1. 每个 stream/file route 通过自己的有界 `QueueHandler`/listener 执行 I/O；数据库使用独立的有界批量 writer。二者都不占用请求或事件循环关键路径。
2. `logging.queue_capacity` 是每条输出 route 的容量，`logging.shutdown_timeout_ms` 是所有输出 worker 共享的总关闭预算；超时 worker 由 daemon reaper 等待 sink 恢复后释放。每次队列满载、prepare/enqueue 失败、sink 失败或关闭超时都累加自监控计数。telemetry 批量管线的 batch size、flush interval、close deadline 和 drop policy 另行显式配置；进程级有界退出还依赖 writer 传播 cancellation 的端口契约，因为 Python coroutine 不能被外部强杀。
3. 数据库 telemetry writer 使用独立仓储/连接路径，且自身错误不得再进入 database sink，避免递归与正反馈环。
4. handler 在命名空间根只安装一次；传播策略固定并测试，避免父子 logger 重复输出。
5. console 与文件使用稳定 UTF-8 JSON Lines；文件按大小轮转并在首次创建及 rollover 后保持 `0600`。面向人的 Dashboard CLI 在 TTY 下使用 Rich 表格和颜色。
6. Python 标准 `FileHandler` 不支持多个进程安全写同一文件。多 worker 部署只能使用集中 listener、外部收集器或每进程独立文件；配置不得静默共享一个轮转文件。[Python 3.14 logging.handlers](https://docs.python.org/3.14/library/logging.handlers.html)
7. prepare/enqueue/sink 故障不调用正常 logger，而是经独立 telemetry pipeline 限频提交稳定失败事件；事件不包含 traceback、异常正文、原始 message 或文件路径。同时覆盖 `QueueHandler.handleError` 与 stream/file handler 的默认错误路径，防止 `logging.raiseExceptions` 开启时把脱敏边界内的异常写入 STDERR。
8. CLI 结果属于 stdout，诊断、警告和进度属于 stderr；`--json` 的机器输出永远不混入运行日志。CSV 尚不是当前 CLI 契约。

容器环境默认只启用 stdout/stderr，由运行时完成采集与轮转；本地文件 sink 是显式能力而不是容器默认值。[Kubernetes Logging Architecture](https://kubernetes.io/docs/concepts/cluster-administration/logging/)

### 4. 分层架构与 Dashboard 领域

三个可执行应用与 shared kernel 保持 src-layout 和单向依赖：

```text
workspace_shared
  └─ 纯身份、租户与通用序列化值对象（无 telemetry 业务 DTO）

backend
  ├─ domain/application：在业务语义处产生信号
  ├─ api/middleware：关联上下文与原生 ASGI HTTP/WebSocket 终态状态机
  ├─ infrastructure：日志 handler、有界队列与数据库 writer
  └─ app/composition：只负责路由、生命周期和依赖装配

dashboard
  ├─ domain：WorkspaceScope、TimeWindow、SLO 与健康策略
  ├─ application：Overview、黄金信号趋势和最近诊断事件 use case/DTO/ports
  ├─ infrastructure：PostgreSQL 聚合读存储、配置、认证与明确的空 demo adapter
  └─ interfaces：Rich CLI、私有 API、PyQt6/Matplotlib GUI 与 Plotly HTML export

dbctl
  └─ migration、bootstrap、retention 与受控 psql maintenance shell
```

约束：

- `dashboard` 不 import `backend` 或 `dbctl`；它只依赖自己的 domain/application 和 `workspace_shared` 稳定契约。
- Domain/Application 不 import FastAPI、Rich、PyQt6、Plotly、Matplotlib、SQLAlchemy 或 psycopg。
- CLI、API 和 GUI 调用相同 use case，消费相同 view model；健康阈值、百分位、SLO 和空数据语义不能在三个 interface 中各实现一次。
- Matplotlib 是当前 PyQt6 GUI 的嵌入式绘图 adapter；Plotly 生成 GUI 可导出的独立交互 HTML。PNG/SVG/PDF 等静态或 headless 导出尚未形成当前接口；若后续增加，也必须作为消费同一 view model 的 adapter。绘图库不是领域模型。
- Dashboard 查询使用独立只读 DSN、statement timeout、最小数据库授权和版本化稳定视图；产品租户界面不得复用可跨工作区读取的 operator 凭据。

### 5. 前端诊断 API 威胁模型

浏览器事件是不可信、可伪造、可能延迟或丢失的数据，不是审计记录、计费事实或业务正确性的依据。W3C Reporting API 同样采用 best-effort、批量、out-of-band 模型，并保留 `age` 来处理延迟；客户端时间只能作分析提示，服务端 `observed_at` 才是接收顺序的事实。[W3C Reporting API](https://www.w3.org/TR/reporting-1/)

v1 事件类型限定为 `error`、`performance` 和 `network` 判别联合。请求 DTO 必须带 `client_event_id`、`occurred_at`、release 与 route template；各分支只接受固定的错误码/fingerprint、Web Vital/value/unit，或 operation/duration/status。环境、service、severity、身份与 observed time 都由服务端决定。

| 威胁 | 典型攻击/事故 | 必须控制 |
|---|---|---|
| 数据伪造 | 客户端伪造管理员、severity、trace 或 release | 服务端重建身份/环境；客户端字段仅为观测声明，不能参与授权 |
| 敏感信息泄漏 | URL query、cookie、token、表单、DOM、storage、源码片段进入日志 | 采集端最小化，服务端再次脱敏；禁止 cookie/header/form/DOM/storage；URL 去 credentials/query/fragment 并模板化敏感 path |
| 存储/CPU 放大 | 巨型 batch、深层 JSON、长 stack、随机 fingerprint、高频重放 | Content-Type 限制、严格 schema、未知字段拒绝、事件/批次/字符串/嵌套上限、rate limit、sampling、稳定 event ID 去重 |
| 日志注入 | CR/LF、控制字符、伪造 JSON 字段 | 结构化编码，清理控制字符，不拼接 SQL 或日志行 |
| 高基数拒绝服务 | 随机 URL、message、user ID 进入 metric dimensions | 维度白名单和 cardinality limit；自由文本只进入有界日志字段 |
| 跨站滥用 | 第三方站点向 endpoint 灌入事件 | HTTPS、明确 first-party CORS allowlist、Origin 校验；匿名模式使用短期随机安装/会话 ID，不使用可跨站追踪的永久 ID |
| 重放与时钟欺骗 | 重复 event ID、未来时间、错误排序 | 幂等去重；时间偏差上限；保留客户端时间并以 `observed_at` 排序 |
| 长期隐私风险 | 原始诊断无限保留并被广泛查询 | 按事件类型设置 retention、访问审计、最小授权、加密与到期删除 |

不得盲目持久化客户端任意字典、完整异常消息或 stack。可保存经限长和路径规范化的 stack 摘要、`exception_type` 与服务端计算的 fingerprint。OWASP 明确建议排除 session/access token、密码、密钥、连接串及敏感个人数据，并对跨信任区事件做校验与日志注入防护。[OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)

当前 endpoint 通过统一 HTTP instrumentation 记录请求量、端到端延迟、状态类别与 server span，并在响应中返回批内 accepted/dropped；严格 schema 采用拒绝而非静默 redaction，数据库按完整 ActorScope 与 client event ID 幂等去重。完成 schema 校验的准入尝试还记录 payload-size histogram、batch counter、ingest-duration histogram，以及 outcome 为 accepted/dropped/rate-limited 的 event counter；维度固定为低基数 `operation`/`outcome`，不把 route、release 或 actor ID 用作 metric attributes。这些指标自身也走有界 best-effort 管线；协议/schema 拒绝由统一 HTTP 信号体现。若后续采样、redaction 或异步 dedupe 成为独立处理阶段，再增加相应阶段计数，不能预先声明不存在的指标。请求大小、批次数、时间窗口和频率均有硬边界，以应对 unrestricted resource consumption。[OWASP API4：Unrestricted Resource Consumption](https://owasp.org/API-Security/editions/2023/en/0xa4-unrestricted-resource-consumption/)

浏览器 OpenTelemetry instrumentation 目前仍标记为 experimental，因此 API 采用项目自己的版本化 DTO，在 adapter 中映射到 OTel 信封，而不直接接受 SDK 导出的任意 payload。[OpenTelemetry Browser Instrumentation](https://opentelemetry.io/docs/languages/js/getting-started/browser/)

### 6. Dashboard 与 CLI 产品策略

Dashboard 的长期信息架构围绕用户问题，而不是数据库表；下列是目标地图，并非都在 v1
同时上线：

1. **Overview**：SLO、错误预算、四个黄金信号、当前异常、变更标记和容量余量。
2. **Traffic & Latency**：rate、p50/p95/p99、histogram/heatmap，并拆分成功与失败。
3. **Errors**：错误率、类型、fingerprint、新增回归和代表性日志。
4. **Saturation**：资源、pool、queue、存储 headroom 与预计耗尽时间。
5. **Database**：操作延迟、连接池、锁、`pg_stat_statements` 归一化 top queries。
6. **Frontend**：LCP、INP、CLS 的 p50/p75/p95，以及 JS/network errors 按 route/release/browser 分组。
7. **Logs**：时间、service、severity、event name、request/trace ID 的受限搜索和上下文跳转。
8. **Changes**：release、配置与 migration 时间线，并与异常窗口叠加。

目标页面共享时间窗口、scope、service、environment 和 release filter；下钻时携带当前上下文。当前 v1 已共享时间窗口、workspace scope 与 service，environment/release 的跨视图过滤仍是路线图。颜色不是唯一状态编码，统一单位、时区、loading/empty/error/stale 状态，并显示数据新鲜度与查询耗时。

Bach 等人基于 144 个 dashboard 的系统分析提出八组设计模式，并在 23 人、两周的设计工作坊中研究其使用；这支持采用可评审的布局/交互模式，而不是为每个页面即兴堆图。该研究是设计过程证据，不代表任何单一图形自动提高运维效果，仍需以任务成功率和事故回放验证。[Bach et al., “Dashboard Design Patterns”, IEEE TVCG 29(1), 2023](https://doi.org/10.1109/TVCG.2022.3209448)

本次 CLI 提供 `overview`、`services`、`traffic`、`latency`、`errors`、`saturation`、`diagnostics`、`frontend`、`health` 九个启发式视图。`frontend` 是固定过滤 `frontend.browser` 的最近事件视图，已显示 error、performance 和 network metric，但不是目标地图中按 route/release/percentile 聚合的完整 Frontend 专页；`health` 读取不带 workspace 归因的 telemetry self-health。Overview 不要求用户重复输入时间、粒度和展示参数；Database、changes、完整 Frontend 聚合与 logs 搜索留在后续、由真实事故查询需求驱动：

- 时间默认最近一小时，结束时间为当前 UTC，粒度按窗口与点预算自动推导。
- 当前可靠性读模型固定以 p95 对照延迟阈值，因此 `latency_target` 必须为 `0.95`；支持其他百分位前，必须先让 SQL、DTO、图例与健康策略作为一个契约共同演进，不能只放开配置值。
- workspace 解析顺序为显式参数 → 配置默认值；无可用默认值时以非零退出，不在非 TTY 环境交互提示。
- 人类 TTY 默认 Rich 表格与颜色；管道环境输出稳定 JSON，`--json` 可强制机器接口。
- `--since`、`--start-at/--end-at`、`--service` 是覆盖项，不是日常样板参数；趋势粒度按窗口与点预算自动推导。
- 五个 console scripts `backend`、`dashboard`、`dashboard-api`、`dashboard-gui`、`dbctl` 均通过 `[project.scripts]` 暴露；正常使用不要求 `python -m ...`。`dashboard-api` 从根配置读取私有监听 host/port。[PyPA：`pyproject.toml` specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/)
- `events`/`diagnostics` 查询包含有界 log/span 和 `source=frontend` metric，因此 Web Vitals 数值及 network duration/status 不会被事件视图静默遗漏。
- `health` 与私有 API `system-health` 只取窗口内按 `observed_at` 最新的一条全 NULL scope worker 快照；多 worker 下它不是聚合或 fleet health，结果可能随最新写入 worker 切换。

### 7. SQL、下采样与存储策略

workspace repository 查询使用半开时间范围 `[start_at, end_at)`，必须带 scope、最大窗口和 statement timeout；唯一例外是 operator-only `system-health`，它只读取全 NULL scope 的最新快照且不制造 workspace 归因。原始事件还必须带硬 limit。当前只提供最近事件窗口，不提供深翻页；未来增加翻页时使用 `(occurred_at, event_id)` keyset pagination，不使用深层 `OFFSET`。

时间序列先在 PostgreSQL 按时间与维度过滤，再聚合或下采样：

- counter 使用窗口增量/rate；gauge 使用语义允许的 min/max/avg/last；histogram 合并 bucket/count/sum；不同 bucket 的 p95 不得再求平均。
- 常规 rollup 用 `date_bin` 对齐固定 1m/5m/1h bucket。[PostgreSQL：Date/Time Functions and Operators](https://www.postgresql.org/docs/current/functions-datetime.html)
- 当前查询层用配置的 `target_points` 和时间窗口自动推导 `date_bin` 桶宽，并允许高级调用方用 `bucket_seconds` 选择不低于安全下限的粒度；它不会把百万行原始样本交给呈现层。若未来需要按实际画布宽度细化预算，再把 `pixel_width`/`max_points` 加入 application/query contract，而不是只在 GUI 丢点。
- 配置校验与 application policy 同时执行不可放大的代码级上限：窗口 31 天、每个 service/time series 的 `target_points` 2,000、事件 `limit` 1,000、显式 `bucket_seconds` 86,400 秒、单条 SQL statement timeout 60,000 ms；部署配置只能进一步收紧。
- 新鲜度有两种语义：实时 Overview 先取每个服务的 `latest_observed_at`，再用 `generated_at - min(service.latest_observed_at)` 表示最陈旧服务，避免新鲜服务掩盖停止上报的服务；窗口结束早于 `generated_at - freshness_target` 的历史查询则用窗口内同一信号行 `observed_at - occurred_at` 的最大值表示最坏采集延迟，既不拼接不同行的独立最大时刻，也不用“距现在多久”把正常历史数据判为 stale。
- 对需要保持尖峰和线段连续性的 gauge line series，可实现 M4 下采样：把时间范围映射为约 `w` 个像素列，每列保留最小值、最大值、首点和末点，返回至多约 `4w` 个候选点。该方法只在论文的 line-rasterization 假设下提供图像保持性质，不用于 histogram、counter 总量、离散事件或审计导出。

M4 论文证明了“图表分辨率是查询基数上界”的设计方向，并在关系查询层完成 value-preserving reduction；因此本系统把点预算作为 application/query contract，而不是仅在 GUI 丢弃点。[Jugel et al., “M4: A Visualization-Oriented Time Series Data Aggregation”, PVLDB 7(10), 2014](https://www.vldb.org/pvldb/vol7/p797-jugel.pdf)，[DOI 10.14778/2732951.2732953](https://doi.org/10.14778/2732951.2732953)

索引与分区遵循测量结果：

1. 默认保留针对近期范围的 B-tree，例如 `(workspace_id, occurred_at DESC)`，以及真实高频查询需要的 `(workspace_id, name, occurred_at DESC)`。
2. ERROR 稀少且查询谓词稳定时才建立 severity partial index；谓词必须能被规划器证明匹配。[PostgreSQL：Partial Indexes](https://www.postgresql.org/docs/current/indexes-partial.html)
3. 当 append-mostly 大表的时间与物理顺序高度相关时，用很小的 BRIN 支撑长窗口扫描；是否替代/补充 B-tree 由基准决定。[PostgreSQL：BRIN Indexes](https://www.postgresql.org/docs/current/brin.html)
4. 只有表规模、保留删除或 vacuum 压力证明需要时才按时间 range partition；过多分区会增加规划和会话内存成本。[PostgreSQL：Table Partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html)
5. INCLUDE/覆盖索引、materialized view 和缓存仅为已观测慢查询添加；每个索引必须说明服务的查询和写放大成本。
6. Canonical Dashboard 查询用生产级统计执行 `EXPLAIN (ANALYZE, BUFFERS)`，并通过 `pg_stat_statements` 跟踪实际频率、规划/执行时间和行数。[PostgreSQL：Using EXPLAIN](https://www.postgresql.org/docs/current/using-explain.html)、[`pg_stat_statements`](https://www.postgresql.org/docs/current/pgstatstatements.html)

Dashboard 在事故期间不能放大故障。当前 v1 使用有界只读连接池、pool checkout/statement timeout、最大窗口、事件硬上限和趋势点预算；短 TTL 缓存、持久 rollup 或额外的 application-level 并发闸门只在实测负载证明需要后增加。遥测数据库变慢时，业务写入采用有界丢弃而不是无限反压。

### 8. 数据库权限与租户边界

- application role 只能向允许的 observability 对象写入，不拥有 migration、retention 或任意 SELECT 权限。
- dashboard role 只读版本化 view，不读取底表；view 只投影无自由文本的强类型列和经过白名单约束的低基数 attributes。
- migrator/owner 执行 schema 变更和受控 backfill；retention 由 `dbctl`/运维路径执行，不由业务请求触发。
- Dashboard application 的每个查询必须携带 `DashboardScope` 并在 repository 再次验证返回数据没有越界。
- 当前 operator dashboard role 若被授权跨 workspace 读取，只能用于受保护的内部运维面。未来租户产品界面必须使用 tenant-scoped session/RLS 或专用 API，不得复用该 operator DSN。
- 视图的 `security_barrier`、owner、`security_invoker`/RLS 行为和 grants 必须通过数据库集成测试验证，不能把应用层 `WHERE workspace_id = ...` 当成唯一隔离边界。

## 迁移策略：Expand–Backfill–Validate–Contract

不为实验性旧 API 承诺兼容，但每次破坏性结构调整均采用四阶段迁移，避免把“允许重构”误解成“允许丢数据”。

### Expand

1. 新增 nullable 列、约束的 `NOT VALID` 版本、v2 稳定视图和必要的新索引；大表避免添加会立即全表重写的 volatile default。
2. 新 writer 在同一行写入新旧可表达字段；这是单表过渡，不建立应用层双表双写。
3. 新 reader 能读取 v2；旧 reader 在发布窗口内仍读取旧视图。数据库升级必须先于依赖新列的应用发布。
4. 大索引使用受控的 online/concurrent 运维步骤，并明确 Alembic transaction 边界、失败后的无效索引清理和重试方法。

### Backfill

1. 按主键或 `(occurred_at, event_id)` 小批量、可重入地回填，设置 statement/lock timeout，并记录进度游标。
2. `observed_at` 回填为可信的数据库创建时间；只有缺少该值时才退回 `occurred_at`。旧 severity text 通过冻结映射生成 `severity_number`。
3. 旧 `service`/`name` 映射到版本化字段；不存在的 trace、unit 或 client time 保持 `NULL`，不得伪造；旧自由文本 body/stack 不复制到 v2。
4. attributes 经过同一白名单/脱敏逻辑；发现非法或敏感值时记录计数并清除/隔离，不把原始值写入迁移日志。
5. 每批提交，允许暂停和重跑；保留期删除不得在同一批事务中与大规模 backfill 竞争。

### Validate

1. 比较迁移前后总行数、按 kind/name/day 的计数、数值 sum/count 与抽样 event ID；记录无法映射的原因分布。
2. 对旧、新 dashboard 视图在重叠窗口比较 golden signals；百分位只比较从同一原始集合重新计算的结果。
3. 执行约束验证、RLS/grant/跨租户负测试、混合版本 reader/writer 测试和 retention dry-run。
4. 用生产规模数据验证 canonical SQL 的计划、buffer、返回点数和 timeout；性能验收失败不得进入 Contract。

### Contract

1. 先切换所有 reader 到 v2 并观察至少一个完整保留/发布窗口，再删除旧视图、列、adapter、重复索引和临时映射。
2. 删除前生成可恢复备份/快照和验证报告；Contract 后的回滚以新版本修复或数据恢复为准，不长期保留双实现。
3. 同步更新数据库 grants、dbctl retention 和稳定 contract version；不留下“已废弃但仍有人写”的影子路径。

## 被拒绝的方案

| 方案 | 拒绝原因 |
|---|---|
| 在业务调用点分别写 stdout、文件和数据库 | 重复副作用、故障语义不一致，无法统一脱敏、采样和测试 |
| 请求路径同步写数据库日志 | 数据库变慢会直接扩大业务延迟，并形成“故障越大、日志越多、数据库越慢”的正反馈 |
| 用事务保证 DB、STDERR、文件 exactly-once | 不同介质没有共同事务；复杂度高且仍不能处理进程崩溃和终端消费者失败 |
| 每个日志等级/指标建表 | 查询和迁移碎片化；等级是属性，不是聚合边界 |
| 所有字段只存 JSONB 或采用 EAV | 类型系统、约束、索引、统计信息和查询可维护性差；高频维度应为列 |
| 所有标签都允许自由扩展 | 导致基数爆炸、隐私泄漏和存储拒绝服务 |
| 用日志搜索代替 metrics/SLI 告警 | 成本、延迟和可靠性不适合告警路径；日志应服务根因分析 |
| 由 GUI 拉原始样本并用 Pandas/Plotly 聚合 | 网络、内存和渲染成本随保留期增长，事故期间容易压垮数据库 |
| 固定平均下采样 | 会抹掉短时尖峰并产生误导；应按指标语义聚合，line series 可采用 M4 类方法 |
| 对各 bucket 的 p95 再求平均 | 百分位不可这样组合，会产生没有统计语义的结果 |
| Dashboard 直接 import backend repository/ORM | 跨应用耦合，CLI/API/GUI 无法共享稳定领域语义，后端迁移会破坏 Dashboard |
| 直接接受浏览器 OTel exporter 或任意 JSON | 浏览器 SDK 尚不稳定且客户端不可信，缺乏版本、容量、隐私和基数边界 |
| 一开始即创建大量分区和覆盖索引 | 写放大、规划开销和维护成本未经真实负载证明 |
| 强制部署独立 Prometheus/Collector 才能上线 | 超出当前模块化单体的必要复杂度；内部 OTel 兼容契约已保留演进路径 |
| 容器默认同时写本地文件与 stdout | 重复存储、轮转责任不清；文件 sink 应由部署模式显式开启 |

## 后果

### 正面

- 运维人员可以同时获得可见 console 日志、持久化事件、交互 Dashboard 和稳定 CLI，而业务代码只承担一次事件表达。
- 当前 Google SRE 黄金信号、SLO 估算和 telemetry 自监控形成可解释的运行时闭环；变更标记仍按路线图推进。
- 前端诊断被建模为受限 ingestion，而不是信任浏览器生成的“日志”。
- Dashboard 成为可测试的领域/应用层产品；GUI、API、CLI 和 Plotly HTML 导出不会复制计算逻辑。
- PostgreSQL 完整窗口聚合、`date_bin` 与代码级点/窗口/timeout 预算限制当前查询成本；真实执行计划仍需在部署数据上建立基线，M4 与 rollup 只在测量证明需要后引入。
- OTel 兼容信封允许未来接入 collector、专用 metrics/logs/traces backend，而不改写业务打桩点。

### 代价与限制

- best-effort 队列在过载和 sink 故障时会丢事件；系统以可观测丢弃和保护业务为优先，不能将其用于合规审计。
- 结构化字段、维度白名单和诊断限长降低任意搜索自由度，需要通过稳定 event name、fingerprint 与 trace correlation 补偿。
- rollup、SLO、版本化视图和迁移验证增加数据库及运维代码，但把复杂度集中在明确边界，而不是散落在每个图表。
- M4 的图像保持结论依赖 line chart 与 rasterization 假设，不能泛化为任意统计查询或图表。
- PostgreSQL 同时承载业务与遥测时仍存在故障域耦合；有界写入只能限制放大，不能提供数据库失效期间的完整遥测。规模或可靠性要求变化时应增加独立 exporter/storage。
- 当前 self-health 只持久化 accepted、dropped、write failure 与 output drop 累计值，并另有请求采样的 queue utilization；flush duration、batch-size distribution、DB pool、CPU/RSS/FD、磁盘余量尚未打桩，不能从路线图反推它们已经存在。
- self-health 累计值属于 worker 进程，使用全 NULL scope 且不带触发请求 ID；内部 operator 可从稳定视图研究它们，但 workspace 视图不会把跨租户累计活动归给任一 workspace。告警严重度按相邻快照的新增损失决定，关停时 best-effort 强制补一条最终快照。
- `aiws.http.server.active_requests` 与 `aiws.websocket.server.active_connections` 是 middleware 实例维护的 worker-local 绝对 gauge，也使用全 NULL ActorScope 且无 request ID。它们不能归因到 workspace，单条最新样本不是 fleet total；多 worker 聚合与进程重启边界需要独立 collector/读模型表达。
- `shutdown_flush_timeout_ms` 是管线 `close()` 的返回上限。生产 writer 的 cancellation-cooperative 契约是进程级上限的前提；一个违反端口契约、吞掉 cancellation 的任意 coroutine 仍可能拖住 Python runner 退出。
- migration `20260721_0006` 在单个 Alembic 事务内使用 shadow table 完成 preflight、回填、语义验证和切换，以保证失败原子回滚；它不是在线、分批双写 migration。事务先执行 `SET LOCAL lock_timeout = '30s'`，再取得 `SHARE ROW EXCLUSIVE` 表锁：先等待在途 writer，再阻止新 DML 直到切表完成，避免回填后插入的行随旧表丢失；任一次锁等待超过 30 秒都会回滚整个 revision。该锁允许 backfill 期间的 `ACCESS SHARE` 读取，但后续 `DROP VIEW`/`DROP TABLE` 仍需更强锁，因此上线前必须暂停 telemetry writer、排空长期 Dashboard/read transaction，并在可接受 telemetry 暂停写入的受控变更窗口执行。仓库当前没有可复现的生产规模数据集、执行计划或性能报告，因此任何行数、p95 或 buffer 数字都不能作为已交付保证；上线前仍需按真实体量排练锁时长、WAL、磁盘余量与 canonical 查询计划。

## 分阶段路线图与验收门槛

### Phase 0：契约已完成，生产基线待部署数据复测

- 冻结事件信封、severity、稳定名称、允许维度、路由策略和前端诊断 v1 schema。
- 已定义 canonical Dashboard SQL 的基线方法；仓库未提交可复现的生产规模行数、p95、buffers 与返回点数报告，部署前必须补齐。
- 验收：类型/序列化测试、跨包依赖测试、敏感字段负测试通过。

### Phase 1：日志与遥测写路径（本次已完成核心范围）

- 完成 stdout/stderr/rotating-file/database fan-out、有界队列、批量写入、rotation、shutdown flush 和 emergency stderr。
- 增加 accepted/dropped/write-failure/output-drop 稀疏快照与 queue utilization；完成事件 schema shadow-table backfill 与新 writer 上线。flush duration/batch distribution 留到测量证明需要时补齐。
- 验收：每个 severity 路由矩阵、handler 去重、queue overflow、DB outage 与 stdout 机器输出测试通过；多 worker 文件输出保持为“每进程独立文件或外部 collector”的部署约束，不宣称已提供跨进程安全轮转。

### Phase 2：SRE 打桩与前端诊断（本次部分完成）

- 已覆盖完整 HTTP 流生命周期与四黄金信号、WebSocket connection/error/duration/span、worker-local HTTP/WS 活跃 gauge、业务后台任务结果、请求采样的 supervisor/telemetry queue 饱和度与遥测自身；DB pool、主机资源及发布/配置变更仍是后续项。
- 已上线版本化诊断 DTO、限额、严格拒绝、rate limit、完整 ActorScope 数据库 dedupe 与统一 retention；准入层已记录 payload/batch/duration 及 accepted/dropped/rate-limited 事件计数，专门的诊断访问审计与 schema-rejection 分阶段计数仍是后续项。
- 验收：四个黄金信号都有数据；恶意 payload、敏感 URL/token、时钟漂移、重放和存储放大测试通过。

### Phase 3：Dashboard/CLI 产品化（本次已完成 v1）

- 按 use case 拆分 domain/application/ports/adapters；CLI、API、PyQt6/Matplotlib GUI 与 Plotly HTML export 共用 view model。
- `overview` 零样板参数可用；v1 共九个视图，并加入最近前端事件 `frontend` 与最新 worker 快照 `health`。Database、完整 Frontend 聚合专页、logs 搜索与 changes 时间线按真实事故需求后续演进。
- 验收：同查询在 CLI/API/GUI 聚合语义一致；无数据、过期、权限失败和查询超时均有明确 UX。

### Phase 4：SQL 读模型与迁移收口（本次部分完成）

- 已上线固定 read view、完整窗口 SQL 聚合、`date_bin` 点预算、查询匹配索引并完成 shadow-table Backfill/Validate/Contract；rollup、keyset logs 与 M4 gauge 查询在实测需要前不实现。
- 根据 `EXPLAIN (ANALYZE, BUFFERS)`/`pg_stat_statements` 决定 B-tree、partial、BRIN、partition 或 materialized view，不以猜测建索引。
- 验收：Overview 和下钻均满足查询预算；任何图表返回点数受控；跨租户直接 SQL 与 API 负测试通过。

### Phase 5：SLO 与外部生态演进（后续）

- 用生产基线定义 SLI/SLO、多窗口燃烧率告警和 error-budget policy；对告警进行历史回放。
- 当 PostgreSQL 故障域、数据量或保留要求成为瓶颈时，再增加 OTel exporter、collector 和专用时序/日志/追踪存储。
- 验收：每个 page/ticket 告警有明确 owner、runbook 和动作；遥测外部化不改变业务打桩契约。

## 参考资料

- [Google SRE Book：Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/)
- [Google SRE Workbook：Monitoring](https://sre.google/workbook/monitoring/)
- [Google SRE Workbook：Alerting on SLOs](https://sre.google/workbook/alerting-on-slos/)
- [OpenTelemetry Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/)
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)
- [OpenTelemetry Metrics SDK：Cardinality Limits](https://opentelemetry.io/docs/specs/otel/metrics/sdk/)
- [OpenTelemetry：Handling Sensitive Data](https://opentelemetry.io/docs/security/handling-sensitive-data/)
- [Python 3.14 Logging Cookbook](https://docs.python.org/3.14/howto/logging-cookbook.html)
- [W3C Reporting API](https://www.w3.org/TR/reporting-1/)
- [W3C Trace Context](https://www.w3.org/TR/trace-context/)
- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
- [PostgreSQL Documentation：Indexes](https://www.postgresql.org/docs/current/indexes.html)
- [Jugel et al., M4, PVLDB 7(10), 2014](https://doi.org/10.14778/2732951.2732953)
- [Bach et al., Dashboard Design Patterns, IEEE TVCG 29(1), 2023](https://doi.org/10.1109/TVCG.2022.3209448)
