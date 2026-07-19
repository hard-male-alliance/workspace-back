# v0.1.0 契约覆盖与安全边界

`contract/ai-job-workspace.contract.schema.json` 是唯一正式的机器可读数据结构来源。它不是 OpenAPI 或 AsyncAPI：目前没有完整、可发布的 `method + path + request + response + header + status` 契约包；部分已实现路由仅通过 FastAPI 暴露 OpenAPI 绑定。本文件不修改、更不推断正式契约；它记录当前实现的覆盖面和所有明确的 mock 边界。

状态含义：**正式结构**表示请求使用已有 Schema 定义校验，或响应使用已有资源结构；**半正式**表示已有 `$defs` 但路径或响应包装未定义；**Mock** 表示使用 `Mock*` DTO、`x-contract-status: mock` 或 mock 传输；**未实现** 表示合同 Markdown 列出但没有路由。

## 覆盖矩阵

| 领域 | Path | 状态 | 缺口与处置 |
|---|---|---|---|
| 身份、Workspace、审计 | 可信代理身份断言；`/me`、workspace/member/invitation/audit/SSE 路径 | 身份边界已实现；资源路径未实现 | HTTP/WS 已可验证 `trusted_proxy_hmac` v1 断言；`CurrentUser`、`Workspace`、`WorkspaceMember`、`AuditEvent` 仍不足以推导产品身份 API、成员资格变更、分页、审计 SSE 或授权响应契约。 |
| 简历模板 | `/resume-templates`、preview、compatibility checks | 列表/详情为正式结构；其余未实现 | 内置模板目录通过 `TemplateManifest` 校验并支持 `locale`、`limit`、`cursor`；媒体 preview 与 compatibility-check 请求/结果尚未实现。 |
| 简历读取与操作 | `GET /resumes`、`GET /resumes/{id}`、历史 revision、`POST /operations` | 正式结构 | `ResumeOperationBatch`/result 已校验；列表支持有界 `limit` 与不透明 `cursor`，尚未冻结公开 sort 协议。 |
| 简历创建及生命周期 | `POST /resumes`；PATCH/DELETE、revision 列表、restore/import/export | 创建为 Mock；其余未实现 | `MockResumeCreateRequest` 不能升级为 `ResumeCreateRequest`；metadata patch、软删除与 job 路径必须先进入合同。 |
| Resume proposal | proposal create/list/GET/decision | 响应与 decision 为正式结构；create 为 Mock | `ResumeProposal` 与 `ProposalDecisionRequest` 已校验，支持按 Resume 恢复 pending Proposal；自然语言到 Proposal 的 create 请求仍是临时 mock adapter，真实 provider 尚未接入。 |
| Render | render-job、job GET、artifact list/metadata/content/source map | 正式结构 | `RenderJobRequest`、`ResumeRenderJob`、`RenderArtifact`、`PdfSourceMap` 已使用；按 Resume 恢复产物、Range/ETag 已实现。job SSE 与取消尚未实现。 |
| Conversation/Message | conversation 与 message 创建 | Mock | 当前为 `MockConversationCreateRequest` / `MockMessageCreateRequest`；资源结构不能反推创建或 PATCH body。 |
| Conversation 管理 | list/detail/PATCH/DELETE/message pagination | 未实现 | 需先定义筛选、游标、软删除和并发语义。 |
| Agent run | run create/GET/SSE/cancel | 正式结构，运行时回放为 Mock | `AgentRunRequest`、`AgentRun`、`AgentStreamEvent` 已校验；事件尚未形成跨重启、跨 worker 的正式重放承诺。 |
| Tool approval / capability / structured output | approval decision；`/_mock/agent-capabilities` | Mock | `ToolApprovalDecision` 尚无路径级绑定；能力发现、`mock.tool_call` approval 与 `structured_json` 标记均是内部 mock 行为。真实 provider adapter 明确声明不支持 tool calling / structured output，直至正式请求、回调与结果契约被冻结。 |
| Knowledge source 读取 | source/list/ingestion-job GET | 正式资源结构 | 列表支持有界 `limit` 与不透明 `cursor`；其它过滤、排序及版本兼容仍未冻结。 |
| Knowledge 创建、索引、搜索 | source create、ingestion create、search | 创建/索引为 Mock；搜索半正式 | 缺少 source/ingestion create 请求；`KnowledgeSearchRequest` 存在但未列为 entrypoint，`{"items": [...]}` 响应包装未冻结。 |
| Knowledge 生命周期 | PATCH/DELETE/version/sync/SSE/access evaluation | 未实现 | 不能从 source 资源结构推导更新、删除或授权评估协议。 |
| 上传与外部连接 | upload、connection、authorization session | 未实现 | 即便已有资源定义，multipart complete、一次性 token 和 secret 生命周期仍必须显式定义。 |
| Interview scenario | scenario list/create/detail/PATCH | 未实现 | 缺少创建/更新 body、分页和 `If-Match` 绑定。 |
| Interview session/report | session create/GET、report GET | 正式结构 | `InterviewSessionCreateRequest`、`InterviewSession` 与 `InterviewReport` 已用于正常路径。 |
| Interview connection/end | connection、end request | Mock | descriptor 没有 connection-request/auth binding；结束请求不能静默等同实时 `SessionEndRequestPayload`。 |
| Interview 其他路径 | session list、report job、transcript、SSE | 未实现 | 需单独定义再生成、transcript 分页/artifact 与 job/event 流。 |
| Interview WebSocket | `WS /interview-sessions/{id}/realtime` | Mock，部分 JSON 控制 | 只处理 ready/interrupt/end/ping；未实现完整 transcript、avatar cue、WebRTC、二进制媒体及恢复协议。 |
| Dashboard | 内部 CLI/API/GUI | 内部运维面 | 不属于产品前端 contract；`mock` access 仅允许 development/test，staging/production 与 PostgreSQL 模式使用独立 dashboard 只读 DSN 且强制 operator token。其 HTTP 路径、token rotation、视图版本、窗口和保留期仍应在独立运维接口契约中冻结。 |

## Resume 到 Knowledge 的内部派生桥

创建 Resume 或接受新的 `ResumeOperationBatch` 后，后端会用既有 `ResumeDocument.knowledge_source_id`（旧快照没有该可选字段时使用稳定的内部回退 ID）创建或刷新同一 `workspace_id + resource_owner_id` 范围内的 `source_type=resume` `KnowledgeSource`。来源 config 严格采用已有的 `ResumeSourceConfig`，`revision_mode=latest`；不新增路径、请求 DTO 或公开字段。来源在入队时固化 revision，过时的异步 job 会以 `skipped` 结束，不能把旧 Resume 内容覆盖到新索引。

该桥仍属于 v0.1 的内部 **Mock** 索引实现：它从 SIR 的用户可读 title/profile/sections 提取有界文本，并用确定性 mock chunk/embedding 建索引。`pinned` 会话语义、Resume 删除时的 source stale/deleted 生命周期、真实 parser/embedding、索引 Job SSE 和跨 worker/outbox 重试均未冻结为公开契约，不能据此推断产品 API 或生产级同步保证。

## 跨路径协议缺口

- **Location**：当前 201/202 响应不保证 `Location`。在正式化前，创建资源/Job 的 Location、幂等重放行为和状态码都不能被客户端依赖。
- **分页、过滤、排序**：Resume、KnowledgeSource、TemplateManifest、ResumeProposal 与 RenderArtifact 列表已使用 `items + page.next_cursor/has_more` 外形，并支持版本化不透明 cursor；其它列表及跨资源 sort/filter 仍未统一，客户端不得解析 cursor 内容。
- **ETag**：Resume GET、artifact content 有 ETag；其它资源尚未统一条件请求和更新并发语义。
- **幂等性**：memory 模式使用进程内注册表；它在重启和多 worker 下不保证 24 小时重放。PostgreSQL 模式有持久幂等记录，但客户端仍不能仅凭本文件依赖固定的 TTL、冲突 ProblemDetails、`Location` 或跨版本重放语义。
- **SSE**：Agent SSE 支持 `Last-Event-ID`，但在持久 outbox/replay 窗口完成前，跨重启恢复、heartbeat 与过期后重新 GET 都仍是运行时 mock 行为。render/knowledge/report job SSE 尚未实现。

## 身份边界与实时传输：已实现部分及禁止误作生产能力的部分

1. **HTTP 与 WebSocket 共用受限的身份边界。** 在 `development`/`test`，`development_mock` 允许 `X-Mock-Actor-Id`、`X-Mock-Workspace-Id`、`X-Mock-Resource-Owner-Id`；在其它环境该模式会被配置加载器拒绝。staging/production 必须选择 `trusted_proxy_hmac`，并使用 `X-AIWS-Identity-Version: v1`、actor/workspace/owner、时间戳和 HMAC-SHA-256 断言。HTTP middleware 与 WS upgrade 都验证未解码 path/query、签名、ID 格式和时钟窗口；`/_internal/healthz` 是例外的私有存活探针。
2. **HMAC 不是登录或授权 API。** backend 只验证可信代理断言；它不验证 Bearer/OIDC credential，不查询 workspace membership/role，不实现 token revocation，也不提供 `/me`、workspace/member/invitation/audit 的产品契约。上游认证代理必须先做这些决策，再为最终原始 request target 重签名。
3. **入口部署是该安全机制的一部分。** 公网必须先剥离所有客户端 `X-AIWS-*` 身份断言与 `X-Mock-*` 头，且 backend 不能被绕过直连。当前签名含时间戳但没有 nonce/replay store；密钥泄漏、可访问 backend 的攻击者、或在有效窗口内的断言重放都属于部署方必须消除的风险。完整 header 与 canonicalization 见 README 的生产身份边界章节。
4. **Realtime token 仍不是认证实现。** `ephemeral_token` 和 `resume_token` 目前没有哈希持久化、TTL/单用途/受众/session/主体绑定、撤销或 WS accept 校验；HMAC 只认证创建或 upgrade 请求的主体，不能赋予这些 token 上述语义。
5. **不得将 `signaling_url` 或 `fallback.websocket_url` 当作可投产连接描述。** 当前没有已冻结的 Origin、subprotocol、连接限流或 WebRTC 握手契约；浏览器部署仍需要明确 cookie/Origin 防护与连接配额。
6. **`aiws-media-v1` 未实现。** 服务端当前只接收 JSON；未实现二进制 frame、flags/字节序、最大帧、丢帧/队列策略、序列号执行或媒体授权。正式握手规范和测试向量确定前必须禁用该宣称。
7. **恢复与 ACK 仅有字段，没有执行语义。** 当前不验证 client sequence 单调性、重复事件、`ack_sequence`、`last_received_sequence`，也不会按 resume token 回放。
8. **token 和媒体敏感信息不得进入日志或 telemetry。** 正式方案至少应定义 issuer、subject、session、purpose、TTL、单用途、撤销、哈希存储及 redaction 规则；长期 token 不得放在 SDP、query、媒体帧或错误对象中。

## 已实现但不声称为 Schema 约束的安全策略

- 录音或录像要求 `user_consent_at` 与 `consent_version`；
- 知识检索为 deny-priority，`mode=none` 不读取任何来源；
- item 级简历修改要求同时提供 `section_id` 与 `item_id`；
- `KnowledgeSource.source_type` 与生成的 source config 保持一致；
- XeLaTeX 仅在 OS sandbox 可用时运行；不可用时 fail closed；
- telemetry 仅允许低基数属性，禁止 prompt、URL、用户文本和异常自由文本。

这些检查是领域安全策略，不能被当作已发布 Schema 的静默收紧。正式化顺序应是：先补路径级 OpenAPI/AsyncAPI 契约与安全语义，再删除 mock 标记，并冻结 OpenAPI 差异基线和互操作测试向量。
