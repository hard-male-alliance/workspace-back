# 前端—后端首轮联调交接（Resume + Knowledge）

版本：2026-07-19  
后端基线分支：`codex/stage1-resume-postgres`  
前端参考基线：PR #1 / 本地 `pr-1`

## 1. 本轮边界

本轮只把 **Resume、Resume Template、Resume Proposal、PDF Render Artifact 和 KnowledgeSource 读取**接到真实后端。

- `WorkspaceGateway` 与 `InterviewGateway` 暂时继续使用 Mock。
- `ResumeGateway` 与 `KnowledgeGateway` 新增 HTTP 实现；不删除现有 Mock，它仍用于 Story/Test/离线演示。
- 后端已提供开发环境 CORS、列表分页、模板目录、Proposal 恢复和最近 PDF 产物恢复。
- 后端的 Proposal 生成器、Knowledge 索引器和 PDF 渲染器当前仍是确定性 Mock；HTTP、并发控制和 PostgreSQL 持久化是真实链路。

## 2. 联调启动参数

后端本地配置使用 `config.postgres.local.jsonc`。数据库 DSN 只通过本地环境变量注入，**不要把 DSN 或密码提交到 Git**：

```powershell
cd F:\workspace-back
$env:AIWS_APP_DATABASE_DSN = "<由本地开发者提供>"
.\.venv\Scripts\python.exe -m backend --config config.postgres.local.jsonc
```

前端新增本地文件 `.env.local`（不要提交）：

```dotenv
VITE_API_BASE_URL=http://127.0.0.1:8000
```

前端只能保存公开地址，不能保存 PostgreSQL DSN、模型 API Key、HMAC 密钥或其它 secret。

后端允许的本地 Origin 已包含：

- `http://127.0.0.1:5173`
- `http://localhost:5173`

## 3. 后端已经提供的接口

所有业务路径以 `${VITE_API_BASE_URL}/api/v1` 为前缀。

| 用途 | Method + Path | 关键说明 |
|---|---|---|
| 模板列表 | `GET /resume-templates?locale=zh-CN&limit=20&cursor=...` | 返回正式 `TemplateManifest` 列表 |
| 模板详情 | `GET /resume-templates/{template_id}/versions/{version}` | 找不到时 404 ProblemDetails |
| 简历列表 | `GET /resumes?limit=20&cursor=...` | 当前身份作用域内列表 |
| 创建简历 | `POST /resumes` | 临时 Mock create DTO；必须带 `Idempotency-Key` |
| 简历详情 | `GET /resumes/{resume_id}` | 响应带 `ETag` |
| 指定版本 | `GET /resumes/{resume_id}/revisions/{revision}` | 响应带 `ETag` |
| 修改简历 | `POST /resumes/{resume_id}/operations` | 正式 `ResumeOperationBatch`；带 `If-Match` 和 `Idempotency-Key` |
| 创建 Proposal | `POST /resumes/{resume_id}/proposals` | 临时结构化 create DTO；尚不是通用聊天接口 |
| 恢复 Proposal | `GET /resumes/{resume_id}/proposals?status=pending&limit=20` | 页面刷新后恢复待确认提案 |
| Proposal 详情 | `GET /resume-proposals/{proposal_id}` | 正式 `ResumeProposal` |
| Proposal 决策 | `POST /resume-proposals/{proposal_id}/decisions` | 正式 `ProposalDecisionRequest`；带 `Idempotency-Key` |
| 创建 PDF Job | `POST /resumes/{resume_id}/render-jobs` | 正式 `RenderJobRequest`；返回 202 |
| 查询 PDF Job | `GET /resume-render-jobs/{job_id}` | 轮询到 terminal state |
| 最近 PDF 列表 | `GET /resumes/{resume_id}/render-artifacts?limit=20` | 页面刷新后恢复 PDF |
| PDF 元数据 | `GET /render-artifacts/{artifact_id}` | 正式 `RenderArtifact` |
| PDF 内容 | `GET /render-artifacts/{artifact_id}/content` | 支持 `Range`、`ETag`、`If-None-Match` |
| PDF source map | `GET /render-artifacts/{artifact_id}/source-map` | 正式 `PdfSourceMap` |
| Knowledge 列表 | `GET /knowledge-sources?limit=20&cursor=...` | 包含 Resume 自动派生来源 |
| Knowledge 详情 | `GET /knowledge-sources/{source_id}` | 正式 `KnowledgeSource` |
| Ingestion Job | `GET /knowledge-ingestion-jobs/{job_id}` | 摄取状态读取 |
| 健康检查 | `GET /_internal/healthz` | 仅用于联调存活检查 |

临时创建请求：

```json
{
  "title": "我的简历",
  "locale": "zh-CN",
  "template_id": "tpl_default_v1",
  "template_version": "1.0"
}
```

临时 Proposal create 请求：

```json
{
  "instruction": "根据证据改写职业摘要",
  "title": "职业摘要优化",
  "source_ids": [],
  "target": { "entity_type": "profile" },
  "field_path": ["summary"],
  "render_hint": "preview"
}
```

`source_ids` 留空时后端可使用当前 Resume 自动关联的 KnowledgeSource。完整 Operation、Render 和 Proposal decision 字段以
`contract/ai-job-workspace.contract.schema.json` 为准，不要由 UI 文案反推请求结构。

## 4. 前端必须修改的部分

### 4.1 新增 HTTP 基础设施

建议放在 `packages/app/src/infrastructure/http/`：

- `http-client.ts`：拼接 base URL、JSON 序列化、AbortSignal、ProblemDetails、ETag 和请求 ID。
- `http-resume-gateway.ts`：实现现有 `ResumeGateway` 能直接表达的真实功能。
- `http-knowledge-gateway.ts`：实现 Knowledge 读取功能。
- `mappers/`：只负责 API `snake_case` DTO 到 UI `camelCase` projection 的转换。
- 对上述 mapper 和 Gateway 增加契约样例测试，禁止页面组件直接读取后端 JSON。

不要把 HTTP 请求写进 `ResumeWorkspace.tsx`、`KnowledgePage.tsx` 或 React 组件。

### 4.2 修改运行时装配

当前 `packages/app/src/app/WorkspaceApp.tsx` 缺省创建四个 Mock Gateway。Web 入口应显式注入混合 Gateway：

```text
workspace -> MockWorkspaceGateway
resume    -> HttpResumeGateway
interview -> MockInterviewGateway
knowledge -> HttpKnowledgeGateway
```

建议由 `apps/web` 读取 `import.meta.env.VITE_API_BASE_URL` 并完成装配；共享 `packages/app` 不直接读取 Vite 环境变量。测试环境继续注入 Mock。

### 4.3 列表分页

后端列表统一返回：

```json
{
  "items": [],
  "page": {
    "next_cursor": null,
    "has_more": false
  }
}
```

- `limit` 范围为 1–100，默认 20。
- `cursor` 是不透明字符串，只能原样回传，不能解析、修改或长期缓存。
- 当前 Gateway 方法只返回数组，首轮可循环拉取至 `has_more=false`；若列表增长，应再把 Gateway 领域接口升级为显式分页模型。

### 4.4 DTO 与 UI projection 映射

- 所有后端字段是 `snake_case`，前端 UI 模型是 `camelCase`。
- `ResumeDocument.template_id/template_version` 映射为 `UiResumeDocument.template`。
- `knowledge_source_id` 映射为 `knowledgeSourceId`。
- Resume 列表卡片的 `templateName` 可先用模板目录按 `template_id + version` 解析；找不到时显示模板 ID，不伪造名称。
- `TemplateManifest.previewAssetUrl` 当前必须为 `null`；后端未提供模板缩略图。
- Knowledge 的 `originLabel`、`ingestionStatus`、计数和可见性必须从 `KnowledgeSource`/config/status 明确映射；缺失字段使用 UI 定义的安全默认值并在 mapper 测试中固定，页面不可猜测。
- `UiResumeEditorModel.assistantMessages` 目前后端没有可恢复的正式 conversation/message 查询接口；接真实 Resume 后先返回空数组，或单独保留明确标记的本地 Mock assistant。不要把本地消息冒充服务端持久化消息。
- `UiKnowledgeVisibilityModel.availableAgentScopes` 后端尚无 capability/catalog 接口；此页继续 Mock 或只读，不能伪装成已持久化的设置页面。

### 4.5 并发和幂等

- 第一次读取 Resume 时保存响应 `ETag`。
- 每次提交 `ResumeOperationBatch` 必须发送该值到 `If-Match`。
- 成功后使用返回的新 revision/ETag 刷新本地状态；不要只做 optimistic revision `+1`。
- 412 表示版本过期：重新 GET Resume，并提示用户决定是否重放编辑。
- 每个会产生副作用的 POST 生成新的 `Idempotency-Key`；同一次网络重试复用原 Key，不同用户动作不得复用。
- 409 的幂等冲突、Proposal 证据冲突或状态冲突都必须展示为可恢复错误，不能自动覆盖。

### 4.6 Proposal 交互必须改语义

当前 `MockResumeGateway.sendAssistantMessage()` 会直接修改 Resume，并提供 `undoAssistantChange()`。真实后端语义是：

```text
创建 Proposal -> 显示 evidence/operation 预览 -> 用户 accept 或 reject -> accept 后才修改 Resume
```

因此前端需要在以下两种方案中选一种；推荐第一种：

1. 扩展前端领域端口，新增 `create/list/decideResumeProposal`，UI 显示待确认卡片；保留 `sendAssistantMessage` 作为以后 Conversation/AI Provider 接入点。
2. 临时让 `sendAssistantMessage` 只创建 Proposal，但它的返回类型也必须改成 pending Proposal，不能返回“已直接修改后的 editor”。

`undoAssistantChange` 不能映射成 Proposal reject：reject 只适用于尚未 accept 的 Proposal，已 accept 的变更需未来单独定义反向操作/restore 契约。

后端当前 Proposal create 接收结构化 instruction/target/field_path，使用确定性 Mock Provider。前端自然语言聊天若继续保留，必须明确显示“演示模式”，不能宣称已接入真实模型。

### 4.7 PDF 预览

推荐流程：

```text
POST render-jobs
  -> 取得 job_id
  -> GET job（短轮询，带退避）
  -> 从成功 job 或 artifact list 取得 artifact_id
  -> GET content，将 Blob URL 交给 PDF viewer
```

- 首次打开编辑器先查询 `/resumes/{id}/render-artifacts?limit=1`，存在产物则直接显示。
- 轮询必须在组件卸载/AbortSignal 取消时停止；不要无限快速轮询。
- 失败时展示 job diagnostics；成功后撤销旧的 `URL.createObjectURL`。
- 后端目前没有 Render Job SSE，前端不要连接不存在的事件流。

### 4.8 错误处理和身份头

非 2xx JSON 错误按 `application/problem+json` 的 ProblemDetails 处理，至少覆盖：

- 400：游标/请求语义错误
- 404：资源不存在或不属于当前作用域
- 409：状态、证据或幂等冲突
- 412：ETag 版本冲突
- 422：Schema 校验失败
- 429：限流，遵循 `Retry-After`（若存在）
- 503：依赖或服务暂不可用

浏览器不得发送 `X-Mock-*` 或 `X-AIWS-*` 身份头。开发环境由后端的受限 mock identity 注入默认作用域；生产环境由可信反向代理完成登录/成员校验并重签名，客户端不可伪造。

## 5. 本轮前端验收清单

- [ ] `VITE_API_BASE_URL` 未设置时给出清晰启动错误或明确回退到 Mock，不静默连错地址。
- [ ] 列表、详情、创建、编辑、刷新后恢复均访问 PostgreSQL 后端。
- [ ] 浏览器 CORS 预检通过，Network 中无任何 secret 或身份伪造头。
- [ ] 两个标签页同时编辑时，后提交的一方收到 412 并能刷新恢复。
- [ ] Proposal 在 accept 前不改变 Resume；reject 后不改变 Resume；刷新页面可恢复 pending Proposal。
- [ ] Resume accept/operation 后，Knowledge 列表能看到或刷新关联的 `source_type=resume` 来源。
- [ ] PDF Job 成功后可显示，刷新页面后仍可从 artifact list 恢复。
- [ ] 404/409/412/422/503 均进入可理解的错误态，不出现未捕获异常。
- [ ] HTTP mapper/Gateway 测试通过；现有 Mock Gateway 测试仍通过。

## 6. 明确不属于本轮的内容

- Workspace/成员/登录 API
- Interview API、WebSocket、WebRTC
- 真实模型 API Key、模型域名和 Provider adapter
- Template preview/compatibility check
- Knowledge visibility PATCH、上传、删除、同步和真实 embedding
- Render Job SSE/取消
- 已接受 AI 变更的 undo/restore

这些功能需要先冻结路径级契约，不能在前端通过猜测后端字段完成。
