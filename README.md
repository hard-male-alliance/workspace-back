# AI Job Workspace Backend

v0.1.0 是一个面向 AI 求职工作流的模块化单体（modular monolith）后端。它提供简历操作与受限 XeLaTeX 渲染、流式 Agent、模拟面试 WebSocket、知识库、PostgreSQL/pgvector 持久化，以及只读运维 Dashboard。

这不是一个可以把默认配置直接暴露到公网的“开箱即用 SaaS”。仓库中的默认 `config.jsonc` 特意是 `development + memory + development_mock`，只用于本地演示和测试；生产启动前必须完成本文的数据库、身份和网络边界配置。

## 进程边界

三个可执行应用共享根 JSONC 配置和 `workspace_shared` 中的纯数据类型，但彼此不导入、也不通过 HTTP 相互调用。

| 可执行程序 | 职责 | 数据库身份/网络边界 |
|---|---|---|
| `workspace-backend` | 产品 REST、SSE、WebSocket 与领域编排 | 仅使用 application DSN；仅绑定 loopback 或 Unix socket |
| `workspace-dashboard` | 只读 CLI；API/可选 PyQt6 GUI 复用同一 application layer | 仅使用 dashboard 只读 DSN；默认仅内部网络 |
| `workspace-dbctl` | 显式 bootstrap、Alembic migration、遥测保留期清理与 `psql` shell | 管理员/迁移器身份；后端启动永不自动迁移 |

产品路径、哪些 DTO 只是 mock，以及当前尚未冻结的传输协议，见 [docs/CONTRACT_GAPS.md](docs/CONTRACT_GAPS.md)。`contract/ai-job-workspace.contract.schema.json` 是唯一机器可读的数据结构事实来源；它目前不是完整 OpenAPI 或 AsyncAPI 描述。

## 本地开发

```bash
uv sync --extra dev
uv run workspace-backend
```

另一个终端可运行测试：

```bash
uv run pytest
```

本地默认值使用确定性的内存 repository、mock 模型和仅开发/测试允许的 `X-Mock-*` 租户头。它们不需要 PostgreSQL 或外部模型密钥，也绝不能用于 staging/production。

## 生产前置条件

1. 将部署配置设为 `"environment": "production"`、`"database.mode": "postgresql"`，并将 `security.identity_mode` 设为 `"trusted_proxy_hmac"`。生产/预发布环境缺少 `security`，或仍使用 `development_mock` 时，后端会拒绝加载配置。
2. 后端仅监听私有 loopback、Unix socket 或受限 Pod 网络；安全组、容器网络和防火墙必须禁止客户端绕过入口认证代理直连它。
3. 入口认证代理必须先完成用户认证、workspace membership/role 决策，再重新签发下文定义的 HMAC 断言。Nginx 本身不生成该签名。
4. PostgreSQL 必须启用 `pgvector`，并由 `workspace-dbctl` 显式创建最小权限角色和运行 migration。
5. 非 mock 模型 provider 需要 HTTPS `ai.base_url` 和对应 API key 环境变量；API 不接收或回显 provider、model、base URL 或密钥。
6. 若启用 `resume_rendering.adapter: xelatex`，宿主机必须提供可工作的 OS sandbox；无法建立 sandbox 时实现会 fail closed，而不是退化为不受限的编译子进程。
7. `ai.provider_rate_limit` 是每个实际 endpoint/credential 在单一 backend worker 内的并发与滑动一分钟请求预算；耗尽时返回可重试的 429。多 worker/Pod 部署必须按实例数向供应商申请配额，或在入口层另设全局限流。`ai.metering` 仅记录 UTF-8 字节除以 4 的可复算 token **估算**与整数 micro-USD 成本快照，保存在 `AgentRun.extensions.aiws.metering`；它不是 provider 账单，也不含系统提示词、隐式 tokenizer 或重试计费。

### 环境变量与最小权限

以下 DSN 必须是相互独立的凭证，不能以 application DSN 代替 dashboard 或 migrator DSN。不要将 DSN、密码或 HMAC secret 写入 JSONC、日志、systemd unit 的命令行或 shell history。

| 环境变量 | 使用者 | 应有身份/权限 |
|---|---|---|
| `AIWS_ADMIN_DATABASE_DSN` | `workspace-dbctl bootstrap` | 仅部署期管理员；可创建受控 role/database |
| `AIWS_MIGRATOR_DATABASE_DSN` | `workspace-dbctl migrate`、`prune-telemetry --apply` | `workspace_migrator`；仅 migration/受控维护，可显式 `SET ROLE workspace_owner` |
| `AIWS_APP_DATABASE_DSN` | `workspace-backend` | `workspace_app`；运行时最小 DML 权限与 RLS（row-level security）上下文 |
| `AIWS_DASHBOARD_DATABASE_DSN` | Dashboard PostgreSQL reader、`workspace-dbctl shell --role dashboard` | `workspace_dashboard`；仅 `observability.dashboard_metric_samples` 稳定视图读权限 |
| `AIWS_TRUSTED_PROXY_HMAC_SECRET` | 入口认证代理与 backend | 至少 32 bytes；仅这两个私有工作负载可读取 |
| `AIWS_DASHBOARD_OPERATOR_TOKEN` | Dashboard 私有 HTTP API（若启用） | 仅运维入口持有；不应出现在产品浏览器请求中 |
| `AIWS_LLM_API_KEY`（或配置指定的变量） | non-mock 模型 provider | 仅 backend 进程可读取 |

`workspace-dbctl bootstrap` 管理四个互不相同的 PostgreSQL 角色：不可登录的 `workspace_owner`、可迁移的 `workspace_migrator`、运行时 `workspace_app` 与只读 `workspace_dashboard`。角色名和数据库名来自 `database_administration`；不要在生产中沿用示例密码或把 owner 变成登录角色。

## PostgreSQL 上线顺序

以下命令均假设已经通过 secret manager 注入上述环境变量，并已审阅生产版 `config.jsonc`。后端不会在启动时创建 schema、创建角色或运行 Alembic。

```bash
# 1. 先审阅脱敏、无连接的 bootstrap 计划
uv run workspace-dbctl --config config.jsonc bootstrap --dry-run

# 2. 以管理员 DSN 执行幂等 role/database/schema/权限计划
uv run workspace-dbctl --config config.jsonc bootstrap

# 本地 PostgreSQL 的唯一 sudo 路径，必须显式选择；不会自动 fallback。
# uv run workspace-dbctl --config config.jsonc bootstrap --local-postgres

# 3. 以 migrator DSN 显式升级数据库
uv run workspace-dbctl --config config.jsonc migrate --revision head
```

首次运行和每次发布都应在受控变更窗口检查 migration revision。`bootstrap` 不能替代 migration；反过来，migration 也不创建缺失的数据库角色。

### 遥测保留期

`observability.retention_days` 是保留边界；`0` 明确禁用清理。清理不是请求路径后台任务，而是由受控调度器显式调用的有界维护命令。先完成 `20260715_0003` 或之后的 migration（它提供 telemetry owner maintenance RLS policy），再按以下方式演练并执行：

```bash
# 默认是 dry-run：不连接 PostgreSQL，也不执行删除。
uv run workspace-dbctl --config config.jsonc prune-telemetry --dry-run

# 只有 --apply 会删除；每批独立短事务，强度需由运维者显式给出或接受安全默认值。
uv run workspace-dbctl --config config.jsonc prune-telemetry --apply \
  --batch-size 1000 --max-batches 10 --statement-timeout-ms 5000
```

执行模式只使用 migrator DSN，并在每个短事务中切换到配置的 owner role；它不会借用 application 或 dashboard 凭证。硬上限分别为每批 10,000 条、每次 100 批、每条 SQL 60,000 ms。建议由单实例、受审计的定时任务执行，保留 dry-run 输出、删除总数和剩余候选数。不要把 `--apply` 放进 Web 请求、CI 的无人工确认步骤或无限循环脚本。

## 启动 backend 与 Dashboard

数据库 migration 完成后：

```bash
uv run workspace-backend

# 内部运维 CLI：只读、同一 application layer
uv run workspace-dashboard overview --workspace-id ws_example --output table

# 可选的人类运维 GUI；正常 headless 环境不会安装 PyQt6。
uv sync --extra gui
uv run workspace-dashboard-gui
```

Dashboard 的 API 是可选的私有运维接口，不由 `workspace-dashboard` CLI 自动监听。若确有需要，可由受控 ASGI 服务启动工厂：

```bash
uv run uvicorn dashboard.api:create_fastapi_app --factory --host 127.0.0.1 --port 8010
```

Dashboard 的 `mock` access 仅允许根配置的 `development`/`test` 环境；`staging`/`production`（以及任意 `database.mode=postgresql`）强制 `dashboard.access.mode=operator_token`。PostgreSQL 模式只从 `AIWS_DASHBOARD_DATABASE_DSN` 读取稳定只读视图，绝不复用 application DSN。`/dashboard/v1/healthz` 不读取业务数据但也不应公网暴露；将整个 Dashboard API 保持在 loopback、受控 VPN 或 mTLS 运维网络之后。

## 生产身份边界：trusted proxy HMAC

后端实现的是**可信代理断言验证器**，不是登录、OAuth 或成员资格系统。入口认证代理应在验证用户凭证、选择 workspace 并执行成员资格/角色授权后，为最终转发到 backend 的原始请求目标签发这些 header：

| Header | 值 |
|---|---|
| `X-AIWS-Identity-Version` | 固定 `v1` |
| `X-AIWS-Actor-Id` | 已验证 actor ID |
| `X-AIWS-Workspace-Id` | 已授权 workspace ID |
| `X-AIWS-Resource-Owner-Id` | 已授权资源 owner ID |
| `X-AIWS-Auth-Timestamp` | 无前导零的 Unix 秒 |
| `X-AIWS-Identity-Signature` | 无 `=` 填充的 URL-safe Base64 HMAC-SHA-256 |

签名原文是 UTF-8、LF 分隔的：

```text
AIWS-TRUSTED-PROXY-HMAC-V1
METHOD
RAW_PATH[?RAW_QUERY]
ACTOR_ID
WORKSPACE_ID
RESOURCE_OWNER_ID
UNIX_TIMESTAMP
```

`RAW_PATH`/`RAW_QUERY` 必须保留百分号编码，且必须与 backend 最终收到的原始 target 完全一致；不得对 `%2F` 等编码先解码再签名。实现校验 HMAC、字段格式和双向时钟窗口（上限 600 秒），并额外只接受 `network.trusted_proxy_cidrs` 中实际 TCP 对端发来的断言；它不读取 `X-Forwarded-For`。将 identity proxy/Nginx 的私网地址加入该 CIDR 白名单，而不是将公网客户端网段加入。当前仍没有 nonce/replay store：任何持有密钥的入口代理必须保护密钥，且不得让攻击者重放或直接注入签名 header。HMAC 的有效性也**不等于**业务授权；membership、owner 选择和撤销仍由上游认证代理负责。

`development_mock` 仅允许 `development`/`test`，用于本地 `X-Mock-*` headers。它在 staging/production 被配置加载器拒绝。即便如此，生产入口也应剥离 `X-Mock-*`，以避免运维排障时误将它们视为可信身份。

仓库中的 [deploy/nginx/ai-job-workspace.conf](deploy/nginx/ai-job-workspace.conf) 是**边界示例**：公网 Nginx 会删除所有已知的 `X-AIWS-*` 身份断言和 `X-Mock-*` 头，然后只将 `/api/v1/` 交给私有 `identity_proxy`。该代理才可以在完成认证后重新签名并转发给 loopback backend。不要将公网 Nginx 直接指向 backend，也不要企图用客户端提供的 HMAC header 绕过该代理。

## 网络与健康检查

- `/_internal/healthz` 是 backend 的存活探针，不在产品 contract 中，也不要求 HMAC。它只能从 loopback/Unix socket 或专用私有监听器访问；公网 virtual host 必须返回 404。
- SSE/Agent 流与模拟面试 WS 都必须经 identity proxy；代理链需保留 Upgrade、Connection 和长读超时，同时保持原始请求目标与签名一致。
- 生产应禁用 FastAPI docs；示例公网 Nginx 仅代理 `/api/v1/`，不转发 `/docs`、`/openapi.json`、Dashboard 或内部 health 路径。
- 将 `X-Request-Id` 在入口覆盖为受控值，避免客户端把任意值扩散到日志、响应关联和 telemetry。

## 当前能力边界

以下点在部署时尤其容易被误解：

- 模拟面试的 `ephemeral_token`、`resume_token` 与 `aiws-media-v1` 二进制媒体协议仍是 mock；HMAC 只验证升级请求的调用者，不会使这些 token 或媒体传输自动变成生产级能力。
- Agent SSE 的跨重启/跨 worker 回放、实时 ACK/恢复、render/knowledge/report job SSE 仍未冻结为正式传输契约。
- 内存模式的幂等性只在单进程生命周期内有效；跨重启、跨 worker 的幂等重放依赖 PostgreSQL 模式及其持久记录。
- Dashboard 是运维读模型，不属于产品前端 API。它只暴露低基数的 SRE 指标，不应承载用户内容、prompt、URL 或自由文本。

发布前请把这些限制与 [docs/CONTRACT_GAPS.md](docs/CONTRACT_GAPS.md) 一起纳入威胁建模、反向代理集成测试和客户端兼容性测试。
