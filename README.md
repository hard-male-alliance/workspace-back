# AI Job Workspace Backend

v0.1.0 是一个面向 AI 求职工作流的模块化单体（modular monolith）后端。它提供简历操作与受限 XeLaTeX 渲染、流式 Agent、模拟面试 WebSocket、知识库、PostgreSQL/pgvector 持久化，以及只读运维 Dashboard。

这不是一个可以把默认配置直接暴露到公网的“开箱即用 SaaS”。仓库提交的是无密钥 `example.jsonc` 与数据库目标状态 `dbinit.jsonc`；本地 `config.jsonc` 被 Git 忽略。首次执行需要加载配置的 `dbctl` 子命令时，若省略 `--config` 且默认 `config.jsonc` 不存在，`dbctl` 会优先复制同一运行目录的 `example.jsonc`，该目录没有模板时再读取 wheel 内置示例，并生成登录角色密码。生产启动前必须完成本文的数据库、身份和网络边界配置。

## 进程边界

三个可执行应用共享根 JSONC 配置和 `workspace_shared` 中的纯数据类型，且不通过 HTTP 相互调用。Dashboard 不导入 backend 或 dbctl；`dbctl migrate` 所执行的 Alembic 环境会复用 backend 的持久化 metadata，这是当前唯一明确的跨应用 Python 依赖。

| 可执行程序 | 职责 | 数据库身份/网络边界 |
|---|---|---|
| `backend` | 产品 REST、SSE、WebSocket 与领域编排 | 仅使用 application DSN；仅绑定 loopback 或 Unix socket |
| `dashboard` / `dashboard-api` / `dashboard-gui` | 只读 CLI、私有 API 与可选 PyQt6 GUI；三者复用同一 application layer | 仅使用 dashboard 只读 DSN；默认仅内部网络 |
| `dbctl` | 显式 bootstrap、Alembic migration、遥测保留期清理与 `psql` shell | bootstrap 使用 sudo 或终端密码验证；其余使用迁移器/目标身份；后端启动永不自动迁移 |

产品路径、哪些 DTO 只是 mock，以及当前尚未冻结的传输协议，见 [docs/CONTRACT_GAPS.md](docs/CONTRACT_GAPS.md)。独立的 `workspace-shared-docs` submodule 是前后端共享契约的唯一事实来源；其中 `contracts/v1/ai-job-workspace.contract.schema.json` 会原样打入 backend wheel，但它目前不是完整 OpenAPI 或 AsyncAPI 描述。首次 clone 必须使用 `git clone --recurse-submodules`，已有 checkout 则执行 `git submodule update --init --recursive`。

## 本地开发

```bash
uv sync --extra dev
# 首次 checkout：生成权限为 0600 的默认 config.jsonc；不连接或修改 PostgreSQL
uv run dbctl bootstrap --dry-run
uv run backend
```

另一个终端可运行测试：

```bash
uv run pytest
```

本地默认值使用确定性的内存 repository、mock 模型和仅开发/测试允许的 `X-Mock-*` 租户头。它们不需要 PostgreSQL 或外部模型密钥，也绝不能用于 staging/production。
`backend` 默认读取当前工作目录的 `config.jsonc`，也可用 `--config` 或 `AIWS_CONFIG` 覆盖；日志与知识库 blob 的相对路径统一以该配置文件所在目录为基准，因此 wheel 安装态不会把运行数据误写进虚拟环境。

## Docker 部署

仓库根目录提供多阶段、非 root 的生产镜像和完整 Compose 拓扑。Docker 不复制数据库初始化逻辑：`dbctl bootstrap` 仍是创建 `config.jsonc`、database、roles、schemas 与 permissions 的唯一入口。持久配置保存在私有 `runtime_config` volume；容器入口只生成临时运行投影，不生成数据库密码或修改 PostgreSQL。

```bash
cp .env.docker.example .env
# 只启动一个尚无业务数据库/角色的 PostgreSQL 17 + pgvector 实例。
docker compose up --build --detach postgres

# 交互式输入一次 .env 中的 PostgreSQL 管理密码；dbctl 生成随机应用凭证并完成 bootstrap。
docker compose run --rm bootstrap

# migration 同样是显式状态变更，不会夹带在 backend 启动流程中。
docker compose run --rm migrate

docker compose up --detach backend
docker compose ps
curl --fail http://127.0.0.1:8000/_internal/healthz
```

`bootstrap` 固定使用 `--access-mode prompt`：容器之间不存在可继承的宿主 `sudo`/Unix peer 身份，因此它通过私有 Compose 网络连接 `postgres`，只在 TTY 中读取一次管理员密码。密码不会进入 argv、应用镜像或 `config.jsonc`；`config.jsonc` 只保存 `dbctl` 自动生成的三个最小权限登录角色凭证。重复执行 bootstrap 会按 `dbctl` 的幂等计划收敛现状，而不是依赖“仅空 volume 执行一次”的镜像初始化脚本。

Compose 不会在 `docker compose up backend` 时暗中 bootstrap 或 migrate；缺少持久配置或 schema 时会直接失败。应用镜像以 UID/GID `10001`、只读根文件系统、全部 capability 被移除的方式运行；只有 `/tmp` tmpfs 与 `application_data` 持久卷可写。backend 和可选 Dashboard 只发布到宿主 loopback。

可选启动只读 Dashboard API：

```bash
docker compose --profile dashboard up --detach dashboard
curl --fail http://127.0.0.1:8010/dashboard/v1/healthz
```

查看服务日志，或在变更窗口再次显式执行 migration：

```bash
docker compose logs --follow backend
docker compose run --rm migrate
```

生产部署前必须至少完成这些修改：

1. 替换 `.env` 中 PostgreSQL 管理密码与 Dashboard token；app、migrator、dashboard 数据库密码由 `dbctl bootstrap` 生成并保存在私有配置 volume，不在 `.env` 维护第二份状态。
2. 设置 `AIWS_ENVIRONMENT=production`、`AIWS_IDENTITY_MODE=trusted_proxy_hmac`、至少 32 bytes 的 `AIWS_TRUSTED_PROXY_HMAC_SECRET` 与真实 `AIWS_PUBLIC_BASE_URL`。
3. 在宿主 loopback 端口前部署能够认证用户、判定 workspace membership/role 并签发 HMAC 断言的 identity proxy；仓库中的 Nginx 示例本身不提供认证。
4. 非 mock 模型需设置 `AIWS_AI_PROVIDER`、`AIWS_AI_MODEL`、HTTPS `AIWS_AI_BASE_URL`、`AIWS_AI_DATA_REGION` 和 `AIWS_LLM_API_KEY`。
5. 基础镜像不安装 TeX Live/bubblewrap，故默认 renderer 为 mock。真实 XeLaTeX 必须使用经审阅的派生镜像与可工作的 OS sandbox，不能通过赋予 backend 广泛容器权限来绕过 fail-closed。

Docker 专用 `deploy/docker/dbinit.jsonc` 只把连接端点从宿主 loopback 改为 Compose 服务名 `postgres`；其 database/role/schema 声明必须与根 `dbinit.jsonc` 保持一致。不要把生产 secret 烘焙进镜像层或提交到仓库。

## 生产前置条件

1. 将部署配置设为 `"environment": "production"`、`"database.mode": "postgresql"`，并将 `security.identity_mode` 设为 `"trusted_proxy_hmac"`。生产/预发布环境缺少 `security`，或仍使用 `development_mock` 时，后端会拒绝加载配置。
2. 后端仅监听私有 loopback、Unix socket 或受限 Pod 网络；安全组、容器网络和防火墙必须禁止客户端绕过入口认证代理直连它。
3. 入口认证代理必须先完成用户认证、workspace membership/role 决策，再重新签发下文定义的 HMAC 断言。Nginx 本身不生成该签名。
4. PostgreSQL 必须启用 `pgvector`，并由 `dbctl` 显式创建最小权限角色和运行 migration。
5. 非 mock 模型 provider 需要 HTTPS `ai.base_url` 和对应 API key 环境变量；API 不接收或回显 provider、model、base URL 或密钥。
6. 若启用 `resume_rendering.adapter: xelatex`，宿主机必须提供可工作的 OS sandbox；无法建立 sandbox 时实现会 fail closed，而不是退化为不受限的编译子进程。
7. `ai.provider_rate_limit` 是每个实际 endpoint/credential 在单一 backend worker 内的并发与滑动一分钟请求预算；耗尽时返回可重试的 429。多 worker/Pod 部署必须按实例数向供应商申请配额，或在入口层另设全局限流。`ai.metering` 仅记录 UTF-8 字节除以 4 的可复算 token **估算**与整数 micro-USD 成本快照，保存在 `AgentRun.extensions.aiws.metering`；它不是 provider 账单，也不含系统提示词、隐式 tokenizer 或重试计费。

### 本地凭证与最小权限

以下 DSN 必须是相互独立的凭证，不能以 application DSN 代替 dashboard 或 migrator DSN。`dbctl` 将生成的角色与密码直接组成 DSN，写入权限为 `0600`、被 Git 忽略的本地 `config.jsonc`；DSN 和 HMAC secret 均不得进入可提交文件、日志、systemd unit 命令行或 shell history。bootstrap 不接受管理员 DSN：POSIX 上 `auto` 优先通过 `sudo -u postgres` 验证，Windows 或找不到 sudo 时在终端提示 `bootstrap_database_user` 的 PostgreSQL 密码。

| 本地 `config.jsonc` 字段 | 使用者 | 应有身份/权限 |
|---|---|---|
| `database.migrator_dsn` | `dbctl migrate`、`prune-telemetry --apply` | `workspace_migrator`；仅 migration/受控维护，可显式 `SET ROLE workspace_owner` |
| `database.application_dsn` | `backend` | `workspace_app`；运行时最小 DML 权限与 RLS（row-level security）上下文 |
| `database.dashboard_dsn` | Dashboard PostgreSQL reader、`dbctl shell --role dashboard` | `workspace_dashboard`；仅 `observability.dashboard_signals` canonical 读模型权限 |

| 其他 secret 环境变量 | 使用者 | 要求 |
|---|---|---|
| `AIWS_TRUSTED_PROXY_HMAC_SECRET` | 入口认证代理与 backend | 至少 32 bytes；仅这两个私有工作负载可读取 |
| `AIWS_DASHBOARD_OPERATOR_TOKEN` | Dashboard 私有 HTTP API（若启用） | 仅运维入口持有；不应出现在产品浏览器请求中 |
| `AIWS_LLM_API_KEY`（或配置指定的变量） | non-mock 模型 provider | 仅 backend 进程可读取 |

`dbctl bootstrap` 管理四个互不相同的 PostgreSQL 角色：不可登录的 `workspace_owner`、可迁移的 `workspace_migrator`、运行时 `workspace_app` 与只读 `workspace_dashboard`。角色名、数据库名、schema 与权限目标来自 `dbinit.jsonc`；三个登录角色的随机密码写入本地 `config.jsonc`，owner 保持 `NOLOGIN` 且没有密码。

`dbinit.jsonc` 的 `database_connection` 声明非敏感的主机和端口；`dbctl` 根据它以及角色、数据库声明生成 `database.application_dsn`、`database.migrator_dsn`、`database.dashboard_dsn`。bootstrap 从这三个实际 DSN 取得相应角色密码，因此写入配置和创建角色使用的是同一份凭证。

## PostgreSQL 上线顺序

以下命令均假设已经审阅 `dbinit.jsonc`，并可在当前终端完成系统 sudo 或 PostgreSQL 管理密码验证。省略 `--config/--dbinit` 时，若默认 `config.jsonc` 不存在，第一次需要配置的 `dbctl` 调用会优先使用同目录 `example.jsonc`，找不到时才使用安装包资源，生成配置和三个登录角色密码，并将权限收紧为 `0600`；默认 `dbinit.jsonc` 同样优先读取同目录文件，缺失时只读取包内声明而不在当前目录复制一份。显式指定但不存在的路径会 fail closed。后端不会在启动时创建 schema、创建角色或运行 Alembic。

```bash
# 1. 先审阅脱敏、无连接的 bootstrap 计划
uv run dbctl bootstrap --dry-run

# 2. 自动选择 POSIX sudo 或跨平台终端密码验证
uv run dbctl bootstrap

# 3. 以 migrator DSN 显式升级数据库
uv run dbctl migrate --revision head

# 4. 自动使用 config.jsonc 的 app role 与密码进入 psql；不会再次询问密码
uv run dbctl shell

# 可选：同样从 config.jsonc 自动选择 migrator 或 dashboard 身份
uv run dbctl shell --role migrator
```

使用非默认位置时，同时传入 `--config <runtime.jsonc>` 与 `--dbinit <dbinit.jsonc>`。`config.jsonc` 属于本机 secret 载体，不应提交；`dbinit.jsonc` 是可审阅、可提交的声明式初始化计划。

可用 `bootstrap --access-mode sudo` 强制 POSIX sudo，或用 `bootstrap --access-mode prompt` 强制跨平台 PostgreSQL 密码提示。prompt 只读取一次不回显密码，临时写入仅供本轮 psql 子进程使用的受限 pgpass 文件，并在执行结束后删除；密码不会进入 argv、`config.jsonc` 或子进程环境变量值。

只有 `bootstrap` 可以创建或补全私密 `config.jsonc`。`migrate`、`shell` 与执行态维护命令只读既有配置；配置缺失时直接失败，绝不会偷偷生成一套尚未写入 PostgreSQL 的新密码。`shell` 为本次 psql 创建权限为 `0600` 的临时 `PGPASSFILE`，强制 `--no-password`，继承真实 TTY，并在 psql 退出后清理文件；配置密码不会进入 argv，也不会被外部 `.pgpass` 或 `PGPASSWORD` 覆盖。

首次运行和每次发布都应在受控变更窗口检查 migration revision。`bootstrap` 不能替代 migration；反过来，migration 也不创建缺失的数据库角色。

### 遥测保留期

`observability.retention_days` 是以服务端 `observed_at` 为准的保留边界；`0` 明确禁用清理。清理不是请求路径后台任务，而是由受控调度器显式调用的有界维护命令。先完成 `20260721_0006` migration（它提供 v2 信号表、owner maintenance RLS policy 与清理索引），再按以下方式演练并执行。

`20260721_0006` 是单事务 shadow-table 切换，不是在线双写 migration。它在取得
`SHARE ROW EXCLUSIVE` 前执行 `SET LOCAL lock_timeout = '30s'`；任一次锁等待超过 30 秒都会让整个
revision 回滚。升级前应暂停 telemetry writer，并排空长期 writer 与 Dashboard/read transaction：
backfill 期间普通 `ACCESS SHARE` 读取仍可继续，但后续 `DROP VIEW`/切表需要更强锁，长读事务同样
会阻塞切换。只在可接受 telemetry 暂停写入的受控维护窗口执行。

```bash
# 默认是 dry-run：不连接 PostgreSQL，也不执行删除。
uv run dbctl prune-telemetry --dry-run

# 只有 --apply 会删除；每批独立短事务，强度需由运维者显式给出或接受安全默认值。
uv run dbctl prune-telemetry --apply \
  --batch-size 1000 --max-batches 10 \
  --statement-timeout-ms 5000 --lock-timeout-ms 500
```

执行模式只使用 migrator DSN，并在每个短事务中切换到配置的 owner role；它不会借用 application 或 dashboard 凭证。删除通过 `FOR UPDATE SKIP LOCKED` 的有界 CTE 完成，硬上限分别为每批 10,000 条、每次 100 批、每条 SQL 60,000 ms、锁等待 5,000 ms。结果只报告删除数与 `has_more`，不会为“精确剩余数”全表计数。建议由单实例、受审计的定时任务执行；不要把 `--apply` 放进 Web 请求、CI 的无人工确认步骤或无限循环脚本。

## 启动 backend 与 Dashboard

数据库 migration 完成后：

```bash
uv run backend

# 内部运维 CLI：零参数即显示默认工作区 Overview；TTY 自动表格、管道自动 JSON
uv run dashboard
uv run dashboard latency --workspace ws_example --since 6h
uv run dashboard diagnostics --since 30m --limit 50
uv run dashboard frontend --since 30m
uv run dashboard health

# 可选私有只读 API；监听地址和端口读取 dashboard.api 配置。
uv run dashboard-api

# 可选的人类运维 GUI；正常 headless 环境不会安装 PyQt6。
uv sync --extra gui
uv run dashboard-gui
```

`uv sync`/wheel 安装会生成 `backend`、`dashboard`、`dashboard-api`、`dashboard-gui`、`dbctl` 五个
console entry point；激活虚拟环境后可直接去掉上例的 `uv run` 前缀，例如 `dbctl migrate
--revision head`。这些入口均已从 wheel 内直接导入并启动验证，不依赖源码目录或
`python -m ...`。

Dashboard 只有一套 `domain → application → infrastructure/interfaces` 分层实现，由
`dashboard.bootstrap` 统一组合。CLI 提供 `overview`、`services`、`traffic`、`latency`、
`errors`、`saturation`、`diagnostics`、`frontend`、`health` 九个视图。`frontend` 是聚焦
`frontend.browser` 的最近事件视图，其中 error、performance 与 network 信号均可见；`health`
不要求 workspace，读取全 NULL scope 的遥测管线快照。私有 API 暴露 `overview`、`trends`、
`events` 与 `system-health`（另有不读取业务数据的 `healthz`）。GUI 的查询在持久后台事件循环执行，Qt 更新通过 queued signal 回到 UI 线程；退出时
先取消在途查询，在同一 worker loop 中关闭 asyncpg pool，再停止 loop。

读模型的时间语义是：`occurred_at` 决定查询窗口。窗口仍接近当前时间时，Overview 先取各服务的
`latest_observed_at`，再以其中最旧的值计算新鲜度（freshness），即
`now - min(service.latest_observed_at)`；一个刚上报的服务不能掩盖另一个已停止上报的服务。
历史窗口结束时间早于当前时间减去 freshness target 时，显示的是
窗口内每条信号在同一行上的 `observed_at - occurred_at` 最大值，即最坏采集延迟（collection lag）；
它不会把不同行的独立最大时刻拼接，也不会把历史数据距今多久误判为 stale。
实时过期遥测显示 `NO_DATA`，不会产生假绿。Overview 在 PostgreSQL 对完整窗口聚合，趋势使用
`date_bin`；有界事件流包括 log/span，以及 `source=frontend` 的 performance/network metric，
统一使用 `LIMIT`。`http.server.request.duration` 按 canonical 单位秒存储，
展示时统一换算为毫秒；流量、错误和饱和度只读取 migration 定义的稳定指标名。错误预算剩余值
是 steady-traffic estimate：`max(0, 1 - burn_rate × min(window / SLO period, 1))`，不是供应商
账单式的精确消耗记录。`database.mode=memory` 是明确标注的空 demo adapter，不代表持久化运行态。

查询预算在配置校验与 application policy 两层收紧：任何窗口最多 31 天，每个 service 的趋势
目标点预算最多 2,000，最近事件最多 1,000 条，显式 `bucket_seconds` 最多 86,400 秒，每条 PostgreSQL
statement timeout 最多 60,000 ms；部署配置可以设得更小，不能放大这些代码级上限。

`health`/`system-health` 返回所选窗口内、按 `observed_at` 排序的**一条最新 worker 快照**，
不是所有 worker 的合计或集群健康。多 worker/Pod 部署时，结果可能随最新写入者在 worker
之间切换，累计计数不能当作 fleet total；需要逐实例诊断时应接入带实例维度的独立读模型或
外部 collector。

Dashboard 的 `mock` access 仅允许根配置的 `development`/`test` 环境；`staging`/`production`（以及任意 `database.mode=postgresql`）强制 `dashboard.access.mode=operator_token`。PostgreSQL 模式优先从 `database.dashboard_dsn` 读取稳定只读视图，绝不复用 application DSN。`/dashboard/v1/healthz` 不读取业务数据但也不应公网暴露；将整个 Dashboard API 保持在 loopback、受控 VPN 或 mTLS 运维网络之后。

当前延迟 SLO 的查询、DTO 与呈现统一采用 p95，因此 `dashboard.health.latency_target` 固定为 `0.95`；若要支持 p99 等其他目标，应同时扩展查询与健康策略，而不是仅修改配置数字。

## 结构化日志、信号与前端诊断

`logging.routes` 将每个标准等级精确映射到一个或多个 `stdout`、`stderr`、`file` sink；
示例配置把 DEBUG/INFO 写到 STDOUT，WARNING/ERROR/CRITICAL 写到 STDERR，并把全部等级写入
权限为 `0600`、按大小轮转的 `data/logs/backend.jsonl`。每条 route 都拥有独立的有界队列和
worker：一个 sink 阻塞、失败或队列满载时，不会阻塞其他 sink 或业务请求，且会累计丢弃数。
同一 stdout/stderr 或同一文件路径只能由一条 route 拥有，避免重复输出及两个 rotator 争用文件。
`logging.shutdown_timeout_ms` 是所有输出 worker 共享的总关闭预算；超时 worker 由 daemon reaper
回收。prepare/enqueue/sink 故障只提交限频、脱敏的稳定事件，不会把 traceback、异常正文或文件路径写到 emergency STDERR。
`logging.persist_structured_events=true` 时，同一 `event_id` 还会进入独立的数据库
管线；PostgreSQL 只保存稳定事件名、严重度、低基数字段和 trace 关联，不保存自由文本 message
或 stack。多 worker 不得共享同一个轮转文件，应改为每进程文件或外部日志收集器。

后端在最外层 ASGI middleware 中观察每个非健康检查 HTTP 请求：只有最后一个
`http.response.body` 成功发送后才记录完整流生命周期，因此 SSE/StreamingResponse 不会把响应头
时刻误当成完成时刻。`http.disconnect`、Starlette `ClientDisconnect` 和发送端断开记为 499；
非断开型流生成异常即使已经发出 200 response start，终态也强制记为 500。请求流量、秒制
duration histogram、仅 5xx 的服务端错误和完成 span 使用低基数路由模板及标准
OpenTelemetry（OTel）HTTP attributes：`http.request.method`、`http.response.status_code`、
`http.route`、`url.scheme`，并关联 W3C Trace Context。

WebSocket 在连接终态记录 connection count、duration histogram 与
`websocket.server.connection` span，并为服务端错误记录 error count；`close_code` 作为受控属性
持久化。已接受连接
以 1000/1001 关闭归为 `success`，未处理服务端异常或 1011–1014 归为 `server_error`，其余拒绝、
异常/协议关闭归为 `client_error`。HTTP/WS 还分别在活跃数变化时写入
`aiws.http.server.active_requests` 与 `aiws.websocket.server.active_connections` gauge。
这两个 gauge 是 worker-local（单 worker）瞬时值，使用全 NULL ActorScope 且无 request ID，
不能归因给某个 workspace，也不能把任一最新样本解释为 fleet total。

业务 supervisor/telemetry queue 饱和度、业务 Job 结果以及前端 Web Vitals/错误/网络耗时也映射为
同一强类型信封。写入采用独立小连接池、批量
`ON CONFLICT DO NOTHING` 和有界 best-effort 队列；数据库故障不会拖垮业务，且会通过不可
递归、限频的 emergency STDERR 事件显式可见。

管线的 accepted、dropped、write-failure 与日志输出 drop 累计值以
`aiws.telemetry.health.snapshot` 稀疏持久化，并在关停时补写最终快照。它们是 worker
进程级状态，固定使用全 NULL scope、无 request ID，不能归因给触发采样的某个 workspace；
告警等级只反映相对上一快照新增的损失，历史上发生过一次 drop 不会造成永久假红。
`shutdown_flush_timeout_ms` 约束 `ObservabilityPipeline.close()`；完整进程的有界退出还要求
`TelemetryWriter` 遵守 cancellation-cooperative（取消协作）端口契约，因为 Python coroutine
不能被外部强制终止。

浏览器诊断入口为 `POST /api/v1/diagnostics`，只接受经过身份边界的严格 error/performance/network
判别联合，具有原始 body、批次、时间漂移、字段、ActorScope token bucket 与 Nginx 入口速率
限制。PostgreSQL 通过 `(workspace_id, resource_owner_id, actor_id, client_event_id)` 部分唯一索引
在完整 ActorScope 内持久去重。入口还以固定低基数指标记录 payload bytes、batch 数、准入耗时，
以及 accepted/dropped/rate-limited 事件数；这些自监控信号本身仍走同一有界 best-effort 管线。
它不是审计或计费事实。前端不得发送 message、stack、URL、query、cookie、header、DOM、
storage、token 或用户文本；完整 schema、单位、重试和隐私约束见
[前端诊断接入契约](docs/FRONTEND_DIAGNOSTICS.md)，架构依据见
[ADR-0003](docs/decisions/0003-observability-platform.md)。

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
