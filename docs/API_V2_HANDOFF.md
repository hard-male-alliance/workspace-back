# API V2 后端交接

更新时间：2026-07-23

分支：`codex/backend-api-v2-alignment`

变更前基线提交：`9a7a6d7`

## 1. 事实源与交接结论

唯一规范来源是只读子模块 `workspace-shared-docs/contracts/v2/`：

- `contract.md` 冻结架构、85 条 Product API 路由和跨请求语义；
- `schema.jsonc` 是请求与响应负载的唯一机器事实源；
- `examples.jsonc` 是端到端样例；
- `diff.md` 只描述 V1 → V2 的破坏性变化与迁移。

本仓库不复制或修改这些发布物。父仓库 clone、CI、测试和构建必须初始化其固定 gitlink；
源码树缺失 V2 契约时直接失败，不回退 V1。构建出的 wheel 只封装该固定 revision 的原始
`schema.jsonc` 供安装态运行，不维护第二份手写 Schema。

当前 PostgreSQL 运行面已经注册并实现契约第 5.1–5.6 节的全部 85 条产品路由，以及
`/userinfo`、OAuth/OIDC 和 Hosted Identity 边界。开发用内存适配器只提供确定性的同步
领域测试面；凡依赖统一 Job、Artifact、Event、outbox 或后台 worker 的请求均返回
`503 service.durable_runtime_required`，不会制造永久 `queued` 的假成功。

## 2. 已实现能力

| 边界 | 当前实现 |
|---|---|
| 契约门禁 | 直接解析 Draft 2020-12 `schema.jsonc`；逐例验证 `examples.jsonc`；从 `contract.md` 动态抽取 85 条路由，对比 method、规范化 path、成功状态与 OpenAPI schema 绑定。 |
| HTTP 内核 | 严格 JSON/merge-patch、原始 body 与 JSON 深度上限、RFC 9457 Problem Details、强 `ETag`/`If-Match`、签名 keyset cursor、`Idempotency-Key`、`X-Request-Id`、精确 CORS、Artifact Range/摘要校验和 SSE 重放。 |
| OAuth/OIDC | Authorization Code + PKCE S256、精确 public-client redirect、RS256/JWKS、RFC 9068 access token、ID token nonce、refresh rotation/reuse-family revocation、token revocation、`/userinfo` 和 scope 元数据。 |
| Hosted Identity | 注册、登录、找回、近期重新认证、同源 flow/step 有限状态机、OTP、密码、WebAuthn passkey、recovery code、登录设备与认证器管理；session 与 token family 精确绑定。 |
| User/Workspace | `/me`、账户删除请求、Workspace、membership、invitation；集中执行 token scope ∩ membership role ∩ resource/state 的默认拒绝授权。 |
| Resume | SIR 聚合、稳定 entity ID operation batch、revision/Proposal、导入/恢复/渲染 Job；导入 TXT/Markdown/PDF/DOCX，渲染 PDF/JSON/DOCX，持久化 Artifact/content/PDF source map 并在提交前交叉验证；不可信解析与 XeLaTeX 编译在可终止强隔离子进程中执行。 |
| Connection/Knowledge | provider authorization session、加密 credential vault、直传 upload session、hash/size/MIME/恶意内容/配额检查、URL 网络策略、版本、ingest/sync worker、混合检索和访问评估；上传 TXT/Markdown/PDF/DOCX 在强隔离子进程解析，父端重验闭合结果。 |
| Conversation/Agent | Conversation/Message、双重执行时重授权、strict native JSON Schema、精确 Knowledge provenance 与服务端 citation；Resume 草案在同事务形成可审阅 Proposal 而不直接写 Resume；生产 tool registry 显式为空，未注册工具稳定失败。 |
| Interview | Scenario/Session、短期 realtime connection、WebSocket 信令、TURN REST 凭据、同意门控 transcript/音视频分块、end/report Job、受管 Artifact、证据与 rubric 校验；报告可使用严格 JSON Schema 的 OpenRouter 模型。 |
| Platform | 统一 Job/Artifact/Event/Audit 查询；按 kind/subject 恢复 Job；已知领域 Job 的原子取消补偿；SSE 至少一次投递与 `Last-Event-ID` 重放。 |
| 运维任务 | V2 receipt/invitation 维护、加密身份邮件 outbox、账户删除执行器、Agent/Knowledge/Resume/Interview worker 均由应用 lifespan 拥有并有界关闭。 |

OAuth scope 由 `backend.domain.oauth_scopes` 单一闭集驱动注册校验、discovery 与授权矩阵：
`openid`、`profile`、`email`、`offline_access`、`workspace.read/write`、
`resume.read/write/render`、`interview.read/write`。Resume 导入/恢复要求 `resume.write`，渲染
单独要求 `resume.render`；Interview 不再借用泛化 Workspace scope。

## 3. 架构与事务边界

代码保持 src-layout 分层模块化单体（modular monolith）：

```text
api/v2_*.py
  -> application/*.py + application/ports/*.py
      -> domain/*.py
      <- infrastructure/*.py
composition.py 负责唯一对象图、进程生命周期与 adapter 选择
```

- Domain 使用 `StrEnum`、`NewType`、冻结 dataclass、判别联合和受控构造器表达 ID、状态机、
  scope、角色、Job kind、ResourceRef、consent 与 revision 约束。
- Application 只依赖窄端口和工作单元（Unit of Work, UoW）；每个 command 显式提交，异常或
  未提交自动回滚。
- Infrastructure 提供内存与 PostgreSQL adapter；生产请求事务安装 actor/Workspace
  scope，数据库以强制行级安全（Row-Level Security, RLS）作为第二道租户边界。
- HTTP adapter 只负责 transport、契约编解码和稳定错误映射，不选择默认 Workspace，也不
  接受 V1 mock/HMAC 身份冒充 V2 Bearer principal。

可重试写入在 PostgreSQL 中把幂等 claim、领域写入、outbox 和逐字响应 receipt 放入同一
事务。普通 receipt 至少保留 24 小时，离线 Resume operation 至少保留 30 天；相同 key
不同请求指纹返回 409，执行中返回 409 + `Retry-After`。

## 4. Worker 与 outbox

统一 `agent.outbox_events` 明确区分两类事件：

- `work`：初始为 `pending`，由 Agent、Knowledge、Resume 或 Interview dispatcher 消费；
- `notification`：与业务事务一起成为 durable truth，写入时即为 `published`，供 SSE/replay
  读取，不再等待一个不存在的发布者。

Dispatcher 使用 PostgreSQL `SKIP LOCKED`、只存摘要的高熵 lease、CAS 续租、指数退避、
有界批次和尝试上限。worker 从已提交 envelope 恢复真实 actor、Workspace、subject 和 Job，
严格校验 event ↔ persisted kind ↔ aggregate binding。典型执行流是：

```text
短事务 A：鉴权 + 领域命令 + Job/outbox/audit/idempotency receipt
    -> lease claim
事务外：provider / fetch / parse / embed / render / revoke
    -> 短事务 B：CAS 校验 + 领域终态 + Job/Artifact/outbox/audit
```

外部副作用使用 event ID 或稳定 operation ID 去重。进程崩溃后租约到期可重放；达到尝试上限
时各领域把非终态 Job/Run/Approval/aggregate 闭合为稳定的失败状态，而不是只把 outbox 标成
失败。通知 retention 只清理超过 replay window 的终态行，永不清理 `pending/processing`
work。

身份邮件和账户删除使用各自的专用 durable 队列：邮件 payload 以 AES-256-GCM 加密，终态
立即清除密文；账户删除先冻结每个 Workspace 的处置决策并撤销认证能力，再以可恢复清单擦除
对象存储和外部凭据。保留的 user tombstone 是假名化数据（pseudonymous data），不宣称完全
匿名。

## 5. 数据迁移链

当前 Alembic 单一 head 是 `20260723_0028`，从既有 `20260722_0012` 线性演进：

| Revision | 数据与安全语义 |
|---|---|
| `0008`–`0012` | 迁移审计、OAuth authorization/code/token、Hosted Identity flow/credential/login session。 |
| `0013` | 预检并回填 V2 User/Workspace/membership/invitation/account deletion；非空旧库要求显式 data region 与逐 Workspace plan 映射；启用强制 RLS。 |
| `0014`–`0015` | V2 原子幂等 receipt；邀请和 receipt 的有界维护函数。 |
| `0016` | 先把旧 Resume 行完整封存到带版本与 SHA-256 的 append-only archive，再按 expand → backfill → validate → constrain 转换为 V2 SIR；不可表示数据不静默丢弃。 |
| `0017`–`0018` | 原位统一 Job/Artifact/outbox/audit；`0017` 只迁移 scope、size、SHA-256 与 metadata 精确一致的 Resume bytes，仍可读但没有受支持可信 bytes 来源或显式处置结果的 Interview recording 在 DDL 前拒绝升级；随后把 authorization code/refresh family 绑定精确 login session，无法推断归属的历史活动 token 安全失效并记审计。 |
| `0019`–`0020` | 统一 Connection/Knowledge/Upload（含旧 Resume upload 搬迁）；新增加密身份邮件 outbox 与原子频控账本。 |
| `0021`–`0022` | Agent 与 Interview 持久化原位演进；只有存在可证明的 V2 frozen snapshot、授权、provenance 和统一 Job 绑定时才转换，否则在 DDL 前失败。 |
| `0023`–`0024` | outbox 可恢复租约、续租和重试；Knowledge provider/credential vault、原子配额 reservation 与 lexical index。 |
| `0025` | 可恢复账户删除、token epoch、Workspace disposition 与外部擦除 manifest。 |
| `0026` | 为活动 Job 验证并补齐可证明的取消补偿快照；逐行 marker 只记录迁移实际插入的 JSON member。 |
| `0027` | 闭合 work/notification 生命周期，精确回填历史通知，并提供仅清理 replay-expired 终态行的 retention 函数。 |

迁移遵循 preflight → expand → backfill → validate → constrain → secure。多数 downgrade 只允许
空业务状态，或要求迁移 marker 与写入值仍完全一致；存在不可逆安全/审计证据时主动拒绝，不能
把“能执行 downgrade”当作回滚方案。生产升级前必须备份并在生产数据副本上演练相同 revision。

## 6. 部署边界

冻结拓扑是：

```text
https://api.hmalliances.org:8022
    -> Nginx TLS / forwarded-header overwrite / public route allowlist
    -> http://127.0.0.1:9000
```

Nginx 只公开 discovery、OAuth、Hosted Identity、`/userinfo` 和 `/api/v2/`；SSE 单独关闭
buffering/gzip。`/_internal/healthz` 只经 loopback `127.0.0.1:8088` 暴露。公网 V1、Dashboard、
docs、OpenAPI 和未登记路径返回 404。

`example.jsonc` 与容器配置投影都把 `api.legacy_v1_enabled` 设为 `false`，且
staging/production 配置为 `true` 会启动失败。旧 V1 模块和部分兼容性 service object 仍留在
代码树/组合根中供 development/test 的显式并行迁移使用，但 V1 router 不在部署态挂载，公网面
也固定返回 404；它不是 V2 回退路径。

staging/production 配置会 fail closed，至少要求：PostgreSQL、真实模型和 embedding endpoint、
非 mock XeLaTeX renderer、SMTP + 加密邮件 outbox、泄露密码检查、持久 cursor/幂等密钥、
Knowledge 持久存储/扫描策略，以及启用 realtime 时的独立 signing keyring。容器监听 9000，
运行镜像包含 XeLaTeX、Noto CJK、libseccomp2 与可选 Bubblewrap。启动时必须真实验证 Linux
Landlock ABI ≥ 3 与 libseccomp；Compose 以 `pids_limit: 256` 限制容器进程树，不增加
`CAP_SYS_ADMIN`，也不关闭默认 seccomp。Bubblewrap 仅在真实 probe 成功时叠加，缺失不会诱导
部署增加特权；必要隔离能力缺失时 staging/production 直接拒绝启动。

## 7. 已知限制与下一步

以下是必须显式处理的能力边界，任何一项都不允许静默降级：

1. **Interview 媒体由浏览器 MediaRecorder 分块上传，不是媒体服务器转码。** WebSocket
   `aiws.interview.realtime.v2` 在认证后接受 `media_chunk` JSON header 及其紧随的 binary
   frame；后端逐块重验 lease、audience、冻结 consent、大小和 SHA-256，并按
   `(input_id, sequence)` 幂等落盘。end worker 将完整内容与 Artifact metadata 在同一个
   PostgreSQL UoW 中提交。多 backend replica 必须把 `/var/lib/aiws/interview-media` 挂载为
   共享持久卷；若需要服务端混流、转码或直播审计，仍应另接专用 media provider。
2. **强文件处理 sandbox 有明确平台前提。** Resume import、Knowledge
   TXT/Markdown/PDF/DOCX parser 与 XeLaTeX renderer 已在独立 session/进程组中执行，并施加
   Landlock、libseccomp、rlimit、输入/结果边界和墙钟 deadline；Knowledge 的 part/result
   放大也有独立预算。但不提供 Landlock ABI ≥ 3/libseccomp 的主机不能承载
   staging/production。
3. **真实外部能力必须由部署完成。** Connection provider allowlist、S3-compatible object
   store、ClamAV、SMTP、模型/embedding、外部 realtime signaling/TURN 和 XeLaTeX sandbox 都没有
   “随便成功”的 fallback；缺失配置会在启动或调用处明确失败。
4. **内存模式不是缩小版生产。** 它保留同步聚合测试能力，但拒绝异步命令和统一 Platform
   projection；端到端 Job/worker/SSE/账户删除验证必须使用 PostgreSQL。
5. **幂等 pending receipt 不会按超时被另一 worker 抢占。** 这是防止双重副作用的保守语义；
   维护任务会报告 stranded receipt，生产需要告警和人工证据闭环流程。
6. **发布前仍需真实部署演练。** 重点包括 PostgreSQL 17 + pgvector 全链升级、生产数据副本
   迁移、TLS/OAuth/Passkey 浏览器流程、S3/ClamAV/SMTP/provider 故障注入、renderer sandbox、
   多 worker crash-recovery、SSE 长连接以及账户删除外部擦除。
7. **旧 V1 内部对象尚未从组合根物理移除。** 部署配置和 router 已关闭 V1，但若要完成代码层
   退役，还应在不影响 V2 对象图后删除 legacy service/repository 构造及其专用模块与测试。

## 8. 设计依据

数据库隔离参考 PostgreSQL [Row Security Policies](https://www.postgresql.org/docs/17/ddl-rowsecurity.html)
和 [`FORCE ROW LEVEL SECURITY`](https://www.postgresql.org/docs/17/sql-altertable.html)；OAuth
参考 [RFC 9700](https://www.rfc-editor.org/info/rfc9700/)；模型输出使用
[JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12)。Resume 子进程边界参考 Linux
[Landlock](https://www.kernel.org/doc/html/latest/userspace-api/landlock.html)、Docker
[seccomp](https://docs.docker.com/engine/security/seccomp/) 与 Compose
[`pids_limit`](https://docs.docker.com/reference/compose-file/services/)。

对 RAG 不采用“模型会听提示词”的安全假设：
[SafeRAG](https://arxiv.org/abs/2501.18636)、
[Machine Against the RAG](https://www.usenix.org/system/files/usenixsecurity25-shafran.pdf) 和
[StruQ](https://www.usenix.org/system/files/usenixsecurity25-chen-sizhe.pdf) 都说明检索污染及
指令/数据混淆仍是现实攻击面。实现据此让服务端拥有 provenance、citation 和工具授权，并把
retrieved text 始终视为不可信数据。

## 9. 验证与交付

本次交付已通过的运行时门禁：

```text
Full repository suite:                  868 passed, 1 skipped
Resume process/import/render/container: 82 passed, 1 skipped
Agent PostgreSQL persistence/Proposal: 10 passed
Knowledge isolated parser/full domain:  75 passed
```

唯一 skip 是只适用于非 POSIX 平台的 fail-closed 分支；Linux 实际 Landlock/libseccomp、
TXT/Markdown/PDF/DOCX 解析和 XeLaTeX smoke 均已执行。后续提交仍必须重新执行：

```bash
git submodule status workspace-shared-docs
git -C workspace-shared-docs status --short
uv sync --locked
uv run alembic heads
uv run ruff check .
uv run mypy --strict
uv run pytest -q
git diff --check
```

共享子模块出现本地修改时必须停止；不得替用户清理、提交或推送。契约上游变更合并后，应先在
外部 clone 审阅 v1 → v2 差异、更新父仓库 gitlink 并通过相关测试，再单独提交 revision 更新。
