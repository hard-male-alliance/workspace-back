# API V2 实现说明

本文描述当前后端如何实现 API Standard V2。规范仍以只读
`workspace-shared-docs/contracts/v2/contract.md` 和 `schema.jsonc` 为准；本文不是第二份契约。

## 1. 实现范围

PostgreSQL runtime 覆盖规范第 5.1–5.6 节全部 85 条 Product API 路由：

| Bounded context | 路由数 | Router | Application | PostgreSQL adapter |
|---|---:|---|---|---|
| Current User / Workspace | 19 | `api/v2_access.py` | `application/access.py` | `infrastructure/access.py` |
| Resume / Template | 16 | `api/v2.py`, `api/v2_resumes.py` | `application/resumes.py` | `infrastructure/resumes.py` |
| Connection / Knowledge | 17 | `api/v2_knowledge.py` | `application/knowledge.py` | `infrastructure/knowledge*.py` |
| Conversation / Agent | 12 | `api/v2_agent.py` | `application/agent_v2.py` | `infrastructure/agent_v2.py` |
| Interview | 12 | `api/v2_interview.py` | `application/interview_v2.py` | `infrastructure/interview*.py` |
| Job / Artifact / Event / Audit | 9 | `api/v2_platform.py` | `application/platform.py` | `infrastructure/platform.py` |

`/userinfo`、OAuth/OIDC、Hosted Identity 位于 Product API 之外，但共用同一个 token/session/user
事实。公开 Resume template 是仅有的匿名 `/api/v2` 资源。

测试从 `contract.md` 动态抽取路由，不维护手写期望清单；它同时检查 method/path、成功状态、
router 所属 bounded context，以及 `x-contract-request`、`x-contract-response`、
`x-contract-stream-item` 对 `schema.jsonc` `$defs` 的绑定。由此防止“路由存在但 schema 或状态码
已经漂移”。构建只把父仓库固定 revision 的原始 JSONC Schema 封装进 wheel，不生成或维护
另一份严格 JSON/OpenAPI 契约。

## 2. 分层设计

### 2.1 Domain

`src/backend/domain/` 保存 transport-independent 模型与不变量：

- 不透明 ID 用 `NewType` 区分，跨聚合引用使用 `ResourceRef`；
- 状态与能力用闭合 `StrEnum`，不同状态的数据用判别联合（discriminated union）或独立冻结
  dataclass 表达；
- 聚合方法执行合法状态迁移并返回新 revision，不允许 repository 绕过领域规则修改状态；
- Resume 的 SIR、日期区间、RichText mark、稳定 entity ID、template compatibility；
- Job、Interview、Upload、AccountDeletion、OAuth token family 等状态关联在构造和迁移方法中
  校验；
- outbox event type 是 `work | notification` 闭集，未知事件没有默认分类。

HTTP `ETag` 是表示校验器，领域 `revision` 是业务版本，两者在类型和调用路径中分离。

### 2.2 Application 与 ports

`src/backend/application/` 编排用例；`application/ports/` 描述最窄依赖。应用层接收已经验证的
`TokenPrincipal`、路径 `WorkspaceId` 和类型化 command，不依赖 FastAPI、SQLAlchemy 或具体
provider SDK。

每个写用例遵循同一结构：

1. 打开短 UoW；
2. 将 token principal 绑定为本地 user；
3. 通过集中 `AccessAuthorizer` 求 scope ∩ role ∩ domain policy；
4. 读取并校验当前 aggregate/revision；
5. 原子写 aggregate、Job、outbox、audit 与幂等 receipt；
6. 显式 commit；异常、取消或遗漏 commit 自动 rollback。

跨域调用传递不可伪造的 `WorkspaceAccessContext` 或 worker dispatch claim，不重新从 payload
猜 actor/Workspace。外部 I/O 不持有数据库事务。

### 2.3 Infrastructure 与 composition

`src/backend/infrastructure/` 实现 PostgreSQL、内存、HTTP provider、对象存储、加密、renderer
与 realtime adapter。`composition.py` 是唯一 composition root：读取经过验证的配置，选择
adapter，构建应用服务，并让 lifespan 拥有数据库、HTTP client、日志/遥测 pipeline、worker
与关闭事件。

内存 UoW 用于领域和 HTTP 的确定性测试，但应用工厂对所有 durable 路径做 capability gate：
异步命令以及 Job/Artifact/Event/Audit projection 返回 503。这样不需要在每个领域增加
“如果是 memory 就假装成功”的特殊分支。

## 3. HTTP 与契约内核

`api/v2_transport.py`、`api/v2_http.py` 与各 bounded-context router 共同实现：

- 除公开 template 外，`/api/v2/*` 必须同时具有 Bearer access token 与合法
  `X-Request-Id`；`/userinfo` 需要 Bearer，但不要求产品 request ID；
- access token 验证 RS256 signature、`typ=at+jwt`、issuer、audience、时间、client、subject、
  JTI 撤销和 user token epoch，拒绝 ID token 与 V1 identity；
- JSON/merge-patch content type、未知 query、空 body、原始字节、解码深度和 schema
  `additionalProperties` 均 fail closed；
- 所有 JSON request/response 对具体 V2 `$defs` 验证，时间/URI/email format assertion 开启；
- 集合使用稳定 keyset order 和 HMAC cursor；cursor 绑定 principal、Workspace、filter、sort 与
  expiry，不能跨查询复用；
- 可变资源返回强 `ETag`，更新/删除/取消要求强 `If-Match`，并在 mutation transaction 内再次
  CAS，关闭 TOCTOU；
- 可重试 POST 使用 16–128 字符 `Idempotency-Key`；指纹包含 canonical body、content type 和
  `If-Match`，重放保留原 status/body/关键 headers，但使用当前请求的 request ID；
- Problem Details 只暴露稳定 `code` 和安全详情；SQL、路径、token、provider 异常正文不会进入
  response、outbox 或 telemetry。

## 4. 身份与授权

Authorization Server 只支持 Authorization Code + PKCE S256。Web/Electron 都是 public
client；redirect 默认精确匹配，Electron 仅允许 RFC 8252 loopback 随机端口例外。授权事务、
一次性 code、refresh family/token 与 access JTI 均持久化为 verifier/hash，不保存可重放明文。

Hosted Identity flow 绑定浏览器 session、OAuth transaction、Origin、Fetch Metadata、CSRF 与
`step_id`。密码使用内存困难 KDF 并在部署环境查询泄露密码；OTP/recovery code 有期限、尝试
预算和单次使用语义；WebAuthn 校验 challenge、exact origin、RP ID、UP/UV、algorithm、
credential ID、signature 和 sign counter。删除 login session 只撤销与其绑定的 token family。

`OAuthScope` 是注册校验、discovery 与 Workspace 授权矩阵的单一目录。授权规则默认拒绝，并以
路径 Workspace 作为唯一租户上下文；`default_workspace_id` 只用于 UI 偏好，永不作为隐式授权。
Repository 查询和数据库 transaction scope 都携带同一 Workspace，PostgreSQL FORCE RLS
防止漏写过滤条件演变为跨租户读取。

## 5. 各领域实现

### 5.1 User、Workspace 与账户删除

User/Workspace/membership/invitation 通过独立聚合和集中 authorizer 实现 owner/admin/editor/
viewer 权限。最后一个 active owner 的降级/删除在锁保护的写事务中拒绝；邀请接受校验调用者
邮箱、状态、期限和唯一 membership。

账户删除要求近期重新认证，先进入 scheduled 冷静期。后台执行器冻结每个 Workspace 的
delete/detach disposition，递增 token epoch 并撤销 session/token，生成外部对象和 credential
擦除 manifest，再以 lease + revision CAS 分批执行。共享审计引用保留到稳定 tombstone，避免
把法律保留义务和“彻底删除所有行”混为一谈。

### 5.2 Resume

Resume 权威数据是 SIR，而不是 HTML/DOM/LaTeX。Operation batch 使用稳定 section/item/contact
ID，在单事务内完成 operation ID 去重、expected revision、全部领域不变量、revision snapshot、
outbox 和 receipt；任一 operation 失败则全部回滚。

Resume Job worker 从 persisted `job.kind + spec + subject` 分派：

- import：从已完成 upload session 读取服务端核验后的对象，支持 TXT、Markdown、PDF、DOCX，
  生成 revision 1 SIR；
- restore：从指定历史 revision 生成新 revision，不改写历史；
- render：冻结 Resume revision 和 template binding，生成 PDF/JSON/DOCX Artifact。

PDF source map 在写入前转换为类型化 `PdfSourceMap`，校验 Artifact ID、Resume ID/revision、PDF
page count、node/rect 数值范围与 SIR field path；metadata、bytes 和 source map 在同一完成事务
提交。Artifact ID 和 operation ID 可确定重建，因此 crash 后不会复制副作用。

TXT/Markdown/PDF/DOCX import parser 与 XeLaTeX renderer 都在独立 session/进程组中执行。
生产模式在加载第三方 parser/compiler 前先安装 Landlock 文件系统 allowlist 和 libseccomp
syscall denylist，并施加 CPU、地址空间、输出文件、FD 等 rlimit、输入/结果大小上限与墙钟
deadline；import parser 还禁止创建子进程。超时或取消会终止并回收整个进程组。Bubblewrap
只有在真实 capability probe 成功时才叠加，强隔离的必要条件是 Landlock ABI ≥ 3 与
libseccomp，而不是容器内可用的特权 namespace。

### 5.3 Connection 与 Knowledge

Connection 响应只返回安全投影。authorization session 与 credential 使用彼此独立的
AES-256-GCM keyring；fingerprint/reference HMAC key 也不能与其他安全域复用。provider 类型和
scope 来自闭合 allowlist，secret 不进入通用幂等 receipt、日志或 outbox payload。

Upload 是 create → signed upload → complete。Complete 在长解析前验证声明、对象大小、SHA-256、
MIME sniff、malware scan、archive expansion 边界和 PostgreSQL 原子 quota reservation。URL
来源对初始 URL 和每次 redirect 都执行 scheme/port/host allowlist、DNS pinning，并阻断
loopback、private、link-local、metadata 与 rebinding。

上传的 TXT/Markdown/PDF/DOCX 不在 backend 进程或线程池内解析。Knowledge parser 使用独立
session/进程组、父进程 wall deadline、有界 stdin/result 和 CORE/CPU/AS/FSIZE/NOFILE/NPROC
rlimit；生产 child 在读取输入和延迟加载 `pypdf`/`python-docx` 前安装 Landlock +
libseccomp。父进程只接受闭合 JSON，并重新验证 part 数、总字符、locator 与 parser metadata；
part 上限与 `index.maximum_chunks` 对齐，heading 和结果信封也有独立上限，防止结构放大。

Knowledge worker 对 connection revoke、source delete、ingest 和 sync 做穷尽分派。fetch、parse、
chunk、embed 位于事务外，最终 version/chunk/index/Job/Event/Audit 在短事务中提交。检索把
PostgreSQL lexical `tsvector` 与显式 embedding space（provider/model/revision/dimension/
metric/normalization）组合，执行时重新检查 membership、visibility、selection 和 pinned version。

### 5.4 Conversation 与 Agent

Conversation、append-only Message、AgentRun 和 ToolApproval 是独立聚合。创建 Run 时冻结创建者、
conversation、knowledge selection、模型路由、数据地域、工具能力和预算；worker 不信任 event
payload 中可伪造的 policy。开始执行时，worker 以真实 actor/Workspace 重新求 execution grant；
模型完成后、提交结果前再次授权并逐值比较 grant、Conversation revision 与精确 Resume base。
任何变化都丢弃模型结果，不把过期授权窗口内生成的内容写入 durable truth。

Knowledge 检索位于数据库事务外，但只使用 grant 中精确的 source/version/policy provenance。
混合检索结果按稳定顺序分配服务端索引，模型只能选择索引；正式 citation 由服务端从授权证据
物化，模型无法伪造 source、version、chunk 或 policy 归属。检索内容作为不可信数据传入，不
获得指令或工具权限；输入、context 和输出都有独立硬上限。

Agent provider 使用 provider-native strict JSON Schema。capability discovery 未证明支持结构化
输出时，在任何模型网络请求前 fail closed；返回值还会在本地按封闭字段、protocol version、
output mode、citation index 和 operation union 再验证。Resume 修改输出只是无身份 operation
草案；服务端锁定精确 Resume revision，生成稳定 Proposal/operation/entity ID 与 preview，并把
可审阅 Proposal、assistant Message、Run/Job 终态和 outbox 在同一事务提交，绝不直接修改 Resume。
生产组合根显式安装空 tool registry，因此未注册工具返回 `agent.tool_unavailable`，不会生成
无法兑现的 approval。已注册工具的副作用才使用 durable decision event 的稳定 invocation
reference 去重。取消或尝试耗尽时，Run、统一 Job 和 pending Approval 在同一领域补偿中闭合。

### 5.5 Interview

Scenario 冻结 rubric 和模型政策；Session 冻结 scenario、locale、media、recording consent、
retention 和 model route。REST connection 返回短期、绑定 session/audience 的 HMAC credential
和 coturn REST credentials；持久 Session 而非连接状态是权威事实。

私有 WebSocket 数据面使用 `aiws.interview.realtime.v2` 子协议，严格校验 Origin、首帧认证、
input replay ledger 和连接租约。候选人文字仅在 transcript consent 开启时持久化；音视频由
`media_chunk` header + binary frame 分块传输，每块在写入前重验 recording consent、MIME、
大小及 SHA-256。结束 worker 将分块归档为受管 `interview_audio`/`interview_video` Artifact，
并在一个 PostgreSQL 事务内提交 metadata、二进制内容、Session/Job 终态和 audit。

报告 provider 通过 OpenAI-compatible streaming endpoint 请求严格、闭合的 JSON Schema；
输出仍需本地验证 rubric dimension、score scale、权重、evidence transcript sequence 和
Session identity。超时、格式错误与供应商失败进入有界 outbox 重试，达到上限后原子闭合
Job/Session。开发模式可使用明确标注的确定性 adapter；部署环境必须使用真实模型。

### 5.6 Platform

统一 `agent.jobs`、`agent.artifacts`、`agent.outbox_events` 和 `identity.audit_events` 是跨领域唯一
projection，不为每个模块再建平行 Job/Event 表。

Job cancellation 使用 `(kind, subject_type)` 闭合策略表。目前覆盖 Resume import/restore/render、
Connection revoke、Knowledge delete/ingest/sync、Agent run、Interview end/report。取消事务在 Job
CAS 后同步恢复或关闭领域 aggregate，写 `job.updated`、audit 与 receipt；未知 kind fail closed。

Artifact content 校验 metadata size/SHA-256，支持单 Range 和正确 `Content-Range`/ETag；PDF
source map 只对相应 artifact 返回。SSE 从 committed outbox 的单 Workspace sequence 读取，先在
同一 snapshot 验证 `Last-Event-ID`，再追赶更大 sequence；超出 replay window 返回稳定 409。

## 6. Durable execution

统一 outbox 的工作事件闭集为：

```text
Agent:     agent.run.queued, agent.tool_decision.recorded
Knowledge: connection.revocation_requested,
           knowledge_source.deletion_requested,
           knowledge_source.job_created
Resume:    resume.job_created
Interview: interview.job.queued
```

通知事件随业务事务写入时已经 `published`。工作事件由通用 dispatcher 以 `FOR UPDATE SKIP
LOCKED` claim，数据库仅保存 lease token digest；处理期间按 lease 的三分之一周期 CAS 续租。
失败只记录白名单稳定 code，按确定性 jitter 的指数退避重试。handler 的外部操作必须使用 event
ID/Job ID 派生的 operation identity，保证至少一次投递不会产生重复外部效果。

`0027` 的 retention 函数只删除 replay-expired 的 `published/failed` 终态行；未知事件或无法
精确分类的历史行会让迁移失败。身份邮件 outbox 和账户删除 manifest 有不同的敏感数据、授权与
retention 语义，因此没有被强塞进通用 outbox 表。

## 7. PostgreSQL 与迁移原则

`20260722_0008` 至 `20260723_0028` 是单 head 线性链。详细 revision 作用见
`API_V2_HANDOFF.md`。实现遵循以下约束：

- 先 preflight 旧数据的可表示性和引用完整性，再执行 DDL；
- 先 expand，再确定性 backfill，验证行数/hash/状态，最后添加 NOT NULL、FK、CHECK、索引、
  grants 和 FORCE RLS；
- Resume 转换前保存 append-only 原始 archive；OAuth session 归属无法证明时安全失效而不猜测；
- `0017` 只迁移 scope、size 与 SHA-256 都和 metadata 精确一致的 Resume Artifact bytes；仍可读
  但没有受支持可信 bytes 来源或显式处置结果的 Interview recording 会在 DDL 前令升级失败；
- 统一表原位演进并删除已验证的平行真相表，避免双写永久化；
- migration-only RLS policy 只在事务内开放所需 owner scope，结束后移除；
- downgrade 不能丢弃业务、安全或审计事实：非空状态拒绝，或只按仍可信的逐行 marker 逆转
  本 revision 实际写入的字段。

## 8. 部署语义

Production origin、issuer 和 resource 固定为 `https://api.hmalliances.org:8022`；应用只监听
`127.0.0.1:9000`。Nginx 覆盖 `Forwarded/X-Forwarded-*`、剥离 legacy/mock identity headers、
为 SSE 禁用 buffering，并将内部 health 固定在 loopback 运维 listener。

staging/production 配置解析器拒绝：memory database、V1、mock/placeholder 模型、mock embedding、
mock renderer、非 HTTPS provider endpoint、memory email、禁用的 breach check、本地 upload、
开发 malware scanner，以及缺失的持久密钥。部署没有功能级 fallback；启动失败优于对外声称一个
无法兑现的能力。

镜像基于 Python 3.14，包含 PostgreSQL client、XeLaTeX、Noto CJK、libseccomp2 与可选
Bubblewrap，以非 root、read-only filesystem、drop-all-capabilities 运行。staging/production
启动时真实探测 Landlock ABI ≥ 3 与 libseccomp；任一缺失都会 fail closed，Bubblewrap 不可用则
仅失去额外 mount-namespace 层。Compose 对共享 runtime service 设置 `pids_limit: 256`，且不增加
`CAP_SYS_ADMIN`、不使用 `seccomp=unconfined`。Compose 只把 9000/8010 映射到 loopback；公网
8022 TLS 由反向代理拥有。

## 9. 明确限制

- Interview 已支持浏览器 MediaRecorder 分块采集与受管原始音视频 Artifact；尚未实现服务端
  codec 转码、音视频混流或第三方录制供应商接入。
- 强 Resume/Knowledge 文件 sandbox 依赖 Linux Landlock ABI ≥ 3 与 libseccomp；不满足该内核/runtime 条件的
  staging/production 主机不会启动。development/test 可显式使用仅 rlimit 的弱模式，不能作为
  生产验收证据。
- memory mode 不拥有跨领域持久真相，故异步命令和 Platform projection 返回 503。
- 真实 Connection/S3/ClamAV/SMTP/model/embedding/realtime/renderer 依赖部署配置和外部服务；
  development mock 不能作为生产验收证据。
- pending 幂等 claim 为避免双重副作用不会自动被新 worker 接管；维护服务只报告 stranded
  records，运维必须依据事务/外部副作用证据处理。
- V1 源码和部分 legacy service object 暂留作 development/test 的显式并行迁移工具，但 router
  默认不挂载，staging/production 禁止启用。完成代码层退役仍需从 composition 删除这些内部
  对象；V2 请求当前不会回退或借用它们。

## 10. 设计依据

- PostgreSQL 的 [Row Security Policies](https://www.postgresql.org/docs/17/ddl-rowsecurity.html)
  与 [`ALTER TABLE ... FORCE ROW LEVEL SECURITY`](https://www.postgresql.org/docs/17/sql-altertable.html)
  支撑 application authorization 之外的数据库租户边界。
- OAuth 安全选择遵循 [RFC 9700: OAuth 2.0 Security Best Current Practice](https://www.rfc-editor.org/info/rfc9700/)；
  Agent 输出边界使用 [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12) 的闭合
  schema，并继续做本地领域验证。
- 子进程隔离以 Linux [Landlock userspace API](https://www.kernel.org/doc/html/latest/userspace-api/landlock.html)、
  Docker [seccomp](https://docs.docker.com/engine/security/seccomp/) 和 Compose
  [`pids_limit`](https://docs.docker.com/reference/compose-file/services/) 为部署基线。
- RAG 证据仍应按敌意输入处理；[SafeRAG](https://arxiv.org/abs/2501.18636)、
  [Machine Against the RAG](https://www.usenix.org/system/files/usenixsecurity25-shafran.pdf) 与
  [StruQ](https://www.usenix.org/system/files/usenixsecurity25-chen-sizhe.pdf) 展示了检索污染和
  指令/数据混淆的现实攻击面。因此实现把 provenance、citation 与工具权限留在服务端，而不是
  依赖提示词说服模型“不要越权”。

## 11. 验证门禁

本次交付完整套件为 `868 passed, 1 skipped`；唯一 skip 是只适用于非 POSIX 平台的
fail-closed 分支。另行验证 Resume process/import/render/container 为 `82 passed, 1 skipped`、
Agent PostgreSQL persistence/Proposal 为 `10 passed`，Knowledge 全域（含真实隔离 parser）
为 `75 passed`。后续交付必须重新执行以下门禁，不能沿用本次数字：

```bash
uv sync --locked
uv run alembic heads                     # 必须只有 20260723_0028
uv run ruff check .
uv run mypy --strict
uv run pytest -q
git diff --check
```

PostgreSQL 集成测试会在本机 `initdb`/`pg_ctl`/`psql` 可用时启动临时 cluster，并在相关路径
要求 pgvector；环境缺失 server binary/extension 时会显式 skip。正式目标仍是 PostgreSQL 17，
发布前必须在该版本的真实生产拓扑执行迁移、OAuth/Passkey、worker crash recovery、外部
adapter 故障注入和账户删除演练，不能用内存测试替代。
