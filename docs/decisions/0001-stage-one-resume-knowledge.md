# ADR-0001：阶段一简历、个人知识库与 AI Proposal 闭环

- 状态：Accepted
- 日期：2026-07-17
- 范围：`backend` 产品应用；不包含 Dashboard、dbctl、Nginx 与部署自动化

## 背景

阶段一需要完成结构化简历的创建、修改、版本控制、PDF 产物，以及简历与个人知识库的双向连接。个人知识用于为 AI 简历助手提供可追溯事实；AI 只能提出结构化修改建议，不能绕过用户确认直接改写简历。

现有代码已经提供 Resume aggregate、`ResumeOperationBatch`、revision/幂等检查、渲染 Job、`ResumeKnowledgeBridge`、确定性 Mock 索引及 PostgreSQL adapter。本决策在这些边界上增量实现，不引入新的应用或分布式服务。

## 决策

### 1. 业务工作流

1. 用户或已接受的 Proposal 通过 `ResumeOperationBatch` 修改简历。
2. 每个 batch 绑定 `base_revision`，原子、按序执行，并生成一个新 revision。
3. revision 持久化成功后，PDF render 与 Resume → Knowledge 索引可并行执行。
4. AI 助手先检索有权限的个人知识，再生成绑定该 revision 的 `ResumeProposal`。
5. Proposal 中每项操作必须包含来源引用、可信等级和原子组；用户可按原子组接受或拒绝。
6. 接受 Proposal 时若简历 revision 已变化，阶段一拒绝执行并要求重新生成，不自动合并。

### 2. AI 权限和事实规则

- AI 不直接修改 ResumeDocument，只能产生 Proposal。
- 无来源的措辞润色可以建议；无来源的经历、技能、证书和具体数字不得作为事实写入。
- AI 生成内容初始为 `generated + pending`；用户接受后为 `user_confirmed`，不会自动升级为 `verified`。
- Proposal 必须保留引用；拒绝或过期 Proposal 不进入可检索个人事实。
- 知识库不可用时，纯措辞润色可继续；要求补充事实的请求必须 fail closed。

### 3. 知识分类

每个 source/chunk 同时携带以下正交维度：

- `source_role`：`personal_evidence`、`resume_current`、`resume_history`、`job_target`、`external_reference`、`ai_generated_draft`
- `content_type`：`profile`、`education`、`work_experience`、`project`、`skill`、`achievement`、`certificate`、`publication`、`open_source`、`job_requirement`
- `trust_level`：`verified`、`user_provided`、`inferred`、`generated`、`user_confirmed`
- `lifecycle`：`pending`、`current`、`stale`、`archived`、`deleted`
- `visibility`：`resume_assistant`、`interview_agent`、`general_agent`、`private_only`、`none`

`source_role`、`trust_level` 与 `lifecycle` 是单值；`content_type` 可以有一个主类型和辅助标签；`visibility` 是集合。检索必须先应用 tenant、owner、visibility、lifecycle 和 source-role 过滤，再进行相关性计算。

### 4. Resume 派生知识

- 每个 Resume 拥有稳定的派生 `knowledge_source_id`。
- 当前 revision 使用 `resume_current + user_confirmed + current`。
- 被替代 revision 视为 `resume_history + user_confirmed + archived`，默认不检索。
- chunk 以 profile、section 和 item 为语义边界，保留 `resume_id`、`resume_revision`、`section_id`、`item_id`、内容哈希与原始路径。
- 新 revision 的索引完成前，旧当前索引可以短暂可见；写入时必须使用 revision guard，旧 Job 不能覆盖新 revision。

### 5. 并发和可靠性

- 单个 OperationBatch 内严格顺序执行，不并行修改 aggregate。
- 单进程以 `workspace_id + resource_owner_id + resume_id` ScopedKeyLock 串行化写入。
- PostgreSQL 使用事务、tenant scope 与 revision 条件保护；不依赖进程锁保证跨 worker 一致性。
- batch ID、operation ID 和 HTTP `Idempotency-Key` 均需可重放，复用 key 但 body 不同返回冲突。
- PDF 与知识索引使用独立有界并发和队列；队列满时保存明确失败状态，不静默丢任务。
- 阶段一允许进程内调度，但 Job 与派生意图必须持久化，使后续可迁移到独立 worker。

### 6. Proposal 原子组

- Proposal 绑定 `resume_id`、`base_revision`、创建者和过期状态。
- 每项建议包含稳定 `proposal_operation_id`、Resume operation、`atomic_group_id`、理由、引用和信任结果。
- 用户按 `atomic_group_id` 选择；同一原子组要么全部应用，要么全部拒绝。
- 接受操作仍通过 Resume aggregate 的正常 operation/revision/idempotency 路径，不创建旁路写入。

### 7. PDF 和模板

- Resume SIR 与 `style_intent` 是后端无关的编辑事实；渲染器是 adapter。
- 阶段一使用受控后端模板，不接受任意用户 LaTeX。
- Windows 开发环境使用 Mock renderer；真实 XeLaTeX 仅在具有 POSIX resource limit 与 OS sandbox 的环境运行，并在缺失时 fail closed。
- render job 永远绑定具体 revision，旧产物不会被冒充为最新 revision。

### 8. API 边界

- Domain/Application 输入输出先稳定，FastAPI 路由最后绑定。
- 正式 contract 缺少的路径继续使用明确 `Mock*` DTO 和 `x-contract-status: mock`，不得静默伪装为正式契约。
- API 层只负责身份、Schema 校验、幂等 header、状态码和序列化，不承载业务规则。

## 阶段一范围

包含：结构化简历、revision、OperationBatch、AI Proposal、手工个人事实、Resume 派生知识、确定性检索、Mock/安全渲染、内存与 PostgreSQL adapter、薄 API 和完整测试。

不包含：Git/博客抓取、任意文件解析、任意用户 LaTeX、自动冲突合并、多人实时协作、完整多语言 variant、真实 WebRTC/数字人和运维应用修改。

## 验收标准

- 并发编辑不会静默丢失更新；过期 Proposal 不会被应用。
- 重复请求不会产生重复 revision、Job 或知识来源。
- 创建/修改简历后会提交对应 revision 的 PDF 与知识索引工作。
- AI Proposal 的事实性操作都有个人知识引用；无证据事实被拒绝。
- 用户可按原子组接受或拒绝 Proposal，接受后产生唯一新 revision。
- 当前简历进入知识库，历史版本默认不参与检索，旧索引 Job 不覆盖新版本。
- Windows Mock 开发、Ruff、mypy、pytest 与后端启动冒烟均通过。

## 后果

该方案优先保证可追溯性、用户控制和并发正确性；代价是 AI 建议需要额外确认，Resume 与 Knowledge 采用最终一致，阶段一不提供自动三方合并和任意模板能力。领域端口保持稳定后，真实 LLM、embedding、pgvector 检索和独立 worker 可以替换 Mock adapter，而无需改写核心业务规则。
