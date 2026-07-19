# ADR-0002：个人知识文件导入与混合检索

- 状态：Accepted
- 日期：2026-07-19
- 范围：后端模块化单体的个人知识库 MVP

## 背景

阶段一需要让用户把个人材料作为可引用证据交给简历助手和面试助手。现有系统已经有 KnowledgeSource、不可变 SourceVersion、Chunk、EmbeddingSpace、pgvector 表、租户隔离、可见性策略和异步 Job，但只有手工 mock 内容和词法检索。

## 决策

1. 第一轮支持 `manual_note`、UTF-8 TXT、Markdown、PDF、DOCX；URL、Git 和 OCR 延后。
2. 单文件上限 10 MiB；解析文本上限 1,000,000 字符。文件扩展名、MIME 和 PDF/DOCX 基础 magic 必须一致。
3. PostgreSQL 保存来源、版本、切块、向量、权限和 Job；原始文件通过 `KnowledgeBlobStorage` Port 保存。开发适配器使用租户隔离、内容寻址的本地目录，部署时该目录必须挂载到私有持久卷。
4. 公开 `KnowledgeSource` 只包含 `file_id`、文件名、MIME、SHA-256、大小和解析摘要。`storage_key` 仅存在后端私有元数据，不进入 API、日志或 telemetry。
5. 同一来源重新上传时保留稳定 `source_id`，生成新的不可变 `source_version_id`；旧版本及其引用不被覆盖。物理 blob 清理延后到独立保留策略任务。
6. 解析器保留 PDF 页码、Markdown 标题/行号、DOCX 标题/段落/表格路径。切块目标为 800 字符、重叠 80 字符，并优先在段落、换行、句号或空格处分割。
7. 默认分类为 `personal_evidence + general + user_provided + current`；默认可供 resume、interview、general 三类 Agent 检索，但仍受 deny-priority visibility policy 约束，且默认不允许外部模型处理。
8. 检索顺序固定为：租户与 owner → enabled/lifecycle/classification/visibility → pgvector 候选排序 → 词法融合。当前权重为向量 0.65、词法 0.35。
9. 在运维确定 embedding 服务前，使用可替换的确定性 1024 维适配器跑通全链路。EmbeddingSpace 保持不可变；切换模型或维度必须新建空间并执行数据迁移/重建索引。
10. 当前直接 multipart 路径属于临时 API 适配器，并标记 `x-contract-status: mock`。正式 UploadSession 路径、对象存储签名协议和完整生命周期必须由前后端与运维另行冻结。

## 并发与失败语义

- 上传、重传和入库以 `workspace_id + resource_owner_id + source_id` 串行化。
- Job 入队时固化 source revision；过时 Job 不得覆盖新版本。
- 解析或向量生成失败会保存 failed Job 和 source 状态，不产生 ready 版本。
- `Idempotency-Key` 对上传命令必填；文件 SHA-256、文件名、MIME 和 source ID 参与请求指纹。

## 后果

该方案已经形成可运行的文件知识闭环，并保留未来替换对象存储、解析服务和真实 embedding provider 的端口。代价是当前本地存储只适用于单节点/共享私有卷，确定性向量不代表生产语义质量，直接 multipart 也不是最终大文件上传协议。

