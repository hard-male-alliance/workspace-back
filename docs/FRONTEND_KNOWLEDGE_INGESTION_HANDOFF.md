# 前端交接：个人知识文件导入（临时接口）

> 这些路径用于当前联调，均显式标记为 pending/mock。正式 UploadSession 契约冻结后可能调整，现有知识来源 list/detail 接口保持兼容。

## 1. 新建文件来源

`POST /api/v1/knowledge-sources/uploads`

- Header：`Idempotency-Key` 必填。
- Body：`multipart/form-data`
- `file`：必填，支持 `.txt`、`.md`、`.markdown`、`.pdf`、`.docx`，最大 10 MiB。
- `name`：可选，最多 300 字符；省略时使用文件名。
- 返回：HTTP 202。

```json
{
  "source": { "id": "src_...", "ingestion": { "status": "queued" } },
  "ingestion_job": { "id": "job_...", "status": "queued" }
}
```

## 2. 上传同一来源的新版本

`POST /api/v1/knowledge-sources/{source_id}/versions`

- Header 和文件约束同上。
- 稳定 `source_id` 不变；完成后 `indexed_version_id` 更新。
- 只能用于 `source_type=file`，否则返回 409。

## 3. 状态轮询

轮询 `GET /api/v1/knowledge-ingestion-jobs/{job_id}`，直到 `status` 为 `succeeded`、`failed` 或 `cancelled`。成功后重新读取：

- `GET /api/v1/knowledge-sources/{source_id}`
- `ingestion.status` 应为 `ready`
- `ingestion.indexed_version_id` 为当前可检索版本

前端不要依赖 Job 完成时间，也不要在浏览器端自行生成 source version。

## 4. 搜索与引用

继续使用 `POST /api/v1/knowledge-searches`。返回引用的 `locator` 可能包含：

- PDF：`page`
- Markdown/TXT：`line_start`、`line_end`、`path`
- DOCX：`symbol`（标题）、`path`

前端可以显示“来源名 + 页码/标题/行号 + quote”。不得尝试读取或推导后端文件系统路径。

## 5. 错误处理

- `knowledge.file_too_large`：文件超过限制（413）
- `knowledge.file_type_unsupported`：扩展名不支持（422）
- `knowledge.file_type_mismatch`：扩展名和 MIME 不一致（422）
- `knowledge.file_encoding_invalid`：TXT/Markdown 不是 UTF-8（异步 Job failed）
- `knowledge.file_no_extractable_text`：没有可提取文本，当前未启用 OCR（异步 Job failed）
- `idempotency.*`：同一 key 被不同文件复用，生成新 key 后再提交

## 6. 仍需三方冻结

前端、后端、运维后续需统一：正式 UploadSession/对象存储流程、生产 embedding provider/model/revision、私有 blob 持久卷、保留与删除周期、病毒扫描、OCR 和批量上传策略。

