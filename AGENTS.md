# 仓库说明

- `workspace-shared-docs` 是只读 submodule，也是共享 API 契约的唯一事实来源；禁止在其中编辑、建分支、commit 或 push。
- 修改契约必须在本仓库外单独 clone、审阅并合并；随后仅在本仓库更新 gitlink。发现 submodule 有本地修改时停止，不得代用户处理。
- API 2.0 新开发只从 `workspace-shared-docs/contracts/v2/` 读取，不得创建副本或缺失时回退；clone、CI、测试和构建必须初始化父仓库固定的 revision。
- `/api/v1` 仅可在明确的并行迁移期间继续读取 `contracts/v1/`；不得将 v1 payload、默认 Workspace 或测试身份旁路冒充为 v2 实现，也不得在 v2 失败时静默回退到 v1。
- `./update-shared.sh` 可在上游变更合并后更新 revision；必须先审阅 v1→v2 差异并通过相关测试，再在父仓库中单独提交 gitlink。
