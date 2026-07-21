# 仓库说明

- `workspace-shared-docs` 是只读 submodule，也是共享 API 契约的唯一事实来源；禁止在其中编辑、建分支、commit 或 push。
- 修改契约必须在本仓库外单独 clone、审阅并合并；随后仅在本仓库更新 gitlink。发现 submodule 有本地修改时停止，不得代用户处理。
- 契约只从 `workspace-shared-docs/contracts/v1/` 读取，不得创建副本或缺失时回退；clone、CI、测试和构建必须初始化父仓库固定的 revision。
- `./update-shared.sh` 可在上游变更合并后更新 revision；测试通过后，gitlink 由用户在父仓库单独提交。
