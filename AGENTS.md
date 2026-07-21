# 仓库协作说明

## 前后端共享 API 契约

- `workspace-shared-docs` 是 Git 子模块（submodule），也是前后端共享 API 契约的唯一事实来源（single source of truth）。
- 不得在本仓库中重新创建或维护第二份 `contract`。backend wheel 可以把当前固定版本的严格 JSON Schema 打包为运行时资源，但该文件属于构建产物，不是另一份独立维护的源文件。
- 读取、测试或构建依赖契约的代码前，必须执行 `git submodule update --init --recursive`，初始化父仓库固定的子模块 revision。
- 契约路径统一从 `workspace-shared-docs/contracts/v1/` 解析。子模块缺失时应明确失败，不得静默回退到过期的本地副本。
- 不得隐式把子模块推进到远端分支最新版。升级契约时必须选择经过审阅的 commit 或 release，运行后端契约与打包测试，并在父仓库提交更新后的 gitlink。
- 契约源码变更与后端指针更新属于两个仓库中的两个独立 commit。必须先在共享文档仓库提交并推送契约变更，再在本仓库提交新的 `workspace-shared-docs` gitlink。
- CI 和 clone 流程必须获取子模块。子模块缺失或 revision 不匹配属于显式环境错误，不能通过重新生成契约文件规避。
