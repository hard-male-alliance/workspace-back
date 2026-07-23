# 历史契约缺口说明（已归档）

本文件原先记录 API V1/mock 联调阶段的临时缺口。自 API STANDARD V2 接管后，它不再是当前
实现状态、产品协议或发布门禁的事实来源，也不得据此恢复 V1 payload、默认 Workspace、可信代理
身份，或任何 mock 路径。

当前资料只有以下三类来源：

- 规范：只读子模块 `workspace-shared-docs/contracts/v2/`；
- 实现与架构：[API_V2_IMPLEMENTATION.md](API_V2_IMPLEMENTATION.md)；
- 交接、已知限制与发布检查：[API_V2_HANDOFF.md](API_V2_HANDOFF.md)。

保留此短文件只为避免历史链接失效。新增缺口应先判断它属于契约还是实现：契约问题必须在本仓库
外修改并审阅共享契约，再更新父仓库固定 gitlink；实现问题应在 V2 交接文档中记录并连同代码、
迁移和测试一起闭环。禁止在这里维护第二份覆盖矩阵或推断尚未发布的协议。
