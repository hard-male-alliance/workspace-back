# ADR 0004：将 dbctl 重构为分层的数据库运维应用

- 状态：Accepted
- 日期：2026-07-21
- 范围：`src/dbctl`、其 CLI/容器入口、PostgreSQL bootstrap 与 Alembic 适配器

## 背景

原实现已经具备正确的安全意图：bootstrap、migration、遥测清理、交互式 shell 和容器启动
均为显式运维动作，运行时身份相互隔离，secret 不进入命令行或操作者错误信息。
问题在于领域状态、用例编排、PostgreSQL/Alembic/subprocess 细节和终端展示被压在同一组
平铺模块中。默认 bootstrap 还会为约 123 条逻辑 SQL 启动约 124 个 `psql` 子进程，
使 Python 类只是 shell script 的外壳。

本次重构把 `dbctl` 视为数据库运维支撑子域（supporting subdomain）。它需要明确的领域
语言和依赖方向，但不需要 Repository、Unit of Work、Command Bus、CQRS 或领域事件。
Eric Evans 的 DDD Reference 强调，应按领域复杂度选择模式，而不是把模式目录本身当成
设计目标；Parnas 的经典模块化准则则要求模块隐藏“可能变化的设计决定”，而不是按执行
步骤机械分组。

## 决策

采用以下源代码分层：

```text
src/dbctl/
├── domain/          # 不可变值对象、数据库目标状态和不变量
├── application/     # 五个用例、与消费者共置的窄端口及强类型进度契约
├── infrastructure/  # JSONC、文件、psql、psycopg、Alembic、包资源
├── interfaces/      # CLI、操作者控制台、容器入口
├── composition.py   # 唯一组合根；按命令装配单一能力
└── __main__.py
```

依赖只能向内：

```text
interfaces ───────┐
                  ├──> application ──> domain
infrastructure ───┘
        ▲
        └──────── composition（唯一允许认识全部外层 adapter）
```

具体规则如下：

1. `domain` 只依赖 Python 标准库，不认识路径、JSON、argparse、psycopg、Alembic 或
   subprocess。
2. `application` 只依赖 `domain`，端口（port）按一次有目的的外部对话定义，并与唯一消费
   它的用例共置，而不是集中放入宽泛的 `ports` 模块或按每个函数生成接口。
3. `infrastructure` 实现 application ports；`interfaces` 只解析输入、调用用例和展示
   结果，不直接构造 adapter。
4. `composition.py` 是唯一组合根（composition root），为每个命令分别装配配置、该命令的
   用例及所需 adapter。它不承载领域判断，也不构造包含所有能力的应用容器。
5. 通过静态 AST 测试持续执行这些依赖规则；目录命名本身不算架构保证。

操作者进度是 application 的强类型输出端口，不是领域事件或日志记录；stdout、stderr、
安全 traceback 与退出码的呈现契约见
[ADR 0005](0005-dbctl-operator-diagnostics.md)。

## 核心模型

- `DatabaseName`、`RoleName`、`SchemaName` 是不同的不可变值对象（value object），避免
  同为 `str` 的 PostgreSQL 名称在 API 中互换。
- `DatabaseTarget` 同时约束 host、port 和 database；三个登录身份必须指向同一目标，
  从而禁止“bootstrap A、migrate B”。
- `RoleSet` 固化 owner/migrator/app/dashboard 的职责；`LoginRole` 在类型上排除
  `NOLOGIN` owner。
- `Secret[T]` 默认不可打印；只有最外层数据库/子进程 adapter 能显式 `reveal()`。
- `DatabaseBlueprint` 是 bootstrap 的唯一目标状态。六个业务 schema 是 migration
  使用的固定 catalog，不再伪装成可任意重命名的配置。
- `RetentionPolicy` 与 `PruneLimits` 表达保留边界和单次运维预算。
- 遥测清理结果采用判别联合（discriminated union）：`RetentionDisabled | PrunePreview |
  PruneApplied`，从类型上消除 `disabled=True, applied=True` 等非法状态。

## Bootstrap 执行模型

bootstrap 计划由有序 `BootstrapStage` 组成。每一阶段显式声明：

- maintenance database 或目标 database；
- 始终执行或仅目标 database 不存在时执行；
- 原子事务或必须独立执行。

因此 `CREATE DATABASE` 不再是执行器中的结构特例，而是一个带条件的非事务阶段。
同一事务阶段通过一个 `psql` 进程和 stdin 脚本执行；`ON_ERROR_STOP` 与事务边界保证阶段
要么全部成功、要么全部回滚。操作者成功摘要继续报告逻辑 SQL 数量，而不是进程数量。

角色 membership 在 PostgreSQL 17 上显式收敛为：migrator 对 owner
`INHERIT FALSE, SET TRUE, ADMIN FALSE`。app/dashboard 到 owner 的直接 membership 被
撤销；若仍存在间接继承或 `SET ROLE` 旁路，bootstrap fail closed。

## Migration 与已有数据

代码分层本身不产生 schema revision。`0001` 至 `0006` 是已经发布的不可变历史：不改
revision ID、链或源码，也不把其中重复的标识符校验提取到运行时模块。

审计发现 application role 可通过 0001/重复 bootstrap 获得
`identity.alembic_version` 的 DML 权限。修复采用新的 head revision，并在每次 bootstrap
末尾再次撤销；不重写历史 revision。该 revision 只收紧 ACL，不改业务行。

未来若删除表或列，必须使用 expand → backfill/validate → cutover → contract。小而确定的
数据转换可在 Alembic revision 内完成；大回填应使用独立、有界、可恢复的迁移程序，避免
在 schema revision 的长事务中静默跳过或长时间阻塞业务写入。

## 被拒绝的方案

- **保留平铺模块，只增加类**：不能建立可执行的依赖规则，也不能消除 adapter 与领域的
  混装。
- **为所有 I/O 建 Repository/UoW**：dbctl 不持久化领域 aggregate，这些抽象没有替换
  轴，只会增加间接层。
- **引入 DI 框架或 Pydantic 重写领域层**：五个同步用例可由显式构造函数装配；额外框架
  会带来 coercion、错误泄密和生命周期复杂度。
- **保留旧模块转发 shim**：仓库没有需要兼容的第三方 Python API；转发层会让旧结构与
  新结构长期并存。
- **修改已有 migration**：已执行 revision 的语义必须冻结；任何修复都通过新 head
  revision 前进。

## 依据

- [Domain-Driven Design Reference（Eric Evans）](https://www.domainlanguage.com/ddd/reference/)
- [On the Criteria To Be Used in Decomposing Systems into Modules（Parnas, CACM 1972）](https://doi.org/10.1145/361598.361623)
- [Using Dependency Models to Manage Complex Software Architecture（Sangal et al., OOPSLA 2005）](https://groups.csail.mit.edu/sdg/pubs/2005/oopsla05-dsm.pdf)
- [Hexagonal Architecture 原始文章（Alistair Cockburn）](https://alistair.cockburn.us/hexagonal-architecture)
- [Python Packaging User Guide：src layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)
- [Python 3.14 typing 与 Protocol](https://docs.python.org/3.14/library/typing.html)
- [Alembic：Data Migrations — General Techniques](https://alembic.sqlalchemy.org/en/latest/cookbook.html#data-migrations-general-techniques)
- [PostgreSQL 17 role membership](https://www.postgresql.org/docs/17/role-membership.html)
- [PostgreSQL 18 psql transaction 与 `ON_ERROR_STOP`](https://www.postgresql.org/docs/18/app-psql.html)
