# ADR 0005：建立 dbctl 的操作者输出与安全诊断契约

- 状态：Accepted
- 日期：2026-07-22
- 范围：`src/dbctl` 的 CLI 输出、用例进度、异常边界与命令组合

## 背景

`dbctl` 执行 bootstrap、schema migration、遥测清理、交互式 `psql` 和容器启动。五个应用用例
可能创建本地私密配置、跨多个事务阶段提交数据库状态、投影容器运行配置，或把退出状态传回
调度器。仅在末尾打印“成功/失败”会让操作者无法回答三个关键问题：当前将做什么、执行到了
哪里、失败后哪些状态已经提交。

诊断信息同时接触 PostgreSQL driver、Alembic、subprocess、文件系统和本地配置。底层异常正文
可能包含 DSN、密码、SQL 参数、触发器文本或业务行；Python 异常注释也可由第三方任意写入。
因此，“有 traceback”与“不会泄密”不能靠事后正则表达式同时实现，必须把可信诊断数据与
不可信异常文本在模型中分开。

外部依据提供了互补约束：

- [Command Line Interface Guidelines](https://clig.dev/) 建议把主结果写入 stdout、把消息写入
  stderr，在耗时 I/O 前说明意图，提供可行动错误，并保持脚本接口向后兼容。
- [Python `traceback` 文档](https://docs.python.org/3.14/library/traceback.html) 提供
  `TracebackException`、因果链和 locals 捕获选项，但该高层对象还会接触异常正文、注释与源码
  信息；本信任边界因此不构造它，而是只遍历内建 traceback 指针。
- [PostgreSQL Error Message Style Guide](https://www.postgresql.org/docs/current/error-style-guide.html)
  将短而事实性的主错误、补充细节和修复提示分开，适合运维者快速判断下一步。
- CHI 2021 论文 [Accessibility of Command Line Interfaces](https://storage.googleapis.com/gweb-research2023-media/pubtools/6065.pdf)
  记录了缺少状态反馈会诱发重复操作，并指出 spinner、动态覆盖和仅靠 ASCII 图形传达状态对
  屏幕阅读器不友好；稳定的逐行文本更适合作为默认进度表达。

## 决策

### 1. stdout 与 stderr 是两个独立契约

`dbctl` 将可管道化的主结果写入 stdout；命令意图、进度、警告和失败诊断写入 stderr。既有成功
结果的文本形状保持稳定，增加可观测性不能污染依赖 stdout 的脚本。帮助和参数解析继续遵循
`argparse` 的标准流与退出行为。

其中 `bootstrap --dry-run` 的 stdout 保持既有字节序列，只输出原有的脱敏 SQL 计划。配置初始化与
target/stage 进度等新增上下文全部进入 stderr，不修改这个可被脚本消费的兼容接口。

命令在首次配置或数据库 I/O 前，先在 stderr 声明子命令、执行模式、配置路径和数据库声明
路径。进度采用稳定逐行记录，不使用 spinner、光标回写或动画。终端支持时可使用颜色作为冗余
线索，但状态始终同时具有文字和符号，颜色不是唯一信息载体。

通用选项语义如下：

| 选项/环境 | 行为 |
|---|---|
| `--quiet` | 抑制命令声明及正常进度；不抑制 stdout 主结果、警告、失败进度或错误诊断。 |
| `--no-color` | 显式禁用 ANSI（American National Standards Institute）颜色。 |
| `NO_COLOR` | 只要环境变量存在，即禁用颜色；空值也有效。 |
| `TERM=dumb` | 自动禁用颜色，保留稳定逐行纯文本。 |

### 2. 进度是应用层的强类型输出

应用层定义不可变 `ProgressUpdate`，包含稳定的 `OperationName`、生命周期状态
`started | succeeded | skipped | failed`、简短动作、可选安全细节以及成对出现的
`current/total`。构造时拒绝空消息、不完整位置和越界步骤。它是同步的用例输出契约
（output port），不是领域事件（domain event），也不是可持久化日志 schema。

CLI 的 `OperatorConsole` 实现单一 `ProgressSink` 并只写 stderr；用例不认识 Rich、TTY 或颜色。
进度发布和终端写入均为尽力而为（best effort）：stderr 关闭、终端断开或自定义 sink 缺陷只会
损失展示，不得把已提交的数据库动作变成失败、触发回滚或改变本应返回的退出码。这条隔离用于
避免“数据库已提交但输出失败”诱发危险重试。

### 3. 异常输出采用摘要、建议和安全 traceback

参数解析完成后的异常路径输出三层信息：事实性的失败原因、按错误类别给出的下一步，以及保留
有限栈信息的安全 traceback（回溯）。实现刻意不构造 `TracebackException`，而是通过内建基类
descriptor 读取 traceback、cause/context，并读取内部异常构造器冻结的正文快照，避免触发
不可信子类的 `__str__`、`__dict__`/`__class__` property、metaclass hook 或动态类元数据。
继承分类仅将真实
`type(error)` 与固定基类的 MRO（method resolution order）比较，不对不可信实例调用
`isinstance`。安全边界执行以下规则：

1. 不读取 locals、源码行、任意外部异常正文或未知异常的 `str(error)`。只有 frame 的 globals
   字典与 `sys.modules` 中精确 `ModuleType` 的 namespace 为同一对象、module `__file__` 与
   code filename 匹配、规范化文件位于 dbctl package 根目录内，且相对路径和函数名通过 ASCII
   白名单时，才显示文件和函数。仅伪造 `co_filename` 不足以获得信任；所有其他帧使用固定占位
   隐藏文件和函数，只保留 traceback 自带的行号。
2. application/domain 正文仅在异常属于精确白名单类型，且最内层 raise frame 通过上述来源验证
   时，才读取异常构造时写入的不可变快照并再次脱敏；展示阶段不读取后来可变的
   `BaseException.args`。快照由进程内模块密钥 HMAC 绑定异常身份和全部字段，读取后替换正文、
   notes、来源授权位或跨异常移植都会校验失败。第三方子类不能借内部基类放行正文。外部安全代理
   的正文也使用该完整性载荷，并显式记录其可沿用外部 traceback 来源。除 dbctl 定义的安全动作标签外，代理只公开固定内建
   基类分类，以及通过 `OSError` 内建 descriptor 读取并限制在安全范围内的整数 `errno`；原始
   对象和正文不复制到这些字段。代理复用原始 traceback 指针，但渲染器不读取帧的 locals；它也
   不反射读取 `SQLSTATE`、行列号或任何第三方属性。
3. 任意 Python `__notes__` 一律不读取。只有 dbctl helper 由已验证非秘密字段构造、通过
   `BaseException.__dict__` 内建 descriptor 写入、由上述 HMAC 完整性标签认证并再次脱敏的
   不可变正文/注释载荷可显示；伪造同名属性、复制载荷类型或子类 `__dict__` property 不会被
   接受或执行。
4. cause/context 链在一次有界遍历中同时验证、捕获并检测环；仅当整条链均由显式安全异常类型组成
   且不超过深度上限时才展示消息链。自环、超长或含未知成员的链退化为顶层安全分类、受信任
   dbctl 栈帧和外部帧占位。

该 HMAC 是同一进程内检测异常载荷篡改的完整性机制，不是把 Python 解释器变成隔离边界。已经能
读取模块私有密钥或取得任意 Python 代码执行能力的攻击者仍可调用公开诊断 helper、改写模块表或
直接写 stderr；这个威胁必须由供应链审阅、独立进程/容器边界和操作系统权限处理。本决策的诊断
边界防御的是不可信异常对象与意外泄露，而不是把同一解释器中的恶意代码变成可信代码。

本策略刻意不尝试“清洗后展示”任意数据库错误正文：启发式脱敏无法证明未知业务数据或 secret
已被覆盖。代价是操作者可能只能结合固定错误分类、可信 dbctl 栈帧、可选的 `OSError.errno` 和
受控 PostgreSQL server log 继续排障；收益是 CLI 不会为了便利诊断而扩大 secret 泄露面。

### 4. 失败必须陈述部分提交的运维影响

进度与安全诊断按实际事务边界描述影响，不承诺跨命令的虚构原子性：

- bootstrap 每个事务阶段单独原子提交；普通阶段失败时报告此前已完成阶段和计划 SQL 数，当前阶段
  不计入完成，后续阶段未执行。`KeyboardInterrupt` 可能落在外部提交与返回之间，因此当前阶段只
  能标为“完成状态未知”；安全影响注释会进入退出码 `130` 的取消摘要。runner 即使错误地从
  `__exit__` 返回真值，也不能把阶段失败或中断抑制成部分成功结果。
- `prune-telemetry --apply` 每批是独立短事务，并在本轮使用固定 UTC cutoff；失败时报告此前已
  提交批次数和删除数。剩余状态探测失败时，明确指出删除已经提交、只有探测结果未知。
- migration 失败时不猜测 Alembic 或数据库的最终 revision；诊断要求重试前读取当前 revision。
- shell 返回规范化后的 `psql` 状态；临时凭据文件创建或清理失败走同一安全异常边界。
- 容器启动先原子投影运行配置，再以 `exec` 替换当前进程；投影失败明确说明目标进程尚未启动，
  `exec` 失败则明确说明配置已经投影但目标进程尚未启动。

诊断增强本身不得遮蔽原始业务失败。安全注释、失败进度或终端渲染失败都不能改变数据库事务
语义或造成额外数据库动作。

### 5. 退出码与组合边界保持明确

新增进度和 traceback 不改变既有命令效果或退出码分类：成功为 `0`，可向操作者解释的参数、
领域、配置、I/O 或运行时失败为 `2`，未预期内部失败为 `1`，操作者中断为 `130`；shell 继续
返回规范化后的 `psql` 状态。已经提交的状态不因 stderr 故障而被错误改写为另一退出结果。
容器入口缺少目标命令或发生预期投影失败时返回 `2`；目标命令无法 `exec` 或发生未预期内部失败
时在安全呈现后返回 `1`。`exec` 成功后入口进程被目标程序替换，后续退出状态由目标程序决定。

组合根按命令提供 `compose_bootstrap`、`compose_migration`、`compose_prune_telemetry`、
`compose_shell` 和 `compose_container`，对应五个 application 用例。每次只构造当前命令需要的
adapter，避免一次配置失败或无关依赖阻断另一条命令。每个基础设施端口与其唯一 application
用例共置，不保留集中式 `ports` 模块或包含全部能力的 `DbctlApplication` 容器。该决定延续
[ADR 0004](0004-dbctl-layered-domain-architecture.md) 的向内依赖规则，同时让操作者动作与代码
能力边界一一对应。

## 后果

- 自动化可以继续只消费 stdout；人类操作者和日志采集器可从 stderr 获得明确、可访问且按阶段
  排列的进度。
- `--quiet` 适合定时任务，但失败仍可见；`--no-color`、`NO_COLOR` 与 `TERM=dumb` 适合持久日志
  和辅助技术（assistive technology）。
- traceback 只显示通过模块 namespace 身份与文件匹配验证的 dbctl 帧文件/行号/函数；外部帧
  隐藏文件与函数，只保留占位和行号。
  它不读取源码行、locals、任意外部消息或 `__notes__`；若需要数据库原始细节，应从权限受控的
  server log 获取，而不是降低 CLI 的 secret 边界。
- 进度是同步且非持久的；本决策不引入 JSON 事件流、遥测后端、动画进度条或跨进程恢复协议。
- stdout 文本仍是兼容接口。未来如增加机器可读格式，必须用显式选项引入并定义独立版本契约，
  不能静默替换现有输出。

## 被拒绝的方案

- **把进度写入 stdout**：会破坏管道和既有脚本。
- **直接打印第三方异常及 locals**：诊断更丰富，但无法建立可信的 secret 上界。
- **只显示一句失败摘要**：缺少栈帧、失败位置和部分提交影响，无法支持可靠排障。
- **使用 spinner 或动态 TTY UI**：不适合持久日志、管道和屏幕阅读器，也掩盖阶段历史。
- **让进度发布参与事务成败**：输出通道故障可能在提交后制造假失败并诱发重复执行。
- **为所有命令预先构造全部 adapter**：扩大故障面和权限面，并让不相关能力互相耦合。
