"""@brief PostgreSQL bootstrap 计划与用例 / PostgreSQL bootstrap plan and use case."""

from dataclasses import dataclass, field
from enum import StrEnum
from types import TracebackType
from typing import Final, Protocol, Self

from dbctl.domain.database import (
    IDENTITY_SCHEMA,
    AppLogin,
    BootstrapAccess,
    DatabaseTarget,
    DbctlSettings,
)
from dbctl.domain.names import DatabaseName, RoleName, SchemaName
from dbctl.domain.roles import DatabaseRole, RoleSet, Secret

from .errors import (
    BootstrapExecutionError,
    DatabaseAlreadyExistsError,
    add_safe_diagnostic_note,
)
from .progress import (
    OperationName,
    ProgressSink,
    ProgressState,
    ProgressUpdate,
    publish_progress,
)


class BootstrapAccessMode(StrEnum):
    """@brief bootstrap 获取管理权限的模式 / Administrative-access mode for bootstrap."""

    AUTO = "auto"
    SUDO = "sudo"
    PROMPT = "prompt"


class ExecutionTarget(StrEnum):
    """@brief bootstrap SQL 的连接目标 / Connection target for bootstrap SQL."""

    MAINTENANCE = "maintenance"
    DATABASE = "database"


class StageCondition(StrEnum):
    """@brief bootstrap 阶段的应用层执行条件 / Application-owned execution condition."""

    ALWAYS = "always"
    DATABASE_ABSENT = "database_absent"


class TransactionMode(StrEnum):
    """@brief bootstrap 批次的事务模式 / Transaction mode for a bootstrap batch."""

    TRANSACTIONAL = "transactional"
    AUTOCOMMIT = "autocommit"


type SqlParameter = str | Secret[str]
"""@brief SQL 文本参数；secret 必须保持显式类型 / SQL text parameter with explicit secrets."""


@dataclass(frozen=True, slots=True)
class SqlStatement:
    """@brief 不执行渲染的参数化 SQL 值 / Parameterized SQL value without rendering behavior.

    @param label 面向运维者的非敏感操作名 / Non-sensitive operator-facing operation label.
    @param sql 使用 ``%s`` 文本占位符的 SQL / SQL using ``%s`` text placeholders.
    @param parameters 由 infrastructure 在执行边界安全绑定的参数 / Parameters safely bound at infrastructure.
    """

    label: str
    sql: str
    parameters: tuple[SqlParameter, ...] = field(default_factory=tuple, repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 SQL 与参数布局 / Validate SQL and parameter layout.

        @return 无返回值 / No return value.
        @raise BootstrapExecutionError 标签、SQL 或参数不合法时抛出。
        / Raised when the label, SQL, or parameter layout is invalid.
        """
        if not isinstance(self.label, str) or not self.label.strip():
            raise BootstrapExecutionError("bootstrap SQL 必须有非空标签。")
        if not isinstance(self.sql, str) or not self.sql.strip():
            raise BootstrapExecutionError("bootstrap SQL 不能为空。")
        parameters = tuple(self.parameters)
        for parameter in parameters:
            value = parameter.reveal() if isinstance(parameter, Secret) else parameter
            if not isinstance(value, str) or "\x00" in value:
                raise BootstrapExecutionError("bootstrap SQL 参数必须是无 NUL 的字符串。")
        if self.sql.count("%s") != len(parameters):
            raise BootstrapExecutionError("bootstrap SQL 占位符与参数数量不一致。")
        object.__setattr__(self, "label", self.label.strip())
        object.__setattr__(self, "parameters", parameters)


@dataclass(frozen=True, slots=True)
class BootstrapStage:
    """@brief 一次连接目标上的有序 SQL 批次 / Ordered SQL batch on one connection target.

    @param label 非敏感阶段名称 / Non-sensitive stage label.
    @param target maintenance 或项目数据库 / Maintenance or project database.
    @param condition 应用层判断的执行条件 / Execution condition decided by application.
    @param transaction_mode 整批事务或 autocommit / Whole-batch transaction or autocommit.
    @param statements 按顺序批量执行的非空 SQL / Non-empty ordered SQL batch.
    """

    label: str
    target: ExecutionTarget
    condition: StageCondition
    transaction_mode: TransactionMode
    statements: tuple[SqlStatement, ...]

    def __post_init__(self) -> None:
        """@brief 校验阶段不变量并消除 CREATE DATABASE 特例 / Validate stage invariants.

        @return 无返回值 / No return value.
        @raise BootstrapExecutionError 条件、目标、事务模式或批次不协调时抛出。
        / Raised when condition, target, transaction mode, or batch is inconsistent.
        """
        if not isinstance(self.label, str) or not self.label.strip():
            raise BootstrapExecutionError("bootstrap stage 必须有非空标签。")
        if not isinstance(self.target, ExecutionTarget):
            raise BootstrapExecutionError("bootstrap stage target 无效。")
        if not isinstance(self.condition, StageCondition):
            raise BootstrapExecutionError("bootstrap stage condition 无效。")
        if not isinstance(self.transaction_mode, TransactionMode):
            raise BootstrapExecutionError("bootstrap stage transaction_mode 无效。")
        statements = tuple(self.statements)
        if not statements or any(
            not isinstance(statement, SqlStatement) for statement in statements
        ):
            raise BootstrapExecutionError("bootstrap stage 必须包含非空 SqlStatement 批次。")
        if self.condition is StageCondition.DATABASE_ABSENT and (
            self.target is not ExecutionTarget.MAINTENANCE
            or self.transaction_mode is not TransactionMode.AUTOCOMMIT
            or len(statements) != 1
        ):
            raise BootstrapExecutionError(
                "DATABASE_ABSENT 阶段必须是在 maintenance 上执行的单语句 autocommit 批次。"
            )
        if self.transaction_mode is TransactionMode.AUTOCOMMIT and len(statements) != 1:
            raise BootstrapExecutionError("autocommit stage 只能包含一条语句。")
        object.__setattr__(self, "label", self.label.strip())
        object.__setattr__(self, "statements", statements)


@dataclass(frozen=True, slots=True)
class BootstrapPlan:
    """@brief 自足、不可变、按阶段有序的 bootstrap 计划 / Self-contained ordered bootstrap plan.

    @param database 项目数据库名 / Project database name.
    @param access 管理连接的非秘密访问设置 / Non-secret administrative access settings.
    @param database_target 项目数据库目标 / Project database target.
    @param stages 严格执行顺序的阶段 / Stages in strict execution order.
    """

    database: DatabaseName
    access: BootstrapAccess
    database_target: DatabaseTarget
    stages: tuple[BootstrapStage, ...]

    def __post_init__(self) -> None:
        """@brief 校验计划拓扑 / Validate plan topology.

        @return 无返回值 / No return value.
        @raise BootstrapExecutionError 计划目标漂移、阶段为空或条件阶段重复时抛出。
        / Raised for target drift, empty stages, or duplicate conditional stages.
        """
        if not isinstance(self.database, DatabaseName):
            raise BootstrapExecutionError("BootstrapPlan.database 必须是 DatabaseName。")
        if not isinstance(self.access, BootstrapAccess):
            raise BootstrapExecutionError("BootstrapPlan.access 必须是 BootstrapAccess。")
        if not isinstance(self.database_target, DatabaseTarget):
            raise BootstrapExecutionError("BootstrapPlan.database_target 必须是 DatabaseTarget。")
        if self.database_target.database != self.database:
            raise BootstrapExecutionError("BootstrapPlan 的项目数据库目标发生漂移。")
        if (
            self.access.maintenance_target.host,
            self.access.maintenance_target.port,
        ) != (self.database_target.host, self.database_target.port):
            raise BootstrapExecutionError("BootstrapPlan 的 maintenance 与项目 endpoint 发生漂移。")
        stages = tuple(self.stages)
        if not stages or any(not isinstance(stage, BootstrapStage) for stage in stages):
            raise BootstrapExecutionError("BootstrapPlan 必须包含非空 BootstrapStage 序列。")
        labels = tuple(stage.label for stage in stages)
        if len(set(labels)) != len(labels):
            raise BootstrapExecutionError("BootstrapPlan 的 stage label 必须唯一。")
        conditional_count = sum(
            stage.condition is StageCondition.DATABASE_ABSENT for stage in stages
        )
        if conditional_count != 1:
            raise BootstrapExecutionError("BootstrapPlan 必须恰好包含一个数据库条件创建阶段。")
        object.__setattr__(self, "stages", stages)


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    """@brief 不含秘密的 bootstrap 执行摘要 / Secret-free bootstrap execution summary.

    @param database_created 本次是否赢得条件创建 / Whether this run won conditional creation.
    @param executed_stage_count 实际执行的 stage 数量 / Number of stages actually executed.
    @param skipped_stage_count 因条件不满足而跳过的 stage 数量 / Conditionally skipped stages.
    @param executed_statement_count 已发送的 SQL 数量 / Number of SQL statements sent.
    """

    database_created: bool
    executed_stage_count: int
    skipped_stage_count: int
    executed_statement_count: int

    def __post_init__(self) -> None:
        """@brief 校验结果计数 / Validate result counters.

        @return 无返回值 / No return value.
        """
        if not isinstance(self.database_created, bool):
            raise BootstrapExecutionError("database_created 必须是布尔值。")
        for label, value in (
            ("executed_stage_count", self.executed_stage_count),
            ("skipped_stage_count", self.skipped_stage_count),
            ("executed_statement_count", self.executed_statement_count),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise BootstrapExecutionError(f"{label} 必须是非负整数。")


class BootstrapRunner(Protocol):
    """@brief 一个 bootstrap 会话的批量执行端口 / Batch-execution port for one bootstrap session."""

    @property
    def access_mode(self) -> BootstrapAccessMode:
        """@brief 返回已经解析的实际管理访问方式 / Return the resolved administrative-access mode.

        @return sudo 或 prompt / ``sudo`` or ``prompt``.
        """
        ...

    def __enter__(self) -> Self:
        """@brief 进入受控 runner 生命周期 / Enter the controlled runner lifecycle.

        @return 当前 runner / Current runner.
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """@brief 关闭 runner 并清理临时凭证 / Close the runner and clean temporary credentials.

        @param exc_type 退出时的异常类型 / Exception type at exit.
        @param exc_value 退出时的异常对象 / Exception instance at exit.
        @param traceback 退出时的 traceback / Traceback at exit.
        @return 无返回值 / No return value.
        """
        ...

    def database_exists(self, database: DatabaseName) -> bool:
        """@brief 判断项目数据库是否存在 / Determine whether the project database exists.

        @param database 强类型数据库名 / Strongly typed database name.
        @return 数据库存在时为真 / True when the database exists.
        """
        ...

    def execute_stage(self, stage: BootstrapStage) -> None:
        """@brief 按 stage 契约批量执行 SQL / Execute SQL as one batch under the stage contract.

        @param stage 已验证的连接目标、事务模式与 SQL 批次 / Validated target, transaction mode, and batch.
        @return 无返回值 / No return value.
        """
        ...


class BootstrapRunnerFactory(Protocol):
    """@brief 按计划与访问模式打开 bootstrap runner / Open a runner for a plan and access mode."""

    def open(
        self,
        plan: BootstrapPlan,
        access_mode: BootstrapAccessMode,
    ) -> BootstrapRunner:
        """@brief 创建尚未进入上下文的 runner / Create a runner not yet entered.

        @param plan 含 maintenance 与项目目标的自足计划 / Self-contained plan with both targets.
        @param access_mode 显式管理权限模式 / Explicit administrative-access mode.
        @return 由应用服务管理生命周期的 BootstrapRunner / Runner lifecycle-owned by application.
        """
        ...


class BootstrapService:
    """@brief 管理 runner 生命周期并执行 bootstrap 计划 / Own runner lifecycle and execute a plan."""

    def __init__(
        self,
        runner_factory: BootstrapRunnerFactory,
        *,
        progress: ProgressSink | None = None,
    ) -> None:
        """@brief 初始化 bootstrap 用例 / Initialize the bootstrap use case.

        @param runner_factory 按访问模式打开 runner 的端口 / Port opening a runner by access mode.
        @param progress 可选同步进度输出端口 / Optional synchronous progress output port.
        """
        self._runner_factory = runner_factory
        self._progress = progress

    def execute(
        self,
        plan: BootstrapPlan,
        *,
        access_mode: BootstrapAccessMode = BootstrapAccessMode.AUTO,
    ) -> BootstrapResult:
        """@brief 按应用层条件执行有序 stage / Execute ordered stages under application conditions.

        @param plan 已由纯函数构建并验证的计划 / Plan built and validated by the pure planner.
        @param access_mode 管理权限获取模式 / Administrative-access mode.
        @return 不含秘密的执行摘要 / Secret-free execution summary.
        @raise DatabaseAlreadyExistsError 仅并发条件创建信号会被消费；其他阶段不会吞掉它。
        / Only the concurrent conditional-create signal is consumed; other stages never swallow it.
        """
        if not isinstance(plan, BootstrapPlan):
            raise BootstrapExecutionError("BootstrapService 只能执行 BootstrapPlan。")
        if not isinstance(access_mode, BootstrapAccessMode):
            raise BootstrapExecutionError("bootstrap access mode 无效。")

        executed_stage_count = 0
        skipped_stage_count = 0
        executed_statement_count = 0
        database_created = False
        self._publish(
            ProgressUpdate(
                operation=OperationName.BOOTSTRAP,
                state=ProgressState.STARTED,
                message="解析 PostgreSQL 管理访问方式",
                detail=f"请求模式={access_mode.value}",
            )
        )
        runner_resource = self._runner_factory.open(plan, access_mode)
        self._publish(
            ProgressUpdate(
                operation=OperationName.BOOTSTRAP,
                state=ProgressState.SUCCEEDED,
                message="PostgreSQL 管理访问方式已确定",
                detail=f"实际模式={runner_resource.access_mode.value}",
            )
        )
        with runner_resource as runner:
            total_stages = len(plan.stages)
            for stage_number, stage in enumerate(plan.stages, start=1):
                stage_detail = (
                    f"目标={stage.target.value}；事务={stage.transaction_mode.value}；"
                    f"SQL={len(stage.statements)} 条"
                )
                self._publish(
                    ProgressUpdate(
                        operation=OperationName.BOOTSTRAP,
                        state=ProgressState.STARTED,
                        message=stage.label,
                        detail=stage_detail,
                        current=stage_number,
                        total=total_stages,
                    )
                )
                try:
                    if stage.condition is StageCondition.DATABASE_ABSENT:
                        if runner.database_exists(plan.database):
                            skipped_stage_count += 1
                            self._publish(
                                ProgressUpdate(
                                    operation=OperationName.BOOTSTRAP,
                                    state=ProgressState.SKIPPED,
                                    message=stage.label,
                                    detail="目标数据库已存在；未执行 CREATE DATABASE",
                                    current=stage_number,
                                    total=total_stages,
                                )
                            )
                            continue
                        try:
                            runner.execute_stage(stage)
                        except DatabaseAlreadyExistsError:
                            skipped_stage_count += 1
                            self._publish(
                                ProgressUpdate(
                                    operation=OperationName.BOOTSTRAP,
                                    state=ProgressState.SKIPPED,
                                    message=stage.label,
                                    detail="并发 bootstrap 已创建目标数据库；安全跳过",
                                    current=stage_number,
                                    total=total_stages,
                                )
                            )
                            continue
                        database_created = True
                    else:
                        runner.execute_stage(stage)
                except Exception as error:
                    impact = (
                        f"此前已完成 {executed_stage_count} 个阶段、"
                        f"执行 {executed_statement_count} 条计划 SQL；"
                        "当前阶段未计入完成结果，后续阶段未执行"
                    )
                    add_safe_diagnostic_note(
                        error,
                        f"dbctl bootstrap 阶段 {stage_number}/{total_stages}：{stage.label}。"
                    )
                    add_safe_diagnostic_note(error, f"运维影响：{impact}。")
                    self._publish(
                        ProgressUpdate(
                            operation=OperationName.BOOTSTRAP,
                            state=ProgressState.FAILED,
                            message=stage.label,
                            detail=impact,
                            current=stage_number,
                            total=total_stages,
                        )
                    )
                    raise
                executed_stage_count += 1
                executed_statement_count += len(stage.statements)
                self._publish(
                    ProgressUpdate(
                        operation=OperationName.BOOTSTRAP,
                        state=ProgressState.SUCCEEDED,
                        message=stage.label,
                        detail=(
                            f"本阶段已提交 {len(stage.statements)} 条计划 SQL；"
                            f"累计完成 {executed_stage_count} 个阶段"
                        ),
                        current=stage_number,
                        total=total_stages,
                    )
                )
        return BootstrapResult(
            database_created=database_created,
            executed_stage_count=executed_stage_count,
            skipped_stage_count=skipped_stage_count,
            executed_statement_count=executed_statement_count,
        )

    def _publish(self, update: ProgressUpdate) -> None:
        """@brief 向可选输出端口同步发布进度 / Publish progress synchronously to the optional output port.

        @param update 已验证且不含 secret 的进度 / Validated secret-free progress update.
        @return 无返回值 / No return value.
        """

        publish_progress(self._progress, update)


def build_bootstrap_plan(settings: DbctlSettings) -> BootstrapPlan:
    """@brief 纯函数构建最小权限 bootstrap 计划 / Purely build a least-privilege bootstrap plan.

    @param settings 已验证且跨对象一致的领域设置 / Validated, cross-object-consistent settings.
    @return 未执行的不可变 BootstrapPlan / Immutable unexecuted BootstrapPlan.
    @note PostgreSQL 17 membership options 在 SQL 中显式收敛，且 app/dashboard 对 owner
    的任意间接成员路径会 fail closed。/ PostgreSQL 17 membership options are converged
    explicitly, and any indirect app/dashboard path to owner fails closed.
    """
    if not isinstance(settings, DbctlSettings):
        raise BootstrapExecutionError("build_bootstrap_plan 需要 DbctlSettings。")
    blueprint = settings.blueprint
    roles = blueprint.roles
    passwords: Final[dict[DatabaseRole, Secret[str]]] = {
        DatabaseRole.MIGRATOR: settings.connections.migrator.password,
        DatabaseRole.APP: settings.connections.application.password,
        DatabaseRole.DASHBOARD: settings.connections.dashboard.password,
    }
    role_stage = BootstrapStage(
        label="角色与成员关系收敛",
        target=ExecutionTarget.MAINTENANCE,
        condition=StageCondition.ALWAYS,
        transaction_mode=TransactionMode.TRANSACTIONAL,
        statements=tuple(_role_statements(roles, passwords)),
    )
    create_stage = BootstrapStage(
        label="条件创建项目数据库",
        target=ExecutionTarget.MAINTENANCE,
        condition=StageCondition.DATABASE_ABSENT,
        transaction_mode=TransactionMode.AUTOCOMMIT,
        statements=(
            SqlStatement(
                label="数据库不存在时创建项目数据库",
                sql=(
                    f"CREATE DATABASE {_quote_identifier(blueprint.database.value)} "
                    f"OWNER {_quote_identifier(roles.owner.value)};"
                ),
            ),
        ),
    )
    database_access_stage = BootstrapStage(
        label="数据库级权限收敛",
        target=ExecutionTarget.MAINTENANCE,
        condition=StageCondition.ALWAYS,
        transaction_mode=TransactionMode.TRANSACTIONAL,
        statements=tuple(_database_access_statements(blueprint.database, roles)),
    )
    schema_stage = BootstrapStage(
        label="schema 与对象权限收敛",
        target=ExecutionTarget.DATABASE,
        condition=StageCondition.ALWAYS,
        transaction_mode=TransactionMode.TRANSACTIONAL,
        statements=tuple(_schema_privilege_statements(settings)),
    )
    migration_metadata_stage = BootstrapStage(
        label="迁移元数据权限收敛",
        target=ExecutionTarget.DATABASE,
        condition=StageCondition.ALWAYS,
        transaction_mode=TransactionMode.TRANSACTIONAL,
        statements=(_conditional_alembic_revoke(settings.connections.application),),
    )
    return BootstrapPlan(
        database=blueprint.database,
        access=settings.access,
        database_target=settings.connections.target,
        stages=(
            role_stage,
            create_stage,
            database_access_stage,
            schema_stage,
            migration_metadata_stage,
        ),
    )


def _quote_identifier(value: str) -> str:
    """@brief 引用已由值对象验证的标识符 / Quote an identifier already validated by a value object.

    @param value 可移植 PostgreSQL 标识符 / Portable PostgreSQL identifier.
    @return 双引号 SQL 标识符 / Double-quoted SQL identifier.
    """
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    """@brief 引用受控 PostgreSQL 文本字面量 / Quote a controlled PostgreSQL text literal.

    @param value 不含 NUL 的非秘密文本 / Non-secret text without NUL.
    @return 单引号 SQL 字面量 / Single-quoted SQL literal.
    """
    if "\x00" in value:
        raise BootstrapExecutionError("PostgreSQL 文本字面量不能包含 NUL。")
    return "'" + value.replace("'", "''") + "'"


def _ensure_role_statement(role_name: RoleName, attributes: str) -> SqlStatement:
    """@brief 构造并发安全的角色创建语句 / Build a race-safe role-creation statement.

    @param role_name 已验证角色名 / Validated role name.
    @param attributes 固定的 CREATE ROLE 属性 / Fixed CREATE ROLE attributes.
    @return 可重复执行的角色创建语句 / Repeatable role-creation statement.
    """
    identifier = _quote_identifier(role_name.value)
    return SqlStatement(
        label=f"确保 {role_name.value} role 存在",
        sql=(
            "DO $dbctl$\n"
            "BEGIN\n"
            "    IF NOT EXISTS (\n"
            "        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = "
            + _quote_literal(role_name.value)
            + "\n    ) THEN\n"
            f"        CREATE ROLE {identifier} {attributes};\n"
            "    END IF;\n"
            "EXCEPTION\n"
            "    WHEN duplicate_object THEN\n"
            "        NULL;\n"
            "END\n"
            "$dbctl$;"
        ),
    )


def _role_statements(
    roles: RoleSet,
    passwords: dict[DatabaseRole, Secret[str]],
) -> list[SqlStatement]:
    """@brief 构造角色属性与成员关系批次 / Build role-attribute and membership statements.

    @param roles DatabaseBlueprint 中的 RoleSet / RoleSet from DatabaseBlueprint.
    @param passwords 三个 LOGIN role 的秘密密码 / Secret passwords for the three LOGIN roles.
    @return 按安全收敛顺序排列的 SQL / SQL in safe convergence order.
    """
    role_specs = (
        (DatabaseRole.OWNER, roles.name_for(DatabaseRole.OWNER), "NOLOGIN NOINHERIT"),
        (DatabaseRole.MIGRATOR, roles.name_for(DatabaseRole.MIGRATOR), "LOGIN NOINHERIT"),
        (DatabaseRole.APP, roles.name_for(DatabaseRole.APP), "LOGIN NOINHERIT"),
        (DatabaseRole.DASHBOARD, roles.name_for(DatabaseRole.DASHBOARD), "LOGIN NOINHERIT"),
    )
    statements: list[SqlStatement] = []
    for role, name, login_attributes in role_specs:
        identifier = _quote_identifier(name.value)
        attributes = (
            f"{login_attributes} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        statements.extend(
            (
                _ensure_role_statement(name, attributes),
                SqlStatement(
                    label=f"收敛 {role.value} role 的非特权属性",
                    sql=f"ALTER ROLE {identifier} {attributes};",
                ),
            )
        )
        password = passwords.get(role)
        if password is not None:
            statements.append(
                SqlStatement(
                    label=f"设置 {role.value} role 密码（已脱敏）",
                    sql=f"ALTER ROLE {identifier} PASSWORD %s;",
                    parameters=(password,),
                )
            )

    owner = _quote_identifier(roles.owner.value)
    migrator = _quote_identifier(roles.migrator.value)
    application = _quote_identifier(roles.application.value)
    dashboard = _quote_identifier(roles.dashboard.value)
    statements.extend(
        (
            SqlStatement(
                label="以 PostgreSQL 17 选项收敛 migrator 的 owner 成员关系",
                sql=(f"GRANT {owner} TO {migrator} WITH INHERIT FALSE, SET TRUE, ADMIN FALSE;"),
            ),
            SqlStatement(
                label="移除 app 与 dashboard 对 owner 的直接成员关系",
                sql=f"REVOKE {owner} FROM {application}, {dashboard};",
            ),
            _indirect_owner_membership_guard(
                owner=roles.owner,
                application=roles.application,
                dashboard=roles.dashboard,
            ),
        )
    )
    return statements


def _indirect_owner_membership_guard(
    *,
    owner: RoleName,
    application: RoleName,
    dashboard: RoleName,
) -> SqlStatement:
    """@brief 构造 app/dashboard 间接 owner 路径的 fail-closed 检查 / Build a fail-closed guard.

    @param owner 不可登录 owner / Non-login owner.
    @param application 应用运行时 role / Runtime application role.
    @param dashboard Dashboard role / Dashboard role.
    @return 检测任意传递成员路径的 DO 语句 / DO statement detecting any transitive membership path.
    @note 检查故意不依赖 INHERIT/SET 选项：任何未来可被改成旁路的路径都拒绝。
    / The guard intentionally ignores INHERIT/SET options and rejects every latent bypass path.
    """
    return SqlStatement(
        label="拒绝 app 或 dashboard 间接成为 owner 成员",
        sql=(
            "DO $dbctl$\n"
            "BEGIN\n"
            "    IF EXISTS (\n"
            "        WITH RECURSIVE membership_path(member_oid, role_oid) AS (\n"
            "            SELECT membership.member, membership.roleid\n"
            "            FROM pg_catalog.pg_auth_members AS membership\n"
            "            UNION\n"
            "            SELECT path.member_oid, membership.roleid\n"
            "            FROM membership_path AS path\n"
            "            JOIN pg_catalog.pg_auth_members AS membership\n"
            "              ON membership.member = path.role_oid\n"
            "        )\n"
            "        SELECT 1\n"
            "        FROM membership_path AS path\n"
            "        WHERE path.member_oid IN (\n"
            "            SELECT role.oid\n"
            "            FROM pg_catalog.pg_roles AS role\n"
            "            WHERE role.rolname IN ("
            + _quote_literal(application.value)
            + ", "
            + _quote_literal(dashboard.value)
            + ")\n"
            "        )\n"
            "          AND path.role_oid = (\n"
            "              SELECT role.oid\n"
            "              FROM pg_catalog.pg_roles AS role\n"
            "              WHERE role.rolname = " + _quote_literal(owner.value) + "\n          )\n"
            "    ) THEN\n"
            "        RAISE EXCEPTION 'dbctl 拒绝 app/dashboard 到 owner 的间接成员路径';\n"
            "    END IF;\n"
            "END\n"
            "$dbctl$;"
        ),
    )


def _database_access_statements(
    database_name: DatabaseName,
    roles: RoleSet,
) -> list[SqlStatement]:
    """@brief 构造数据库级最小权限语句 / Build database-level least-privilege statements.

    @param database_name 项目数据库名 / Project database name.
    @param roles DatabaseBlueprint 中的 RoleSet / RoleSet from DatabaseBlueprint.
    @return 有序数据库级权限 SQL / Ordered database-level privilege SQL.
    """
    database = _quote_identifier(database_name.value)
    owner = _quote_identifier(roles.owner.value)
    migrator = _quote_identifier(roles.migrator.value)
    application = _quote_identifier(roles.application.value)
    dashboard = _quote_identifier(roles.dashboard.value)
    login_roles = f"{migrator}, {application}, {dashboard}"
    return [
        SqlStatement(
            label="收敛项目数据库 owner",
            sql=f"ALTER DATABASE {database} OWNER TO {owner};",
        ),
        SqlStatement(
            label="移除 PUBLIC 的项目数据库默认权限",
            sql=f"REVOKE ALL ON DATABASE {database} FROM PUBLIC;",
        ),
        SqlStatement(
            label="移除登录角色的直接数据库建模权限",
            sql=f"REVOKE CREATE, TEMPORARY ON DATABASE {database} FROM {login_roles};",
        ),
        SqlStatement(
            label="授予 migrator 数据库连接权限",
            sql=f"GRANT CONNECT ON DATABASE {database} TO {migrator};",
        ),
        SqlStatement(
            label="授予 app 数据库连接权限",
            sql=f"GRANT CONNECT ON DATABASE {database} TO {application};",
        ),
        SqlStatement(
            label="授予 dashboard 数据库连接权限",
            sql=f"GRANT CONNECT ON DATABASE {database} TO {dashboard};",
        ),
    ]


def _schema_privilege_statements(settings: DbctlSettings) -> list[SqlStatement]:
    """@brief 构造固定 schema 目录的最小权限 SQL / Build least-privilege SQL for canonical schemas.

    @param settings 已验证领域设置 / Validated domain settings.
    @return 按 schema 顺序排列的 SQL / SQL ordered by schema catalog.
    """
    roles = settings.blueprint.roles
    owner = _quote_identifier(roles.owner.value)
    migrator = _quote_identifier(roles.migrator.value)
    application = _quote_identifier(roles.application.value)
    dashboard = _quote_identifier(roles.dashboard.value)
    login_roles = f"{migrator}, {application}, {dashboard}"
    statements = [
        SqlStatement(label="确保 pgvector 扩展存在", sql="CREATE EXTENSION IF NOT EXISTS vector;"),
        SqlStatement(
            label="移除 PUBLIC 在 public schema 的建表权限",
            sql="REVOKE CREATE ON SCHEMA public FROM PUBLIC;",
        ),
        SqlStatement(
            label="移除登录角色在 public schema 的直接权限",
            sql=f"REVOKE ALL ON SCHEMA public FROM {login_roles};",
        ),
    ]
    for schema_name in settings.blueprint.schemas:
        schema = _quote_identifier(schema_name.value)
        statements.extend(
            _schema_baseline_statements(
                schema_name=schema_name,
                schema_identifier=schema,
                owner_identifier=owner,
                login_role_identifiers=login_roles,
            )
        )
        if schema_name == settings.blueprint.observability_schema:
            statements.extend(
                (
                    SqlStatement(
                        label="授予 app 使用 observability schema 的权限",
                        sql=f"GRANT USAGE ON SCHEMA {schema} TO {application};",
                    ),
                    SqlStatement(
                        label="授予 dashboard 使用 observability schema 的权限",
                        sql=f"GRANT USAGE ON SCHEMA {schema} TO {dashboard};",
                    ),
                    _conditional_relation_grant(
                        schema_name=schema_name,
                        relation_name="telemetry_records",
                        relation_kinds=("r", "p"),
                        privileges="INSERT",
                        role_identifier=application,
                        label="授予 app 写入现有 telemetry 表的最小权限",
                    ),
                    _conditional_relation_grant(
                        schema_name=schema_name,
                        relation_name="dashboard_signals",
                        relation_kinds=("v", "m"),
                        privileges="SELECT",
                        role_identifier=dashboard,
                        label="授予 dashboard 读取稳定 observability 视图的权限",
                    ),
                )
            )
            continue
        statements.extend(
            (
                SqlStatement(
                    label=f"授予 app 使用 schema {schema_name.value} 的权限",
                    sql=f"GRANT USAGE ON SCHEMA {schema} TO {application};",
                ),
                SqlStatement(
                    label=f"授予 app 对 schema {schema_name.value} 表的必要 DML 权限",
                    sql=(
                        f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} "
                        f"TO {application};"
                    ),
                ),
                SqlStatement(
                    label=f"授予 app 对 schema {schema_name.value} 序列的必要权限",
                    sql=(
                        f"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} "
                        f"TO {application};"
                    ),
                ),
                SqlStatement(
                    label=f"授予 app 对 schema {schema_name.value} 未来表的必要 DML 权限",
                    sql=(
                        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {application};"
                    ),
                ),
                SqlStatement(
                    label=f"授予 app 对 schema {schema_name.value} 未来序列的必要权限",
                    sql=(
                        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                        f"GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {application};"
                    ),
                ),
            )
        )
    return statements


def _schema_baseline_statements(
    *,
    schema_name: SchemaName,
    schema_identifier: str,
    owner_identifier: str,
    login_role_identifiers: str,
) -> tuple[SqlStatement, ...]:
    """@brief 构造单个 schema 的拒绝优先基线 / Build a deny-first baseline for one schema.

    @param schema_name 已验证 schema 名 / Validated schema name.
    @param schema_identifier 已引用 schema 标识符 / Quoted schema identifier.
    @param owner_identifier 已引用 owner 标识符 / Quoted owner identifier.
    @param login_role_identifiers 已引用的登录角色列表 / Quoted login-role list.
    @return 当前与未来对象的权限撤销 SQL / Revocation SQL for current and future objects.
    """
    label_name = schema_name.value
    schema = schema_identifier
    owner = owner_identifier
    login_roles = login_role_identifiers
    return (
        SqlStatement(
            label=f"确保 schema {label_name} 存在",
            sql=f"CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION {owner};",
        ),
        SqlStatement(
            label=f"收敛 schema {label_name} 的所有者",
            sql=f"ALTER SCHEMA {schema} OWNER TO {owner};",
        ),
        SqlStatement(
            label=f"移除 PUBLIC 在 schema {label_name} 的权限",
            sql=f"REVOKE ALL ON SCHEMA {schema} FROM PUBLIC;",
        ),
        SqlStatement(
            label=f"移除登录角色在 schema {label_name} 的直接权限",
            sql=f"REVOKE ALL ON SCHEMA {schema} FROM {login_roles};",
        ),
        SqlStatement(
            label=f"移除 PUBLIC 对 schema {label_name} 现有表的权限",
            sql=f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC;",
        ),
        SqlStatement(
            label=f"移除登录角色对 schema {label_name} 现有表的直接权限",
            sql=f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {login_roles};",
        ),
        SqlStatement(
            label=f"移除 PUBLIC 对 schema {label_name} 现有序列的权限",
            sql=f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM PUBLIC;",
        ),
        SqlStatement(
            label=f"移除登录角色对 schema {label_name} 现有序列的直接权限",
            sql=f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM {login_roles};",
        ),
        SqlStatement(
            label=f"收紧 schema {label_name} 的未来表 PUBLIC 默认权限",
            sql=(
                f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                "REVOKE ALL ON TABLES FROM PUBLIC;"
            ),
        ),
        SqlStatement(
            label=f"收紧 schema {label_name} 的未来序列 PUBLIC 默认权限",
            sql=(
                f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                "REVOKE ALL ON SEQUENCES FROM PUBLIC;"
            ),
        ),
        SqlStatement(
            label=f"移除 schema {label_name} 的未来表登录角色默认权限",
            sql=(
                f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                f"REVOKE ALL ON TABLES FROM {login_roles};"
            ),
        ),
        SqlStatement(
            label=f"移除 schema {label_name} 的未来序列登录角色默认权限",
            sql=(
                f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                f"REVOKE ALL ON SEQUENCES FROM {login_roles};"
            ),
        ),
    )


def _conditional_relation_grant(
    *,
    schema_name: SchemaName,
    relation_name: str,
    relation_kinds: tuple[str, ...],
    privileges: str,
    role_identifier: str,
    label: str,
) -> SqlStatement:
    """@brief 构造 relation 存在时的最小授权 / Build a least-privilege grant if a relation exists.

    @param schema_name 已验证 schema 名 / Validated schema name.
    @param relation_name 内部固定 relation 名 / Internally fixed relation name.
    @param relation_kinds PostgreSQL relkind 白名单 / PostgreSQL relkind allow-list.
    @param privileges 内部固定授权文本 / Internally fixed privilege text.
    @param role_identifier 已引用 role 标识符 / Quoted role identifier.
    @param label 非敏感 SQL 标签 / Non-sensitive SQL label.
    @return 条件授权 DO 语句 / Conditional-grant DO statement.
    """
    schema = _quote_identifier(schema_name.value)
    relation = _quote_identifier(relation_name)
    allowed_kinds = ", ".join(_quote_literal(kind) for kind in relation_kinds)
    return SqlStatement(
        label=label,
        sql=(
            "DO $dbctl$\n"
            "BEGIN\n"
            "    IF EXISTS (\n"
            "        SELECT 1\n"
            "        FROM pg_catalog.pg_class AS relation\n"
            "        JOIN pg_catalog.pg_namespace AS namespace\n"
            "          ON namespace.oid = relation.relnamespace\n"
            "        WHERE namespace.nspname = "
            + _quote_literal(schema_name.value)
            + "\n          AND relation.relname = "
            + _quote_literal(relation_name)
            + "\n          AND relation.relkind IN ("
            + allowed_kinds
            + ")\n"
            "    ) THEN\n"
            f"        GRANT {privileges} ON TABLE {schema}.{relation} TO {role_identifier};\n"
            "    END IF;\n"
            "END\n"
            "$dbctl$;"
        ),
    )


def _conditional_alembic_revoke(application_login: AppLogin) -> SqlStatement:
    """@brief 构造每次 bootstrap 末尾的 Alembic 权限撤销 / Build final Alembic revoke.

    @param application_login 强类型 app 登录 / Purpose-typed application login.
    @return identity.alembic_version 存在时撤销 app 全部表权限的语句。
    / Statement revoking every app table privilege when identity.alembic_version exists.
    """
    schema_name = IDENTITY_SCHEMA.value
    relation_name: Final[str] = "alembic_version"
    schema = _quote_identifier(schema_name)
    relation = _quote_identifier(relation_name)
    application = _quote_identifier(application_login.role_name.value)
    return SqlStatement(
        label="若存在则撤销 app 对 identity.alembic_version 的直接权限",
        sql=(
            "DO $dbctl$\n"
            "BEGIN\n"
            "    IF EXISTS (\n"
            "        SELECT 1\n"
            "        FROM pg_catalog.pg_class AS relation\n"
            "        JOIN pg_catalog.pg_namespace AS namespace\n"
            "          ON namespace.oid = relation.relnamespace\n"
            "        WHERE namespace.nspname = "
            + _quote_literal(schema_name)
            + "\n          AND relation.relname = "
            + _quote_literal(relation_name)
            + "\n          AND relation.relkind IN ('r', 'p')\n"
            "    ) THEN\n"
            f"        REVOKE ALL ON TABLE {schema}.{relation} FROM {application};\n"
            "    END IF;\n"
            "END\n"
            "$dbctl$;"
        ),
    )
