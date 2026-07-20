"""@brief PostgreSQL bootstrap 计划与执行抽象 / PostgreSQL bootstrap planning and execution abstractions."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, runtime_checkable

from .config import DatabaseAdministrationSettings, DatabaseRole
from .errors import DatabaseAlreadyExistsError, DbctlConfigurationError
from .identifiers import quote_postgres_identifier, quote_postgres_literal


class ExecutionTarget(StrEnum):
    """@brief bootstrap SQL 的连接目标 / Connection target for bootstrap SQL."""

    MAINTENANCE = "maintenance"
    DATABASE = "database"


@dataclass(frozen=True, slots=True)
class SqlStatement:
    """@brief 带脱敏参数的 SQL 语句 / SQL statement with redacted parameters.

    @param label 面向运维者的非敏感操作名称 / Non-sensitive operator-facing operation name.
    @param sql 使用 ``%s`` 占位符的 SQL；仅允许文本字面量参数。
    / SQL using ``%s`` placeholders; only text-literal parameters are allowed.
    @param parameters 仅在内存中保存的参数；``repr`` 故意隐藏它们。
    / Parameters retained only in memory; ``repr`` deliberately hides them.
    @param sensitive_parameter_indices 必须在 dry-run 中脱敏的参数下标。
    / Parameter indexes that must be redacted in dry-run output.
    """

    label: str
    sql: str
    parameters: tuple[str, ...] = field(default_factory=tuple, repr=False)
    sensitive_parameter_indices: frozenset[int] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        """@brief 校验语句参数布局 / Validate statement parameter layout.

        @return 无返回值 / No return value.
        @raise DbctlConfigurationError 语句标签、SQL 或参数布局无效时抛出。
        / Raised when label, SQL, or parameter layout is invalid.
        """
        if not isinstance(self.label, str) or not self.label.strip():
            raise DbctlConfigurationError("bootstrap SQL 语句必须有非空标签。")
        if not isinstance(self.sql, str) or not self.sql.strip():
            raise DbctlConfigurationError("bootstrap SQL 语句不能为空。")
        normalized_parameters = tuple(self.parameters)
        if any(not isinstance(parameter, str) for parameter in normalized_parameters):
            raise DbctlConfigurationError("bootstrap SQL 参数必须是字符串。")
        if self.sql.count("%s") != len(normalized_parameters):
            raise DbctlConfigurationError("bootstrap SQL 占位符与参数数量不一致。")
        if any(
            index < 0 or index >= len(normalized_parameters)
            for index in self.sensitive_parameter_indices
        ):
            raise DbctlConfigurationError("bootstrap SQL 的敏感参数下标无效。")
        object.__setattr__(self, "label", self.label.strip())
        object.__setattr__(self, "parameters", normalized_parameters)

    def render_for_display(self) -> str:
        """@brief 渲染安全的 dry-run SQL / Render safe dry-run SQL.

        @return 不含参数原文的 SQL 文本 / SQL text containing no parameter values.
        @note 即使参数当前不是 secret，也不会在计划预览中显示，防止未来调用方误用。
        / Even non-secret parameters are not shown in plan previews, preventing future misuse.
        """
        return self._render(lambda _: "<redacted>")

    def render_for_psql(self) -> str:
        """@brief 渲染供本地 psql stdin 执行的 SQL / Render SQL for local psql stdin execution.

        @return 已正确引用文本参数的 SQL / SQL with correctly quoted text parameters.
        @note 返回值可能含密码，只能写入受控子进程 stdin，不能写入日志、异常或命令行。
        / The return value may contain a password; it may only be written to controlled child-process
        stdin, never logs, exceptions, or command arguments.
        """
        return self._render(quote_postgres_literal)

    def render_with_literals(self, literal_renderer: Callable[[str], str]) -> str:
        """@brief 用调用方提供的安全字面量策略渲染 SQL / Render SQL with caller-provided safe literals.

        @param literal_renderer 将一个文本参数转换为 SQL 字面量的函数。
        / Function converting one text parameter into an SQL literal.
        @return 已替换所有 ``%s`` 占位符的 SQL / SQL with every ``%s`` placeholder replaced.
        @raise DbctlConfigurationError 字面量渲染器返回非文本时抛出。
        / Raised when the literal renderer returns non-text.
        """
        def render(parameter: str) -> str:
            literal = literal_renderer(parameter)
            if not isinstance(literal, str):
                raise DbctlConfigurationError("SQL 字面量渲染器必须返回字符串。")
            return literal

        return self._render(render)

    def _render(self, parameter_renderer: Callable[[str], str]) -> str:
        """@brief 内部替换 SQL 文本参数 / Internally substitute SQL text parameters.

        @param parameter_renderer 单参数渲染函数 / Per-parameter renderer.
        @return 替换后的 SQL / Substituted SQL.
        """
        pieces = self.sql.split("%s")
        if len(pieces) == 1:
            return self.sql
        rendered: list[str] = [pieces[0]]
        for index, parameter in enumerate(self.parameters):
            rendered.append(parameter_renderer(parameter))
            rendered.append(pieces[index + 1])
        return "".join(rendered)


@dataclass(frozen=True, slots=True)
class BootstrapPlan:
    """@brief 可审阅、可干跑的 PostgreSQL bootstrap 计划 / Reviewable, dry-runnable PostgreSQL bootstrap plan.

    @param database_name 目标数据库名 / Target database name.
    @param pre_database_statements 创建数据库前在 maintenance database 上执行的语句。
    / Statements executed on the maintenance database before database creation.
    @param create_database 数据库不存在时执行的特殊语句 / Special statement run only if the database is absent.
    @param maintenance_statements 在管理员 maintenance database 上执行的语句。
    / Statements executed on the administrator maintenance database.
    @param database_statements 在目标项目数据库上执行的语句。
    / Statements executed on the target project database.
    """

    database_name: str
    pre_database_statements: tuple[SqlStatement, ...]
    create_database: SqlStatement
    maintenance_statements: tuple[SqlStatement, ...]
    database_statements: tuple[SqlStatement, ...]

    def iter_statements(self) -> Iterable[tuple[ExecutionTarget, SqlStatement]]:
        """@brief 按实际执行顺序迭代计划语句 / Iterate planned statements in execution order.

        @return ``(target, statement)`` 迭代器；创建数据库语句由执行器条件处理。
        / Iterator of ``(target, statement)``; database creation is conditionally handled by executor.
        """
        yield from (
            (ExecutionTarget.MAINTENANCE, statement) for statement in self.pre_database_statements
        )
        yield from ((ExecutionTarget.MAINTENANCE, statement) for statement in self.maintenance_statements)
        yield from ((ExecutionTarget.DATABASE, statement) for statement in self.database_statements)

    def render_dry_run(self) -> str:
        """@brief 渲染不含 secret 的 dry-run 计划 / Render a secret-free dry-run plan.

        @return 可直接输出到终端的计划文本 / Plan text safe to print to a terminal.
        @note 本计划不改 ``pg_hba.conf``，也不创建 PostgreSQL superuser。
        / This plan does not modify ``pg_hba.conf`` and does not create a PostgreSQL superuser.
        """
        lines = [
            "-- dbctl bootstrap dry-run（不执行任何 SQL）",
            "-- 不修改 pg_hba.conf；不创建 PostgreSQL superuser。",
        ]
        for statement in self.pre_database_statements:
            lines.extend((f"-- [maintenance] {statement.label}", statement.render_for_display()))
        lines.extend(
            (
                "-- [maintenance/conditional] " + self.create_database.label,
                self.create_database.render_for_display(),
            )
        )
        for statement in self.maintenance_statements:
            lines.extend((f"-- [maintenance] {statement.label}", statement.render_for_display()))
        for statement in self.database_statements:
            lines.extend((f"-- [database] {statement.label}", statement.render_for_display()))
        return "\n".join(lines)


class BootstrapPlanBuilder:
    """@brief 从 dbctl 设置构造最小权限 bootstrap 计划 / Build least-privilege bootstrap plans from dbctl settings.

    计划把数据库 owner 与登录身份分开：migrator 是 owner 的成员但 ``NOINHERIT``，
    因而必须显式 ``SET ROLE``；app 没有 schema ``CREATE`` 权限；Dashboard 只读
    observability schema。
    / The plan separates database ownership from login identities: migrator is a member of owner
    but ``NOINHERIT`` and must explicitly ``SET ROLE``; app has no schema ``CREATE`` privilege;
    Dashboard is read-only on the observability schema.
    """

    def build(
        self,
        administration: DatabaseAdministrationSettings,
        *,
        role_passwords: Mapping[DatabaseRole, str] | None = None,
    ) -> BootstrapPlan:
        """@brief 生成安全、幂等的 PostgreSQL bootstrap 计划 / Build a safe, idempotent PostgreSQL bootstrap plan.

        @param administration 已验证的非敏感管理设置 / Validated non-secret administration settings.
        @param role_passwords 可选登录角色密码；只保存在参数化 SqlStatement 内存字段中。
        / Optional login-role passwords, retained only in parameterized SqlStatement memory fields.
        @return 尚未执行的 BootstrapPlan / BootstrapPlan that has not been executed.
        @raise DbctlConfigurationError 密码映射试图设置 owner 或包含非法值时抛出。
        / Raised when password mapping tries to set owner or contains an invalid value.
        """
        passwords = _normalize_role_passwords(role_passwords)
        owner = quote_postgres_identifier(administration.owner_role, kind="owner role")
        migrator = quote_postgres_identifier(administration.migrator_role, kind="migrator role")
        app = quote_postgres_identifier(administration.app_role, kind="app role")
        dashboard = quote_postgres_identifier(administration.dashboard_role, kind="dashboard role")
        database = quote_postgres_identifier(administration.database_name, kind="数据库名")

        pre_database_statements: list[SqlStatement] = []
        role_specs = (
            (DatabaseRole.OWNER, administration.owner_role, "NOLOGIN NOINHERIT"),
            (DatabaseRole.MIGRATOR, administration.migrator_role, "LOGIN NOINHERIT"),
            (DatabaseRole.APP, administration.app_role, "LOGIN NOINHERIT"),
            (DatabaseRole.DASHBOARD, administration.dashboard_role, "LOGIN NOINHERIT"),
        )
        for role, role_name, login_options in role_specs:
            role_identifier = quote_postgres_identifier(role_name, kind=f"{role.value} role")
            attributes = (
                f"{login_options} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
            )
            pre_database_statements.append(
                _ensure_role_statement(role_name, role_identifier, attributes)
            )
            pre_database_statements.append(
                SqlStatement(
                    label=f"收敛 {role.value} role 的非特权属性",
                    sql=f"ALTER ROLE {role_identifier} {attributes};",
                )
            )
            password = passwords.get(role)
            if password is not None:
                pre_database_statements.append(
                    SqlStatement(
                        label=f"设置 {role.value} role 密码（已脱敏）",
                        sql=f"ALTER ROLE {role_identifier} PASSWORD %s;",
                        parameters=(password,),
                        sensitive_parameter_indices=frozenset({0}),
                    )
                )

        pre_database_statements.append(
            SqlStatement(
                label="允许 migrator 显式 SET ROLE 为 owner",
                sql=f"GRANT {owner} TO {migrator};",
            )
        )
        maintenance_statements = [
            SqlStatement(
                label="将项目数据库 owner 收敛为 workspace_owner",
                sql=f"ALTER DATABASE {database} OWNER TO {owner};",
            ),
            SqlStatement(
                label="移除 PUBLIC 的项目数据库默认权限",
                sql=f"REVOKE ALL ON DATABASE {database} FROM PUBLIC;",
            ),
            SqlStatement(
                label="移除登录角色的直接数据库建模权限",
                sql=(
                    f"REVOKE CREATE, TEMPORARY ON DATABASE {database} "
                    f"FROM {migrator}, {app}, {dashboard};"
                ),
            ),
            SqlStatement(
                label="授予 migrator 数据库连接权限",
                sql=f"GRANT CONNECT ON DATABASE {database} TO {migrator};",
            ),
            SqlStatement(
                label="授予 app 数据库连接权限",
                sql=f"GRANT CONNECT ON DATABASE {database} TO {app};",
            ),
            SqlStatement(
                label="授予 dashboard 数据库连接权限",
                sql=f"GRANT CONNECT ON DATABASE {database} TO {dashboard};",
            ),
        ]

        database_statements = _database_privilege_statements(administration)
        return BootstrapPlan(
            database_name=administration.database_name,
            pre_database_statements=tuple(pre_database_statements),
            create_database=SqlStatement(
                label="数据库不存在时创建项目数据库",
                sql=f"CREATE DATABASE {database} OWNER {owner};",
            ),
            maintenance_statements=tuple(maintenance_statements),
            database_statements=tuple(database_statements),
        )


@runtime_checkable
class BootstrapRunner(Protocol):
    """@brief bootstrap 执行端口 / Bootstrap execution port.

    测试可实现此协议记录 SQL，而生产环境使用跨平台本地 ``psql`` runner；计划本身
    不依赖任何 subprocess、网络客户端或框架。
    / Tests can implement this protocol to record SQL, while production uses the cross-platform
    local ``psql`` runner; the plan itself depends on no subprocess, network client, or framework.
    """

    def database_exists(self, database_name: str) -> bool:
        """@brief 判断目标数据库是否存在 / Determine whether the target database exists.

        @param database_name 已验证的数据库名 / Validated database name.
        @return 数据库存在时为 ``True`` / ``True`` when the database exists.
        """
        ...

    def execute(self, target: ExecutionTarget, statement: SqlStatement) -> None:
        """@brief 在指定目标执行一条 SQL / Execute one SQL statement at a specified target.

        @param target maintenance 或目标项目数据库 / Maintenance or target project database.
        @param statement 待执行的参数化安全语句 / Parameterized safe statement to execute.
        @return 无返回值 / No return value.
        """
        ...


@dataclass(frozen=True, slots=True)
class BootstrapExecutionResult:
    """@brief bootstrap 执行结果摘要 / Bootstrap execution-result summary.

    @param database_created 本次是否创建了目标数据库 / Whether this run created the target database.
    @param executed_statement_count 本次实际发送的计划 SQL 数量。
    / Number of planned SQL statements actually sent this run.
    """

    database_created: bool
    executed_statement_count: int


class BootstrapExecutor:
    """@brief 按固定顺序执行 BootstrapPlan / Execute a BootstrapPlan in a fixed order."""

    def __init__(self, runner: BootstrapRunner) -> None:
        """@brief 初始化执行器 / Initialize the executor.

        @param runner 实现 BootstrapRunner 协议的受控 I/O 适配器。
        / Controlled I/O adapter implementing the BootstrapRunner protocol.
        """
        self._runner = runner

    def apply(self, plan: BootstrapPlan) -> BootstrapExecutionResult:
        """@brief 执行幂等 bootstrap 计划 / Execute an idempotent bootstrap plan.

        @param plan 已审阅或新生成的 bootstrap 计划 / Reviewed or newly generated bootstrap plan.
        @return 不含敏感数据的执行结果 / Execution result without sensitive data.
        @raise DatabaseAlreadyExistsError 不会向调用方暴露；并发创建被视为幂等成功。
        / A concurrent create is treated as idempotent success rather than exposed to caller.
        """
        executed_statement_count = 0
        for statement in plan.pre_database_statements:
            self._runner.execute(ExecutionTarget.MAINTENANCE, statement)
            executed_statement_count += 1

        database_created = False
        if not self._runner.database_exists(plan.database_name):
            try:
                self._runner.execute(ExecutionTarget.MAINTENANCE, plan.create_database)
                database_created = True
                executed_statement_count += 1
            except DatabaseAlreadyExistsError:
                database_created = False

        for statement in plan.maintenance_statements:
            self._runner.execute(ExecutionTarget.MAINTENANCE, statement)
            executed_statement_count += 1

        for statement in plan.database_statements:
            self._runner.execute(ExecutionTarget.DATABASE, statement)
            executed_statement_count += 1
        return BootstrapExecutionResult(
            database_created=database_created,
            executed_statement_count=executed_statement_count,
        )


def _normalize_role_passwords(
    role_passwords: Mapping[DatabaseRole, str] | None,
) -> dict[DatabaseRole, str]:
    """@brief 校验仅内存角色密码映射 / Validate in-memory role-password mapping.

    @param role_passwords 候选角色密码映射 / Candidate role-password mapping.
    @return 已规范化的可登录角色密码映射 / Normalized login-role password mapping.
    @raise DbctlConfigurationError owner 密码、空值或 NUL 值出现时抛出。
    / Raised for owner passwords, empty values, or values containing NUL.
    """
    if role_passwords is None:
        return {}
    result: dict[DatabaseRole, str] = {}
    for raw_role, password in role_passwords.items():
        try:
            role = DatabaseRole(raw_role)
        except ValueError as error:
            raise DbctlConfigurationError("bootstrap 密码映射包含未知角色。") from error
        if role is DatabaseRole.OWNER:
            raise DbctlConfigurationError("NOLOGIN owner role 不能设置密码。")
        if not isinstance(password, str) or not password or "\x00" in password:
            raise DbctlConfigurationError("登录 role 密码必须是非空且不含 NUL 的字符串。")
        result[role] = password
    return result


def _ensure_role_statement(role_name: str, role_identifier: str, attributes: str) -> SqlStatement:
    """@brief 构造条件创建角色语句 / Construct a conditionally-create-role statement.

    @param role_name 原始已校验角色名 / Raw validated role name.
    @param role_identifier 安全引用的 role SQL 标识符 / Safely quoted role SQL identifier.
    @param attributes ``CREATE ROLE`` 所需的固定属性 / Fixed attributes for ``CREATE ROLE``.
    @return 可重复执行的角色创建 SqlStatement / Repeatable role-creation SqlStatement.
    """
    return SqlStatement(
        label=f"确保 {role_name} role 存在",
        sql=(
            "DO $dbctl$\n"
            "BEGIN\n"
            "    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = "
            + quote_postgres_literal(role_name)
            + ") THEN\n"
            f"        CREATE ROLE {role_identifier} {attributes};\n"
            "    END IF;\n"
            "EXCEPTION\n"
            "    WHEN duplicate_object THEN\n"
            "        NULL;\n"
            "END\n"
            "$dbctl$;"
        ),
    )


def _database_privilege_statements(
    administration: DatabaseAdministrationSettings,
) -> list[SqlStatement]:
    """@brief 构造目标数据库的 schema 与权限语句 / Build target database schema and privilege statements.

    @param administration 已验证的管理设置 / Validated administration settings.
    @return 依执行顺序排列的目标数据库 SQL / Target-database SQL in execution order.
    """
    owner = quote_postgres_identifier(administration.owner_role, kind="owner role")
    migrator = quote_postgres_identifier(administration.migrator_role, kind="migrator role")
    app = quote_postgres_identifier(administration.app_role, kind="app role")
    dashboard = quote_postgres_identifier(administration.dashboard_role, kind="dashboard role")
    login_roles = f"{migrator}, {app}, {dashboard}"
    statements = [
        SqlStatement(
            label="确保 pgvector 扩展存在",
            sql="CREATE EXTENSION IF NOT EXISTS vector;",
        ),
        SqlStatement(
            label="移除 PUBLIC 在 public schema 的建表权限",
            sql="REVOKE CREATE ON SCHEMA public FROM PUBLIC;",
        ),
        SqlStatement(
            label="移除登录角色在 public schema 的直接权限",
            sql=f"REVOKE ALL ON SCHEMA public FROM {login_roles};",
        ),
    ]

    for schema_name in administration.schemas:
        schema = quote_postgres_identifier(schema_name, kind="schema 名")
        statements.extend(
            (
                SqlStatement(
                    label=f"确保 schema {schema_name} 存在",
                    sql=f"CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION {owner};",
                ),
                SqlStatement(
                    label=f"收敛 schema {schema_name} 的所有者",
                    sql=f"ALTER SCHEMA {schema} OWNER TO {owner};",
                ),
                SqlStatement(
                    label=f"移除 PUBLIC 在 schema {schema_name} 的权限",
                    sql=f"REVOKE ALL ON SCHEMA {schema} FROM PUBLIC;",
                ),
                SqlStatement(
                    label=f"移除登录角色在 schema {schema_name} 的直接权限",
                    sql=f"REVOKE ALL ON SCHEMA {schema} FROM {login_roles};",
                ),
                SqlStatement(
                    label=f"移除 PUBLIC 对 schema {schema_name} 现有表的权限",
                    sql=f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC;",
                ),
                SqlStatement(
                    label=f"移除登录角色对 schema {schema_name} 现有表的直接权限",
                    sql=f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {login_roles};",
                ),
                SqlStatement(
                    label=f"移除 PUBLIC 对 schema {schema_name} 现有序列的权限",
                    sql=f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM PUBLIC;",
                ),
                SqlStatement(
                    label=f"移除登录角色对 schema {schema_name} 现有序列的直接权限",
                    sql=f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM {login_roles};",
                ),
                SqlStatement(
                    label=f"收紧 schema {schema_name} 的未来表 PUBLIC 默认权限",
                    sql=(
                        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                        "REVOKE ALL ON TABLES FROM PUBLIC;"
                    ),
                ),
                SqlStatement(
                    label=f"收紧 schema {schema_name} 的未来序列 PUBLIC 默认权限",
                    sql=(
                        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                        "REVOKE ALL ON SEQUENCES FROM PUBLIC;"
                    ),
                ),
                SqlStatement(
                    label=f"移除 schema {schema_name} 的未来表登录角色默认权限",
                    sql=(
                        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                        f"REVOKE ALL ON TABLES FROM {login_roles};"
                    ),
                ),
                SqlStatement(
                    label=f"移除 schema {schema_name} 的未来序列登录角色默认权限",
                    sql=(
                        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                        f"REVOKE ALL ON SEQUENCES FROM {login_roles};"
                    ),
                ),
            )
        )
        if schema_name == administration.observability_schema:
            statements.extend(
                (
                    SqlStatement(
                        label="授予 app 使用 observability schema 的权限",
                        sql=f"GRANT USAGE ON SCHEMA {schema} TO {app};",
                    ),
                    SqlStatement(
                        label="授予 dashboard 使用 observability schema 的权限",
                        sql=f"GRANT USAGE ON SCHEMA {schema} TO {dashboard};",
                    ),
                    SqlStatement(
                        label="授予 app 写入未来 observability 表的最小权限",
                        sql=(
                            f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                            f"GRANT INSERT ON TABLES TO {app};"
                        ),
                    ),
                    _conditional_relation_grant_statement(
                        schema_name=schema_name,
                        relation_name="telemetry_records",
                        relation_kinds=("r", "p"),
                        privileges="INSERT",
                        role_identifier=app,
                        label="授予 app 写入现有 telemetry 表的最小权限",
                    ),
                    _conditional_relation_grant_statement(
                        schema_name=schema_name,
                        relation_name="dashboard_metric_samples",
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
                    label=f"授予 app 使用 schema {schema_name} 的权限",
                    sql=f"GRANT USAGE ON SCHEMA {schema} TO {app};",
                ),
                SqlStatement(
                    label=f"授予 app 对 schema {schema_name} 表的必要 DML 权限",
                    sql=(
                        f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} "
                        f"TO {app};"
                    ),
                ),
                SqlStatement(
                    label=f"授予 app 对 schema {schema_name} 序列的必要权限",
                    sql=(
                        f"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} "
                        f"TO {app};"
                    ),
                ),
                SqlStatement(
                    label=f"授予 app 对 schema {schema_name} 未来表的必要 DML 权限",
                    sql=(
                        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {app};"
                    ),
                ),
                SqlStatement(
                    label=f"授予 app 对 schema {schema_name} 未来序列的必要权限",
                    sql=(
                        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} "
                        f"GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {app};"
                    ),
                ),
            )
        )
    return statements


def _conditional_relation_grant_statement(
    *,
    schema_name: str,
    relation_name: str,
    relation_kinds: tuple[str, ...],
    privileges: str,
    role_identifier: str,
    label: str,
) -> SqlStatement:
    """@brief 构造仅在 relation 已存在时授予权限的语句 / Construct a grant executed only when a relation exists.

    @param schema_name 已验证 schema 名 / Validated schema name.
    @param relation_name 已验证 relation 名 / Validated relation name.
    @param relation_kinds PostgreSQL ``relkind`` 白名单 / PostgreSQL ``relkind`` allow-list.
    @param privileges 固定、受控的 GRANT 权限文本 / Fixed controlled GRANT privilege text.
    @param role_identifier 已安全引用的 role 标识符 / Safely quoted role identifier.
    @param label 非敏感计划标签 / Non-sensitive plan label.
    @return 幂等、不会因 migration 尚未创建 relation 而失败的 SqlStatement。
    / Idempotent SqlStatement that does not fail when migration has not created relation yet.
    """
    schema = quote_postgres_identifier(schema_name, kind="schema 名")
    relation = quote_postgres_identifier(relation_name, kind="relation 名")
    allowed_kinds = ", ".join(quote_postgres_literal(kind) for kind in relation_kinds)
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
            + quote_postgres_literal(schema_name)
            + "\n"
            "          AND relation.relname = "
            + quote_postgres_literal(relation_name)
            + "\n"
            "          AND relation.relkind IN ("
            + allowed_kinds
            + ")\n"
            "    ) THEN\n"
            f"        GRANT {privileges} ON TABLE {schema}.{relation} TO {role_identifier};\n"
            "    END IF;\n"
            "END\n"
            "$dbctl$;"
        ),
    )
