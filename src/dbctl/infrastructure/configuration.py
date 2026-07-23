"""@brief dbctl JSONC 配置存储与类型化装配 / dbctl JSONC configuration store and typed assembly."""

import json
import os
import secrets
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final, cast

import json5

from dbctl.application.errors import (
    DbctlConfigurationError,
    add_safe_diagnostic_note,
    safe_domain_error_message,
    safe_external_cause,
)
from dbctl.application.progress import (
    OperationName,
    ProgressSink,
    ProgressState,
    ProgressUpdate,
    publish_progress,
)
from dbctl.domain.database import (
    BootstrapAccess,
    ConnectionCatalog,
    DatabaseBlueprint,
    DatabaseTarget,
    DataRegion,
    DbctlSettings,
    WorkspacePlan,
)
from dbctl.domain.errors import DomainError
from dbctl.domain.names import DatabaseName, RoleName, SchemaName
from dbctl.domain.retention import RetentionPolicy
from dbctl.domain.roles import LoginRole, RoleSet, Secret
from dbctl.infrastructure.postgres.conninfo import build_postgres_dsn, parse_database_login
from dbctl.infrastructure.private_files import atomic_write_private_text
from dbctl.infrastructure.resources import read_default_text

_DEFAULT_CONFIG_PATH: Final = Path("config.jsonc")
"""@brief 默认私密配置路径 / Default private-configuration path."""

_DEFAULT_DBINIT_NAME: Final = "dbinit.jsonc"
"""@brief 默认数据库目标状态文件名 / Default database-target-state filename."""

_DEFAULT_EXAMPLE_NAME: Final = "example.jsonc"
"""@brief 默认公开配置模板名 / Default public configuration-template filename."""

_CANONICAL_SCHEMAS: Final = (
    "identity",
    "resume",
    "agent",
    "interview",
    "knowledge",
    "observability",
)
"""@brief 冻结 migration 实际使用的 schema catalog / Schema catalog frozen into migrations."""


class DbctlConfigStore:
    """@brief 初始化或只读加载 dbctl 的私密配置 / Initialize or read dbctl private configuration.

    只有 ``initialize`` 允许创建配置与生成登录 secret；``load`` 始终只读。公开
    ``dbinit.jsonc`` 定义唯一数据库目标，私密 ``config.jsonc`` 只携带运行凭证。
    / Only ``initialize`` may create configuration and generate login secrets; ``load`` is
    read-only. Public ``dbinit.jsonc`` defines the sole target, while private ``config.jsonc``
    carries runtime credentials.
    """

    def __init__(
        self,
        config_path: Path | str | None = None,
        dbinit_path: Path | str | None = None,
        *,
        progress: ProgressSink | None = None,
    ) -> None:
        """@brief 创建配置存储 / Create a configuration store.

        @param config_path 私密配置路径；None 使用当前目录默认值和包资源回退。
        / Private configuration path; None enables current-directory defaults and package fallback.
        @param dbinit_path 数据库目标状态路径；None 使用配置同目录默认值。
        / Database target-state path; None uses the default beside the private configuration.
        @param progress 可选同步进度输出端口 / Optional synchronous progress output port.
        """

        uses_default_config = config_path is None
        self._config_path = _DEFAULT_CONFIG_PATH if config_path is None else Path(config_path)
        self._dbinit_path = (
            self._config_path.parent / _DEFAULT_DBINIT_NAME
            if dbinit_path is None
            else Path(dbinit_path)
        )
        self._allow_packaged_dbinit_fallback = uses_default_config and dbinit_path is None
        self._progress = progress

    @property
    def config_path(self) -> Path:
        """@brief 返回调用方语义下的私密配置路径 / Return the caller-relative private-config path.

        @return 未强制解析为绝对路径的 Path / Path without forced absolute resolution.
        """

        return self._config_path

    @property
    def dbinit_path(self) -> Path:
        """@brief 返回数据库目标状态路径 / Return the database-target-state path.

        @return 调用方选择的 dbinit Path / Caller-selected dbinit Path.
        """

        return self._dbinit_path

    def load(self) -> DbctlSettings:
        """@brief 只读加载已经初始化的配置 / Read an already initialized configuration.

        @return 完整验证并强类型化的 dbctl 设置 / Fully validated and typed dbctl settings.
        @raise DbctlConfigurationError 文件、权限或任一不变量不合法时抛出。
        / Raised when files, permissions, or any invariant are invalid.
        """

        try:
            return self._load(initialize=False)
        except DomainError as error:
            message = safe_domain_error_message(error) or "数据库配置违反领域约束。"
            wrapped = DbctlConfigurationError(message)
            add_safe_diagnostic_note(wrapped, self._diagnostic_context(initialize=False))
            self._report_failure(initialize=False)
            raise wrapped from error
        except Exception as error:
            add_safe_diagnostic_note(error, self._diagnostic_context(initialize=False))
            self._report_failure(initialize=False)
            raise

    def initialize(self) -> DbctlSettings:
        """@brief 在内存完成配置后一次原子提交 / Complete configuration in memory and commit it atomically once.

        @return 完整验证并已持久化的 dbctl 设置 / Fully validated and persisted dbctl settings.
        @raise DbctlConfigurationError 模板、目标状态或安全写入失败时抛出。
        / Raised when the template, target state, or secure write fails.
        """

        try:
            return self._load(initialize=True)
        except DomainError as error:
            message = safe_domain_error_message(error) or "数据库配置违反领域约束。"
            wrapped = DbctlConfigurationError(message)
            add_safe_diagnostic_note(wrapped, self._diagnostic_context(initialize=True))
            self._report_failure(initialize=True)
            raise wrapped from error
        except Exception as error:
            add_safe_diagnostic_note(error, self._diagnostic_context(initialize=True))
            self._report_failure(initialize=True)
            raise

    def _load(self, *, initialize: bool) -> DbctlSettings:
        """@brief 按显式副作用权限加载配置 / Load configuration under explicit side-effect permission.

        @param initialize 是否允许生成 secret 与写入 / Whether secret generation and writes are allowed.
        @return 类型化设置 / Typed settings.
        """

        dbinit_source = (
            str(self._dbinit_path)
            if self._dbinit_path.is_file() or not self._allow_packaged_dbinit_fallback
            else "内置资源 dbinit.jsonc"
        )
        self._publish(
            ProgressUpdate(
                operation=OperationName.CONFIGURATION,
                state=ProgressState.STARTED,
                message="读取数据库目标声明",
                detail=f"来源={dbinit_source}",
            )
        )
        dbinit = self._load_dbinit_mapping()
        blueprint, access, target = _decode_database_declaration(dbinit)
        self._publish(
            ProgressUpdate(
                operation=OperationName.CONFIGURATION,
                state=ProgressState.SUCCEEDED,
                message="数据库目标声明已验证",
                detail=(
                    f"目标={target.host}:{target.port}/{target.database.value}；"
                    f"受管角色=owner/migrator/app/dashboard"
                ),
            )
        )
        config_was_missing = not self._config_path.is_file()
        self._publish(
            ProgressUpdate(
                operation=OperationName.CONFIGURATION,
                state=ProgressState.STARTED,
                message=("初始化私密运行配置" if initialize else "只读加载私密运行配置"),
                detail=f"路径={self._config_path}",
            )
        )
        if config_was_missing:
            if not initialize:
                raise DbctlConfigurationError(f"dbctl 配置文件不存在：{self._config_path}")
            root = self._load_initial_template()
        else:
            root = self._load_mapping_file(self._config_path, "dbctl 配置")
        if not initialize:
            self._validate_private_permissions()
        generated_credentials = _ensure_database_dsns(root, blueprint, target) if initialize else 0
        settings = _decode_private_settings(root, blueprint, access, target)
        if initialize:
            if config_was_missing or generated_credentials:
                self._write_private_root(root)
            else:
                self._tighten_private_permissions()
            if config_was_missing:
                message = "私密运行配置已创建"
            elif generated_credentials:
                message = "私密运行配置已补全"
            else:
                message = "私密运行配置已验证并收紧权限"
            detail = f"路径={self._config_path}；生成凭据={generated_credentials} 组；权限=0600"
        else:
            message = "私密运行配置已只读加载"
            detail = f"路径={self._config_path}；未写入文件"
        self._publish(
            ProgressUpdate(
                operation=OperationName.CONFIGURATION,
                state=ProgressState.SUCCEEDED,
                message=message,
                detail=detail,
            )
        )
        return settings

    def _diagnostic_context(self, *, initialize: bool) -> str:
        """@brief 构造不含 secret 的配置失败上下文 / Build secret-free configuration-failure context.

        @param initialize 是否允许配置初始化副作用 / Whether initialization side effects were authorized.
        @return 可附着到 traceback 的安全 note / Safe note suitable for a traceback.
        """

        mode = "初始化" if initialize else "只读加载"
        return f"dbctl 配置{mode}失败：config={self._config_path}；dbinit={self._dbinit_path}。"

    def _report_failure(self, *, initialize: bool) -> None:
        """@brief 发布不读取异常正文的配置失败事件 / Publish a failure without reading exception text.

        @param initialize 是否允许配置初始化副作用 / Whether initialization side effects were authorized.
        @return 无返回值 / No return value.
        """

        self._publish(
            ProgressUpdate(
                operation=OperationName.CONFIGURATION,
                state=ProgressState.FAILED,
                message=("私密运行配置初始化失败" if initialize else "私密运行配置只读加载失败"),
                detail=f"路径={self._config_path}；请查看下方安全 traceback",
            )
        )

    def _publish(self, update: ProgressUpdate) -> None:
        """@brief 向可选输出端口同步发布进度 / Publish progress synchronously to the optional output port.

        @param update 已验证且不含 secret 的进度 / Validated secret-free progress update.
        @return 无返回值 / No return value.
        """

        publish_progress(self._progress, update)

    def _load_dbinit_mapping(self) -> dict[str, Any]:
        """@brief 读取显式 dbinit 或内置默认目标 / Read explicit dbinit or the bundled default target.

        @return 可变 JSON 根对象 / Mutable JSON root object.
        """

        if self._dbinit_path.is_file() or not self._allow_packaged_dbinit_fallback:
            return self._load_mapping_file(self._dbinit_path, "dbinit")
        return self._load_mapping_text(read_default_text("dbinit.jsonc"), "dbinit")

    def _load_initial_template(self) -> dict[str, Any]:
        """@brief 只在内存解析首次初始化模板 / Parse the first-run template only in memory.

        @return 尚未写入 secret 的配置根对象 / Configuration root before secret insertion.

        @note 与旧的两次写入不同，模板不会先以半初始化状态落盘。
        / Unlike the former two-write flow, the template is never persisted half-initialized.
        """

        template_path = self._config_path.parent / _DEFAULT_EXAMPLE_NAME
        if template_path.is_file():
            try:
                text = template_path.read_text(encoding="utf-8")
            except (OSError, UnicodeError) as error:
                raise DbctlConfigurationError(
                    "无法读取 example.jsonc 初始化模板。"
                ) from safe_external_cause(
                    error,
                    operation="读取 example.jsonc 初始化模板",
                )
        else:
            text = read_default_text("example.jsonc")
        return self._load_mapping_text(text, "example.jsonc")

    def _validate_private_permissions(self) -> None:
        """@brief 拒绝 group/world 可读的 secret 文件 / Reject a secret file readable by group or world.

        @return 无返回值 / No return value.
        """

        if os.name == "nt":
            return
        try:
            mode = self._config_path.stat().st_mode & 0o777
        except OSError as error:
            raise DbctlConfigurationError(
                "无法检查 config.jsonc 文件权限。"
            ) from safe_external_cause(
                error,
                operation="检查 config.jsonc 文件权限",
            )
        if mode & 0o077:
            raise DbctlConfigurationError(
                "config.jsonc 权限必须为 owner-only，拒绝读取可外泄的数据库密码。"
            )

    def _tighten_private_permissions(self) -> None:
        """@brief 将 bootstrap 已验证配置收紧为 0600 / Tighten bootstrap-validated configuration to mode 0600.

        @return 无返回值 / No return value.
        """

        try:
            self._config_path.chmod(0o600)
        except OSError as error:
            raise DbctlConfigurationError(
                "无法收紧 config.jsonc 文件权限。"
            ) from safe_external_cause(
                error,
                operation="将 config.jsonc 权限收紧为 0600",
            )

    def _write_private_root(self, root: Mapping[str, Any]) -> None:
        """@brief 序列化并安全替换完整私密配置 / Serialize and securely replace the complete private configuration.

        @param root 已在内存通过全部不变量的配置 / Configuration that passed every invariant in memory.
        @return 无返回值 / No return value.
        """

        content = json.dumps(root, ensure_ascii=False, indent=2) + "\n"
        try:
            atomic_write_private_text(self._config_path, content)
        except OSError as error:
            raise DbctlConfigurationError("无法写入私密 config.jsonc。") from safe_external_cause(
                error,
                operation="原子写入私密 config.jsonc",
            )

    @staticmethod
    def _load_mapping_file(path: Path, label: str) -> dict[str, Any]:
        """@brief 读取一个 JSONC 根对象文件 / Read one JSONC root-object file.

        @param path 文件路径 / File path.
        @param label 不含 secret 的错误标签 / Secret-free error label.
        @return 可变根对象 / Mutable root object.
        """

        if not path.is_file():
            raise DbctlConfigurationError(f"{label}文件不存在：{path}")
        try:
            content = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise DbctlConfigurationError(f"{label}文件无法读取。") from safe_external_cause(
                error,
                operation=f"读取 {label} 文件",
            )
        return DbctlConfigStore._load_mapping_text(content, label)

    @staticmethod
    def _load_mapping_text(content: str, label: str) -> dict[str, Any]:
        """@brief 将 JSONC 文本解码为根对象 / Decode JSONC text into a root object.

        @param content UTF-8 JSONC 文本 / UTF-8 JSONC text.
        @param label 不含 secret 的错误标签 / Secret-free error label.
        @return 可变根对象 / Mutable root object.
        """

        try:
            parsed = json5.loads(content)
        except ValueError as error:
            raise DbctlConfigurationError(f"{label}文件无法解析。") from safe_external_cause(
                error,
                operation=f"解析 {label} JSONC",
            )
        if not isinstance(parsed, Mapping):
            raise DbctlConfigurationError(f"{label}根必须是对象。")
        return dict(parsed)


def _decode_database_declaration(
    root: Mapping[str, Any],
) -> tuple[DatabaseBlueprint, BootstrapAccess, DatabaseTarget]:
    """@brief 将 dbinit 解码为唯一数据库目标状态 / Decode dbinit into the sole database target state.

    @param root dbinit 根对象 / dbinit root object.
    @return blueprint、bootstrap access 和登录 target / Blueprint, bootstrap access, and login target.
    """

    administration = _require_mapping(
        root.get("database_administration"), "database_administration"
    )
    endpoint = _require_mapping(root.get("database_connection"), "database_connection")
    database = DatabaseName(_required_text(administration, "database_name"))
    target = DatabaseTarget(
        host=_required_text(endpoint, "host"),
        port=_required_port(endpoint, "port"),
        database=database,
    )
    roles = RoleSet(
        owner=RoleName(_required_text(administration, "owner_role")),
        migrator=RoleName(_required_text(administration, "migrator_role")),
        application=RoleName(_required_text(administration, "app_role")),
        dashboard=RoleName(_required_text(administration, "dashboard_role")),
    )
    schemas = _decode_schema_catalog(administration)
    observability_schema = _text_with_default(
        administration, "observability_schema", "observability"
    )
    if observability_schema != "observability":
        raise DbctlConfigurationError(
            "observability_schema 已由 migration 固定为 observability，不能重命名。"
        )
    data_region = _required_text(administration, "v2_default_data_region")
    if data_region not in {"cn", "global", "private_deployment"}:
        raise DbctlConfigurationError(
            "database_administration.v2_default_data_region 必须是 cn、global 或 private_deployment。"
        )
    raw_workspace_plans = _require_mapping(
        administration.get("v2_legacy_workspace_plans"),
        "database_administration.v2_legacy_workspace_plans",
    )
    workspace_plans: list[tuple[str, WorkspacePlan]] = []
    for workspace_id, plan in raw_workspace_plans.items():
        if (
            not isinstance(workspace_id, str)
            or not workspace_id
            or workspace_id.strip() != workspace_id
            or plan not in {"personal", "team", "enterprise"}
        ):
            raise DbctlConfigurationError(
                "database_administration.v2_legacy_workspace_plans 必须将非空 Workspace ID 映射到 personal、team 或 enterprise。"
            )
        workspace_plans.append((workspace_id, cast(WorkspacePlan, plan)))
    blueprint = DatabaseBlueprint(
        database=database,
        roles=roles,
        v2_default_data_region=cast(DataRegion, data_region),
        v2_legacy_workspace_plans=tuple(sorted(workspace_plans)),
        schemas=schemas,
    )
    maintenance_database = DatabaseName(
        _text_with_default(administration, "maintenance_database", "postgres")
    )
    access = BootstrapAccess(
        maintenance_target=DatabaseTarget(
            host=target.host,
            port=target.port,
            database=maintenance_database,
        ),
        local_postgres_user=_text_with_default(administration, "local_postgres_user", "postgres"),
        bootstrap_database_user=RoleName(
            _text_with_default(administration, "bootstrap_database_user", "postgres")
        ),
    )
    return blueprint, access, target


def _decode_private_settings(
    root: Mapping[str, Any],
    blueprint: DatabaseBlueprint,
    access: BootstrapAccess,
    target: DatabaseTarget,
) -> DbctlSettings:
    """@brief 将私密 JSON 对象解码为应用设置 / Decode private JSON data into application settings.

    @param root 私密配置根对象 / Private configuration root object.
    @param blueprint 已验证数据库目标状态 / Validated database target state.
    @param access 已验证 bootstrap 接入策略 / Validated bootstrap access policy.
    @param target 三个登录必须共享的目标 / Target all three logins must share.
    @return 完整 dbctl 设置 / Complete dbctl settings.
    """

    database = _require_mapping(root.get("database"), "database")
    observability = _require_mapping(root.get("observability"), "observability")
    _required_text(database, "mode")
    connections = ConnectionCatalog(
        target=target,
        migrator=parse_database_login(
            _required_text(database, "migrator_dsn"),
            role=LoginRole.MIGRATOR,
            expected_role_name=blueprint.roles.migrator,
            expected_target=target,
        ),
        application=parse_database_login(
            _required_text(database, "application_dsn"),
            role=LoginRole.APP,
            expected_role_name=blueprint.roles.application,
            expected_target=target,
        ),
        dashboard=parse_database_login(
            _required_text(database, "dashboard_dsn"),
            role=LoginRole.DASHBOARD,
            expected_role_name=blueprint.roles.dashboard,
            expected_target=target,
        ),
    )
    return DbctlSettings(
        blueprint=blueprint,
        access=access,
        connections=connections,
        retention=RetentionPolicy(days=_required_non_negative_int(observability, "retention_days")),
    )


def _ensure_database_dsns(
    root: dict[str, Any],
    blueprint: DatabaseBlueprint,
    target: DatabaseTarget,
) -> int:
    """@brief 补齐缺失 DSN，保留并验证既有 DSN / Complete missing DSNs while retaining and validating existing ones.

    @param root 可变私密配置根对象 / Mutable private configuration root.
    @param blueprint 数据库和 role 目标状态 / Database and role target state.
    @param target 每个 DSN 必须显式匹配的 target / Target every DSN must explicitly match.
    @return 本轮生成的独立登录凭据数量 / Number of independent login credentials generated.
    """

    raw_database = root.get("database")
    if not isinstance(raw_database, dict):
        raise DbctlConfigurationError("database 必须是对象。")
    if "database_role_passwords" in root:
        raise DbctlConfigurationError(
            "不再支持 database_role_passwords；请删除旧配置并重新运行 bootstrap。"
        )
    fields = (
        (LoginRole.MIGRATOR, "migrator_dsn", blueprint.roles.migrator),
        (LoginRole.APP, "application_dsn", blueprint.roles.application),
        (LoginRole.DASHBOARD, "dashboard_dsn", blueprint.roles.dashboard),
    )
    passwords: list[str] = []
    generated_credentials = 0
    for role, field_name, role_name in fields:
        raw_dsn = raw_database.get(field_name)
        if isinstance(raw_dsn, str) and raw_dsn.strip():
            login = parse_database_login(
                raw_dsn,
                role=role,
                expected_role_name=role_name,
                expected_target=target,
            )
            passwords.append(login.password.reveal())
            continue
        password = Secret(secrets.token_urlsafe(32))
        raw_database[field_name] = build_postgres_dsn(
            target=target,
            role_name=role_name,
            password=password,
        )
        passwords.append(password.reveal())
        generated_credentials += 1
    if len(set(passwords)) != len(passwords):
        raise DbctlConfigurationError("三个登录 role 必须使用互不相同的密码。")
    return generated_credentials


def _decode_schema_catalog(administration: Mapping[str, Any]) -> tuple[SchemaName, ...]:
    """@brief 读取并锁定 migration 的 canonical schema catalog / Read and lock the canonical migration schema catalog.

    @param administration database_administration 配置节 / database_administration section.
    @return 与历史 migration 完全一致的 schema tuple / Schema tuple exactly matching historical migrations.
    """

    raw = administration.get("schemas", list(_CANONICAL_SCHEMAS))
    if not isinstance(raw, list) or not raw:
        raise DbctlConfigurationError("database_administration.schemas 必须是非空数组。")
    schemas: list[SchemaName] = []
    for value in raw:
        if not isinstance(value, str) or not value.strip():
            raise DbctlConfigurationError("database_administration.schemas 必须只包含非空字符串。")
        schemas.append(SchemaName(value.strip()))
    if tuple(schema.value for schema in schemas) != _CANONICAL_SCHEMAS:
        raise DbctlConfigurationError(
            "database_administration.schemas 必须与已发布 migration 的 canonical catalog 一致。"
        )
    return tuple(schemas)


def _require_mapping(value: object, section_name: str) -> dict[str, Any]:
    """@brief 校验一个 JSON 对象配置节 / Validate one JSON object section.

    @param value 候选 JSON 值 / Candidate JSON value.
    @param section_name 安全字段名 / Safe field name.
    @return 可变字典副本 / Mutable dictionary copy.
    """

    if not isinstance(value, Mapping):
        raise DbctlConfigurationError(f"{section_name} 必须是对象。")
    return dict(value)


def _required_text(mapping: Mapping[str, Any], key: str) -> str:
    """@brief 读取必填非空文本 / Read required non-empty text.

    @param mapping JSON 对象 / JSON object.
    @param key 字段名 / Field name.
    @return 修剪后的文本 / Trimmed text.
    """

    return _text_with_default(mapping, key, None)


def _text_with_default(mapping: Mapping[str, Any], key: str, default: str | None) -> str:
    """@brief 读取带可选默认值的非空文本 / Read non-empty text with an optional default.

    @param mapping JSON 对象 / JSON object.
    @param key 字段名 / Field name.
    @param default 缺失时默认值；None 表示必填 / Default for absence; None means required.
    @return 修剪后的文本 / Trimmed text.
    """

    value = mapping.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise DbctlConfigurationError(f"{key} 必须是非空字符串。")
    return value.strip()


def _required_port(mapping: Mapping[str, Any], key: str) -> int:
    """@brief 读取合法 TCP port / Read a valid TCP port.

    @param mapping JSON 对象 / JSON object.
    @param key 字段名 / Field name.
    @return 1..65535 的端口 / Port in the range 1..65535.
    """

    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 65535:
        raise DbctlConfigurationError(f"{key} 必须是 1 到 65535 的整数。")
    return value


def _required_non_negative_int(mapping: Mapping[str, Any], key: str) -> int:
    """@brief 读取非负整数 / Read a non-negative integer.

    @param mapping JSON 对象 / JSON object.
    @param key 字段名 / Field name.
    @return 非负整数 / Non-negative integer.
    """

    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise DbctlConfigurationError(f"{key} 必须是非负整数。")
    return value


__all__ = ["DbctlConfigStore"]
