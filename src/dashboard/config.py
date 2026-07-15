"""Dashboard 的独立配置服务（configuration service）。"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Final, cast

from workspace_shared.jsonc import ConfigurationError
from workspace_shared.jsonc import load_jsonc as _load_shared_jsonc

from .errors import DashboardConfigurationError, DashboardValidationError
from .models import HealthPolicy

_DATABASE_MODES: Final[frozenset[str]] = frozenset({"memory", "postgresql"})
_OPERATOR_ACCESS_MODES: Final[frozenset[str]] = frozenset({"mock", "operator_token"})
_SUPPORTED_ENVIRONMENTS: Final[frozenset[str]] = frozenset(
    {"development", "test", "staging", "production"}
)
"""@brief Dashboard 接受的部署环境 / Deployment environments accepted by Dashboard."""

_MOCK_ACCESS_ENVIRONMENTS: Final[frozenset[str]] = frozenset({"development", "test"})
"""@brief 允许 mock operator access 的本地环境 / Local environments permitting mock operator access."""

_ENVIRONMENT_VARIABLE_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HTTP_HEADER_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z][A-Za-z0-9-]*$")


@dataclass(frozen=True, slots=True)
class DashboardSettings:
    """@brief Dashboard 从根 config.jsonc 派生的不可变配置快照（settings snapshot）。

    根文件采用以下面向运维者的结构，字段均可省略：

    ```jsonc
    {
      "dashboard": {
        "enabled": true,
        "api": {"host": "127.0.0.1", "port": 8081, "prefix": "/dashboard/v1"},
        "query": {"default_window_minutes": 60, "max_window_hours": 168, "max_samples": 10000},
        "observability_view": "observability.dashboard_metric_samples",
        "access": {
          "mode": "operator_token",
          "operator_id": "workspace_dashboard",
          "token_env": "AIWS_DASHBOARD_OPERATOR_TOKEN",
          "token_header": "X-AIWS-Dashboard-Token"
        },
        "health": {"warning_error_rate": 0.01}
      },
      "database": {
        "mode": "postgresql",
        "dashboard_dsn_env": "AIWS_DASHBOARD_DATABASE_DSN"
      }
    }
    ```

    @param enabled: 是否允许入口适配器暴露 Dashboard。
    @param api_host: 独立运行 API 时的绑定地址；部署时仍应经 Nginx 反向代理。
    @param api_port: 独立运行 API 时的绑定端口。
    @param api_prefix: 私有运维 API 的稳定路径前缀。
    @param default_window: 未显式传入时间时采用的查询窗口。
    @param max_window: 单次查询允许的最大时间窗口。
    @param max_samples: 单个仓库查询的最大样本数，作为背压上限。
    @param observability_view: PostgreSQL 只读视图的 schema.table 名称。
    @param database_mode: 从根 database.mode 读取的存储模式；只允许 memory 或 postgresql。
    @param dashboard_dsn_env: PostgreSQL dashboard 只读 DSN 所在的环境变量名。
    @param database_pool_size: Dashboard 只读连接池的常驻连接数。
    @param database_max_overflow: Dashboard 连接池忙碌时允许的临时连接数。
    @param database_connect_timeout_ms: 建立 PostgreSQL 连接的超时上限。
    @param query_timeout_ms: 单个只读 Dashboard 查询的 PostgreSQL statement timeout。
    @param operator_access_mode: ``mock`` 或 ``operator_token``；staging/production 与 PostgreSQL
        均强制后者。
    @param operator_id: 配置的稳定运维身份，不接受请求任意声明。
    @param operator_token_env: operator token 的环境变量名；secret 不写入 JSONC。
    @param operator_token_header: HTTP API 接收 token 的受控 header 名称。
    @param health_policy: 将聚合信号转为健康状态的阈值策略。
    @param environment: 从根配置读取的部署环境；``mock`` access 只允许 development/test。
    """

    enabled: bool = True
    api_host: str = "127.0.0.1"
    api_port: int = 8081
    api_prefix: str = "/dashboard/v1"
    default_window: timedelta = timedelta(hours=1)
    max_window: timedelta = timedelta(days=7)
    max_samples: int = 10_000
    observability_view: str = "observability.dashboard_metric_samples"
    database_mode: str = "memory"
    dashboard_dsn_env: str = "AIWS_DASHBOARD_DATABASE_DSN"
    database_pool_size: int = 5
    database_max_overflow: int = 5
    database_connect_timeout_ms: int = 3_000
    query_timeout_ms: int = 5_000
    operator_access_mode: str = "mock"
    operator_id: str = "workspace_dashboard"
    operator_token_env: str = "AIWS_DASHBOARD_OPERATOR_TOKEN"
    operator_token_header: str = "X-AIWS-Dashboard-Token"
    health_policy: HealthPolicy = field(default_factory=HealthPolicy)
    environment: str = "development"

    def __post_init__(self) -> None:
        """@brief 校验配置边界，避免错误配置扩大查询或网络暴露面。

        @return: 无返回值；无效配置抛出 DashboardConfigurationError。
        """

        if not isinstance(self.environment, str):
            raise DashboardConfigurationError("environment 必须是字符串。")
        if not isinstance(self.enabled, bool):
            raise DashboardConfigurationError("dashboard.enabled 必须是布尔值。")
        if not isinstance(self.api_host, str):
            raise DashboardConfigurationError("dashboard.api.host 必须是字符串。")
        if not isinstance(self.api_prefix, str):
            raise DashboardConfigurationError("dashboard.api.prefix 必须是字符串。")
        if not isinstance(self.observability_view, str):
            raise DashboardConfigurationError("dashboard.observability_view 必须是字符串。")
        if not isinstance(self.database_mode, str):
            raise DashboardConfigurationError("database.mode 必须是字符串。")
        if not isinstance(self.dashboard_dsn_env, str):
            raise DashboardConfigurationError("database.dashboard_dsn_env 必须是字符串。")
        if not isinstance(self.operator_access_mode, str):
            raise DashboardConfigurationError("dashboard.access.mode 必须是字符串。")
        if not isinstance(self.operator_id, str):
            raise DashboardConfigurationError("dashboard.access.operator_id 必须是字符串。")
        if not isinstance(self.operator_token_env, str):
            raise DashboardConfigurationError("dashboard.access.token_env 必须是字符串。")
        if not isinstance(self.operator_token_header, str):
            raise DashboardConfigurationError("dashboard.access.token_header 必须是字符串。")
        if not isinstance(self.default_window, timedelta) or not isinstance(
            self.max_window, timedelta
        ):
            raise DashboardConfigurationError("Dashboard 查询窗口必须是 timedelta。")
        environment = self.environment.strip().casefold()
        if environment not in _SUPPORTED_ENVIRONMENTS:
            raise DashboardConfigurationError(
                "environment 必须是 development、test、staging 或 production。"
            )
        api_host = self.api_host.strip()
        if not api_host:
            raise DashboardConfigurationError("dashboard.api.host 不能为空。")
        if (
            isinstance(self.api_port, bool)
            or not isinstance(self.api_port, int)
            or not 1 <= self.api_port <= 65_535
        ):
            raise DashboardConfigurationError("dashboard.api.port 必须在 1 到 65535 之间。")
        api_prefix = self.api_prefix.strip()
        if not api_prefix.startswith("/") or api_prefix == "/" or api_prefix.endswith("/"):
            raise DashboardConfigurationError(
                "dashboard.api.prefix 必须以 / 开头，且不能是 / 或以 / 结尾。"
            )
        if self.default_window <= timedelta(0):
            raise DashboardConfigurationError("dashboard.query.default_window_minutes 必须大于 0。")
        if self.max_window < self.default_window:
            raise DashboardConfigurationError(
                "dashboard.query.max_window_hours 不能小于默认查询窗口。"
            )
        if (
            isinstance(self.max_samples, bool)
            or not isinstance(self.max_samples, int)
            or self.max_samples < 1
        ):
            raise DashboardConfigurationError("dashboard.query.max_samples 必须是正整数。")
        observability_view = self.observability_view.strip()
        if len(observability_view.split(".")) != 2:
            raise DashboardConfigurationError(
                "dashboard.observability_view 必须是 schema.table 形式。"
            )
        if not isinstance(self.health_policy, HealthPolicy):
            raise DashboardConfigurationError("dashboard.health 必须能转换为 HealthPolicy。")

        database_mode = self.database_mode.strip()
        if database_mode not in _DATABASE_MODES:
            raise DashboardConfigurationError("database.mode 必须是 memory 或 postgresql。")
        dashboard_dsn_env = self.dashboard_dsn_env.strip()
        if not _ENVIRONMENT_VARIABLE_PATTERN.fullmatch(dashboard_dsn_env):
            raise DashboardConfigurationError("database.dashboard_dsn_env 必须是合法环境变量名。")
        for field_name, value in (
            ("database.pool_size", self.database_pool_size),
            ("database.max_overflow", self.database_max_overflow),
            ("database.connect_timeout_ms", self.database_connect_timeout_ms),
            ("dashboard.query.timeout_ms", self.query_timeout_ms),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise DashboardConfigurationError(f"{field_name} 必须是非负整数。")
        if self.database_pool_size < 1:
            raise DashboardConfigurationError("database.pool_size 必须是正整数。")
        if self.database_connect_timeout_ms < 1 or self.query_timeout_ms < 1:
            raise DashboardConfigurationError("数据库连接和查询超时必须是正整数。")

        operator_access_mode = self.operator_access_mode.strip()
        if operator_access_mode not in _OPERATOR_ACCESS_MODES:
            raise DashboardConfigurationError(
                "dashboard.access.mode 必须是 mock 或 operator_token。"
            )
        if database_mode == "postgresql" and operator_access_mode != "operator_token":
            raise DashboardConfigurationError(
                "database.mode=postgresql 时 dashboard.access.mode 必须是 operator_token。"
            )
        if (
            environment not in _MOCK_ACCESS_ENVIRONMENTS
            and operator_access_mode == "mock"
        ):
            raise DashboardConfigurationError(
                "staging/production 的 dashboard.access.mode 必须是 operator_token；"
                "mock 仅允许 development/test。"
            )
        operator_id = self.operator_id.strip()
        if not operator_id:
            raise DashboardConfigurationError("dashboard.access.operator_id 不能为空。")
        operator_token_env = self.operator_token_env.strip()
        if not _ENVIRONMENT_VARIABLE_PATTERN.fullmatch(operator_token_env):
            raise DashboardConfigurationError("dashboard.access.token_env 必须是合法环境变量名。")
        operator_token_header = self.operator_token_header.strip()
        if not _HTTP_HEADER_PATTERN.fullmatch(operator_token_header):
            raise DashboardConfigurationError("dashboard.access.token_header 必须是安全 HTTP header 名称。")

        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "api_host", api_host)
        object.__setattr__(self, "api_prefix", api_prefix)
        object.__setattr__(self, "observability_view", observability_view)
        object.__setattr__(self, "database_mode", database_mode)
        object.__setattr__(self, "dashboard_dsn_env", dashboard_dsn_env)
        object.__setattr__(self, "operator_access_mode", operator_access_mode)
        object.__setattr__(self, "operator_id", operator_id)
        object.__setattr__(self, "operator_token_env", operator_token_env)
        object.__setattr__(self, "operator_token_header", operator_token_header)

    @classmethod
    def from_mapping(
        cls,
        mapping: Mapping[str, Any],
        *,
        environment: str = "development",
        database_mode: str = "memory",
        dashboard_dsn_env: str = "AIWS_DASHBOARD_DATABASE_DSN",
        database_pool_size: int = 5,
        database_max_overflow: int = 5,
        database_connect_timeout_ms: int = 3_000,
    ) -> DashboardSettings:
        """@brief 从 `dashboard` 配置对象创建严格校验的配置快照。

        @param mapping: 根配置中 dashboard 键对应的对象；未知键会被拒绝。
        @param environment: 从根 environment 节派生的部署环境。
        @param database_mode: 从根 database 节派生的存储模式。
        @param dashboard_dsn_env: dashboard 只读 DSN 的环境变量名。
        @param database_pool_size: 连接池常驻连接数。
        @param database_max_overflow: 连接池临时连接上限。
        @param database_connect_timeout_ms: 连接超时。
        @return: 可供 composition root 使用的 DashboardSettings。
        """

        _require_mapping(mapping, "dashboard")
        _reject_unknown_keys(
            mapping,
            {
                "enabled",
                "api",
                "query",
                "observability_view",
                "access",
                "health",
            },
            "dashboard",
        )

        api = _optional_mapping(mapping, "api", "dashboard.api")
        query = _optional_mapping(mapping, "query", "dashboard.query")
        access = _optional_mapping(mapping, "access", "dashboard.access")
        health = _optional_mapping(mapping, "health", "dashboard.health")

        _reject_unknown_keys(api, {"host", "port", "prefix"}, "dashboard.api")
        _reject_unknown_keys(
            query,
            {"default_window_minutes", "max_window_hours", "max_samples", "timeout_ms"},
            "dashboard.query",
        )
        _reject_unknown_keys(
            access,
            {"mode", "operator_id", "token_env", "token_header"},
            "dashboard.access",
        )
        _reject_unknown_keys(
            health,
            {
                "warning_error_rate",
                "critical_error_rate",
                "warning_latency_ms",
                "critical_latency_ms",
                "warning_saturation",
                "critical_saturation",
            },
            "dashboard.health",
        )

        enabled = _optional_bool(mapping, "enabled", True, "dashboard.enabled")
        api_host = _optional_str(api, "host", "127.0.0.1", "dashboard.api.host")
        api_port = _optional_int(api, "port", 8081, "dashboard.api.port")
        api_prefix = _optional_str(api, "prefix", "/dashboard/v1", "dashboard.api.prefix")
        default_window_minutes = _optional_int(
            query,
            "default_window_minutes",
            60,
            "dashboard.query.default_window_minutes",
        )
        max_window_hours = _optional_int(
            query,
            "max_window_hours",
            24 * 7,
            "dashboard.query.max_window_hours",
        )
        max_samples = _optional_int(
            query,
            "max_samples",
            10_000,
            "dashboard.query.max_samples",
        )
        query_timeout_ms = _optional_int(
            query,
            "timeout_ms",
            5_000,
            "dashboard.query.timeout_ms",
        )
        access_mode_default = "operator_token" if database_mode == "postgresql" else "mock"
        operator_access_mode = _optional_str(
            access,
            "mode",
            access_mode_default,
            "dashboard.access.mode",
        )
        operator_id = _optional_str(
            access,
            "operator_id",
            "workspace_dashboard",
            "dashboard.access.operator_id",
        )
        operator_token_env = _optional_str(
            access,
            "token_env",
            "AIWS_DASHBOARD_OPERATOR_TOKEN",
            "dashboard.access.token_env",
        )
        operator_token_header = _optional_str(
            access,
            "token_header",
            "X-AIWS-Dashboard-Token",
            "dashboard.access.token_header",
        )
        observability_view = _optional_str(
            mapping,
            "observability_view",
            "observability.dashboard_metric_samples",
            "dashboard.observability_view",
        )
        try:
            health_policy = HealthPolicy(
                warning_error_rate=_optional_number(
                    health,
                    "warning_error_rate",
                    0.01,
                    "dashboard.health.warning_error_rate",
                ),
                critical_error_rate=_optional_number(
                    health,
                    "critical_error_rate",
                    0.05,
                    "dashboard.health.critical_error_rate",
                ),
                warning_latency_ms=_optional_number(
                    health,
                    "warning_latency_ms",
                    1_000.0,
                    "dashboard.health.warning_latency_ms",
                ),
                critical_latency_ms=_optional_number(
                    health,
                    "critical_latency_ms",
                    3_000.0,
                    "dashboard.health.critical_latency_ms",
                ),
                warning_saturation=_optional_number(
                    health,
                    "warning_saturation",
                    0.70,
                    "dashboard.health.warning_saturation",
                ),
                critical_saturation=_optional_number(
                    health,
                    "critical_saturation",
                    0.90,
                    "dashboard.health.critical_saturation",
                ),
            )
            return cls(
                environment=environment,
                enabled=enabled,
                api_host=api_host,
                api_port=api_port,
                api_prefix=api_prefix,
                default_window=timedelta(minutes=default_window_minutes),
                max_window=timedelta(hours=max_window_hours),
                max_samples=max_samples,
                observability_view=observability_view,
                database_mode=database_mode,
                dashboard_dsn_env=dashboard_dsn_env,
                database_pool_size=database_pool_size,
                database_max_overflow=database_max_overflow,
                database_connect_timeout_ms=database_connect_timeout_ms,
                query_timeout_ms=query_timeout_ms,
                operator_access_mode=operator_access_mode,
                operator_id=operator_id,
                operator_token_env=operator_token_env,
                operator_token_header=operator_token_header,
                health_policy=health_policy,
            )
        except (
            TypeError,
            ValueError,
            DashboardConfigurationError,
            DashboardValidationError,
        ) as error:
            if isinstance(error, DashboardConfigurationError):
                raise
            raise DashboardConfigurationError(str(error)) from error

    @classmethod
    def from_root_mapping(cls, root: Mapping[str, Any]) -> DashboardSettings:
        """@brief 从共享根配置派生 Dashboard 设置，不依赖其他应用的配置服务。

        @param root: config.jsonc 的完整根对象。
        @return: DashboardSettings；优先读取可选 dashboard 节，否则兼容稳定的
        operator_interface 节。

        @note `operator_interface` 是全局面向运维者的配置；本方法只读取本应用
        所需字段，因而不会因其他应用新增字段而拒绝整个根配置。
        """

        _require_mapping(root, "根配置")
        environment = _optional_str(root, "environment", "development", "environment")
        (
            database_mode,
            dashboard_dsn_env,
            database_pool_size,
            database_max_overflow,
            database_connect_timeout_ms,
        ) = _root_database_values(root)
        if "dashboard" in root:
            dashboard = root["dashboard"]
            _require_mapping(dashboard, "dashboard")
            return cls.from_mapping(
                dashboard,
                environment=environment,
                database_mode=database_mode,
                dashboard_dsn_env=dashboard_dsn_env,
                database_pool_size=database_pool_size,
                database_max_overflow=database_max_overflow,
                database_connect_timeout_ms=database_connect_timeout_ms,
            )

        operator_interface = _optional_mapping(
            root,
            "operator_interface",
            "operator_interface",
        )
        compatibility_mapping: dict[str, Any] = {
            "api": {
                "host": _optional_str(
                    operator_interface,
                    "api_bind_host",
                    "127.0.0.1",
                    "operator_interface.api_bind_host",
                ),
                "port": _optional_int(
                    operator_interface,
                    "api_bind_port",
                    8081,
                    "operator_interface.api_bind_port",
                ),
            },
            "query": {
                "default_window_minutes": _optional_int(
                    operator_interface,
                    "default_window_minutes",
                    60,
                    "operator_interface.default_window_minutes",
                )
            },
        }
        return cls.from_mapping(
            compatibility_mapping,
            environment=environment,
            database_mode=database_mode,
            dashboard_dsn_env=dashboard_dsn_env,
            database_pool_size=database_pool_size,
            database_max_overflow=database_max_overflow,
            database_connect_timeout_ms=database_connect_timeout_ms,
        )

    @classmethod
    def from_file(cls, path: str | Path) -> DashboardSettings:
        """@brief 从共享根 config.jsonc 创建 DashboardSettings。

        @param path: 根 JSONC 配置文件路径。
        @return: 从该文件的 dashboard 或 operator_interface 节派生的设置快照。
        """

        return cls.from_root_mapping(load_jsonc(path))


class DashboardConfigService:
    """@brief 从项目根 config.jsonc 独立读取 Dashboard 配置的服务。

    @param path: 共享配置文件路径，默认相对当前工作目录的 config.jsonc。
    @param allow_missing: 为 True 时，开发与测试中缺少文件会返回安全默认值。

    @note 服务优先读取 `dashboard`，也兼容共享的 `operator_interface`；不导入
    backend 或 dbctl 的配置服务。
    """

    def __init__(
        self,
        path: str | Path = "config.jsonc",
        *,
        allow_missing: bool = True,
    ) -> None:
        """@brief 创建尚未读取磁盘的 DashboardConfigService。

        @param path: config.jsonc 文件路径。
        @param allow_missing: 缺少文件时是否采用默认配置。
        @return: 新建的 DashboardConfigService 实例。
        """

        self._path = Path(path)
        self._allow_missing = allow_missing
        self._cached: DashboardSettings | None = None

    @property
    def path(self) -> Path:
        """@brief 返回配置文件路径（configuration path）。

        @return: 未解析为绝对路径的 Path，以保留调用方的工作目录语义。
        """

        return self._path

    def load(self, *, force_reload: bool = False) -> DashboardSettings:
        """@brief 读取并校验根 config.jsonc 的 dashboard 配置。

        @param force_reload: 为 True 时忽略本进程内缓存并重新读取磁盘。
        @return: 不可变的 DashboardSettings 快照。
        """

        if self._cached is not None and not force_reload:
            return self._cached

        if not self._path.is_file():
            if not self._allow_missing:
                raise DashboardConfigurationError(f"找不到配置文件：{self._path}")
            settings = DashboardSettings()
        else:
            root = load_jsonc(self._path)
            _require_mapping(root, "根配置")
            settings = DashboardSettings.from_root_mapping(root)

        self._cached = settings
        return settings


def load_jsonc(path: str | Path) -> Mapping[str, Any]:
    """@brief 读取 JSONC（JSON with Comments）文件并返回根对象。

    @param path: 待读取的 JSONC 文件路径。
    @return: 解析后的根 Mapping；顶层数组和标量会被拒绝。
    """

    config_path = Path(path)
    try:
        parsed = _load_shared_jsonc(config_path)
    except ConfigurationError as error:
        raise DashboardConfigurationError(f"无法读取 JSONC 配置 {config_path}: {error}") from error
    return _require_mapping(parsed, "根配置")


def _require_mapping(value: object, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DashboardConfigurationError(f"{path} 必须是对象。")
    return cast(Mapping[str, Any], value)


def _optional_mapping(
    source: Mapping[str, Any],
    key: str,
    path: str,
) -> Mapping[str, Any]:
    value = source.get(key, {})
    return _require_mapping(value, path)


def _root_database_values(root: Mapping[str, Any]) -> tuple[str, str, int, int, int]:
    """@brief 从共享根读取 Dashboard 所需的数据库配置（database settings）。

    @param root: 已确认是对象的共享根配置。
    @return: ``(mode, dashboard_dsn_env, pool_size, max_overflow, connect_timeout_ms)``。
    @raise DashboardConfigurationError: database 节存在但类型或字段不合法时抛出。

    @note 仅消费 Dashboard 所需字段，故不会因 backend/dbctl 的新增配置破坏该独立包。
    缺失 ``dashboard_dsn_env`` 时只派生为公开约定的变量名
    ``AIWS_DASHBOARD_DATABASE_DSN``，绝不回退使用 application DSN。
    """

    if "database" not in root:
        return ("memory", "AIWS_DASHBOARD_DATABASE_DSN", 5, 5, 3_000)
    database = _require_mapping(root["database"], "database")
    return (
        _optional_str(database, "mode", "memory", "database.mode"),
        _optional_str(
            database,
            "dashboard_dsn_env",
            "AIWS_DASHBOARD_DATABASE_DSN",
            "database.dashboard_dsn_env",
        ),
        _optional_int(database, "pool_size", 5, "database.pool_size"),
        _optional_int(database, "max_overflow", 5, "database.max_overflow"),
        _optional_int(
            database,
            "connect_timeout_ms",
            3_000,
            "database.connect_timeout_ms",
        ),
    )


def _reject_unknown_keys(
    source: Mapping[str, Any],
    allowed: set[str],
    path: str,
) -> None:
    unknown = sorted(set(source) - allowed)
    if unknown:
        unknown_text = ", ".join(unknown)
        raise DashboardConfigurationError(f"{path} 包含不支持的配置项：{unknown_text}。")


def _optional_bool(
    source: Mapping[str, Any],
    key: str,
    default: bool,
    path: str,
) -> bool:
    value = source.get(key, default)
    if not isinstance(value, bool):
        raise DashboardConfigurationError(f"{path} 必须是布尔值。")
    return value


def _optional_int(
    source: Mapping[str, Any],
    key: str,
    default: int,
    path: str,
) -> int:
    value = source.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DashboardConfigurationError(f"{path} 必须是整数。")
    return int(value)


def _optional_number(
    source: Mapping[str, Any],
    key: str,
    default: float,
    path: str,
) -> float:
    value = source.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DashboardConfigurationError(f"{path} 必须是数值。")
    return float(value)


def _optional_str(
    source: Mapping[str, Any],
    key: str,
    default: str,
    path: str,
) -> str:
    value = source.get(key, default)
    if not isinstance(value, str):
        raise DashboardConfigurationError(f"{path} 必须是字符串。")
    if not value.strip():
        raise DashboardConfigurationError(f"{path} 不能为空。")
    return value


__all__ = ["DashboardConfigService", "DashboardSettings", "load_jsonc"]
