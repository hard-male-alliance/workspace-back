"""@brief Dashboard 独立强类型配置 / Dashboard-owned strongly typed configuration."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal, cast

from dashboard.application.errors import DashboardConfigurationError
from dashboard.application.service import (
    MAX_EVENT_LIMIT,
    MAX_QUERY_WINDOW,
    MAX_TARGET_POINTS,
)
from dashboard.domain.model import ServiceLevelObjective
from workspace_shared.jsonc import ConfigurationError, load_jsonc, require_mapping

DatabaseMode = Literal["memory", "postgresql"]
"""@brief Dashboard 数据库模式 / Dashboard database modes."""

AccessMode = Literal["mock", "operator_token"]
"""@brief Dashboard 运维身份模式 / Dashboard operator-access modes."""

_HEADER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9-]*$")
"""@brief 安全 HTTP header 名模式 / Safe HTTP-header-name pattern."""


@dataclass(frozen=True, slots=True)
class DashboardDatabaseSettings:
    """@brief Dashboard 只读数据库设置 / Dashboard read-only database settings.

    @param mode memory demo 或 PostgreSQL / Memory demo or PostgreSQL mode.
    @param dsn 独立 dashboard role DSN / Dedicated Dashboard-role DSN.
    @param pool_size 常驻连接数 / Persistent pool size.
    @param max_overflow 临时连接数 / Temporary overflow connections.
    @param connect_timeout_ms 建连超时 / Connection timeout.
    """

    mode: DatabaseMode
    dsn: str | None = field(default=None, repr=False)
    pool_size: int = 5
    max_overflow: int = 5
    connect_timeout_ms: int = 3_000


@dataclass(frozen=True, slots=True)
class DashboardQuerySettings:
    """@brief Dashboard 查询边界 / Dashboard query bounds.

    @param default_window 默认窗口 / Default window.
    @param max_window 最大窗口 / Maximum window.
    @param statement_timeout_ms SQL 语句超时 / SQL statement timeout.
    @param freshness_target 新鲜度目标 / Freshness objective.
    @param target_points 自动分桶目标点数 / Automatic bucket target.
    @param max_event_limit 诊断事件硬上限 / Hard diagnostic-event limit.
    """

    default_window: timedelta = timedelta(hours=1)
    max_window: timedelta = timedelta(days=7)
    statement_timeout_ms: int = 5_000
    freshness_target: timedelta = timedelta(minutes=2)
    target_points: int = 600
    max_event_limit: int = 500

    def __post_init__(self) -> None:
        """@brief 强制不可由配置绕过的查询预算 / Enforce query budgets configuration cannot bypass.

        @return 无返回值 / No return value.
        @raise DashboardConfigurationError 任一预算越过代码级上限时抛出 / Raised when any budget exceeds a code-level ceiling.
        """

        if self.default_window <= timedelta(0) or self.max_window < self.default_window:
            raise DashboardConfigurationError("Dashboard 时间窗口配置无效。")
        if self.max_window > MAX_QUERY_WINDOW:
            raise DashboardConfigurationError("dashboard.query.max_window 不能超过 31 天。")
        if not 1 <= self.statement_timeout_ms <= 60_000:
            raise DashboardConfigurationError("dashboard.query.timeout_ms 必须在 1..60000。")
        if not timedelta(seconds=1) <= self.freshness_target <= timedelta(days=1):
            raise DashboardConfigurationError(
                "dashboard.query.freshness_target 必须在 1 秒到 1 天。"
            )
        if not 10 <= self.target_points <= MAX_TARGET_POINTS:
            raise DashboardConfigurationError(
                f"dashboard.query.target_points 必须在 10..{MAX_TARGET_POINTS}。"
            )
        if not 1 <= self.max_event_limit <= MAX_EVENT_LIMIT:
            raise DashboardConfigurationError(
                f"dashboard.query.max_event_limit 必须在 1..{MAX_EVENT_LIMIT}。"
            )


@dataclass(frozen=True, slots=True)
class DashboardAccessSettings:
    """@brief Dashboard 运维身份设置 / Dashboard operator-identity settings.

    @param mode mock 或 operator_token / ``mock`` or ``operator_token``.
    @param operator_id 稳定运维主体 / Stable operator principal.
    @param token 直接来自私有 config.jsonc 的 secret / Secret read directly from private config.jsonc.
    @param token_header HTTP 凭证 header / HTTP credential header.
    """

    mode: AccessMode = "mock"
    operator_id: str = "workspace_dashboard"
    token: str | None = field(default=None, repr=False)
    token_header: str = "X-Dashboard-Operator-Token"


@dataclass(frozen=True, slots=True)
class DashboardApiSettings:
    """@brief Dashboard API 设置 / Dashboard API settings.

    @param host 内部绑定地址 / Internal bind host.
    @param port 内部绑定端口 / Internal bind port.
    @param prefix API 路径前缀 / API path prefix.
    """

    host: str = "127.0.0.1"
    port: int = 8_010
    prefix: str = "/dashboard/v1"


@dataclass(frozen=True, slots=True)
class DashboardSettings:
    """@brief Dashboard 完整配置快照 / Complete Dashboard settings snapshot.

    @param environment 部署环境 / Deployment environment.
    @param enabled 是否启用 / Whether Dashboard is enabled.
    @param default_workspace_id 零参数 CLI 默认工作区 / Default workspace for the zero-argument CLI.
    @param database 只读数据库设置 / Read-only database settings.
    @param query 查询边界 / Query bounds.
    @param access 运维身份设置 / Operator identity settings.
    @param api API 设置 / API settings.
    @param objective SLO / Service-level objective.
    """

    environment: str
    enabled: bool
    default_workspace_id: str
    database: DashboardDatabaseSettings
    query: DashboardQuerySettings
    access: DashboardAccessSettings
    api: DashboardApiSettings
    objective: ServiceLevelObjective

    @classmethod
    def from_file(cls, path: str | Path) -> DashboardSettings:
        """@brief 从根 JSONC 读取 Dashboard 配置 / Load Dashboard settings from root JSONC.

        @param path 根配置路径 / Root configuration path.
        @return 已校验配置快照 / Validated settings snapshot.
        """

        resolved_path = Path(path)
        if not resolved_path.is_file():
            raise DashboardConfigurationError(f"找不到配置文件：{resolved_path}")
        try:
            root = load_jsonc(resolved_path)
            return cls.from_root_mapping(root)
        except DashboardConfigurationError:
            raise
        except ConfigurationError as error:
            raise DashboardConfigurationError(str(error)) from error

    @classmethod
    def from_root_mapping(cls, root: Mapping[str, Any]) -> DashboardSettings:
        """@brief 从已解析根对象构建 Dashboard 配置 / Build Dashboard settings from a parsed root object.

        @param root 根配置对象 / Root configuration mapping.
        @return 已校验配置快照 / Validated settings snapshot.
        """

        try:
            environment = _required_text(root, "environment")
            workspace = require_mapping(root.get("workspace"), "workspace")
            database = require_mapping(root.get("database"), "database")
            dashboard = require_mapping(root.get("dashboard"), "dashboard")
            api = require_mapping(dashboard.get("api", {}), "dashboard.api")
            query = require_mapping(dashboard.get("query", {}), "dashboard.query")
            access = require_mapping(dashboard.get("access", {}), "dashboard.access")
            health = require_mapping(dashboard.get("health", {}), "dashboard.health")

            mode = cast(DatabaseMode, _choice(database, "mode", {"memory", "postgresql"}))
            access_default = "operator_token" if mode == "postgresql" else "mock"
            access_mode = cast(
                AccessMode,
                _choice(access, "mode", {"mock", "operator_token"}, default=access_default),
            )
            dsn = _optional_text(database.get("dashboard_dsn"))
            if mode == "postgresql" and dsn is None:
                raise DashboardConfigurationError(
                    "database.dashboard_dsn 在 postgresql 模式下必须配置。"
                )
            if mode == "postgresql" and access_mode != "operator_token":
                raise DashboardConfigurationError(
                    "PostgreSQL Dashboard 必须使用 operator_token。"
                )
            if environment in {"staging", "production"} and access_mode == "mock":
                raise DashboardConfigurationError("staging/production 禁止 mock Dashboard 身份。")

            if "token" not in access:
                raise DashboardConfigurationError("dashboard.access.token 必须显式配置。")
            token = _optional_text(access["token"])
            token_header = _text(access, "token_header", "X-Dashboard-Operator-Token")
            if access_mode == "operator_token" and token is None:
                raise DashboardConfigurationError(
                    "dashboard.access.token 在 operator_token 模式下必须配置。"
                )
            if not _HEADER_PATTERN.fullmatch(token_header):
                raise DashboardConfigurationError("dashboard.access.token_header 不是安全 header。")

            warning_error_rate = _number(health, "warning_error_rate", 0.01)
            if not 0 < warning_error_rate < 1:
                raise DashboardConfigurationError("warning_error_rate 必须位于 0 与 1 之间。")
            settings = cls(
                environment=environment,
                enabled=_boolean(dashboard, "enabled", True),
                default_workspace_id=_required_text(workspace, "default_workspace_id"),
                database=DashboardDatabaseSettings(
                    mode=mode,
                    dsn=dsn,
                    pool_size=_positive_int(database, "pool_size", 5),
                    max_overflow=_non_negative_int(database, "max_overflow", 5),
                    connect_timeout_ms=_positive_int(database, "connect_timeout_ms", 3_000),
                ),
                query=DashboardQuerySettings(
                    default_window=timedelta(
                        minutes=_positive_int(query, "default_window_minutes", 60)
                    ),
                    max_window=timedelta(
                        hours=_positive_int(query, "max_window_hours", 24 * 7)
                    ),
                    statement_timeout_ms=_positive_int(query, "timeout_ms", 5_000),
                    freshness_target=timedelta(
                        seconds=_positive_int(query, "freshness_target_seconds", 120)
                    ),
                    target_points=_positive_int(query, "target_points", 600),
                    max_event_limit=_positive_int(query, "max_event_limit", 500),
                ),
                access=DashboardAccessSettings(
                    mode=access_mode,
                    operator_id=_text(access, "operator_id", "workspace_dashboard"),
                    token=token,
                    token_header=token_header,
                ),
                api=DashboardApiSettings(
                    host=_text(api, "host", "127.0.0.1"),
                    port=_bounded_port(api, "port", 8_010),
                    prefix=_api_prefix(api.get("prefix", "/dashboard/v1")),
                ),
                objective=ServiceLevelObjective(
                    availability_target=1.0 - warning_error_rate,
                    latency_target=_number(health, "latency_target", 0.95),
                    latency_threshold_ms=_number(health, "warning_latency_ms", 1_000.0),
                    period=timedelta(days=_positive_int(health, "period_days", 30)),
                ),
            )
            if settings.query.max_window < settings.query.default_window:
                raise DashboardConfigurationError("max_window 不能小于 default_window。")
            return settings
        except DashboardConfigurationError:
            raise
        except (ConfigurationError, TypeError, ValueError) as error:
            raise DashboardConfigurationError(str(error)) from error


def _required_text(source: Mapping[str, Any], key: str) -> str:
    """@brief 读取必填文本 / Read required text.

    @param source 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @return 修剪后的文本 / Stripped text.
    """

    value = source.get(key)
    if not isinstance(value, str) or not value.strip():
        raise DashboardConfigurationError(f"{key} 必须是非空字符串。")
    return value.strip()


def _text(source: Mapping[str, Any], key: str, default: str) -> str:
    """@brief 读取可选文本 / Read optional text.

    @param source 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param default 缺省值 / Default value.
    @return 修剪后的文本 / Stripped text.
    """

    value = source.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise DashboardConfigurationError(f"{key} 必须是非空字符串。")
    return value.strip()


def _optional_text(value: object) -> str | None:
    """@brief 读取可空文本 / Read nullable text.

    @param value 候选值 / Candidate value.
    @return 修剪文本或 None / Stripped text or ``None``.
    """

    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise DashboardConfigurationError("可空文本必须是非空字符串或 null。")
    return value.strip()


def _choice(
    source: Mapping[str, Any],
    key: str,
    allowed: set[str],
    *,
    default: str | None = None,
) -> str:
    """@brief 读取枚举配置 / Read an enumerated setting.

    @param source 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param allowed 允许值 / Allowed values.
    @param default 可选缺省值 / Optional default.
    @return 合法文本值 / Valid text value.
    """

    value = source.get(key, default)
    if not isinstance(value, str) or value not in allowed:
        raise DashboardConfigurationError(f"{key} 必须是 {sorted(allowed)} 之一。")
    return value


def _boolean(source: Mapping[str, Any], key: str, default: bool) -> bool:
    """@brief 读取布尔配置 / Read a boolean setting.

    @param source 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param default 缺省值 / Default value.
    @return 布尔值 / Boolean value.
    """

    value = source.get(key, default)
    if not isinstance(value, bool):
        raise DashboardConfigurationError(f"{key} 必须是布尔值。")
    return value


def _positive_int(source: Mapping[str, Any], key: str, default: int) -> int:
    """@brief 读取正整数 / Read a positive integer.

    @param source 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param default 缺省值 / Default value.
    @return 正整数 / Positive integer.
    """

    value = source.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise DashboardConfigurationError(f"{key} 必须是正整数。")
    return int(value)


def _non_negative_int(source: Mapping[str, Any], key: str, default: int) -> int:
    """@brief 读取非负整数 / Read a non-negative integer.

    @param source 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param default 缺省值 / Default value.
    @return 非负整数 / Non-negative integer.
    """

    value = source.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DashboardConfigurationError(f"{key} 必须是非负整数。")
    return int(value)


def _number(source: Mapping[str, Any], key: str, default: float) -> float:
    """@brief 读取数值配置 / Read a numeric setting.

    @param source 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param default 缺省值 / Default value.
    @return 浮点值 / Floating-point value.
    """

    value = source.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DashboardConfigurationError(f"{key} 必须是数值。")
    return float(value)


def _bounded_port(source: Mapping[str, Any], key: str, default: int) -> int:
    """@brief 读取 TCP 端口 / Read a TCP port.

    @param source 配置对象 / Configuration mapping.
    @param key 字段名 / Field name.
    @param default 缺省值 / Default value.
    @return 合法端口 / Valid port.
    """

    value = _positive_int(source, key, default)
    if value > 65_535:
        raise DashboardConfigurationError(f"{key} 必须不大于 65535。")
    return value


def _api_prefix(value: object) -> str:
    """@brief 校验 API 前缀 / Validate the API prefix.

    @param value 候选路径 / Candidate path.
    @return 规范路径 / Normalized path.
    """

    if not isinstance(value, str):
        raise DashboardConfigurationError("dashboard.api.prefix 必须是字符串。")
    prefix = value.strip()
    if not prefix.startswith("/") or prefix == "/" or prefix.endswith("/"):
        raise DashboardConfigurationError("dashboard.api.prefix 必须是非根绝对路径且无尾斜杠。")
    return prefix


__all__ = [
    "AccessMode",
    "DashboardAccessSettings",
    "DashboardApiSettings",
    "DashboardDatabaseSettings",
    "DashboardQuerySettings",
    "DashboardSettings",
    "DatabaseMode",
]
