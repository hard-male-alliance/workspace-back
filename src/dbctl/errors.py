"""@brief dbctl 的受控错误类型 / Controlled error types for dbctl."""

from __future__ import annotations


class DbctlError(RuntimeError):
    """@brief dbctl 基础异常 / Base exception for dbctl.

    @note 该层异常的消息必须可安全展示，绝不能附带 DSN、密码或数据库原始错误。
    / Messages at this layer must be safe to show and must never contain a DSN,
    password, or raw database error.
    """


class DbctlConfigurationError(DbctlError):
    """@brief dbctl 配置无效 / Invalid dbctl configuration."""


class UnsafeIdentifierError(DbctlConfigurationError):
    """@brief PostgreSQL 标识符不安全 / Unsafe PostgreSQL identifier."""


class DbctlDependencyError(DbctlError):
    """@brief 可选运行依赖不可用 / Required runtime dependency is unavailable."""


class BootstrapExecutionError(DbctlError):
    """@brief PostgreSQL bootstrap 执行失败 / PostgreSQL bootstrap execution failed.

    @note 底层异常可作为 ``__cause__`` 保留给受控调用方，但不会拼接到消息中，
    以免意外泄露连接凭证。
    / The underlying exception may be retained as ``__cause__`` for controlled
    callers, but is never interpolated into the message to avoid credential leaks.
    """


class MigrationExecutionError(DbctlError):
    """@brief Alembic 迁移执行失败 / Alembic migration execution failed.

    @note 消息不拼接 Alembic、SQLAlchemy 或数据库的原始异常，避免 DSN 或密码出现在
    CLI 输出中。
    / The message does not interpolate raw Alembic, SQLAlchemy, or database errors, preventing a
    DSN or password from appearing in CLI output.
    """


class TelemetryRetentionExecutionError(DbctlError):
    """@brief 遥测保留期清理失败 / Telemetry-retention pruning failed.

    @note 此异常消息只描述可恢复的运维动作，绝不拼接 psycopg、PostgreSQL、DSN、
    原始 SQL 或认证错误内容。
    / This exception message describes only recoverable operator action and never interpolates
    psycopg, PostgreSQL, DSN, raw SQL, or authentication-error content.
    """


class DatabaseAlreadyExistsError(BootstrapExecutionError):
    """@brief 并发 bootstrap 已创建目标数据库 / Target database was created concurrently."""
