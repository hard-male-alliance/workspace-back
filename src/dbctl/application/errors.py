"""@brief dbctl 应用层错误 / Application-layer errors for dbctl."""


class ApplicationError(RuntimeError):
    """@brief dbctl 用例未能完成 / A dbctl use case could not complete."""


class DbctlConfigurationError(ApplicationError, ValueError):
    """@brief 外部配置无法构造合法领域模型 / External configuration cannot form a valid model."""


class DatabaseAlreadyExistsError(ApplicationError):
    """@brief 并发创建发现数据库已存在 / Concurrent creation found the database already exists.

    @note 仅 infrastructure runner 应抛出；bootstrap 用例只在条件创建阶段消费该信号。
    / Only an infrastructure runner should raise this signal; bootstrap consumes it solely for
    conditional database creation.
    """


class BootstrapExecutionError(ApplicationError):
    """@brief bootstrap 计划或执行失败 / Bootstrap planning or execution failed."""


class MigrationExecutionError(ApplicationError):
    """@brief 数据库迁移执行失败 / Database migration execution failed."""


class RetentionExecutionError(ApplicationError):
    """@brief 遥测保留清理执行失败 / Telemetry-retention pruning failed."""


class ShellExecutionError(ApplicationError):
    """@brief 交互式数据库 shell 执行失败 / Interactive database-shell execution failed."""
