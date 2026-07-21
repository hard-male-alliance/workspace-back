"""@brief dbctl 领域错误 / Domain errors for dbctl."""


class DomainError(ValueError):
    """@brief 领域不变量被破坏 / A domain invariant was violated."""


class InvalidNameError(DomainError):
    """@brief PostgreSQL 名称不满足可移植约束 / A PostgreSQL name is not portable."""


class InvalidRoleSetError(DomainError):
    """@brief 数据库角色集合违反隔离约束 / A database role set violates isolation rules."""


class InvalidDatabaseModelError(DomainError):
    """@brief 数据库领域模型内部不一致 / The database domain model is inconsistent."""


class InvalidRetentionPolicyError(DomainError):
    """@brief 遥测保留策略或执行边界无效 / A telemetry policy or execution bound is invalid."""
