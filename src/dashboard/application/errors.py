"""@brief Dashboard 应用层稳定异常 / Stable Dashboard application errors."""

from __future__ import annotations


class DashboardError(Exception):
    """@brief Dashboard 受控错误基类 / Base class for controlled Dashboard errors."""


class DashboardConfigurationError(DashboardError):
    """@brief Dashboard 配置无效 / Dashboard configuration is invalid."""


class DashboardAuthorizationError(DashboardError):
    """@brief 运维主体未获授权 / The operator principal is unauthorized."""


class DashboardQueryError(DashboardError):
    """@brief 查询语义或范围无效 / Query semantics or bounds are invalid."""


class DashboardReadStoreUnavailable(DashboardError):
    """@brief 可观测性读存储不可用 / The observability read store is unavailable."""


class DashboardDependencyError(DashboardError):
    """@brief 可选呈现依赖缺失 / An optional presentation dependency is missing."""


__all__ = [
    "DashboardAuthorizationError",
    "DashboardConfigurationError",
    "DashboardDependencyError",
    "DashboardError",
    "DashboardQueryError",
    "DashboardReadStoreUnavailable",
]
