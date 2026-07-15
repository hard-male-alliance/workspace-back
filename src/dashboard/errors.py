"""Dashboard 的领域异常（domain exceptions）。"""

from __future__ import annotations


class DashboardError(Exception):
    """@brief Dashboard 基础异常（base exception）。

    @param message: 面向调用方的错误说明。
    """


class DashboardConfigurationError(DashboardError):
    """@brief Dashboard 配置（configuration）无效或无法读取时抛出的异常。

    @param message: 配置错误的具体说明。
    """


class DashboardValidationError(DashboardError):
    """@brief Dashboard 查询或本地模型（local model）不满足约束时抛出的异常。

    @param message: 校验失败的具体说明。
    """


class DashboardAuthorizationError(DashboardError):
    """@brief Dashboard 运维身份（operator identity）未获授权时抛出的异常。

    @param message: 不包含凭证、DSN 或租户数据的安全错误说明。

    @note 该异常专用于入口边界；调用方必须将它映射为认证/授权失败，而不是把
    任意 ``actor_id`` 当作已验证身份。
    """


class DashboardUnavailableError(DashboardError):
    """@brief Dashboard 被禁用或暂不可服务时抛出的异常。

    @param message: 面向运维者的安全可用性说明。
    """


class DashboardDataError(DashboardError):
    """@brief 可观测性数据（observability data）形状不符合读取协议时抛出的异常。

    @param message: 数据错误的具体说明。
    """


class DashboardDependencyError(DashboardError):
    """@brief 可选运行时依赖（optional runtime dependency）缺失时抛出的异常。

    @param message: 缺失依赖及其安装或替代方式。
    """
