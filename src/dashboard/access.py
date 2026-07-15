"""Dashboard 的显式运维身份与工作区访问边界（operator access boundary）。

Dashboard 是只读运维面而不是公开业务 API。它不会把查询参数中的 ``actor_id``
误认为已认证用户：内存演示使用显式 mock 身份；连接 PostgreSQL 时则要求受控的
operator token，并将身份固定为配置中的运维主体。
"""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from dataclasses import dataclass

from .config import DashboardSettings
from .errors import (
    DashboardAuthorizationError,
    DashboardConfigurationError,
    DashboardUnavailableError,
)
from .models import DashboardScope


@dataclass(frozen=True, slots=True)
class DashboardAccessPolicy:
    """@brief 将已验证运维身份映射到有界工作区查询（workspace query scope）。

    @param settings: 已校验的 Dashboard 配置快照。
    @param operator_token: ``operator_token`` 模式的内存凭证；绝不序列化、日志记录
        或返回给入口调用方。

    @note PostgreSQL 的 dashboard role 可以读取多个工作区的稳定视图。因此“可查任意
    指定工作区”是一个明确的、受 token 保护的全局运维权限，而不是无鉴权的租户绕过。
    """

    settings: DashboardSettings
    operator_token: str | None = None

    @classmethod
    def from_settings(
        cls,
        settings: DashboardSettings,
        environ: Mapping[str, str],
    ) -> DashboardAccessPolicy:
        """@brief 从配置与进程环境创建访问策略（access policy）。

        @param settings: Dashboard 不可变配置。
        @param environ: 可注入环境变量映射，便于测试且避免把 secret 写入配置文件。
        @return: 可用于本地 CLI/GUI 与 HTTP API 的 DashboardAccessPolicy。
        @raise DashboardConfigurationError: 生产 operator token 缺失或配置不合法时抛出。
        """

        if not isinstance(settings, DashboardSettings):
            raise DashboardConfigurationError("Dashboard access policy 需要 DashboardSettings。")
        if not isinstance(environ, Mapping):
            raise DashboardConfigurationError("环境变量映射必须是 Mapping。")

        if settings.operator_access_mode == "mock":
            return cls(settings=settings)

        token = environ.get(settings.operator_token_env)
        if not isinstance(token, str) or not token:
            raise DashboardConfigurationError(
                "Dashboard operator 凭证环境变量未设置："
                f"{settings.operator_token_env}"
            )
        return cls(settings=settings, operator_token=token)

    def scope_for_local_operator(
        self,
        workspace_id: str,
        *,
        requested_actor_id: str | None = None,
    ) -> DashboardScope:
        """@brief 为受控本地 CLI/GUI 进程创建工作区范围（local operator scope）。

        @param workspace_id: 必填的目标工作区；不支持“全部工作区”哨兵值。
        @param requested_actor_id: 可选审计字段；若提供，必须等于配置的 operator_id。
        @return: actor_id 固定为受控 operator_id 的 DashboardScope。
        @raise DashboardAuthorizationError: 调用方试图冒充另一身份时抛出。

        @note 本方法仅供本地进程入口使用。``operator_token`` 模式在组合根已要求
        环境中存在凭证；CLI/GUI 的本地进程边界即为该凭证的信任边界。
        """

        self._ensure_enabled()
        self._validate_requested_actor(requested_actor_id)
        return DashboardScope(workspace_id=workspace_id, actor_id=self.settings.operator_id)

    def scope_for_http_operator(
        self,
        workspace_id: str,
        *,
        presented_token: str | None,
        requested_actor_id: str | None = None,
    ) -> DashboardScope:
        """@brief 验证 HTTP operator 凭证后创建工作区范围（HTTP operator scope）。

        @param workspace_id: 必填的单一目标工作区。
        @param presented_token: HTTP header 中提供的 operator token；mock 模式必须为 None。
        @param requested_actor_id: 可选的审计请求值；仅允许等于配置 operator_id。
        @return: 使用配置 operator_id 的 DashboardScope。
        @raise DashboardAuthorizationError: 凭证不存在、错误或身份冒充时抛出。
        """

        self._ensure_enabled()
        self._validate_requested_actor(requested_actor_id)
        if self.settings.operator_access_mode == "mock":
            if presented_token is not None:
                raise DashboardAuthorizationError("mock Dashboard 不接受 operator token。")
            return DashboardScope(workspace_id=workspace_id, actor_id=self.settings.operator_id)

        if self.operator_token is None:
            raise DashboardAuthorizationError("Dashboard operator 凭证不可用。")
        if not isinstance(presented_token, str) or not secrets.compare_digest(
            self.operator_token,
            presented_token,
        ):
            raise DashboardAuthorizationError("Dashboard operator 凭证无效。")
        return DashboardScope(workspace_id=workspace_id, actor_id=self.settings.operator_id)

    def _ensure_enabled(self) -> None:
        """@brief 在读数据前拒绝已禁用的 Dashboard。

        @return: 无返回值。
        @raise DashboardUnavailableError: dashboard.enabled 为 false 时抛出。
        """

        if not self.settings.enabled:
            raise DashboardUnavailableError("Dashboard 当前被配置为禁用。")

    def _validate_requested_actor(self, requested_actor_id: str | None) -> None:
        """@brief 防止请求参数伪造审计身份（actor spoofing）。

        @param requested_actor_id: 来自 CLI 或 HTTP 的可选身份声明。
        @return: 无返回值。
        @raise DashboardAuthorizationError: 与配置身份不一致时抛出。
        """

        if requested_actor_id is None:
            return
        if not isinstance(requested_actor_id, str):
            raise DashboardAuthorizationError("Dashboard operator 身份必须是字符串。")
        if not secrets.compare_digest(requested_actor_id.strip(), self.settings.operator_id):
            raise DashboardAuthorizationError("不能以其他 actor_id 查询 Dashboard。")


__all__ = ["DashboardAccessPolicy"]
