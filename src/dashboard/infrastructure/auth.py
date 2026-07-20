"""@brief Dashboard 运维主体认证 / Dashboard operator authentication."""

from __future__ import annotations

import secrets
from collections.abc import Mapping

from dashboard.application.errors import DashboardAuthorizationError, DashboardConfigurationError
from dashboard.domain.model import OperatorPrincipal

from .config import DashboardAccessSettings


class OperatorAuthenticator:
    """@brief 将可信入口凭证解析为主体 / Resolve trusted-boundary credentials into a principal.

    @param settings 运维身份设置 / Operator identity settings.
    @param environ 进程环境快照 / Process-environment snapshot.
    """

    def __init__(self, settings: DashboardAccessSettings, environ: Mapping[str, str]) -> None:
        """@brief 创建认证器且不暴露 secret / Create an authenticator without exposing its secret.

        @param settings 身份设置 / Identity settings.
        @param environ 环境变量 / Environment variables.
        @return 新认证器 / New authenticator.
        """

        self._settings = settings
        self._principal = OperatorPrincipal(settings.operator_id)
        self._token: str | None = None
        if settings.mode == "operator_token":
            token = environ.get(settings.token_env)
            if not isinstance(token, str) or not token:
                raise DashboardConfigurationError("Dashboard operator token 环境变量未设置。")
            self._token = token

    @property
    def token_header(self) -> str:
        """@brief 返回 HTTP token header 名 / Return the HTTP token-header name.

        @return 安全 header 名 / Safe header name.
        """

        return self._settings.token_header

    def authenticate_local(self) -> OperatorPrincipal:
        """@brief 认证受控本地 CLI/GUI 进程 / Authenticate the controlled local CLI/GUI process.

        @return 配置绑定的运维主体 / Configuration-bound operator principal.

        @note operator_token 模式已在构造时要求 secret 存在；本地 OS 进程边界负责限制访问。
        / In operator-token mode the secret is required at construction; the local OS process boundary restricts access.
        """

        return self._principal

    def authenticate_http(self, presented_token: str | None) -> OperatorPrincipal:
        """@brief 认证 HTTP 请求 / Authenticate an HTTP request.

        @param presented_token 请求 header 中的 token / Token from the request header.
        @return 已认证运维主体 / Authenticated operator principal.
        """

        if self._settings.mode == "mock":
            if presented_token is not None:
                raise DashboardAuthorizationError("mock Dashboard 不接受 operator token。")
            return self._principal
        if self._token is None or not isinstance(presented_token, str):
            raise DashboardAuthorizationError("Dashboard operator token 无效。")
        if not secrets.compare_digest(self._token, presented_token):
            raise DashboardAuthorizationError("Dashboard operator token 无效。")
        return self._principal


__all__ = ["OperatorAuthenticator"]
