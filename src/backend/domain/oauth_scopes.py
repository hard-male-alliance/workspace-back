"""@brief OAuth/OIDC scope 的唯一领域目录 / Single domain catalog for OAuth/OIDC scopes.

授权服务器注册校验、discovery 元数据与 Workspace 授权矩阵必须从本模块读取同一组
scope，避免出现“路由要求但 token 无法签发”或“公开声明却没有任何语义”的漂移。
"""

from __future__ import annotations

from enum import StrEnum


class OAuthScope(StrEnum):
    """@brief 产品支持的闭合 OAuth scope 集 / Closed set of product-supported OAuth scopes."""

    OPENID = "openid"
    """@brief OIDC 身份 scope / OIDC identity scope."""

    PROFILE = "profile"
    """@brief OIDC profile claims scope / OIDC profile-claims scope."""

    EMAIL = "email"
    """@brief OIDC email claims scope / OIDC email-claims scope."""

    OFFLINE_ACCESS = "offline_access"
    """@brief refresh token 授权 scope / Refresh-token authorization scope."""

    WORKSPACE_READ = "workspace.read"
    """@brief Workspace 通用读取 scope / General Workspace read scope."""

    WORKSPACE_WRITE = "workspace.write"
    """@brief Workspace 通用写入 scope / General Workspace write scope."""

    RESUME_READ = "resume.read"
    """@brief Resume 读取 scope / Resume read scope."""

    RESUME_WRITE = "resume.write"
    """@brief Resume 编辑与导入/恢复 scope / Resume editing and import/restore scope."""

    RESUME_RENDER = "resume.render"
    """@brief Resume 渲染能力 scope / Resume-rendering capability scope."""

    INTERVIEW_READ = "interview.read"
    """@brief Interview 读取 scope / Interview read scope."""

    INTERVIEW_WRITE = "interview.write"
    """@brief Interview 写入 scope / Interview write scope."""


SUPPORTED_OAUTH_SCOPES: tuple[str, ...] = tuple(scope.value for scope in OAuthScope)
"""@brief discovery、注册与签发共用的有序 scope 目录 / Ordered scope catalog shared by discovery, registration, and issuance."""

RESOURCE_OAUTH_SCOPES: tuple[str, ...] = tuple(
    scope.value for scope in OAuthScope if "." in scope.value
)
"""@brief Resource Server 对外发布的资源 scope / Resource scopes published by the Resource Server."""

SUPPORTED_OAUTH_SCOPE_SET: frozenset[str] = frozenset(SUPPORTED_OAUTH_SCOPES)
"""@brief 配置边界使用的闭合集合视图 / Closed-set view used at configuration boundaries."""


__all__ = [
    "RESOURCE_OAUTH_SCOPES",
    "SUPPORTED_OAUTH_SCOPES",
    "SUPPORTED_OAUTH_SCOPE_SET",
    "OAuthScope",
]
