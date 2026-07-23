"""@brief OAuth 登录会话绑定迁移与模型门禁 / OAuth login-session binding migration and model gates."""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

from backend.infrastructure.persistence.models import (
    OAuthAuthorizationCodeRecord,
    OAuthRefreshTokenFamilyRecord,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 仓库根目录 / Repository root."""

MIGRATION = (
    PROJECT_ROOT
    / "alembic"
    / "versions"
    / "20260723_0018_identity_session_token_binding.py"
)
"""@brief 会话令牌绑定迁移 / Session-token binding migration."""


def test_0018_is_linear_and_remains_on_the_single_migration_chain() -> None:
    """@brief 0018 必须保持在线性单头迁移链 / 0018 remains on the linear single-head chain."""

    configuration = Config()
    configuration.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    scripts = ScriptDirectory.from_config(configuration)

    assert scripts.get_heads() == ["20260723_0027"]
    script = scripts.get_revision("20260723_0018")
    assert script is not None
    assert script.down_revision == "20260723_0017"


def test_0018_fails_closed_for_unattributable_legacy_credentials() -> None:
    """@brief 旧凭据归属不可猜测，必须失效并追加审计 / Unattributable legacy credentials fail closed with audit."""

    source = MIGRATION.read_text(encoding="utf-8")
    consume = "SET consumed_at = transaction_timestamp() WHERE consumed_at IS NULL"
    revoke = "SET revoked_at = transaction_timestamp() WHERE revoked_at IS NULL"
    code_constraint = '"oauth_authorization_codes_active_session"'
    family_constraint = '"oauth_refresh_token_families_active_session"'

    assert consume in source
    assert revoke in source
    assert source.index(consume) < source.index(code_constraint)
    assert source.index(revoke) < source.index(family_constraint)
    assert "invalidated_unbound_authorization_codes" in source
    assert "revoked_unbound_refresh_families" in source
    assert "historical login-session ownership is unknowable" in source
    assert "cannot downgrade session-bound OAuth state" in source


def test_oauth_active_credentials_require_a_login_session_foreign_key() -> None:
    """@brief 活动 code/family 由类型映射和数据库约束绑定会话 / Active code/family records require a session FK."""

    code_table = OAuthAuthorizationCodeRecord.__table__
    family_table = OAuthRefreshTokenFamilyRecord.__table__

    assert code_table.c.login_session_id.nullable
    assert family_table.c.login_session_id.nullable
    assert {
        foreign_key.target_fullname for foreign_key in code_table.c.login_session_id.foreign_keys
    } == {"identity.identity_login_sessions.id"}
    assert {
        foreign_key.target_fullname
        for foreign_key in family_table.c.login_session_id.foreign_keys
    } == {"identity.identity_login_sessions.id"}
    code_checks = " ".join(
        str(constraint.sqltext)
        for constraint in code_table.constraints
        if hasattr(constraint, "sqltext")
    )
    family_checks = " ".join(
        str(constraint.sqltext)
        for constraint in family_table.constraints
        if hasattr(constraint, "sqltext")
    )
    assert "consumed_at IS NOT NULL OR login_session_id IS NOT NULL" in code_checks
    assert "revoked_at IS NOT NULL OR login_session_id IS NOT NULL" in family_checks
