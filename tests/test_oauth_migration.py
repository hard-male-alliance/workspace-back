"""Static safety checks for OAuth authorization transaction persistence."""

from __future__ import annotations

from pathlib import Path


def test_oauth_authorization_migration_is_linear_and_pkce_only() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260722_0009_oauth_authorization_requests.py"
    ).read_text(encoding="utf-8")
    assert 'revision = "20260722_0009"' in source
    assert 'down_revision = "20260722_0008"' in source
    assert "code_challenge_method = 'S256'" in source
    assert "GRANT SELECT, INSERT, UPDATE, DELETE" in source
    assert "cannot drop non-empty OAuth authorization request table" in source
