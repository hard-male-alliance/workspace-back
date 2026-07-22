"""Static safety checks for durable OAuth token persistence."""

from __future__ import annotations

from pathlib import Path


def test_oauth_token_migration_is_linear_hashed_and_reuse_aware() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260722_0010_oauth_tokens.py"
    ).read_text(encoding="utf-8")
    assert 'revision = "20260722_0010"' in source
    assert 'down_revision = "20260722_0009"' in source
    assert "code_hash" in source
    assert "token_hash" in source
    assert "reuse_detected_at" in source
    assert "jti_hash" in source
    assert "cannot drop non-empty OAuth token tables" in source
