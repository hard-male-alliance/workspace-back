"""Static safety checks for hosted identity persistence."""

from pathlib import Path


def test_hosted_identity_migration_is_linear_and_hashes_browser_secrets() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "alembic/versions/20260722_0011_hosted_identity_flows.py"
    ).read_text(encoding="utf-8")
    assert 'revision = "20260722_0011"' in source
    assert 'down_revision = "20260722_0010"' in source
    assert "browser_secret_hash" in source
    assert "csrf_token_hash" in source
    assert "cannot drop non-empty hosted identity tables" in source
