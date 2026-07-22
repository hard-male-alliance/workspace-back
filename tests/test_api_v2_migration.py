"""Static safety checks for the API v2 migration audit ledger."""

from __future__ import annotations

from pathlib import Path


def test_api_v2_migration_audit_is_append_only_and_downgrade_safe() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260722_0008_api_v2_migration_audit.py"
    ).read_text(encoding="utf-8")
    assert 'revision = "20260722_0008"' in source
    assert 'down_revision = "20260721_0007"' in source
    assert "BEFORE UPDATE OR DELETE" in source
    assert "GRANT SELECT, INSERT" in source
    assert "source_snapshot_sha256" in source
    assert "cannot drop non-empty API migration audit ledger" in source
