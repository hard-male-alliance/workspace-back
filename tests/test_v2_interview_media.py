"""Interview V2 consent-gated media capture and finalization tests."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.infrastructure.interview_media import LocalInterviewMediaStore

NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_media_chunks_are_idempotent_bounded_and_finalize_to_verified_artifact(
    tmp_path: Path,
) -> None:
    root = tmp_path / "interview-media"
    store = LocalInterviewMediaStore(
        root,
        api_origin="https://api.example.test",
        maximum_chunk_bytes=64,
        maximum_session_bytes=128,
    )
    first = b"webm-header-and-cluster-1"
    second = b"webm-cluster-2"
    common = {
        "workspace_id": "workspace_media0001",
        "session_id": "session_media000001",
        "kind": "audio",
        "media_type": "audio/webm",
    }
    assert not await store.append(
        **common,
        input_id="input_media000001",
        sequence=1,
        content=first,
        sha256=hashlib.sha256(first).hexdigest(),
    )
    assert await store.append(
        **common,
        input_id="input_media000001",
        sequence=1,
        content=first,
        sha256=hashlib.sha256(first).hexdigest(),
    )
    assert not await store.append(
        **common,
        input_id="input_media000002",
        sequence=2,
        content=second,
        sha256=hashlib.sha256(second).hexdigest(),
    )
    recording = SimpleNamespace(
        record_audio=True,
        record_video=False,
        retention_until=NOW + timedelta(days=30),
    )
    session = SimpleNamespace(
        workspace_id="workspace_media0001",
        meta=SimpleNamespace(id="session_media000001", revision=4),
        spec=SimpleNamespace(recording=recording),
    )
    output = await store.finalize(
        session,
        operation_id="interview.end:operation_media0001",
    )
    assert len(output.artifacts) == 1
    artifact = output.artifacts[0]
    assert output.contents == (first + second,)
    assert artifact.size_bytes == len(first + second)
    assert artifact.sha256 == hashlib.sha256(first + second).hexdigest()
    assert artifact.media_type == "audio/webm"
    assert artifact.expires_at == recording.retention_until


@pytest.mark.asyncio
async def test_media_store_rejects_digest_and_idempotency_key_reuse(tmp_path: Path) -> None:
    root = tmp_path / "interview-media"
    store = LocalInterviewMediaStore(
        root,
        api_origin="https://api.example.test",
        maximum_chunk_bytes=64,
        maximum_session_bytes=128,
    )
    values = {
        "workspace_id": "workspace_media0001",
        "session_id": "session_media000001",
        "input_id": "input_media000001",
        "kind": "audio",
        "sequence": 1,
        "media_type": "audio/webm",
    }
    with pytest.raises(ValueError, match="digest"):
        await store.append(**values, content=b"first", sha256="0" * 64)
    await store.append(
        **values,
        content=b"first",
        sha256=hashlib.sha256(b"first").hexdigest(),
    )
    with pytest.raises(ValueError, match="input_id"):
        await store.append(
            **values,
            content=b"other",
            sha256=hashlib.sha256(b"other").hexdigest(),
        )
