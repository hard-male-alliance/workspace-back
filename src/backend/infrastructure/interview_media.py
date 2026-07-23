"""Durable, consent-gated local capture and finalization for Interview media."""

from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path

from backend.application.ports.interview_v2 import (
    EndSessionOutput,
    InterviewWorkerOperationId,
    InterviewWorkerPortFailure,
)
from backend.domain.interview_v2 import InterviewSession
from backend.domain.platform import (
    ApiArtifactContentUrl,
    Artifact,
    ArtifactId,
    ArtifactKind,
)
from backend.domain.principals import ResourceMeta, WorkspaceId
from backend.domain.resources import ResourceRef

_MEDIA_TYPES = {
    ("audio", "audio/webm"): ("webm", ArtifactKind.INTERVIEW_AUDIO),
    ("audio", "audio/ogg"): ("ogg", ArtifactKind.INTERVIEW_AUDIO),
    ("audio", "audio/mp4"): ("m4a", ArtifactKind.INTERVIEW_AUDIO),
    ("video", "video/webm"): ("webm", ArtifactKind.INTERVIEW_VIDEO),
    ("video", "video/mp4"): ("mp4", ArtifactKind.INTERVIEW_VIDEO),
}


class LocalInterviewMediaStore:
    """Store bounded MediaRecorder chunks and finalize them into managed Artifacts.

    The directory must be a shared persistent volume when more than one backend worker is used.
    Chunk publication uses ``O_EXCL`` plus ``os.replace`` so a crash cannot expose a partial chunk.
    """

    def __init__(
        self,
        root: Path,
        *,
        api_origin: str,
        maximum_chunk_bytes: int,
        maximum_session_bytes: int,
    ) -> None:
        self._root = root.resolve()
        self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._api_origin = api_origin
        self._maximum_chunk_bytes = maximum_chunk_bytes
        self._maximum_session_bytes = maximum_session_bytes
        self._lock = asyncio.Lock()

    async def append(
        self,
        *,
        workspace_id: WorkspaceId,
        session_id: str,
        input_id: str,
        kind: str,
        sequence: int,
        media_type: str,
        content: bytes,
        sha256: str,
    ) -> bool:
        """Append one idempotent chunk; return ``True`` for an exact replay."""
        if (kind, media_type) not in _MEDIA_TYPES or sequence < 1:
            raise ValueError("unsupported Interview media chunk")
        if not content or len(content) > self._maximum_chunk_bytes:
            raise ValueError("Interview media chunk size is invalid")
        digest = hashlib.sha256(content).hexdigest()
        if digest != sha256:
            raise ValueError("Interview media chunk digest mismatch")
        extension, _ = _MEDIA_TYPES[(kind, media_type)]
        directory = self._directory(str(workspace_id), session_id, kind)
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        target = directory / f"{sequence:012d}-{input_id}-{digest}.{extension}"
        async with self._lock:
            existing = tuple(directory.glob(f"*-{input_id}-*"))
            if existing:
                if existing == (target,) and target.read_bytes() == content:
                    return True
                raise ValueError("Interview media input_id was reused")
            if tuple(directory.glob(f"{sequence:012d}-*")):
                raise ValueError("Interview media sequence was reused")
            total = sum(item.stat().st_size for item in directory.parent.rglob("*") if item.is_file())
            if total + len(content) > self._maximum_session_bytes:
                raise ValueError("Interview media Session budget exceeded")
            temporary = directory / f".{target.name}.tmp-{os.getpid()}"
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        return False

    async def finalize(
        self,
        session: InterviewSession,
        *,
        operation_id: InterviewWorkerOperationId,
    ) -> EndSessionOutput:
        """Build immutable managed Artifacts from the frozen consent snapshot."""
        del operation_id
        artifacts: list[Artifact] = []
        contents: list[bytes] = []
        requested = (
            ("audio", session.spec.recording.record_audio),
            ("video", session.spec.recording.record_video),
        )
        async with self._lock:
            for kind, enabled in requested:
                if not enabled:
                    continue
                directory = self._directory(
                    str(session.workspace_id), str(session.meta.id), kind
                )
                files = sorted(directory.glob("*")) if directory.is_dir() else []
                if not files:
                    raise InterviewWorkerPortFailure(
                        "interview.media_capture_missing",
                        retryable=False,
                    )
                extensions = {item.suffix for item in files}
                if len(extensions) != 1:
                    raise InterviewWorkerPortFailure(
                        "interview.media_capture_mixed_format",
                        retryable=False,
                    )
                content = b"".join(item.read_bytes() for item in files)
                media_type, artifact_kind = self._media_identity(kind, files[0].suffix)
                now = datetime.now(UTC)
                artifact_id = ArtifactId(
                    "artifact_"
                    + hashlib.sha256(
                        f"{session.workspace_id}:{session.meta.id}:{kind}".encode()
                    ).hexdigest()[:32]
                )
                digest = hashlib.sha256(content).hexdigest()
                artifacts.append(
                    Artifact(
                        ResourceMeta(artifact_id, 1, now, now),
                        session.workspace_id,
                        artifact_kind,
                        ResourceRef(
                            "interview_session",
                            session.meta.id,
                            session.meta.revision,
                        ),
                        media_type,
                        len(content),
                        digest,
                        ApiArtifactContentUrl.build(
                            self._api_origin,
                            session.workspace_id,
                            artifact_id,
                        ),
                        None,
                        session.spec.recording.retention_until,
                    )
                )
                contents.append(content)
        return EndSessionOutput(tuple(artifacts), tuple(contents))

    def _directory(self, workspace_id: str, session_id: str, kind: str) -> Path:
        for value in (workspace_id, session_id, kind):
            if not value or "/" in value or "\\" in value or value in {".", ".."}:
                raise ValueError("Interview media path identity is invalid")
        return self._root / workspace_id / session_id / kind

    @staticmethod
    def _media_identity(kind: str, suffix: str) -> tuple[str, ArtifactKind]:
        for (candidate_kind, media_type), (extension, artifact_kind) in _MEDIA_TYPES.items():
            if candidate_kind == kind and suffix == f".{extension}":
                return media_type, artifact_kind
        raise InterviewWorkerPortFailure(
            "interview.media_capture_format_invalid",
            retryable=False,
        )


__all__ = ["LocalInterviewMediaStore"]
