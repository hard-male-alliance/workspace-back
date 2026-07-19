"""Private local-filesystem blob adapter for knowledge-source uploads."""

from __future__ import annotations

import asyncio
import hashlib
import os
import secrets
from pathlib import Path, PurePosixPath

from backend.domain.knowledge import StoredKnowledgeBlob
from workspace_shared.tenancy import ActorScope


class KnowledgeBlobNotFoundError(FileNotFoundError):
    """A scoped storage key does not resolve to an existing blob."""


class LocalKnowledgeBlobStorage:
    """Content-addressed local storage with tenant-scoped opaque keys."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    async def put(
        self,
        scope: ActorScope,
        file_id: str,
        filename: str,
        content_type: str,
        content: bytes,
    ) -> StoredKnowledgeBlob:
        """Atomically write a bounded upload under a non-user-controlled path."""
        sha256 = hashlib.sha256(content).hexdigest()
        suffix = Path(filename).suffix.lower()
        relative = PurePosixPath(
            self._scope_token(scope),
            hashlib.sha256(file_id.encode("utf-8")).hexdigest()[:24],
            f"{sha256}{suffix}",
        )
        path = self._path_for(scope, relative.as_posix())
        await asyncio.to_thread(self._write_atomic, path, content)
        return StoredKnowledgeBlob(
            file_id=file_id,
            storage_key=relative.as_posix(),
            filename=filename,
            content_type=content_type,
            sha256=sha256,
            size_bytes=len(content),
        )

    async def read(self, scope: ActorScope, storage_key: str) -> bytes:
        """Read a tenant-scoped blob without permitting path traversal."""
        path = self._path_for(scope, storage_key)
        try:
            return await asyncio.to_thread(path.read_bytes)
        except FileNotFoundError as error:
            raise KnowledgeBlobNotFoundError(storage_key) from error

    async def delete(self, scope: ActorScope, storage_key: str) -> None:
        """Delete an owned blob; missing blobs are already in the desired state."""
        path = self._path_for(scope, storage_key)
        await asyncio.to_thread(path.unlink, True)

    def _path_for(self, scope: ActorScope, storage_key: str) -> Path:
        relative = PurePosixPath(storage_key)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("knowledge storage key is invalid")
        if not relative.parts or relative.parts[0] != self._scope_token(scope):
            raise ValueError("knowledge storage key does not belong to this actor scope")
        candidate = self._root.joinpath(*relative.parts).resolve()
        if candidate != self._root and self._root not in candidate.parents:
            raise ValueError("knowledge storage key escapes its configured root")
        return candidate

    @staticmethod
    def _scope_token(scope: ActorScope) -> str:
        identity = f"{scope.workspace_id}\0{scope.resource_owner_id}".encode()
        return hashlib.sha256(identity).hexdigest()[:32]

    @staticmethod
    def _write_atomic(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != content:
                raise RuntimeError("content-addressed knowledge blob hash collision")
            return
        temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
        try:
            temporary.write_bytes(content)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


__all__ = ["KnowledgeBlobNotFoundError", "LocalKnowledgeBlobStorage"]
