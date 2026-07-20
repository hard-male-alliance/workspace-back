"""@brief 产物值语义 / Artifact value semantics."""

from __future__ import annotations

import hashlib


def artifact_sha256(content: bytes) -> str:
    """@brief 计算产物摘要 / Compute an artifact digest.

    @param content 二进制内容 / Binary content.
    @return 小写 SHA-256 / Lowercase SHA-256.
    """

    return hashlib.sha256(content).hexdigest()
