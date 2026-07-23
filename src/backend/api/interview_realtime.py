"""@brief API V2 Interview 的私有 WebSocket signaling 数据面 / Private WebSocket signaling data plane for API V2 Interview.

公开 REST 契约只返回 signaling URL 与短期凭据；本模块承载该 URL 的受控 WebSocket
协议，但不进入 OpenAPI 路由集合。客户端必须先发送一次认证帧，随后只能发送封闭的
control 或 candidate_utterance 消息。所有正文持久化仍由 Interview 应用服务依据冻结的
Transcript consent 决定。
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Protocol, cast

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from backend.application.interview_v2 import (
    InterviewApplicationError,
    InterviewApplicationService,
)
from backend.application.ports.interview_v2 import (
    InterviewRealtimeCredentialVerifier,
    RealtimeInputKeyReused,
)
from backend.config import BackendSettings
from backend.domain.interview_v2 import (
    CandidateUtteranceInput,
    InterviewSessionId,
    RealtimeConnectionId,
    RealtimeControl,
    RealtimeControlInput,
    RealtimeInputEnvelope,
    RealtimeInputId,
    realtime_input_fingerprint,
)
from backend.domain.principals import WorkspaceId
from backend.domain.resources import ResourceRef
from workspace_shared.ids import new_opaque_id

router_interview_realtime = APIRouter()

_PROTOCOL = "aiws.interview.realtime.v2"
_AUTH_TIMEOUT_SECONDS = 5.0
_MAX_FRAME_BYTES = 32 * 1024
_OPAQUE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")


class _RealtimeContainer(Protocol):
    """@brief WebSocket adapter 所需的最小 lifespan 对象图 / Minimal lifespan graph required by the WebSocket adapter."""

    settings: BackendSettings
    interview_v2: InterviewApplicationService
    interview_realtime_verifier: InterviewRealtimeCredentialVerifier
    interview_media_store: _MediaCaptureStore


class _MediaCaptureStore(Protocol):
    """Minimal bounded media-capture port exposed to the WebSocket adapter."""

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
    ) -> bool: ...


@router_interview_realtime.websocket("/realtime/v2/interview")
async def interview_realtime(websocket: WebSocket) -> None:
    """@brief 验证一次性 grant 并把实时文字输入原子提交给 Interview / Verify a one-time grant and atomically submit realtime text input to Interview."""
    container = cast(_RealtimeContainer, websocket.app.state.container)
    disconnect_context: tuple[
        WorkspaceId,
        InterviewSessionId,
        ResourceRef,
        RealtimeConnectionId,
    ] | None = None
    if not _origin_allowed(websocket, container.settings.network.cors_allowed_origins):
        await websocket.close(code=4403, reason="origin_not_allowed")
        return
    if _PROTOCOL not in websocket.scope.get("subprotocols", ()):
        await websocket.close(code=4406, reason="subprotocol_required")
        return
    await websocket.accept(subprotocol=_PROTOCOL)
    try:
        auth = await asyncio.wait_for(_receive_object(websocket), _AUTH_TIMEOUT_SECONDS)
        workspace_id, session_id, audience, token = _authentication(auth)
        claims = await container.interview_realtime_verifier.verify(
            token,
            workspace_id=workspace_id,
            session_id=session_id,
            audience=audience,
        )
        if claims.get("transport") != "websocket":
            raise _ProtocolFailure("realtime.transport_mismatch")
        connection_id = RealtimeConnectionId(_opaque(claims.get("jti"), "connection_id"))
        disconnect_context = (workspace_id, session_id, audience, connection_id)
        connected = RealtimeControlInput(RealtimeControl.CONNECTED)
        receipt = await container.interview_v2.ingest_realtime_input(
            audience,
            _envelope(
                workspace_id,
                session_id,
                connection_id,
                RealtimeInputId(new_opaque_id("input")),
                connected,
            ),
        )
        await websocket.send_json(
            {
                "type": "authenticated",
                "connection_id": str(connection_id),
                "sequence": receipt.sequence,
            }
        )
        while True:
            frame = await _receive_object(websocket)
            if frame.get("type") == "media_chunk":
                header = _media_header(frame)
                content = await websocket.receive_bytes()
                await container.interview_v2.authorize_media_capture(
                    audience,
                    workspace_id,
                    session_id,
                    connection_id,
                    header[1],
                )
                replayed = await container.interview_media_store.append(
                    workspace_id=workspace_id,
                    session_id=str(session_id),
                    input_id=str(header[0]),
                    kind=header[1],
                    sequence=header[2],
                    media_type=header[3],
                    content=content,
                    sha256=header[4],
                )
                await websocket.send_json(
                    {
                        "type": "media_ack",
                        "input_id": str(header[0]),
                        "sequence": header[2],
                        "replayed": replayed,
                    }
                )
                continue
            input_id, payload = _input(frame)
            receipt = await container.interview_v2.ingest_realtime_input(
                audience,
                _envelope(
                    workspace_id,
                    session_id,
                    connection_id,
                    input_id,
                    payload,
                ),
            )
            await websocket.send_json(
                {
                    "type": "ack",
                    "input_id": str(input_id),
                    "sequence": receipt.sequence,
                    "replayed": receipt.replayed,
                }
            )
    except WebSocketDisconnect:
        if disconnect_context is not None:
            await _record_disconnect(container, disconnect_context)
        return
    except TimeoutError:
        await _fail(websocket, "realtime.authentication_timeout", 4408)
    except RealtimeInputKeyReused:
        await _fail(websocket, "realtime.input_id_reused", 4409)
    except InterviewApplicationError as error:
        await _fail(websocket, error.code, 4409)
    except (PermissionError, ValueError, TypeError, _ProtocolFailure):
        await _fail(websocket, "realtime.invalid_message", 4400)


class _ProtocolFailure(ValueError):
    """@brief 不向 peer 回显解析细节的协议失败 / Protocol failure whose parsing detail is never reflected to the peer."""


async def _receive_object(websocket: WebSocket) -> Mapping[str, object]:
    """@brief 读取一个有界 JSON object 帧 / Read one bounded JSON-object frame."""
    text = await websocket.receive_text()
    if len(text.encode("utf-8")) > _MAX_FRAME_BYTES:
        raise _ProtocolFailure("realtime.frame_too_large")
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise _ProtocolFailure("realtime.invalid_json") from error
    if not isinstance(value, dict):
        raise _ProtocolFailure("realtime.object_required")
    return cast(Mapping[str, object], value)


def _authentication(
    value: Mapping[str, object],
) -> tuple[WorkspaceId, InterviewSessionId, ResourceRef, str]:
    """@brief 解析首帧并保持 token 不进入 URL 或日志 / Parse the first frame while keeping the token out of URLs and logs."""
    if set(value) != {
        "type",
        "workspace_id",
        "session_id",
        "audience_id",
        "ephemeral_token",
    } or value.get("type") != "authenticate":
        raise _ProtocolFailure("realtime.authentication_required")
    workspace_id = WorkspaceId(_opaque(value.get("workspace_id"), "workspace_id"))
    session_id = InterviewSessionId(_opaque(value.get("session_id"), "session_id"))
    audience = ResourceRef("user", _opaque(value.get("audience_id"), "audience_id"))
    token = value.get("ephemeral_token")
    if not isinstance(token, str) or not 20 <= len(token) <= 8192:
        raise _ProtocolFailure("realtime.invalid_token")
    return workspace_id, session_id, audience, token


def _input(
    value: Mapping[str, object],
) -> tuple[RealtimeInputId, CandidateUtteranceInput | RealtimeControlInput]:
    """@brief 解析封闭的客户端 realtime 消息联合 / Parse the closed client realtime-message union."""
    kind = value.get("type")
    input_id = RealtimeInputId(_opaque(value.get("input_id"), "input_id"))
    if kind == "control":
        if set(value) != {"type", "input_id", "control"}:
            raise _ProtocolFailure("realtime.invalid_control")
        control = value.get("control")
        if not isinstance(control, str):
            raise _ProtocolFailure("realtime.invalid_control")
        return input_id, RealtimeControlInput(RealtimeControl(control))
    if kind == "candidate_utterance":
        if set(value) != {"type", "input_id", "text", "start_ms", "end_ms"}:
            raise _ProtocolFailure("realtime.invalid_utterance")
        text = value.get("text")
        start_ms = value.get("start_ms")
        end_ms = value.get("end_ms")
        if (
            not isinstance(text, str)
            or isinstance(start_ms, bool)
            or not isinstance(start_ms, int)
            or isinstance(end_ms, bool)
            or not isinstance(end_ms, int)
        ):
            raise _ProtocolFailure("realtime.invalid_utterance")
        return input_id, CandidateUtteranceInput(text, start_ms, end_ms)
    raise _ProtocolFailure("realtime.message_type_unsupported")


def _media_header(
    value: Mapping[str, object],
) -> tuple[RealtimeInputId, str, int, str, str]:
    """Parse the JSON header whose immediately following WebSocket message is binary content."""
    if set(value) != {
        "type",
        "input_id",
        "media_kind",
        "sequence",
        "media_type",
        "sha256",
    }:
        raise _ProtocolFailure("realtime.invalid_media_header")
    input_id = RealtimeInputId(_opaque(value.get("input_id"), "input_id"))
    kind = value.get("media_kind")
    sequence = value.get("sequence")
    media_type = value.get("media_type")
    digest = value.get("sha256")
    if (
        kind not in {"audio", "video"}
        or isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or sequence < 1
        or not isinstance(media_type, str)
        or not isinstance(digest, str)
        or re.fullmatch(r"[a-f0-9]{64}", digest) is None
    ):
        raise _ProtocolFailure("realtime.invalid_media_header")
    return input_id, kind, sequence, media_type, digest


def _envelope(
    workspace_id: WorkspaceId,
    session_id: InterviewSessionId,
    connection_id: RealtimeConnectionId,
    input_id: RealtimeInputId,
    payload: CandidateUtteranceInput | RealtimeControlInput,
) -> RealtimeInputEnvelope:
    """@brief 使用服务端接收时间和规范指纹构造应用输入 / Build an application input with server receive time and canonical fingerprint."""
    return RealtimeInputEnvelope(
        input_id,
        workspace_id,
        session_id,
        connection_id,
        datetime.now(UTC),
        payload,
        realtime_input_fingerprint(payload),
    )


def _opaque(value: object, label: str) -> str:
    """@brief 在访问持久层前校验契约级 opaque ID / Validate a contract-level opaque ID before persistence access."""
    if not isinstance(value, str) or _OPAQUE_ID.fullmatch(value) is None:
        raise _ProtocolFailure(f"realtime.invalid_{label}")
    return value


def _origin_allowed(websocket: WebSocket, allowed: tuple[str, ...]) -> bool:
    """@brief 浏览器有 Origin 时执行精确 allowlist；非浏览器 service client 可省略 / Enforce an exact browser Origin allowlist while allowing non-browser service clients to omit it."""
    origin = websocket.headers.get("origin")
    return origin is None or origin in allowed


async def _fail(websocket: WebSocket, code: str, close_code: int) -> None:
    """@brief 发送稳定脱敏错误并关闭连接 / Send a stable redacted error and close the connection."""
    try:
        await websocket.send_json({"type": "error", "code": code})
        await websocket.close(code=close_code, reason=code[:120])
    except (RuntimeError, OSError, WebSocketDisconnect):
        return


async def _record_disconnect(
    container: _RealtimeContainer,
    context: tuple[
        WorkspaceId,
        InterviewSessionId,
        ResourceRef,
        RealtimeConnectionId,
    ],
) -> None:
    """Best-effort durable disconnect marker; reconnect remains possible until lease expiry."""
    workspace_id, session_id, audience, connection_id = context
    payload = RealtimeControlInput(RealtimeControl.DISCONNECTED)
    try:
        await container.interview_v2.ingest_realtime_input(
            audience,
            _envelope(
                workspace_id,
                session_id,
                connection_id,
                RealtimeInputId(new_opaque_id("input")),
                payload,
            ),
        )
    except (InterviewApplicationError, OSError, RuntimeError):
        # The socket is already gone. Lease expiry and Session maintenance are the durable
        # recovery boundary, so a failed observability marker must not mask disconnect.
        return


__all__ = ["router_interview_realtime"]
