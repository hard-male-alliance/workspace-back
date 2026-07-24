"""@brief API V2 Interview WebSocket 数据面测试 / API V2 Interview WebSocket data-plane tests."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import backend.api.interview_realtime as realtime_module
from backend.api.interview_realtime import router_interview_realtime
from backend.application.interview_v2 import RealtimeCoachingContext
from backend.domain.interview_v2 import (
    CandidateUtteranceInput,
    RealtimeControl,
    RealtimeControlInput,
    RealtimeInputEnvelope,
    RealtimeInputReceipt,
)
from backend.domain.resources import ResourceRef

WORKSPACE_ID = "workspace_realtime01"
SESSION_ID = "session_realtime0001"
USER_ID = "user_realtime000001"
CONNECTION_ID = "connection_realtime01"
TOKEN = "token-realtime-credential-000001"
PROTOCOL = "aiws.interview.realtime.v2"


@dataclass(slots=True)
class _Verifier:
    """@brief 记录严格 binding 的验签 fake / Verification fake recording exact bindings."""

    calls: list[tuple[str, str, str, str]] = field(default_factory=list)

    async def verify(
        self,
        token: str,
        *,
        workspace_id: str,
        session_id: str,
        audience: ResourceRef,
    ) -> dict[str, object]:
        self.calls.append(
            (
                token,
                str(workspace_id),
                str(session_id),
                audience.id,
            )
        )
        if token != TOKEN:
            raise PermissionError("bad token")
        return {"jti": CONNECTION_ID, "transport": "websocket"}


@dataclass(slots=True)
class _Service:
    """@brief 模拟原子 ledger 重放并记录明文只进入应用端口 / Fake atomic ledger replay while recording plaintext only at the application port."""

    calls: list[RealtimeInputEnvelope] = field(default_factory=list)
    receipts: dict[str, tuple[str, int]] = field(default_factory=dict)
    media_authorizations: list[tuple[str, str, str, str, str]] = field(
        default_factory=list
    )
    coaching_media_kinds: list[str | None] = field(default_factory=list)

    async def ingest_realtime_input(
        self, audience: ResourceRef, envelope: RealtimeInputEnvelope
    ) -> RealtimeInputReceipt:
        assert audience.id == USER_ID
        self.calls.append(envelope)
        key = str(envelope.input_id)
        prior = self.receipts.get(key)
        if prior is not None:
            assert prior[0] == envelope.fingerprint_sha256
            return RealtimeInputReceipt(prior[1], True)
        sequence = len(self.receipts) + 1
        self.receipts[key] = (envelope.fingerprint_sha256, sequence)
        return RealtimeInputReceipt(sequence, False)

    async def authorize_media_capture(
        self,
        audience: ResourceRef,
        workspace_id: str,
        session_id: str,
        connection_id: str,
        kind: str,
    ) -> None:
        self.media_authorizations.append(
            (
                audience.id,
                str(workspace_id),
                str(session_id),
                str(connection_id),
                kind,
            )
        )

    async def prepare_realtime_coaching(
        self,
        audience: ResourceRef,
        workspace_id: str,
        session_id: str,
        connection_id: str,
        *,
        media_kind: str | None = None,
    ) -> RealtimeCoachingContext:
        assert audience.id == USER_ID
        assert str(workspace_id) == WORKSPACE_ID
        assert str(session_id) == SESSION_ID
        assert str(connection_id) == CONNECTION_ID
        self.coaching_media_kinds.append(media_kind)
        return RealtimeCoachingContext(
            "Backend interview",
            "Assess practical backend engineering",
            "technical",
            "medium",
            ("Python", "databases"),
            "zh-CN",
            True,
            (),
            "global",
        )


@dataclass(slots=True)
class _MediaStore:
    calls: list[dict[str, object]] = field(default_factory=list)

    async def append(self, **values: object) -> bool:
        self.calls.append(values)
        return len(self.calls) > 1


@dataclass(slots=True)
class _Coach:
    transcriptions: list[bytes] = field(default_factory=list)
    frames: list[bytes] = field(default_factory=list)
    followups: list[tuple[str, str | None]] = field(default_factory=list)
    histories: list[tuple[tuple[str, str], ...]] = field(default_factory=list)

    async def transcribe_audio(
        self,
        content: bytes,
        media_type: str,
        locale: str,
        *,
        operation_id: str,
    ) -> str:
        assert media_type == "audio/webm"
        assert locale == "zh-CN"
        assert operation_id.startswith("input_")
        self.transcriptions.append(content)
        return "我使用指标和日志定位了数据库连接池耗尽。"

    async def observe_frame(
        self,
        content: bytes,
        media_type: str,
        *,
        operation_id: str,
    ) -> str:
        assert media_type == "image/jpeg"
        assert operation_id.startswith("input_")
        self.frames.append(content)
        return "候选人正在展示一张系统架构图。"

    async def stream_followup(
        self,
        context: RealtimeCoachingContext,
        candidate_text: str,
        visual_observation: str | None,
        live_history: tuple[tuple[str, str], ...],
        *,
        operation_id: str,
    ) -> AsyncIterator[str]:
        assert context.interview_type == "technical"
        assert operation_id.endswith(":followup")
        self.followups.append((candidate_text, visual_observation))
        self.histories.append(live_history)
        yield "你如何确认"
        yield "连接池是根因？"


@dataclass(slots=True)
class _Container:
    settings: object
    interview_v2: _Service
    interview_realtime_verifier: _Verifier
    interview_media_store: _MediaStore
    interview_realtime_coach: _Coach


def _client(*, origins: tuple[str, ...] = ("http://127.0.0.1:5173",)) -> tuple[TestClient, _Container]:
    app = FastAPI()
    app.include_router(router_interview_realtime)
    container = _Container(
        SimpleNamespace(
            network=SimpleNamespace(cors_allowed_origins=origins),
            interview=SimpleNamespace(
                media_analysis_max_audio_bytes=1024 * 1024,
                video_frame_max_bytes=1024 * 1024,
            ),
        ),
        _Service(),
        _Verifier(),
        _MediaStore(),
        _Coach(),
    )
    app.state.container = container
    return TestClient(app), container


def _authenticate(socket: Any, *, token: str = TOKEN) -> None:
    socket.send_json(
        {
            "type": "authenticate",
            "workspace_id": WORKSPACE_ID,
            "session_id": SESSION_ID,
            "audience_id": USER_ID,
            "ephemeral_token": token,
        }
    )


def test_websocket_auth_control_text_and_replay_flow() -> None:
    """@brief 首帧验签、激活、文字输入与幂等 replay 全部经过应用层 / First-frame verification, activation, text input, and idempotent replay all cross the application boundary."""
    client, container = _client()
    with client.websocket_connect(
        "/realtime/v2/interview",
        subprotocols=[PROTOCOL],
        headers={"Origin": "http://127.0.0.1:5173"},
    ) as socket:
        _authenticate(socket)
        authenticated = socket.receive_json()
        assert authenticated == {
            "type": "authenticated",
            "connection_id": CONNECTION_ID,
            "sequence": 1,
        }
        control = {
            "type": "control",
            "input_id": "input_media_started01",
            "control": "media_started",
        }
        socket.send_json(control)
        assert socket.receive_json() == {
            "type": "ack",
            "input_id": "input_media_started01",
            "sequence": 2,
            "replayed": False,
        }
        utterance = {
            "type": "candidate_utterance",
            "input_id": "input_candidate00001",
            "text": "请介绍一次后端故障排查经历。",
            "start_ms": 100,
            "end_ms": 2400,
        }
        socket.send_json(utterance)
        assert socket.receive_json()["replayed"] is False
        assert socket.receive_json() == {
            "type": "interviewer_start",
            "in_reply_to": "input_candidate00001",
        }
        assert socket.receive_json()["delta"] == "你如何确认"
        assert socket.receive_json()["delta"] == "连接池是根因？"
        assert socket.receive_json() == {
            "type": "interviewer_followup",
            "in_reply_to": "input_candidate00001",
            "text": "你如何确认连接池是根因？",
        }
        socket.send_json(utterance)
        assert socket.receive_json() == {
            "type": "ack",
            "input_id": "input_candidate00001",
            "sequence": 3,
            "replayed": True,
        }

    assert container.interview_realtime_verifier.calls == [
        (TOKEN, WORKSPACE_ID, SESSION_ID, USER_ID)
    ]
    assert isinstance(container.interview_v2.calls[0].payload, RealtimeControlInput)
    assert container.interview_v2.calls[0].payload.control is RealtimeControl.CONNECTED
    assert isinstance(container.interview_v2.calls[1].payload, RealtimeControlInput)
    assert container.interview_v2.calls[1].payload.control is RealtimeControl.MEDIA_STARTED
    assert isinstance(container.interview_v2.calls[2].payload, CandidateUtteranceInput)
    assert container.interview_v2.calls[2].payload.text == "请介绍一次后端故障排查经历。"
    assert isinstance(container.interview_v2.calls[-1].payload, RealtimeControlInput)
    assert (
        container.interview_v2.calls[-1].payload.control
        is RealtimeControl.DISCONNECTED
    )
    assert container.interview_realtime_coach.followups == [
        ("请介绍一次后端故障排查经历。", None)
    ]


def test_websocket_streams_audio_transcript_and_contextual_followup() -> None:
    """Ordered audio fragments are transcribed once on final and produce a streamed question."""
    client, container = _client()
    chunks = (b"webm-header", b"webm-final")
    with client.websocket_connect(
        "/realtime/v2/interview",
        subprotocols=[PROTOCOL],
        headers={"Origin": "http://127.0.0.1:5173"},
    ) as socket:
        _authenticate(socket)
        socket.receive_json()
        for sequence, content in enumerate(chunks, start=1):
            socket.send_json(
                {
                    "type": "realtime_audio",
                    "input_id": "input_audio_answer01",
                    "sequence": sequence,
                    "final": sequence == len(chunks),
                    "media_type": "audio/webm",
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "start_ms": 1_000,
                    "end_ms": 4_000,
                }
            )
            socket.send_bytes(content)
            assert socket.receive_json() == {
                "type": "audio_ack",
                "input_id": "input_audio_answer01",
                "sequence": sequence,
                "final": sequence == len(chunks),
            }
        transcript = socket.receive_json()
        assert transcript["type"] == "transcript_final"
        assert transcript["text"] == "我使用指标和日志定位了数据库连接池耗尽。"
        assert socket.receive_json()["type"] == "interviewer_start"
        assert socket.receive_json()["type"] == "interviewer_delta"
        assert socket.receive_json()["type"] == "interviewer_delta"
        assert socket.receive_json()["type"] == "interviewer_followup"

    assert container.interview_realtime_coach.transcriptions == [b"".join(chunks)]
    assert isinstance(container.interview_v2.calls[1].payload, CandidateUtteranceInput)
    assert container.interview_v2.calls[1].payload.start_ms == 1_000


def test_websocket_camera_keyframe_informs_next_followup() -> None:
    """A sampled camera frame is analyzed and passed only as bounded context to the next turn."""
    client, container = _client()
    frame = b"jpeg-frame"
    with client.websocket_connect(
        "/realtime/v2/interview",
        subprotocols=[PROTOCOL],
        headers={"Origin": "http://127.0.0.1:5173"},
    ) as socket:
        _authenticate(socket)
        socket.receive_json()
        socket.send_json(
            {
                "type": "video_frame",
                "input_id": "input_video_frame001",
                "sequence": 1,
                "media_type": "image/jpeg",
                "sha256": hashlib.sha256(frame).hexdigest(),
                "captured_at_ms": 2_000,
            }
        )
        socket.send_bytes(frame)
        assert socket.receive_json()["type"] == "video_ack"
        socket.send_json(
            {
                "type": "candidate_utterance",
                "input_id": "input_after_video01",
                "text": "这是我设计的高可用架构。",
                "start_ms": 2_100,
                "end_ms": 4_000,
            }
        )
        assert socket.receive_json()["type"] == "ack"
        assert socket.receive_json()["type"] == "interviewer_start"
        socket.receive_json()
        socket.receive_json()
        assert socket.receive_json()["type"] == "interviewer_followup"

    assert container.interview_realtime_coach.frames == [frame]
    assert container.interview_realtime_coach.followups[-1] == (
        "这是我设计的高可用架构。",
        "候选人正在展示一张系统架构图。",
    )


def test_websocket_rejects_out_of_order_audio_before_provider_call() -> None:
    """An out-of-order fragment closes the protocol without sending bytes to STT."""
    client, container = _client()
    content = b"late-fragment"
    with client.websocket_connect(
        "/realtime/v2/interview",
        subprotocols=[PROTOCOL],
        headers={"Origin": "http://127.0.0.1:5173"},
    ) as socket:
        _authenticate(socket)
        socket.receive_json()
        socket.send_json(
            {
                "type": "realtime_audio",
                "input_id": "input_audio_order001",
                "sequence": 2,
                "final": True,
                "media_type": "audio/webm",
                "sha256": hashlib.sha256(content).hexdigest(),
                "start_ms": 0,
                "end_ms": 100,
            }
        )
        socket.send_bytes(content)
        assert socket.receive_json() == {
            "type": "error",
            "code": "realtime.invalid_message",
        }
        with pytest.raises(WebSocketDisconnect) as raised:
            socket.receive_json()
        assert raised.value.code == 4400
    assert container.interview_realtime_coach.transcriptions == []


def test_websocket_carries_bounded_live_history_into_the_next_turn() -> None:
    """Generated questions remain available to subsequent turns without durable storage."""
    client, container = _client()
    with client.websocket_connect(
        "/realtime/v2/interview",
        subprotocols=[PROTOCOL],
        headers={"Origin": "http://127.0.0.1:5173"},
    ) as socket:
        _authenticate(socket)
        socket.receive_json()
        for index, text in enumerate(("第一轮回答", "第二轮回答"), start=1):
            socket.send_json(
                {
                    "type": "candidate_utterance",
                    "input_id": f"input_history_turn0{index}",
                    "text": text,
                    "start_ms": index * 1_000,
                    "end_ms": index * 1_000 + 500,
                }
            )
            assert socket.receive_json()["type"] == "ack"
            assert socket.receive_json()["type"] == "interviewer_start"
            socket.receive_json()
            socket.receive_json()
            assert socket.receive_json()["type"] == "interviewer_followup"

    assert container.interview_realtime_coach.histories == [
        (),
        (
            ("candidate", "第一轮回答"),
            ("interviewer", "你如何确认连接池是根因？"),
        ),
    ]


def test_websocket_consent_authorizes_and_persists_bounded_media_chunk() -> None:
    """A media header is authorized before its immediately following binary chunk is stored."""
    client, container = _client()
    content = b"webm-media-chunk"
    header = {
        "type": "media_chunk",
        "input_id": "input_media_chunk01",
        "media_kind": "audio",
        "sequence": 1,
        "media_type": "audio/webm",
        "sha256": hashlib.sha256(content).hexdigest(),
    }
    with client.websocket_connect(
        "/realtime/v2/interview",
        subprotocols=[PROTOCOL],
        headers={"Origin": "http://127.0.0.1:5173"},
    ) as socket:
        _authenticate(socket)
        socket.receive_json()
        socket.send_json(header)
        socket.send_bytes(content)
        assert socket.receive_json() == {
            "type": "media_ack",
            "input_id": "input_media_chunk01",
            "sequence": 1,
            "replayed": False,
        }
        socket.send_json(header)
        socket.send_bytes(content)
        assert socket.receive_json()["replayed"] is True

    assert container.interview_v2.media_authorizations == [
        (USER_ID, WORKSPACE_ID, SESSION_ID, CONNECTION_ID, "audio"),
        (USER_ID, WORKSPACE_ID, SESSION_ID, CONNECTION_ID, "audio"),
    ]
    assert container.interview_media_store.calls[0]["content"] == content
    assert container.interview_media_store.calls[0]["media_type"] == "audio/webm"


@pytest.mark.parametrize(
    ("headers", "protocols", "close_code"),
    [
        ({"Origin": "https://evil.example"}, [PROTOCOL], 4403),
        ({"Origin": "http://127.0.0.1:5173"}, ["other.protocol"], 4406),
    ],
)
def test_websocket_rejects_cross_origin_and_wrong_subprotocol(
    headers: dict[str, str], protocols: list[str], close_code: int
) -> None:
    """@brief 浏览器 Origin 与子协议均 fail closed / Browser Origin and subprotocol both fail closed."""
    client, _container = _client()
    with pytest.raises(WebSocketDisconnect) as raised:
        with client.websocket_connect(
            "/realtime/v2/interview",
            subprotocols=protocols,
            headers=headers,
        ):
            pass
    assert raised.value.code == close_code


def test_websocket_rejects_invalid_credential_without_persistence() -> None:
    """@brief 错误 token 不触达 realtime ledger / Invalid tokens never reach the realtime ledger."""
    client, container = _client()
    with client.websocket_connect(
        "/realtime/v2/interview",
        subprotocols=[PROTOCOL],
        headers={"Origin": "http://127.0.0.1:5173"},
    ) as socket:
        _authenticate(socket, token="invalid-realtime-token-0000")
        assert socket.receive_json() == {
            "type": "error",
            "code": "realtime.invalid_message",
        }
        with pytest.raises(WebSocketDisconnect) as raised:
            socket.receive_json()
        assert raised.value.code == 4400
    assert container.interview_v2.calls == []


def test_websocket_authentication_timeout_is_stable_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A client that never authenticates receives a stable timeout close."""
    monkeypatch.setattr(realtime_module, "_AUTH_TIMEOUT_SECONDS", 0.01)
    client, container = _client()
    with client.websocket_connect(
        "/realtime/v2/interview",
        subprotocols=[PROTOCOL],
        headers={"Origin": "http://127.0.0.1:5173"},
    ) as socket:
        assert socket.receive_json() == {
            "type": "error",
            "code": "realtime.authentication_timeout",
        }
        with pytest.raises(WebSocketDisconnect) as raised:
            socket.receive_json()
        assert raised.value.code == 4408
    assert container.interview_v2.calls == []
