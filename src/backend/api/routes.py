"""@brief 已确认契约与 mock 适配器的 HTTP/WS 路由 / HTTP/WS routes for confirmed contracts and mock adapters."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, cast

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response, StreamingResponse

from backend.api.models import (
    MockConversationCreateRequest,
    MockEndRequest,
    MockKnowledgeSourceCreateRequest,
    MockMessageCreateRequest,
    MockResumeCreateRequest,
    MockToolApprovalDecision,
)
from backend.composition import BackendContainer
from backend.domain.common import DomainError, Problem
from backend.infrastructure.identity import IdentityVerificationError, peer_is_trusted_proxy
from workspace_shared.tenancy import ActorScope

router = APIRouter(prefix="/api/v1")

_MAX_IDEMPOTENCY_KEY_LENGTH = 256
"""@brief 与持久化 ``VARCHAR(256)`` 对齐的 Idempotency-Key 最大长度 / Maximum Idempotency-Key length matching persistence."""


def _container(request: Request) -> BackendContainer:
    """@brief 获取当前 worker 的容器 / Get the current worker container.

    @param request HTTP 请求 / HTTP request.
    @return 组合根创建的容器 / Composition-root-created container.
    """
    return cast(BackendContainer, request.app.state.container)


def _scope_from_headers(request: Request) -> ActorScope:
    """@brief 获取中间件验证过的 ActorScope / Get the ActorScope verified by middleware.

    @param request HTTP 请求 / HTTP request.
    @return 始终完整的 actor/workspace/owner 范围 / Always-complete actor/workspace/owner scope.
    @raise RuntimeError 身份中间件未接线时抛出。

    @note 路由不能重新读取用户可控 header；可信代理 HMAC（Hash-based Message
    Authentication Code）与开发 mock 均只在应用中间件这一单一边界处解析一次。
    """
    scope = getattr(request.state, "actor_scope", None)
    if not isinstance(scope, ActorScope):
        raise RuntimeError("request identity middleware did not install an ActorScope")
    return scope


def _scope_from_websocket(websocket: WebSocket) -> ActorScope:
    """@brief 从单一身份边界解析 WebSocket ActorScope / Resolve WebSocket ActorScope through the single identity boundary.

    @param websocket WebSocket 连接 / WebSocket connection.
    @return 完整多租户范围 / Complete multi-tenant scope.
    @raise IdentityVerificationError 身份头或签名不合法时抛出。
    """
    container: BackendContainer = websocket.app.state.container
    if (
        container.settings.security.identity_mode == "trusted_proxy_hmac"
        and not peer_is_trusted_proxy(
            websocket.client.host if websocket.client is not None else None,
            container.settings.network.trusted_proxy_cidrs,
        )
    ):
        raise IdentityVerificationError("identity.proxy_source_not_trusted")
    raw_path = websocket.scope.get("raw_path")
    raw_query = websocket.scope.get("query_string", b"")
    if not isinstance(raw_path, (str, bytes)) or not isinstance(raw_query, (str, bytes)):
        raise IdentityVerificationError("identity.request_target_invalid")
    return container.identity.resolve(
        method="GET",
        path=raw_path,
        query_string=raw_query,
        headers=websocket.headers,
    )


def _request_id(request: Request) -> str | None:
    """@brief 获取请求追踪 ID / Get a request trace ID.

    @param request HTTP 请求 / HTTP request.
    @return request ID 或 None / Request ID or None.
    """
    return getattr(request.state, "request_id", None)


def _if_none_match_matches(request: Request, etag: str) -> bool:
    """@brief 判断条件下载 ETag 是否命中 / Determine whether a conditional-download ETag matches.

    @param request HTTP 请求 / HTTP request.
    @param etag 当前资源强 ETag / Current resource strong ETag.
    @return 客户端缓存仍有效时为真 / True when the client cache is still valid.
    """
    candidates = request.headers.get("If-None-Match", "").split(",")
    return any(candidate.strip() in {"*", etag, f"W/{etag}"} for candidate in candidates)


def _single_byte_range(range_header: str, total_size: int) -> tuple[int, int]:
    """@brief 解析单个 HTTP byte range / Parse one HTTP byte range.

    @param range_header Range 请求头 / Range request header.
    @param total_size 可下载对象总字节数 / Total downloadable object size.
    @return 包含端点的 ``(start, end)`` / Inclusive ``(start, end)``.
    @raise DomainError Range 非法、多个 range 或无法满足时抛出 / Raised for invalid, multi-range, or unsatisfiable ranges.
    """
    if not range_header.startswith("bytes=") or "," in range_header:
        raise DomainError(Problem("http.range_not_satisfiable", 416, "Requested range is not satisfiable"))
    start_text, separator, end_text = range_header[6:].partition("-")
    if separator != "-" or total_size < 1:
        raise DomainError(Problem("http.range_not_satisfiable", 416, "Requested range is not satisfiable"))
    try:
        if not start_text:
            suffix_length = int(end_text)
            if suffix_length < 1:
                raise ValueError
            return max(0, total_size - suffix_length), total_size - 1
        start = int(start_text)
        end = total_size - 1 if not end_text else int(end_text)
    except ValueError as error:
        raise DomainError(Problem("http.range_not_satisfiable", 416, "Requested range is not satisfiable")) from error
    if start < 0 or end < start or start >= total_size:
        raise DomainError(Problem("http.range_not_satisfiable", 416, "Requested range is not satisfiable"))
    return start, min(end, total_size - 1)


def _idempotency_key(request: Request) -> str:
    """@brief 读取必需 Idempotency-Key / Read the required Idempotency-Key.

    @param request HTTP 请求 / HTTP request.
    @return 非空幂等键 / Non-empty idempotency key.
    @raise DomainError 头缺失时抛出 / Raised when the header is absent.
    """
    values = request.headers.getlist("Idempotency-Key")
    if len(values) != 1 or not values[0]:
        raise DomainError(Problem("http.idempotency_key_required", 400, "Idempotency-Key is required"))
    key = values[0]
    if (
        len(key) > _MAX_IDEMPOTENCY_KEY_LENGTH
        or not key.isascii()
        or any(ord(character) < 33 or ord(character) > 126 for character in key)
    ):
        raise DomainError(
            Problem("http.invalid_idempotency_key", 400, "Idempotency-Key is invalid")
        )
    return key


async def _idempotent(
    request: Request,
    scope: ActorScope,
    route_template: str,
    payload: object,
    status_code: int,
    operation: Callable[[], Awaitable[dict[str, Any]]],
) -> JSONResponse:
    """@brief 执行或重放 HTTP 命令 / Execute or replay an HTTP command.

    @param request HTTP 请求 / HTTP request.
    @param scope 多租户范围 / Multi-tenant scope.
    @param route_template 稳定路由模板 / Stable route template.
    @param payload 请求载荷 / Request payload.
    @param status_code 首次响应状态 / First response status.
    @param operation 首次执行函数 / First execution function.
    @return JSON 响应 / JSON response.
    """
    target = f"{route_template}:{json.dumps(dict(request.path_params), sort_keys=True, separators=(',', ':'))}"
    response = await _container(request).idempotency.execute(
        scope,
        target,
        _idempotency_key(request),
        payload,
        status_code,
        operation,
    )
    return JSONResponse(response.body, status_code=response.status_code)


async def _formal_payload(request: Request, entrypoint: str) -> dict[str, Any]:
    """@brief 读取并校验正式 JSON Schema body / Read and validate a formal JSON Schema body.

    @param request HTTP 请求 / HTTP request.
    @param entrypoint 正式 Schema entrypoint / Formal schema entrypoint.
    @return 已验证对象 / Validated object.
    @raise DomainError body 不是对象或无法通过 Schema 时抛出 / Raised for non-object or invalid schema body.
    """
    try:
        payload = await request.json()
    except json.JSONDecodeError as error:
        raise DomainError(Problem("http.invalid_json", 400, "Request body is not valid JSON")) from error
    if not isinstance(payload, dict):
        raise DomainError(Problem("http.invalid_json", 422, "Request body must be a JSON object"))
    _container(request).contracts.validate_declared(entrypoint, payload)
    return payload


@router.get("/resumes")
async def list_resumes(request: Request) -> dict[str, Any]:
    """@brief 列出当前 workspace 简历 / List resumes in the current workspace.

    @param request HTTP 请求 / HTTP request.
    @return ListResumesResponse / ListResumesResponse.
    """
    scope = _scope_from_headers(request)
    records = await _container(request).resume.list_resumes(scope)
    return {"items": [record.snapshot() for record in records], "page": {"next_cursor": None, "has_more": False, "total_estimate": len(records)}}


@router.post("/resumes", status_code=201, openapi_extra={"x-contract-status": "mock", "x-pending-contract": "ResumeCreateRequest"})
async def create_resume(request: Request, body: MockResumeCreateRequest) -> JSONResponse:
    """@brief 创建简历（mock 输入适配器）/ Create a resume (mock input adapter).

    @param request HTTP 请求 / HTTP request.
    @param body mock 创建 body / Mock creation body.
    @return ResumeDocument / ResumeDocument.
    """
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行简历创建 / Execute resume creation.

        @return ResumeDocument / ResumeDocument.
        """
        record = await _container(request).resume.create_resume(
            scope,
            **body.model_dump(),
            request_id=_request_id(request),
        )
        return record.snapshot()

    return await _idempotent(request, scope, "/resumes", body.model_dump(mode="json"), 201, operation)


@router.get("/resumes/{resume_id}")
async def get_resume(request: Request, resume_id: str) -> Response:
    """@brief 获取简历快照 / Get a resume snapshot.

    @param request HTTP 请求 / HTTP request.
    @param resume_id 简历 ID / Resume ID.
    @return ResumeDocument 与 ETag / ResumeDocument with ETag.
    """
    record = await _container(request).resume.get_resume(_scope_from_headers(request), resume_id)
    return JSONResponse(record.snapshot(), headers={"ETag": record.etag()})


@router.get("/resumes/{resume_id}/revisions/{revision}")
async def get_resume_revision(request: Request, resume_id: str, revision: int) -> Response:
    """@brief 获取指定简历版本 / Get a specified resume revision.

    @param request HTTP 请求 / HTTP request.
    @param resume_id 简历 ID / Resume ID.
    @param revision 领域版本 / Domain revision.
    @return ResumeDocument 与 ETag / ResumeDocument with ETag.
    """
    record = await _container(request).resume.get_resume(_scope_from_headers(request), resume_id, revision)
    return JSONResponse(record.snapshot(revision), headers={"ETag": record.etag(revision)})


@router.post("/resumes/{resume_id}/operations")
async def apply_resume_operations(request: Request, resume_id: str) -> JSONResponse:
    """@brief 应用正式 ResumeOperationBatch / Apply a formal ResumeOperationBatch.

    @param request HTTP 请求 / HTTP request.
    @param resume_id 简历 ID / Resume ID.
    @return ResumeOperationBatchResult / ResumeOperationBatchResult.
    """
    body = await _formal_payload(request, "ResumeOperationBatch")
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行正式操作批次 / Execute the formal operation batch.

        @return ResumeOperationBatchResult / ResumeOperationBatchResult.
        """
        return await _container(request).resume.apply_operations(
            scope,
            resume_id,
            body,
            request.headers.get("If-Match"),
            _request_id(request),
        )

    return await _idempotent(
        request,
        scope,
        "/resumes/{resume_id}/operations",
        body,
        200,
        operation,
    )


@router.post("/resumes/{resume_id}/render-jobs", status_code=202)
async def create_render_job(request: Request, resume_id: str) -> JSONResponse:
    """@brief 创建正式 RenderJobRequest / Create a formal RenderJobRequest.

    @param request HTTP 请求 / HTTP request.
    @param resume_id 简历 ID / Resume ID.
    @return ResumeRenderJob / ResumeRenderJob.
    """
    body = await _formal_payload(request, "RenderJobRequest")
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行 Job 创建 / Execute job creation.

        @return ResumeRenderJob / ResumeRenderJob.
        """
        return await _container(request).resume.create_render_job(scope, resume_id, body, _request_id(request))

    return await _idempotent(request, scope, "/resumes/{resume_id}/render-jobs", body, 202, operation)


@router.get("/resume-render-jobs/{job_id}")
async def get_render_job(request: Request, job_id: str) -> dict[str, Any]:
    """@brief 获取简历编译 Job / Get a resume-rendering Job.

    @param request HTTP 请求 / HTTP request.
    @param job_id Job ID / Job ID.
    @return ResumeRenderJob / ResumeRenderJob.
    """
    return await _container(request).resume.get_render_job(_scope_from_headers(request), job_id)


@router.get("/render-artifacts/{artifact_id}")
async def get_render_artifact(request: Request, artifact_id: str) -> dict[str, Any]:
    """@brief 获取产物 metadata / Get artifact metadata.

    @param request HTTP 请求 / HTTP request.
    @param artifact_id 产物 ID / Artifact ID.
    @return RenderArtifact / RenderArtifact.
    """
    artifact, _, _ = await _container(request).resume.get_artifact(_scope_from_headers(request), artifact_id)
    return artifact


@router.get("/render-artifacts/{artifact_id}/content")
async def get_render_artifact_content(request: Request, artifact_id: str) -> Response:
    """@brief 下载 PDF 产物 / Download a PDF artifact.

    @param request HTTP 请求 / HTTP request.
    @param artifact_id 产物 ID / Artifact ID.
    @return PDF 响应 / PDF response.
    """
    artifact, content, _ = await _container(request).resume.get_artifact(
        _scope_from_headers(request), artifact_id
    )
    etag = f'"sha256-{artifact["sha256"]}"'
    common_headers = {"ETag": etag, "Accept-Ranges": "bytes"}
    if _if_none_match_matches(request, etag):
        return Response(status_code=304, headers=common_headers)
    range_header = request.headers.get("Range")
    if range_header is None:
        return Response(
            content,
            media_type=artifact["content_type"],
            headers={**common_headers, "Content-Length": str(len(content))},
        )
    start, end = _single_byte_range(range_header, len(content))
    partial_content = content[start : end + 1]
    return Response(
        partial_content,
        status_code=206,
        media_type=artifact["content_type"],
        headers={
            **common_headers,
            "Content-Length": str(len(partial_content)),
            "Content-Range": f"bytes {start}-{end}/{len(content)}",
        },
    )


@router.get("/render-artifacts/{artifact_id}/source-map")
async def get_render_artifact_source_map(request: Request, artifact_id: str) -> dict[str, Any]:
    """@brief 获取 PDF source map / Get a PDF source map.

    @param request HTTP 请求 / HTTP request.
    @param artifact_id 产物 ID / Artifact ID.
    @return PdfSourceMap / PdfSourceMap.
    """
    _, _, source_map = await _container(request).resume.get_artifact(_scope_from_headers(request), artifact_id)
    if source_map is None:
        raise DomainError(Problem("resume.source_map_not_found", 404, "PDF source map was not found"))
    return source_map


@router.post("/conversations", status_code=201, openapi_extra={"x-contract-status": "mock", "x-pending-contract": "ConversationCreateRequest"})
async def create_conversation(request: Request, body: MockConversationCreateRequest) -> JSONResponse:
    """@brief 创建会话（mock 输入适配器）/ Create a conversation (mock input adapter).

    @param request HTTP 请求 / HTTP request.
    @param body mock 创建 body / Mock creation body.
    @return Conversation / Conversation.
    """
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行会话创建 / Execute conversation creation.

        @return Conversation / Conversation.
        """
        record = await _container(request).agent.create_conversation(scope, **body.model_dump())
        return record.as_dict()

    return await _idempotent(request, scope, "/conversations", body.model_dump(mode="json"), 201, operation)


@router.post("/conversations/{conversation_id}/messages", status_code=201, openapi_extra={"x-contract-status": "mock", "x-pending-contract": "CreateUserMessageRequest"})
async def create_message(request: Request, conversation_id: str, body: MockMessageCreateRequest) -> JSONResponse:
    """@brief 创建用户消息（mock 输入适配器）/ Create a user message (mock input adapter).

    @param request HTTP 请求 / HTTP request.
    @param conversation_id 会话 ID / Conversation ID.
    @param body mock body / Mock body.
    @return ChatMessage / ChatMessage.
    """
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行消息创建 / Execute message creation.

        @return ChatMessage / ChatMessage.
        """
        message = await _container(request).agent.create_user_message(scope, conversation_id, **body.model_dump())
        return message.as_dict()

    return await _idempotent(request, scope, "/conversations/{conversation_id}/messages", body.model_dump(mode="json"), 201, operation)


@router.post("/agent-runs", status_code=202)
async def start_agent_run(request: Request) -> JSONResponse:
    """@brief 创建正式 AgentRunRequest / Create a formal AgentRunRequest.

    @param request HTTP 请求 / HTTP request.
    @return AgentRun / AgentRun.
    """
    body = await _formal_payload(request, "AgentRunRequest")
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行 Run 创建 / Execute Run creation.

        @return AgentRun / AgentRun.
        """
        run = await _container(request).agent.start_run(scope, body, _request_id(request))
        return run.as_dict(f"{_container(request).settings.network.public_base_url}/api/v1/agent-runs/{run.id}/events")

    return await _idempotent(request, scope, "/agent-runs", body, 202, operation)


@router.get("/agent-runs/{run_id}")
async def get_agent_run(request: Request, run_id: str) -> dict[str, Any]:
    """@brief 获取 Agent Run / Get an Agent Run.

    @param request HTTP 请求 / HTTP request.
    @param run_id Run ID / Run ID.
    @return AgentRun / AgentRun.
    """
    run = await _container(request).agent.get_run(_scope_from_headers(request), run_id)
    return run.as_dict(f"{_container(request).settings.network.public_base_url}/api/v1/agent-runs/{run.id}/events")


@router.get("/agent-runs/{run_id}/events")
async def stream_agent_events(request: Request, run_id: str) -> StreamingResponse:
    """@brief SSE 流式输出 Agent events / Stream Agent events over SSE.

    @param request HTTP 请求 / HTTP request.
    @param run_id Run ID / Run ID.
    @return text/event-stream 响应 / text/event-stream response.
    """
    scope = _scope_from_headers(request)
    last_event_id = request.headers.get("Last-Event-ID")

    async def events() -> AsyncIterator[bytes]:
        """@brief 生成 SSE frame / Generate SSE frames.

        @return 异步字节流 / Async byte stream.
        """
        async for event in _container(request).agent.stream_events(scope, run_id, last_event_id):
            yield f"id: {event['event_id']}\nevent: {event['event_type']}\ndata: {json.dumps(event, ensure_ascii=False)}\n\n".encode()

    return StreamingResponse(events(), media_type="text/event-stream", headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})


@router.post("/agent-runs/{run_id}/cancellations", status_code=202)
async def cancel_agent_run(request: Request, run_id: str) -> JSONResponse:
    """@brief 取消 Agent Run / Cancel an Agent Run.

    @param request HTTP 请求 / HTTP request.
    @param run_id Run ID / Run ID.
    @return AgentRun / AgentRun.
    """
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行取消命令 / Execute the cancellation command.

        @return AgentRun / AgentRun.
        """
        return (await _container(request).agent.cancel_run(scope, run_id)).as_dict()

    return await _idempotent(request, scope, "/agent-runs/{run_id}/cancellations", {"run_id": run_id}, 202, operation)


@router.post("/tool-approvals/{approval_id}/decisions", openapi_extra={"x-contract-status": "mock", "x-pending-contract": "ToolApprovalDecision path binding"})
async def decide_tool_approval(request: Request, approval_id: str, body: MockToolApprovalDecision) -> JSONResponse:
    """@brief 决定 mock tool approval / Decide a mock tool approval.

    @param request HTTP 请求 / HTTP request.
    @param approval_id approval ID / Approval ID.
    @param body mock 决策 / Mock decision.
    @return AgentRun / AgentRun.
    """
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行 approval 决策 / Execute the approval decision.

        @return AgentRun / AgentRun.
        """
        return (await _container(request).agent.decide_tool_approval(scope, approval_id, body.decision)).as_dict()

    return await _idempotent(request, scope, "/tool-approvals/{approval_id}/decisions", body.model_dump(mode="json"), 200, operation)


@router.post("/knowledge-sources", status_code=201, openapi_extra={"x-contract-status": "mock", "x-pending-contract": "KnowledgeSourceCreateRequest"})
async def create_knowledge_source(request: Request, body: MockKnowledgeSourceCreateRequest) -> JSONResponse:
    """@brief 创建知识来源（mock 输入适配器）/ Create a knowledge source (mock input adapter).

    @param request HTTP 请求 / HTTP request.
    @param body mock 创建 body / Mock creation body.
    @return KnowledgeSource / KnowledgeSource.
    """
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行来源创建 / Execute source creation.

        @return KnowledgeSource / KnowledgeSource.
        """
        source = await _container(request).knowledge.create_mock_source(scope, **body.model_dump())
        return source.as_dict()

    return await _idempotent(request, scope, "/knowledge-sources", body.model_dump(mode="json"), 201, operation)


@router.get("/knowledge-sources")
async def list_knowledge_sources(request: Request) -> dict[str, Any]:
    """@brief 列出知识来源 / List knowledge sources.

    @param request HTTP 请求 / HTTP request.
    @return ListKnowledgeSourcesResponse / ListKnowledgeSourcesResponse.
    """
    scope = _scope_from_headers(request)
    sources = await _container(request).knowledge.list_sources(scope)
    return {"items": [source.as_dict() for source in sources], "page": {"next_cursor": None, "has_more": False, "total_estimate": len(sources)}}


@router.get("/knowledge-sources/{source_id}")
async def get_knowledge_source(request: Request, source_id: str) -> dict[str, Any]:
    """@brief 获取知识来源 / Get a knowledge source.

    @param request HTTP 请求 / HTTP request.
    @param source_id 来源 ID / Source ID.
    @return KnowledgeSource / KnowledgeSource.
    """
    return (await _container(request).knowledge.get_source(_scope_from_headers(request), source_id)).as_dict()


@router.post("/knowledge-sources/{source_id}/ingestion-jobs", status_code=202, openapi_extra={"x-contract-status": "mock", "x-pending-contract": "KnowledgeIngestionJobCreateRequest"})
async def create_ingestion_job(request: Request, source_id: str) -> JSONResponse:
    """@brief 创建知识索引 Job（mock 输入适配器）/ Create a knowledge-ingestion Job (mock input adapter).

    @param request HTTP 请求 / HTTP request.
    @param source_id 来源 ID / Source ID.
    @return KnowledgeIngestionJob / KnowledgeIngestionJob.
    """
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行 Job 创建 / Execute Job creation.

        @return KnowledgeIngestionJob / KnowledgeIngestionJob.
        """
        return await _container(request).knowledge.create_ingestion_job(scope, source_id, _request_id(request))

    return await _idempotent(request, scope, "/knowledge-sources/{source_id}/ingestion-jobs", {"source_id": source_id}, 202, operation)


@router.get("/knowledge-ingestion-jobs/{job_id}")
async def get_ingestion_job(request: Request, job_id: str) -> dict[str, Any]:
    """@brief 获取知识索引 Job / Get a knowledge-ingestion Job.

    @param request HTTP 请求 / HTTP request.
    @param job_id Job ID / Job ID.
    @return KnowledgeIngestionJob / KnowledgeIngestionJob.
    """
    return await _container(request).knowledge.get_ingestion_job(_scope_from_headers(request), job_id)


@router.post("/knowledge-searches")
async def search_knowledge(request: Request) -> dict[str, Any]:
    """@brief 执行正式 KnowledgeSearchRequest / Execute a formal KnowledgeSearchRequest.

    @param request HTTP 请求 / HTTP request.
    @return 检索结果列表的 mock wrapper / Mock wrapper containing search results.
    @note MOCK — Schema 有请求/结果定义，但没有路径级 response wrapper。
    """
    body = await _formal_payload(request, "KnowledgeSearchRequest")
    return {"items": await _container(request).knowledge.search(_scope_from_headers(request), body)}


@router.post("/interview-sessions", status_code=201)
async def create_interview_session(request: Request) -> JSONResponse:
    """@brief 创建正式 InterviewSessionCreateRequest / Create a formal InterviewSessionCreateRequest.

    @param request HTTP 请求 / HTTP request.
    @return InterviewSession / InterviewSession.
    """
    body = await _formal_payload(request, "InterviewSessionCreateRequest")
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行 Session 创建 / Execute Session creation.

        @return InterviewSession / InterviewSession.
        """
        return (await _container(request).interview.create_session(scope, body)).as_dict()

    return await _idempotent(request, scope, "/interview-sessions", body, 201, operation)


@router.get("/interview-sessions/{session_id}")
async def get_interview_session(request: Request, session_id: str) -> dict[str, Any]:
    """@brief 获取面试 Session / Get an interview Session.

    @param request HTTP 请求 / HTTP request.
    @param session_id Session ID / Session ID.
    @return InterviewSession / InterviewSession.
    """
    return (await _container(request).interview.get_session(_scope_from_headers(request), session_id)).as_dict()


@router.post("/interview-sessions/{session_id}/connections", openapi_extra={"x-contract-status": "mock", "x-pending-contract": "RealtimeConnectionRequest path binding"})
async def create_interview_connection(request: Request, session_id: str) -> JSONResponse:
    """@brief 创建短期实时连接描述（mock endpoint）/ Create a short-lived realtime descriptor (mock endpoint).

    @param request HTTP 请求 / HTTP request.
    @param session_id Session ID / Session ID.
    @return RealtimeConnectionDescriptor / RealtimeConnectionDescriptor.
    """
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行连接描述创建 / Execute connection-descriptor creation.

        @return RealtimeConnectionDescriptor / RealtimeConnectionDescriptor.
        """
        return await _container(request).interview.create_connection(scope, session_id, _request_id(request))

    return await _idempotent(request, scope, "/interview-sessions/{session_id}/connections", {"session_id": session_id}, 200, operation)


@router.post("/interview-sessions/{session_id}/end-requests", status_code=202, openapi_extra={"x-contract-status": "mock", "x-pending-contract": "InterviewEndRequest"})
async def end_interview_session(request: Request, session_id: str, body: MockEndRequest) -> JSONResponse:
    """@brief 结束面试 Session（mock 输入适配器）/ End an interview Session (mock input adapter).

    @param request HTTP 请求 / HTTP request.
    @param session_id Session ID / Session ID.
    @param body mock 结束请求 / Mock end request.
    @return Job / Job.
    """
    scope = _scope_from_headers(request)

    async def operation() -> dict[str, Any]:
        """@brief 执行结束命令 / Execute the end command.

        @return Job / Job.
        """
        return await _container(request).interview.end_session(scope, session_id, _request_id(request))

    return await _idempotent(request, scope, "/interview-sessions/{session_id}/end-requests", body.model_dump(mode="json"), 202, operation)


@router.get("/interview-reports/{report_id}")
async def get_interview_report(request: Request, report_id: str) -> dict[str, Any]:
    """@brief 获取面试报告 / Get an interview report.

    @param request HTTP 请求 / HTTP request.
    @param report_id 报告 ID / Report ID.
    @return InterviewReport / InterviewReport.
    """
    return await _container(request).interview.get_report(_scope_from_headers(request), report_id)


@router.get("/_mock/agent-capabilities", openapi_extra={"x-contract-status": "mock", "x-pending-contract": "Agent capability discovery"})
async def discover_agent_capabilities(request: Request) -> dict[str, Any]:
    """@brief 暴露确定性 mock capability discovery / Expose deterministic mock capability discovery.

    @param request HTTP 请求 / HTTP request.
    @return mock 能力列表 / Mock capability list.
    """
    capabilities = _container(request).model_provider.capabilities()
    return {"mock": True, "items": [{"name": item.name, "streaming": item.supports_streaming, "tool_calling": item.supports_tool_calling, "structured_output": item.supports_structured_output} for item in capabilities]}


class _RealtimeClosed(Exception):
    """@brief 正常结束实时 TaskGroup 的私有异常 / Private exception for normal realtime TaskGroup termination."""


@router.websocket("/interview-sessions/{session_id}/realtime")
async def interview_realtime(websocket: WebSocket, session_id: str) -> None:
    """@brief mock 全双工实时控制 WebSocket / Mock full-duplex realtime-control WebSocket.

    @param websocket WebSocket 连接 / WebSocket connection.
    @param session_id Session ID / Session ID.
    @note MOCK — 真实 WebRTC 信令/二进制媒体握手仍待正式契约；此路由只处理 JSON 控制事件。
    """
    try:
        scope = _scope_from_websocket(websocket)
    except IdentityVerificationError:
        await websocket.close(code=1008)
        return
    container: BackendContainer = websocket.app.state.container
    await websocket.accept()
    outbound: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=64)

    async def enqueue_outbound(message: dict[str, Any]) -> None:
        """@brief 无阻塞地排入服务端事件 / Non-blockingly enqueue a server event.

        @param message 待发送 JSON 事件 / JSON event to send.
        @return 无返回值 / No return value.
        @raise _RealtimeClosed 客户端无法消费有界队列时抛出 / Raised when the client cannot consume the bounded queue.
        """
        try:
            outbound.put_nowait(message)
        except asyncio.QueueFull as error:
            await websocket.close(code=1013)
            raise _RealtimeClosed from error

    async def receiver() -> None:
        """@brief 接收并处理客户端事件 / Receive and process client events."""
        while True:
            try:
                event = await websocket.receive_json()
            except WebSocketDisconnect as error:
                raise _RealtimeClosed from error
            if not isinstance(event, dict) or event.get("session_id") != session_id:
                await enqueue_outbound(
                    {
                        "event_type": "interview.error",
                        "payload": {
                            "problem": Problem(
                                "interview.invalid_event",
                                422,
                                "Realtime event targets a different session",
                            ).as_dict(),
                            "close_connection": False,
                        },
                    }
                )
                continue
            try:
                container.contracts.validate("InterviewRealtimeEvent", event)
                responses = await container.interview.handle_realtime_event(scope, session_id, event, None)
                for response in responses:
                    await enqueue_outbound(response)
            except DomainError as error:
                await enqueue_outbound(
                    {
                        "event_type": "interview.error",
                        "payload": {"problem": error.problem.as_dict(), "close_connection": False},
                    }
                )

    async def sender() -> None:
        """@brief 发送服务器事件 / Send server events."""
        while True:
            message = await outbound.get()
            try:
                await websocket.send_json(message)
            except WebSocketDisconnect as error:
                raise _RealtimeClosed from error
            finally:
                outbound.task_done()

    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(receiver(), name=f"aiws:ws-recv:{session_id}")
            group.create_task(sender(), name=f"aiws:ws-send:{session_id}")
    except* _RealtimeClosed:
        pass
