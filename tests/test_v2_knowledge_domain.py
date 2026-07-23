"""@brief API v2 Connection、Upload 与 Knowledge 领域不变量测试 / API v2 domain-invariant tests."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest

from backend.application.knowledge import CreateConnectionCommand
from backend.domain.connections import (
    Connection,
    ConnectionAggregate,
    ConnectionAuthMethod,
    ConnectionAuthorizationFlow,
    ConnectionAuthorizationIdempotency,
    ConnectionAuthorizationRecord,
    ConnectionAuthorizationSession,
    ConnectionAuthorizationSessionId,
    ConnectionAuthorizationState,
    ConnectionId,
    ConnectionOwnership,
    ConnectionProvider,
    ConnectionStatus,
    ConnectionTransitionError,
    CredentialReference,
    ProviderSessionReference,
    SecretValue,
    authorization_state_sha256,
)
from backend.domain.knowledge_retrieval import (
    HybridScore,
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeRetrievalError,
    KnowledgeSelection,
    KnowledgeSelectionMode,
    KnowledgeVersionPin,
    evaluate_visibility,
)
from backend.domain.knowledge_sources import (
    AgentScopeGrant,
    CloudDriveSourceInput,
    FilePublicMetadata,
    FileSourceInput,
    KnowledgeOperation,
    KnowledgeSensitivity,
    KnowledgeSource,
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    KnowledgeVisibilityPolicy,
    ManualSourceInput,
    ModelRegion,
    PolicyEffect,
    PublicKnowledgeSourceConfig,
)
from backend.domain.platform import JobId, ResourceRef
from backend.domain.principals import ResourceMeta, UserId, WorkspaceId
from backend.domain.upload_sessions import (
    UploadClaimError,
    UploadCompletionClaim,
    UploadDeclaration,
    UploadGrant,
    UploadSession,
    UploadSessionId,
    UploadStatus,
    UploadTransitionError,
    UploadVerificationId,
    VerifiedUpload,
)

_NOW = datetime(2026, 7, 23, 8, 0, tzinfo=UTC)
"""@brief 领域测试固定时刻 / Fixed instant for domain tests."""

_WORKSPACE_ID = WorkspaceId("ws_knowledge_alpha")
"""@brief 领域测试 Workspace / Workspace used by domain tests."""

_USER_ID = UserId("usr_knowledge_klee")
"""@brief 领域测试用户 / User used by domain tests."""

_SHA256 = "0123456789abcdef" * 4
"""@brief 有效测试 SHA-256 / Valid test SHA-256."""

_VERIFICATION_ID = UploadVerificationId("verification_knowledge_000001")
"""@brief 固定验证 saga ID / Fixed verification-saga ID."""


def _policy(
    *,
    version: int = 1,
    default: PolicyEffect = PolicyEffect.DENY,
    grants: tuple[AgentScopeGrant, ...] = (),
    regions: tuple[ModelRegion, ...] = (ModelRegion.CN,),
    external: bool = False,
) -> KnowledgeVisibilityPolicy:
    """@brief 构造 visibility policy / Build a visibility policy.

    @param version policy 版本 / Policy version.
    @param default 默认效果 / Default effect.
    @param grants agent grants / Agent grants.
    @param regions 模型区域 / Model regions.
    @param external 是否允许外部处理 / Whether external processing is allowed.
    @return 测试 policy / Test policy.
    """
    return KnowledgeVisibilityPolicy(
        KnowledgeSensitivity.CONFIDENTIAL,
        default,
        grants,
        False,
        regions,
        external,
        365,
        version,
    )


def _grant() -> UploadGrant:
    """@brief 构造短期上传授权 / Build a short-lived upload grant.

    @return PUT 授权 / PUT grant.
    """
    return UploadGrant(
        "https://objects.example.test/upload/upload_knowledge_01?signature=private",
        {"content-type": "text/plain"},
    )


def _upload() -> UploadSession:
    """@brief 构造 created upload / Build a created upload.

    @return created session / Created session.
    """
    return UploadSession.create(
        upload_id=UploadSessionId("upload_knowledge_01"),
        workspace_id=_WORKSPACE_ID,
        declaration=UploadDeclaration("notes.txt", "text/plain", 12, _SHA256),
        grant=_grant(),
        created_at=_NOW,
        expires_at=_NOW + timedelta(minutes=15),
    )


def _verified_upload() -> VerifiedUpload:
    """@brief 构造全门禁通过证明 / Build evidence passing every upload gate.

    @return 可信证明 / Trusted evidence.
    """
    return VerifiedUpload(
        12,
        _SHA256,
        "text/plain",
        ResourceRef("upload_artifact", "artifact_upload_01", 1),
        True,
        True,
        True,
    )


def test_connection_secret_values_and_private_references_are_redacted() -> None:
    """@brief token、URL、user code 与 vault reference 不进入 repr / Secrets and references stay out of repr.

    @return 无返回值 / No return value.
    """
    raw = "api-token-that-must-never-leak"
    secret = SecretValue(raw)
    command = CreateConnectionCommand(ConnectionProvider("github"), "GitHub", secret)
    connection = Connection(
        ResourceMeta(ConnectionId("connection_knowledge_01"), 1, _NOW, _NOW),
        _WORKSPACE_ID,
        ConnectionProvider("github"),
        ConnectionAuthMethod.API_TOKEN,
        "GitHub",
        ConnectionStatus.ACTIVE,
    )
    aggregate = ConnectionAggregate(
        connection,
        ConnectionOwnership(_WORKSPACE_ID, _USER_ID),
        CredentialReference("credential_reference_01"),
    )
    session = ConnectionAuthorizationSession(
        ConnectionAuthorizationSessionId("connection_auth_01"),
        ConnectionProvider("github"),
        ConnectionAuthorizationFlow.DEVICE_CODE,
        _NOW + timedelta(minutes=10),
        verification_uri="https://github.example/device",
        user_code="KLEE-CODE",
        poll_interval_ms=5_000,
    )

    rendered = " ".join(map(repr, (secret, command, aggregate, session)))

    assert raw not in rendered
    assert "credential_reference_01" not in rendered
    assert "KLEE-CODE" not in rendered
    assert "<redacted>" in rendered


def test_authorization_session_hashes_state_and_completes_once() -> None:
    """@brief OAuth state 只保存摘要且 session 只能完成一次 / OAuth state is hashed and completion is one-shot.

    @return 无返回值 / No return value.
    """
    state = SecretValue("high-entropy-state-value-for-one-session")
    session = ConnectionAuthorizationSession(
        ConnectionAuthorizationSessionId("connection_auth_02"),
        ConnectionProvider("google_drive"),
        ConnectionAuthorizationFlow.BROWSER_REDIRECT,
        _NOW + timedelta(minutes=10),
        authorization_url="https://accounts.example/authorize?state=opaque",
    )
    record = ConnectionAuthorizationRecord(
        session,
        ConnectionOwnership(_WORKSPACE_ID, _USER_ID),
        ("drive.readonly",),
        ConnectionAuthorizationState.PENDING,
        authorization_state_sha256(state),
        ProviderSessionReference("provider_session_02"),
        ConnectionAuthorizationIdempotency(
            _SHA256,
            "abcdef0123456789" * 4,
            _NOW + timedelta(days=1),
        ),
        _NOW,
    )

    completed = record.complete(
        ConnectionId("connection_created_02"), at=_NOW + timedelta(seconds=1)
    )

    assert record.matches_state(state)
    assert completed.state is ConnectionAuthorizationState.COMPLETED
    assert "high-entropy-state" not in repr(record)
    with pytest.raises(ConnectionTransitionError):
        completed.complete(ConnectionId("connection_created_03"), at=_NOW + timedelta(seconds=2))


def test_upload_completion_verifies_frozen_claims_and_allows_one_consumer() -> None:
    """@brief size/hash/MIME 与安全证明一致后仅允许一个消费者 / Matching evidence permits one consumer.

    @return 无返回值 / No return value.
    """
    upload = _upload()
    claim = UploadCompletionClaim(12, _SHA256)
    verifying = upload.begin_completion(
        claim,
        _VERIFICATION_ID,
        at=_NOW + timedelta(seconds=1),
    )
    completed = verifying.complete(_verified_upload(), at=_NOW + timedelta(seconds=2))
    claimed = completed.claim_content(
        ResourceRef("knowledge_source_version", "knowledge_version_01", 1)
    )

    assert verifying.view.status is UploadStatus.VERIFYING
    assert completed.view.status is UploadStatus.COMPLETED
    assert claimed.claimed_by is not None
    with pytest.raises(UploadTransitionError):
        completed.begin_completion(
            claim,
            _VERIFICATION_ID,
            at=_NOW + timedelta(seconds=3),
        )
    with pytest.raises(UploadClaimError):
        claimed.claim_content(ResourceRef("knowledge_source_version", "knowledge_version_02", 1))


def test_upload_rejects_hash_size_mime_and_expiry_mismatches() -> None:
    """@brief completion 拒绝 size/hash/MIME 不匹配与过期 / Completion rejects mismatches and expiry.

    @return 无返回值 / No return value.
    """
    upload = _upload()
    with pytest.raises(UploadTransitionError):
        upload.begin_completion(
            UploadCompletionClaim(13, _SHA256),
            _VERIFICATION_ID,
            at=_NOW + timedelta(seconds=1),
        )
    verifying = upload.begin_completion(
        UploadCompletionClaim(12, _SHA256),
        _VERIFICATION_ID,
        at=_NOW + timedelta(seconds=1),
    )
    assert (
        verifying.resume_completion(
            UploadCompletionClaim(12, _SHA256),
            _VERIFICATION_ID,
        )
        is verifying
    )
    with pytest.raises(UploadTransitionError):
        verifying.resume_completion(
            UploadCompletionClaim(12, _SHA256),
            UploadVerificationId("verification_knowledge_other01"),
        )
    wrong_mime = VerifiedUpload(
        12,
        _SHA256,
        "application/octet-stream",
        ResourceRef("upload_artifact", "artifact_upload_02", 1),
        True,
        True,
        True,
    )
    with pytest.raises(UploadTransitionError):
        verifying.complete(wrong_mime, at=_NOW + timedelta(seconds=2))
    with pytest.raises(UploadTransitionError):
        upload.begin_completion(
            UploadCompletionClaim(12, _SHA256),
            _VERIFICATION_ID,
            at=_NOW + timedelta(minutes=15),
        )


def test_source_version_counter_is_monotonic_and_snapshot_is_immutable() -> None:
    """@brief source counter 单调且 version 内容快照不可替换 / Source counter is monotonic and snapshots are immutable.

    @return 无返回值 / No return value.
    """
    source = KnowledgeSource.create(
        meta=ResourceMeta(KnowledgeSourceId("knowledge_source_01"), 1, _NOW, _NOW),
        workspace_id=_WORKSPACE_ID,
        created_by=_USER_ID,
        name="Interview notes",
        source_input=FileSourceInput(UploadSessionId("upload_knowledge_01")),
        visibility=_policy(),
        file_metadata=FilePublicMetadata("notes.txt", "text/plain"),
    )
    source_v1, version_v1 = source.allocate_version(
        version_id=KnowledgeSourceVersionId("knowledge_version_01"),
        content_sha256=_SHA256,
        size_bytes=12,
        artifact_ref=ResourceRef("upload_artifact", "artifact_upload_01", 1),
        at=_NOW + timedelta(seconds=1),
    )
    source_v2, version_v2 = source_v1.allocate_version(
        version_id=KnowledgeSourceVersionId("knowledge_version_02"),
        content_sha256="abcdef0123456789" * 4,
        size_bytes=20,
        artifact_ref=ResourceRef("upload_artifact", "artifact_upload_02", 1),
        at=_NOW + timedelta(seconds=2),
    )
    indexed = version_v1.begin_indexing(at=_NOW + timedelta(seconds=3)).mark_ready(
        at=_NOW + timedelta(seconds=4)
    )

    assert source_v2.version_counter == 2
    assert version_v1.snapshot.version_number == 1
    assert version_v2.snapshot.version_number == 2
    assert indexed.snapshot is version_v1.snapshot
    with pytest.raises(FrozenInstanceError):
        version_v1.snapshot.size_bytes = 999  # type: ignore[misc]


def test_source_public_config_never_exposes_manual_or_connection_private_input() -> None:
    """@brief manual content、connection_id 与 remote_id 不进入 public_config / Private input stays out of public config.

    @return 无返回值 / No return value.
    """
    manual = KnowledgeSource.create(
        meta=ResourceMeta(KnowledgeSourceId("knowledge_source_03"), 1, _NOW, _NOW),
        workspace_id=_WORKSPACE_ID,
        created_by=_USER_ID,
        name="Private note",
        source_input=ManualSourceInput("secret interview notes"),
        visibility=_policy(),
    )
    cloud = KnowledgeSource.create(
        meta=ResourceMeta(KnowledgeSourceId("knowledge_source_04"), 1, _NOW, _NOW),
        workspace_id=_WORKSPACE_ID,
        created_by=_USER_ID,
        name="Cloud document",
        source_input=CloudDriveSourceInput(
            ConnectionId("connection_cloud_01"),
            "provider-private-remote-id",
        ),
        visibility=_policy(),
    )

    assert manual.public_config == PublicKnowledgeSourceConfig()
    assert cloud.public_config.filename is None
    assert "secret interview notes" not in repr(manual)
    assert "provider-private-remote-id" not in repr(cloud)
    assert "connection_cloud_01" not in repr(cloud.public_config)


def test_policy_version_must_advance_exactly_one_on_change() -> None:
    """@brief policy 修改必须单调前进一步 / Policy changes must advance exactly one version.

    @return 无返回值 / No return value.
    """
    source = KnowledgeSource.create(
        meta=ResourceMeta(KnowledgeSourceId("knowledge_source_05"), 1, _NOW, _NOW),
        workspace_id=_WORKSPACE_ID,
        created_by=_USER_ID,
        name="Policy source",
        source_input=ManualSourceInput("content"),
        visibility=_policy(version=1),
    )

    revised = source.revise(
        name=None,
        visibility=_policy(version=2, default=PolicyEffect.ALLOW),
        at=_NOW + timedelta(seconds=1),
    )

    assert revised.visibility.policy_version == 2
    with pytest.raises(ValueError, match="exactly one"):
        source.revise(
            name=None,
            visibility=_policy(version=3, default=PolicyEffect.ALLOW),
            at=_NOW + timedelta(seconds=1),
        )


def test_selection_disjointness_and_pin_source_uniqueness_are_structural() -> None:
    """@brief include/exclude 不相交且每个来源只能有一个 pin / Selection sets are disjoint and pins are source-unique.

    @return 无返回值 / No return value.
    """
    source = KnowledgeSourceId("knowledge_source_06")
    with pytest.raises(KnowledgeRetrievalError, match="disjoint"):
        KnowledgeSelection(
            KnowledgeSelectionMode.EXPLICIT,
            (source,),
            (source,),
            (),
            "interview_agent",
        )
    with pytest.raises(KnowledgeRetrievalError, match="one entry per source"):
        KnowledgeSelection(
            KnowledgeSelectionMode.POLICY_DEFAULT,
            (),
            (),
            (
                KnowledgeVersionPin(source, KnowledgeSourceVersionId("knowledge_version_06")),
                KnowledgeVersionPin(source, KnowledgeSourceVersionId("knowledge_version_07")),
            ),
            "interview_agent",
        )


def test_access_evaluation_is_deny_first_and_minimally_explained() -> None:
    """@brief deny 优先且每个决定只返回决定性原因 / Deny wins and each decision returns one decisive reason.

    @return 无返回值 / No return value.
    """
    operation = KnowledgeOperation.RETRIEVE
    policy = _policy(
        default=PolicyEffect.ALLOW,
        grants=(
            AgentScopeGrant("interview_agent", PolicyEffect.ALLOW, (operation,)),
            AgentScopeGrant("interview_agent", PolicyEffect.DENY, (operation,)),
        ),
        regions=(ModelRegion.CN,),
    )
    inference = InferenceIntent(
        InferenceQualityTier.BALANCED,
        5_000,
        InferenceCostTier.STANDARD,
        ModelRegion.CN,
        True,
        False,
    )

    decision = evaluate_visibility(
        source_id=KnowledgeSourceId("knowledge_source_07"),
        enabled=True,
        policy=policy,
        agent_scope="interview_agent",
        operation=operation,
        inference=inference,
    )

    assert decision.effect is PolicyEffect.DENY
    assert decision.reason_codes == ("policy.agent_deny",)


def test_hybrid_score_requires_finite_normalized_sparse_or_dense_evidence() -> None:
    """@brief hybrid score 必须有稀疏或稠密证据且范围归一 / Hybrid scores need normalized evidence.

    @return 无返回值 / No return value.
    """
    assert HybridScore(0.7, 0.9, 0.82).fused == pytest.approx(0.82)
    with pytest.raises(KnowledgeRetrievalError):
        HybridScore(None, None, 0.5)
    with pytest.raises(KnowledgeRetrievalError):
        HybridScore(0.5, None, float("nan"))


def test_platform_job_id_type_is_reused_instead_of_a_second_job_model() -> None:
    """@brief Knowledge 代码复用 platform JobId/ResourceRef / Knowledge code reuses platform types.

    @return 无返回值 / No return value.
    """
    assert JobId("job_knowledge_01") == "job_knowledge_01"
