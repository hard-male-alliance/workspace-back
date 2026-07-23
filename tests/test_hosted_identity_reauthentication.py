"""@brief 重新认证完成证明的领域与服务测试 / Reauthentication completion-proof tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from backend.application.identity import HostedIdentityService
from backend.application.oauth import OAuthAuthorizationService
from backend.domain.identity import IdentityFlowRecord
from backend.domain.ports import IdentityEmailSender
from backend.domain.principals import UserId
from backend.domain.workspaces import DataRegion
from backend.infrastructure.hosted_identity import InMemoryHostedIdentityRepository

NOW = datetime(2026, 7, 23, 12, tzinfo=UTC)
"""@brief 测试判定时刻 / Test verification instant."""

KLEE_ID = UserId("usr_klee")
"""@brief 测试用户标识 / Test user identifier."""


def _service(
    repository: InMemoryHostedIdentityRepository,
    *,
    recent_seconds: int = 300,
) -> HostedIdentityService:
    """@brief 构造仅使用 repository 的测试服务 / Build a repository-only test service.

    @param repository 进程内身份存储 / In-memory identity store.
    @param recent_seconds 重新认证有效窗口秒数 / Reauthentication validity window in seconds.
    @return Hosted identity 应用服务 / Hosted identity application service.
    """
    return HostedIdentityService(
        repository,
        cast(OAuthAuthorizationService, object()),
        cast(IdentityEmailSender, object()),
        recent_reauthentication_seconds=recent_seconds,
    )


def _flow(
    *,
    flow_id: str = "idflow_recent",
    purpose: str = "reauthenticate",
    status: str = "completed",
    user_id: str | None = str(KLEE_ID),
    completed_at: datetime | None = NOW - timedelta(minutes=1),
) -> IdentityFlowRecord:
    """@brief 构造带明确完成语义的测试流程 / Build a flow with explicit completion semantics.

    @param flow_id 流程标识 / Flow identifier.
    @param purpose 流程用途 / Flow purpose.
    @param status 流程状态 / Flow status.
    @param user_id 绑定的本地用户 / Bound local user.
    @param completed_at 精确完成时刻 / Exact completion instant.
    @return 身份流程记录 / Identity flow record.
    """
    return IdentityFlowRecord(
        id=flow_id,
        purpose=purpose,
        status=status,
        allowed_steps=(),
        authorization_request_id="oauthreq_recent",
        browser_session_id="idsess_recent",
        client_id="client_web",
        redirect_uri="https://app.example.test/oauth/callback",
        code_challenge="A" * 43,
        created_at=NOW - timedelta(minutes=9),
        expires_at=NOW + timedelta(minutes=1),
        user_id=user_id,
        completed_at=completed_at,
    )


async def test_verify_recent_uses_completion_instant_not_flow_creation() -> None:
    """@brief 窗口从完成时刻而非创建时刻计算 / Window starts at completion, not creation.

    @return 无返回值 / No return value.
    """
    repository = InMemoryHostedIdentityRepository(data_region=DataRegion.CN)
    await repository.create_flow(_flow())

    assert await _service(repository).verify_recent(KLEE_ID, "idflow_recent", NOW)


@pytest.mark.parametrize(
    ("flow", "user_id", "verified_at"),
    [
        (_flow(flow_id="idflow_wrong_purpose", purpose="login"), KLEE_ID, NOW),
        (
            _flow(flow_id="idflow_wrong_user", user_id="usr_amber"),
            KLEE_ID,
            NOW,
        ),
        (
            _flow(flow_id="idflow_expired", completed_at=NOW - timedelta(minutes=5)),
            KLEE_ID,
            NOW,
        ),
        (
            _flow(flow_id="idflow_future", completed_at=NOW + timedelta(seconds=1)),
            KLEE_ID,
            NOW,
        ),
    ],
)
async def test_verify_recent_fails_closed_for_invalid_proofs(
    flow: IdentityFlowRecord,
    user_id: UserId,
    verified_at: datetime,
) -> None:
    """@brief 非重认证、跨用户、边界过期与未来证明均拒绝 / Invalid proofs fail closed.

    @param flow 待验证流程 / Flow under test.
    @param user_id 请求用户 / Requesting user.
    @param verified_at 判定时刻 / Verification instant.
    @return 无返回值 / No return value.
    """
    repository = InMemoryHostedIdentityRepository(data_region=DataRegion.GLOBAL)
    await repository.create_flow(flow)

    assert not await _service(repository).verify_recent(user_id, flow.id, verified_at)


async def test_verify_recent_rejects_missing_and_naive_verification_time() -> None:
    """@brief 缺失 flow 与无时区判定时间均安全拒绝 / Missing flow and naive time fail closed.

    @return 无返回值 / No return value.
    """
    repository = InMemoryHostedIdentityRepository(data_region=DataRegion.PRIVATE_DEPLOYMENT)
    service = _service(repository)

    assert not await service.verify_recent(KLEE_ID, "idflow_missing", NOW)
    assert not await service.verify_recent(
        KLEE_ID,
        "idflow_missing",
        datetime(2026, 7, 23, 12),
    )


async def test_transition_persists_exact_completion_instant() -> None:
    """@brief 状态转换持久化调用方提供的精确完成时刻 / Transition persists exact completion.

    @return 无返回值 / No return value.
    """
    repository = InMemoryHostedIdentityRepository(data_region=DataRegion.CN)
    created_at = datetime.now(UTC)
    pending = IdentityFlowRecord(
        id="idflow_transition",
        purpose="reauthenticate",
        status="verified",
        allowed_steps=("complete",),
        authorization_request_id="oauthreq_transition",
        browser_session_id="idsess_transition",
        client_id="client_web",
        redirect_uri="https://app.example.test/oauth/callback",
        code_challenge="A" * 43,
        created_at=created_at,
        expires_at=created_at + timedelta(minutes=10),
        user_id=str(KLEE_ID),
    )
    await repository.create_flow(pending)
    completed_at = created_at + timedelta(seconds=2)

    completed = await repository.transition_flow(
        pending.id,
        browser_session_id=pending.browser_session_id,
        step_id="step_complete",
        expected_step="complete",
        allowed_steps=(),
        status="completed",
        state_updates={},
        completed_at=completed_at,
    )

    assert completed is not None
    assert completed.status == "completed"
    assert completed.completed_at == completed_at
    assert (await repository.get_flow(pending.id)) == completed


def test_flow_domain_rejects_missing_or_spurious_completion_instant() -> None:
    """@brief 领域模型拒绝状态与完成时刻不一致 / Domain rejects inconsistent completion.

    @return 无返回值 / No return value.
    """
    with pytest.raises(ValueError, match="require exactly one completion instant"):
        _flow(completed_at=None)
    with pytest.raises(ValueError, match="require exactly one completion instant"):
        _flow(status="verified")
