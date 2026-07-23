"""API v2 Interview 领域核心测试。"""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from datetime import UTC, datetime, timedelta

import pytest

from backend.domain.interview_v2 import (
    INTERVIEW_END_JOB_KIND,
    CandidateUtteranceInput,
    EndInterviewReason,
    EphemeralToken,
    FallbackTransport,
    IceServer,
    InterviewAvatarPreferences,
    InterviewCommunicationMetrics,
    InterviewDifficulty,
    InterviewDomainError,
    InterviewEvidence,
    InterviewExecutionGrant,
    InterviewMediaPreferences,
    InterviewReportDraft,
    InterviewRichText,
    InterviewRubric,
    InterviewScenario,
    InterviewScenarioId,
    InterviewScenarioPatch,
    InterviewScenarioSpec,
    InterviewScenarioStatus,
    InterviewSession,
    InterviewSessionId,
    InterviewSessionSpec,
    InterviewSessionStatus,
    InterviewSessionView,
    InterviewTransitionError,
    JobTarget,
    RealtimeConnection,
    RealtimeConnectionId,
    RealtimeInputEnvelope,
    RealtimeInputId,
    RealtimeTransport,
    RecordingConsent,
    RubricDimension,
    RubricScore,
    ScoreScale,
    TranscriptSegment,
    TranscriptSegmentId,
    TranscriptSpeaker,
    realtime_input_fingerprint,
    validate_interview_job_alignment,
)
from backend.domain.knowledge_retrieval import (
    InferenceCostTier,
    InferenceIntent,
    InferenceQualityTier,
    KnowledgeSelection,
    KnowledgeSelectionMode,
)
from backend.domain.knowledge_sources import ModelRegion
from backend.domain.platform import Job, JobId
from backend.domain.principals import ResourceMeta, WorkspaceId
from backend.domain.resources import ResourceRef

NOW = datetime(2026, 7, 23, 3, 0, tzinfo=UTC)
WORKSPACE = WorkspaceId("workspace_0001")
SCENARIO_ID = InterviewScenarioId("scenario_0001")
SESSION_ID = InterviewSessionId("session_0001")
JOB_ID = JobId("job_end_0001")


def _rubric() -> InterviewRubric:
    return InterviewRubric(
        rubric_id="rubric_0001",
        rubric_version="2026-07",
        name="系统设计 Rubric",
        dimensions=(
            RubricDimension(
                dimension_id="dimension_0001",
                name="一致性",
                description="能够准确分析一致性取舍。",
                weight=0.4,
                observable_indicators=("说明线性一致性",),
                scoring_scale=ScoreScale(0, 100),
            ),
            RubricDimension(
                dimension_id="dimension_0002",
                name="容错",
                description="能够准确分析故障模型。",
                weight=0.6,
                observable_indicators=("说明故障域",),
                scoring_scale=ScoreScale(0, 100),
            ),
        ),
        overall_scale=ScoreScale(0, 100),
    )


def _scenario(*, status: InterviewScenarioStatus = InterviewScenarioStatus.ACTIVE) -> InterviewScenario:
    return InterviewScenario(
        ResourceMeta(SCENARIO_ID, 1, NOW, NOW),
        WORKSPACE,
        InterviewScenarioSpec(
            name="分布式系统面试",
            description="评估系统设计能力",
            locale="zh-CN",
            interview_type="system_design",
            difficulty=InterviewDifficulty.ADVANCED,
            duration_minutes=45,
            target_question_count=8,
            focus_areas=("一致性", "容错"),
            allow_followups=True,
            allow_barge_in=True,
            rubric=_rubric(),
        ),
        status,
    )


def _media() -> InterviewMediaPreferences:
    from backend.domain.interview_v2 import AvatarOutputMode

    return InterviewMediaPreferences(
        user_audio=True,
        user_video=False,
        screen_share=False,
        max_video_width=1920,
        max_video_height=1080,
        max_video_fps=30,
        avatar=InterviewAvatarPreferences(
            AvatarOutputMode.AUDIO_ONLY,
            None,
            "voice_zh_0001",
            ("opus",),
            (),
            False,
            False,
        ),
        fallback_transport=FallbackTransport.WEBSOCKET,
    )


def _recording(*, store_transcript: bool = True) -> RecordingConsent:
    return RecordingConsent(
        record_audio=True,
        record_video=False,
        store_transcript=store_transcript,
        retention_days=30,
        consented_at=NOW,
        consent_version="consent-2026-07",
    )


def _spec() -> InterviewSessionSpec:
    return InterviewSessionSpec(
        scenario_id=SCENARIO_ID,
        scenario_revision=1,
        rubric_snapshot=_rubric(),
        resume_ref=ResourceRef("resume", "resume_0001", 3),
        job_target=JobTarget(
            "Senior Distributed Systems Engineer",
            "HM Alliances",
            "Singapore",
            "Build reliable platforms",
            "https://example.com/jobs/1",
            "senior",
            ("distributed-systems",),
        ),
        knowledge=KnowledgeSelection(
            KnowledgeSelectionMode.NONE,
            (),
            (),
            (),
            "interview_agent",
        ),
        locale="zh-CN",
        media=_media(),
        recording=_recording(),
        inference=InferenceIntent(
            InferenceQualityTier.BALANCED,
            10_000,
            InferenceCostTier.STANDARD,
            ModelRegion.CN,
            False,
            False,
        ),
    )


def _grant() -> InterviewExecutionGrant:
    return InterviewExecutionGrant(
        scenario_ref=ResourceRef("interview_scenario", SCENARIO_ID, 1),
        resume_ref=ResourceRef("resume", "resume_0001", 3),
        agent_scope="interview_agent",
        model_ref=ResourceRef("model", "model_0001", 4),
        model_region=ModelRegion.CN,
        external_model_processing=False,
        knowledge_contexts=(),
        policy_version=2,
    )


def _session() -> InterviewSession:
    spec = _spec()
    return InterviewSession(
        InterviewSessionView(
            ResourceMeta(SESSION_ID, 1, NOW, NOW),
            WORKSPACE,
            SCENARIO_ID,
            spec.resume_ref,
            spec.job_target,
            InterviewSessionStatus.CREATED,
            spec.locale,
            spec.media,
            spec.recording,
            None,
            None,
            None,
        ),
        spec,
        _grant(),
    )


def _segments() -> tuple[TranscriptSegment, ...]:
    return (
        TranscriptSegment(
            TranscriptSegmentId("segment_0001"),
            WORKSPACE,
            SESSION_ID,
            1,
            ResourceRef("realtime_input", "input_domain_0001"),
            TranscriptSpeaker.CANDIDATE,
            1_000,
            5_000,
            "线性一致性要求每次操作看起来在一个瞬间生效。",
        ),
    )


def _draft(*, evidence_end: int = 4_000) -> InterviewReportDraft:
    evidence = InterviewEvidence("segment_0001", 1_000, evidence_end, "线性一致性")  # type: ignore[arg-type]
    return InterviewReportDraft(
        report_version="1",
        rubric_id="rubric_0001",
        rubric_version="2026-07",
        engine_version="engine-1",
        overall_score=82,
        overall_confidence=0.8,
        executive_summary=InterviewRichText("总体表现稳健。"),
        rubric_scores=(
            RubricScore(
                "dimension_0001",
                85,
                0.9,
                InterviewRichText("一致性理解清晰。"),
                (evidence,),
                ("补充形式化定义",),
            ),
            RubricScore(
                "dimension_0002",
                80,
                0.7,
                InterviewRichText("容错分析完整。"),
                (),
                ("补充故障注入练习",),
            ),
        ),
        strengths=(InterviewRichText("取舍明确。"),),
        improvements=(InterviewRichText("增加定量分析。"),),
        communication_metrics=InterviewCommunicationMetrics(
            10_000,
            5_000,
            120,
            1,
            0,
            0,
            (),
        ),
        action_plan=(),
        limitations=("仅基于本次 Transcript",),
    )


def test_rubric_requires_unique_dimensions_and_unit_weight() -> None:
    first = _rubric().dimensions[0]
    with pytest.raises(InterviewDomainError):
        InterviewRubric(
            "rubric_bad_0001",
            "1",
            "bad",
            (first, first),
            ScoreScale(0, 100),
        )
    with pytest.raises(InterviewDomainError):
        InterviewRubric(
            "rubric_bad_0002",
            "1",
            "bad",
            (
                RubricDimension(
                    "dimension_bad1",
                    "bad",
                    "bad",
                    0.2,
                    (),
                    ScoreScale(0, 100),
                ),
            ),
            ScoreScale(0, 100),
        )


def test_scenario_state_is_one_way_and_patch_preserves_nested_types() -> None:
    scenario = _scenario(status=InterviewScenarioStatus.DRAFT)
    active = scenario.update(
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE, "name": "新场景"}),
        at=NOW + timedelta(seconds=1),
    )
    assert active.meta.revision == 2
    assert isinstance(active.spec.rubric, InterviewRubric)

    archived = active.update(
        InterviewScenarioPatch({"status": InterviewScenarioStatus.ARCHIVED}),
        at=NOW + timedelta(seconds=2),
    )
    with pytest.raises(InterviewTransitionError):
        archived.update(
            InterviewScenarioPatch({"status": InterviewScenarioStatus.ACTIVE}),
            at=NOW + timedelta(seconds=3),
        )


def test_recording_requires_explicit_consent_and_matching_media() -> None:
    with pytest.raises(InterviewDomainError):
        RecordingConsent(True, False, False, 30, None, None)
    with pytest.raises(InterviewDomainError):
        InterviewSessionSpec(
            scenario_id=SCENARIO_ID,
            scenario_revision=1,
            rubric_snapshot=_rubric(),
            resume_ref=None,
            job_target=_spec().job_target,
            knowledge=_spec().knowledge,
            locale="zh-CN",
            media=InterviewMediaPreferences(
                False,
                False,
                False,
                640,
                480,
                30,
                _media().avatar,
                FallbackTransport.NONE,
            ),
            recording=_recording(),
            inference=_spec().inference,
        )
    assert _recording().retention_until == NOW + timedelta(days=30)


def test_session_fsm_and_unified_job_alignment_are_strict() -> None:
    session = _session()
    connecting = session.mark_connecting(at=NOW + timedelta(seconds=1))
    active = connecting.activate(at=NOW + timedelta(seconds=2))
    ending = active.begin_end(
        JOB_ID,
        EndInterviewReason.COMPLETED,
        at=NOW + timedelta(seconds=3),
    )
    job = Job(
        ResourceMeta(JOB_ID, 1, NOW + timedelta(seconds=3), NOW + timedelta(seconds=3)),
        WORKSPACE,
        INTERVIEW_END_JOB_KIND,
        ResourceRef("interview_session", SESSION_ID, ending.meta.revision),
    )
    validate_interview_job_alignment(ending, job)

    running = job.start(at=NOW + timedelta(seconds=4))
    completed = ending.finish_end(at=NOW + timedelta(seconds=5))
    succeeded = running.succeed((), at=NOW + timedelta(seconds=5))
    validate_interview_job_alignment(completed, succeeded)
    assert completed.view.status is InterviewSessionStatus.COMPLETED
    with pytest.raises(InterviewTransitionError):
        completed.mark_connecting(at=NOW + timedelta(seconds=6))


def test_realtime_connection_is_short_lived_bound_and_secret_redacted() -> None:
    connection = RealtimeConnection(
        RealtimeConnectionId("connection_0001"),
        WORKSPACE,
        SESSION_ID,
        ResourceRef("user", "user_actor_0001"),
        RealtimeTransport.WEBRTC,
        "wss://realtime.example.com/session",
        EphemeralToken("x" * 32),
        (IceServer(("turn:turn.example.com",), "user", "credential"),),
        NOW,
        NOW + timedelta(minutes=5),
        10_000,
    )
    assert "x" * 32 not in repr(connection)
    assert str(connection.ephemeral_token) == "<redacted>"
    with pytest.raises(InterviewDomainError):
        RealtimeConnection(
            RealtimeConnectionId("connection_0002"),
            WORKSPACE,
            SESSION_ID,
            ResourceRef("user", "user_actor_0001"),
            RealtimeTransport.WEBRTC,
            "wss://realtime.example.com/session",
            EphemeralToken("y" * 32),
            (),
            NOW,
            NOW + timedelta(minutes=16),
            10_000,
        )


def test_realtime_ledger_discards_plaintext_and_transcript_is_append_only() -> None:
    payload = CandidateUtteranceInput("我会先定义一致性模型。", 0, 2_000)
    envelope = RealtimeInputEnvelope(
        RealtimeInputId("input_0001"),
        WORKSPACE,
        SESSION_ID,
        RealtimeConnectionId("connection_0001"),
        NOW,
        payload,
        realtime_input_fingerprint(payload),
    )
    ledger = envelope.ledger_record()
    assert not hasattr(ledger, "payload")
    assert not hasattr(ledger, "text")

    segment = _segments()[0]
    with pytest.raises(FrozenInstanceError):
        segment.sequence = 2  # type: ignore[misc]


def test_report_is_validated_against_frozen_rubric_and_real_segments() -> None:
    draft = _draft()
    draft.validate_against(_rubric(), _segments(), SESSION_ID)

    with pytest.raises(InterviewDomainError):
        _draft(evidence_end=8_000).validate_against(_rubric(), _segments(), SESSION_ID)
    with pytest.raises(InterviewDomainError):
        _draft().validate_against(
            _rubric(),
            (
                TranscriptSegment(
                    TranscriptSegmentId("segment_0001"),
                    WORKSPACE,
                    InterviewSessionId("session_other_0001"),
                    1,
                    ResourceRef("realtime_input", "input_domain_0001"),
                    TranscriptSpeaker.CANDIDATE,
                    1_000,
                    5_000,
                    "线性一致性",
                ),
            ),
            SESSION_ID,
        )


def test_public_report_draft_has_no_private_reasoning_or_raw_provider_field() -> None:
    names = {item.name for item in fields(InterviewReportDraft)}
    forbidden = {"reasoning", "chain_of_thought", "raw_provider_response", "prompt", "embedding"}
    assert not names & forbidden
