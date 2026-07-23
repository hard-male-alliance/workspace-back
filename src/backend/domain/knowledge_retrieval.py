"""@brief API v2 Knowledge 选择、混合检索与访问解释 / API v2 Knowledge selection, hybrid retrieval, and access explanation.

本模块把不可信检索 adapter 的候选结果与公开 citation 分开，并为 visibility policy
产生最小充分（minimal sufficient）的单一决定原因。评估结果是审计快照；执行路径仍须
重新读取 membership、policy 和版本归属。
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType

from backend.domain.knowledge_sources import (
    KnowledgeOperation,
    KnowledgeSourceId,
    KnowledgeSourceVersionId,
    KnowledgeVisibilityPolicy,
    ModelRegion,
    PolicyEffect,
)
from backend.domain.principals import DomainInvariantError, UserId, WorkspaceId

_OPAQUE_ID_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{7,159}$")
"""@brief API v2 不透明标识语法 / API v2 opaque-identifier grammar."""

_STABLE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{2,100}$")
"""@brief agent scope 与 reason code 语法 / Agent-scope and reason-code grammar."""

type SearchFilterScalar = None | bool | int | float | str
"""@brief 搜索 filter 的 JSON scalar / JSON scalar accepted in search filters."""

type SearchFilterValue = (
    SearchFilterScalar
    | tuple[SearchFilterValue, ...]
    | Mapping[str, SearchFilterValue]
)
"""@brief 深度不可变搜索 filter JSON 值 / Deeply immutable search-filter JSON value."""


class KnowledgeRetrievalError(DomainInvariantError):
    """@brief Knowledge 检索或访问不变量错误 / Knowledge retrieval or access invariant error."""


class KnowledgeSelectionMode(StrEnum):
    """@brief 契约冻结的 KnowledgeSelection 模式 / Contract-frozen selection modes."""

    NONE = "none"
    POLICY_DEFAULT = "policy_default"
    EXPLICIT = "explicit"


class InferenceQualityTier(StrEnum):
    """@brief 推理质量层级 / Inference quality tiers."""

    FAST = "fast"
    BALANCED = "balanced"
    DEEP = "deep"


class InferenceCostTier(StrEnum):
    """@brief 推理成本层级 / Inference cost tiers."""

    ECONOMY = "economy"
    STANDARD = "standard"
    PREMIUM = "premium"


@dataclass(frozen=True, slots=True)
class InferenceIntent:
    """@brief 访问判断所需最小推理意图 / Minimal inference intent required for access evaluation.

    @param quality_tier 质量层级 / Quality tier.
    @param latency_budget_ms 可空时延预算 / Optional latency budget in milliseconds.
    @param cost_tier 成本层级 / Cost tier.
    @param data_region 实际候选模型数据区域 / Data region of candidate models.
    @param allow_provider_fallback 是否允许 provider fallback / Whether provider fallback is allowed.
    @param allow_external_model_processing 是否允许外部模型处理 / Whether external model processing is requested.
    """

    quality_tier: InferenceQualityTier
    latency_budget_ms: int | None
    cost_tier: InferenceCostTier
    data_region: ModelRegion
    allow_provider_fallback: bool
    allow_external_model_processing: bool

    def __post_init__(self) -> None:
        """@brief 校验推理意图边界 / Validate inference-intent bounds.

        @raise KnowledgeRetrievalError 时延预算非法时抛出 / Raised for an invalid latency budget.
        """
        if self.latency_budget_ms is not None and not 100 <= self.latency_budget_ms <= 600_000:
            raise KnowledgeRetrievalError("inference latency budget must be 100 to 600000 ms")


@dataclass(frozen=True, slots=True)
class KnowledgeVersionPin:
    """@brief KnowledgeSource 与指定 version 的 pin / Pin from a source to one version.

    @param source_id 来源标识 / Source identifier.
    @param version_id 版本标识 / Version identifier.
    """

    source_id: KnowledgeSourceId
    version_id: KnowledgeSourceVersionId

    def __post_init__(self) -> None:
        """@brief 校验 pin 标识 / Validate pin identifiers.

        @raise KnowledgeRetrievalError 标识非法时抛出 / Raised for invalid identifiers.
        """
        _require_opaque_id(self.source_id, "knowledge pin source id")
        _require_opaque_id(self.version_id, "knowledge pin version id")


@dataclass(frozen=True, slots=True)
class KnowledgeSelection:
    """@brief Agent/session 使用的强类型 KnowledgeSelection / Typed KnowledgeSelection for an agent/session.

    @param mode 选择模式 / Selection mode.
    @param include_source_ids 显式包含来源 / Explicitly included sources.
    @param exclude_source_ids 显式排除来源 / Explicitly excluded sources.
    @param pinned_versions 每个来源最多一个 pin / At most one pin per source.
    @param agent_scope 执行 agent scope / Executing agent scope.
    """

    mode: KnowledgeSelectionMode
    include_source_ids: tuple[KnowledgeSourceId, ...]
    exclude_source_ids: tuple[KnowledgeSourceId, ...]
    pinned_versions: tuple[KnowledgeVersionPin, ...]
    agent_scope: str

    def __post_init__(self) -> None:
        """@brief 校验集合不相交、唯一和模式关联 / Validate disjointness, uniqueness, and mode associations.

        @raise KnowledgeRetrievalError selection 不满足契约语义时抛出 / Raised for invalid selection.
        """
        _require_stable_name(self.agent_scope, "knowledge agent scope")
        _require_unique_ids(self.include_source_ids, "included knowledge sources")
        _require_unique_ids(self.exclude_source_ids, "excluded knowledge sources")
        if len(self.include_source_ids) > 200 or len(self.exclude_source_ids) > 200:
            raise KnowledgeRetrievalError("knowledge selection lists cannot exceed 200 sources")
        if set(self.include_source_ids) & set(self.exclude_source_ids):
            raise KnowledgeRetrievalError("knowledge include and exclude sets must be disjoint")
        pin_sources = tuple(pin.source_id for pin in self.pinned_versions)
        if len(self.pinned_versions) > 200 or len(set(pin_sources)) != len(pin_sources):
            raise KnowledgeRetrievalError("pinned versions must contain at most one entry per source")
        if self.mode is KnowledgeSelectionMode.NONE and (
            self.include_source_ids or self.exclude_source_ids or self.pinned_versions
        ):
            raise KnowledgeRetrievalError("none selection cannot include, exclude, or pin sources")
        if self.mode is KnowledgeSelectionMode.EXPLICIT and not self.include_source_ids:
            raise KnowledgeRetrievalError("explicit selection requires at least one included source")


@dataclass(frozen=True, slots=True)
class SearchFilters:
    """@brief 有界且深度不可变的搜索 filters / Bounded, deeply immutable search filters.

    @param values adapter allowlist 解释的 JSON filter map / JSON filter map interpreted by an adapter allowlist.
    """

    values: Mapping[str, SearchFilterValue]

    def __post_init__(self) -> None:
        """@brief 验证并冻结 filters / Validate and freeze filters.

        @raise KnowledgeRetrievalError filter 数量、键、值或深度非法时抛出 / Raised for invalid filters.
        """
        if len(self.values) > 20:
            raise KnowledgeRetrievalError("knowledge search filters cannot exceed 20 entries")
        copied: dict[str, SearchFilterValue] = {}
        for key, value in self.values.items():
            if not key or len(key) > 100 or key.strip() != key:
                raise KnowledgeRetrievalError("knowledge search filter key is invalid")
            copied[key] = _freeze_filter(value, depth=0)
        object.__setattr__(self, "values", MappingProxyType(copied))


@dataclass(frozen=True, slots=True)
class KnowledgeSearchRequest:
    """@brief KnowledgeSearchRequest 的领域表示 / Domain representation of KnowledgeSearchRequest.

    @param query 非空查询 / Non-empty query.
    @param selection Knowledge selection / Knowledge selection.
    @param top_k 最大引用数 / Maximum citation count.
    @param filters 有界 filters / Bounded filters.
    """

    query: str
    selection: KnowledgeSelection
    top_k: int
    filters: SearchFilters = SearchFilters(MappingProxyType({}))

    def __post_init__(self) -> None:
        """@brief 校验 query 与 top-k / Validate query and top-k.

        @raise KnowledgeRetrievalError query 或 top-k 非法时抛出 / Raised for invalid input.
        """
        if not 1 <= len(self.query) <= 8_000 or not self.query.strip():
            raise KnowledgeRetrievalError("knowledge search query must contain 1 to 8000 characters")
        if isinstance(self.top_k, bool) or not 1 <= self.top_k <= 100:
            raise KnowledgeRetrievalError("knowledge search top_k must be between one and 100")


@dataclass(frozen=True, slots=True)
class HybridScore:
    """@brief sparse、dense 与融合后的归一化检索分数 / Normalized sparse, dense, and fused score.

    @param lexical 可空 lexical/BM25 归一化分数 / Optional normalized lexical/BM25 score.
    @param semantic 可空 dense semantic 归一化分数 / Optional normalized dense semantic score.
    @param fused 对外引用使用的最终融合分数 / Final fused score exposed on citations.
    """

    lexical: float | None
    semantic: float | None
    fused: float

    def __post_init__(self) -> None:
        """@brief 校验混合分数范围与信号存在性 / Validate score ranges and signal presence.

        @raise KnowledgeRetrievalError 分数非有限、越界或无信号时抛出 / Raised for invalid scores.
        """
        if self.lexical is None and self.semantic is None:
            raise KnowledgeRetrievalError("hybrid score requires lexical or semantic evidence")
        for label, value in (
            ("lexical", self.lexical),
            ("semantic", self.semantic),
            ("fused", self.fused),
        ):
            if value is not None and (not math.isfinite(value) or not 0.0 <= value <= 1.0):
                raise KnowledgeRetrievalError(f"{label} score must be finite and between zero and one")


@dataclass(frozen=True, slots=True)
class KnowledgeSearchScope:
    """@brief 已授权的 source/version 检索边界 / Authorized source/version retrieval boundary.

    @param source_id 来源 / Source.
    @param version_id 已验证属于来源的目标版本 / Target version verified to belong to the source.
    @param policy_version 执行时读取的 policy 版本 / Policy version read at execution time.
    """

    source_id: KnowledgeSourceId
    version_id: KnowledgeSourceVersionId
    policy_version: int

    def __post_init__(self) -> None:
        """@brief 校验检索边界 / Validate the retrieval boundary.

        @raise KnowledgeRetrievalError 标识或 policy 版本非法时抛出 / Raised for invalid fields.
        """
        _require_opaque_id(self.source_id, "search scope source id")
        _require_opaque_id(self.version_id, "search scope version id")
        if self.policy_version < 1:
            raise KnowledgeRetrievalError("search scope policy version must be positive")


@dataclass(frozen=True, slots=True)
class KnowledgeSearchPlan:
    """@brief 交给混合检索 adapter 的已授权计划 / Authorized plan passed to a hybrid-search adapter.

    @param workspace_id 路径 Workspace / Path Workspace.
    @param actor_id 已完成授权且必须安装进数据库 RLS 的真实用户 / Authenticated user that
        must be installed into database RLS.
    @param query 查询 / Query.
    @param scopes 精确 source/version allowlist / Exact source/version allowlist.
    @param agent_scope agent scope / Agent scope.
    @param top_k 最大结果数 / Maximum result count.
    @param filters adapter 严格 allowlist 的 filters / Filters interpreted through an adapter allowlist.
    """

    workspace_id: WorkspaceId
    actor_id: UserId
    query: str
    scopes: tuple[KnowledgeSearchScope, ...]
    agent_scope: str
    top_k: int
    filters: SearchFilters

    def __post_init__(self) -> None:
        """@brief 校验计划无重复且有界 / Validate a unique, bounded plan.

        @raise KnowledgeRetrievalError Workspace、scope 或数量非法时抛出 / Raised for invalid plan.
        """
        _require_opaque_id(self.workspace_id, "search plan workspace id")
        _require_opaque_id(self.actor_id, "search plan actor id")
        if len(self.scopes) > 200 or len({scope.source_id for scope in self.scopes}) != len(
            self.scopes
        ):
            raise KnowledgeRetrievalError("search plan scopes must be source-unique and at most 200")


@dataclass(frozen=True, slots=True)
class KnowledgeSearchHit:
    """@brief 不可信检索 adapter 返回的带 provenance 候选 / Candidate with provenance from a search adapter.

    @param chunk_id 不可变索引 chunk 标识 / Immutable index-chunk identifier.
    @param workspace_id 候选所属 Workspace / Candidate Workspace.
    @param source_id 来源 / Source.
    @param version_id 版本 / Version.
    @param locator 稳定 source locator / Stable source locator.
    @param quote 有界引用文本 / Bounded quoted text.
    @param score sparse/dense 融合分数 / Sparse/dense fused score.
    """

    chunk_id: str
    workspace_id: WorkspaceId
    source_id: KnowledgeSourceId
    version_id: KnowledgeSourceVersionId
    locator: str
    quote: str
    score: HybridScore

    def __post_init__(self) -> None:
        """@brief 校验候选 provenance 与文本 / Validate candidate provenance and text.

        @raise KnowledgeRetrievalError 字段非法时抛出 / Raised for invalid fields.
        """
        _require_opaque_id(self.chunk_id, "search hit chunk id")
        _require_opaque_id(self.workspace_id, "search hit workspace id")
        _require_opaque_id(self.source_id, "search hit source id")
        _require_opaque_id(self.version_id, "search hit version id")
        if not 1 <= len(self.locator) <= 1_000 or len(self.quote) > 4_000:
            raise KnowledgeRetrievalError("search hit locator or quote violates contract bounds")


@dataclass(frozen=True, slots=True)
class KnowledgeCitation:
    """@brief 契约公开的 Knowledge citation / Contract-public Knowledge citation.

    @param source_id 来源 / Source.
    @param version_id 版本 / Version.
    @param locator 稳定 locator / Stable locator.
    @param quote 引用文本 / Quoted text.
    @param score 归一化融合分数 / Normalized fused score.
    """

    source_id: KnowledgeSourceId
    version_id: KnowledgeSourceVersionId
    locator: str
    quote: str
    score: float

    def __post_init__(self) -> None:
        """@brief 校验公开 citation 的 provenance 与边界 / Validate public citation provenance and bounds.

        @raise KnowledgeRetrievalError ID、文本或分数非法时抛出 / Raised for invalid
            identifiers, text, or score.
        """
        _require_opaque_id(self.source_id, "citation source id")
        _require_opaque_id(self.version_id, "citation version id")
        if not 1 <= len(self.locator) <= 1_000 or len(self.quote) > 4_000:
            raise KnowledgeRetrievalError("citation locator or quote violates contract bounds")
        if isinstance(self.score, bool) or not math.isfinite(self.score) or not 0.0 <= self.score <= 1.0:
            raise KnowledgeRetrievalError("citation score must be finite and between zero and one")

    @classmethod
    def from_hit(cls, hit: KnowledgeSearchHit) -> KnowledgeCitation:
        """@brief 从已重新授权的 hit 构造 citation / Build a citation from a reauthorized hit.

        @param hit 已验证 provenance 的候选 / Candidate with verified provenance.
        @return 仅公开 fused score 的 citation / Citation exposing only the fused score.
        """
        return cls(hit.source_id, hit.version_id, hit.locator, hit.quote, hit.score.fused)


@dataclass(frozen=True, slots=True)
class KnowledgeSearchResult:
    """@brief KnowledgeSearchResult 的领域表示 / Domain representation of KnowledgeSearchResult.

    @param query 原查询 / Original query.
    @param citations 按 fused score 稳定排序的 citations / Citations stably ordered by fused score.
    @param policy_version 检索 adapter 使用的 policy snapshot 水位 / Policy snapshot watermark used by search.
    """

    query: str
    citations: tuple[KnowledgeCitation, ...]
    policy_version: int

    def __post_init__(self) -> None:
        """@brief 校验结果数量、排序与版本 / Validate result count, order, and version.

        @raise KnowledgeRetrievalError 结果不满足契约时抛出 / Raised for invalid results.
        """
        if len(self.citations) > 100:
            raise KnowledgeRetrievalError("knowledge search cannot return more than 100 citations")
        if self.policy_version < 1:
            raise KnowledgeRetrievalError("knowledge search policy version must be positive")
        if any(
            left.score < right.score
            for left, right in zip(self.citations, self.citations[1:], strict=False)
        ):
            raise KnowledgeRetrievalError("knowledge citations must be ordered by descending score")


@dataclass(frozen=True, slots=True)
class KnowledgeAccessEvaluationRequest:
    """@brief Knowledge access evaluation 请求 / Knowledge access-evaluation request.

    @param source_ids 待评估来源 / Sources to evaluate.
    @param agent_scope agent scope / Agent scope.
    @param operation 目标操作 / Target operation.
    @param inference 推理意图 / Inference intent.
    """

    source_ids: tuple[KnowledgeSourceId, ...]
    agent_scope: str
    operation: KnowledgeOperation
    inference: InferenceIntent

    def __post_init__(self) -> None:
        """@brief 校验评估请求 / Validate the evaluation request.

        @raise KnowledgeRetrievalError 数量、重复或 scope 非法时抛出 / Raised for invalid input.
        """
        if not 1 <= len(self.source_ids) <= 200:
            raise KnowledgeRetrievalError("access evaluation requires one to 200 sources")
        _require_unique_ids(self.source_ids, "access evaluation sources")
        _require_stable_name(self.agent_scope, "access evaluation agent scope")


@dataclass(frozen=True, slots=True)
class KnowledgeAccessDecision:
    """@brief 一个来源的可审计访问决定 / Auditable access decision for one source.

    @param source_id 来源 / Source.
    @param effect allow 或 deny / Allow or deny.
    @param policy_version 作决定时 policy 版本 / Policy version used for the decision.
    @param reason_codes 最小充分稳定解释 / Minimal sufficient stable explanation.
    """

    source_id: KnowledgeSourceId
    effect: PolicyEffect
    policy_version: int
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        """@brief 校验决定解释 / Validate the decision explanation.

        @raise KnowledgeRetrievalError 字段或原因非法时抛出 / Raised for invalid fields or reasons.
        """
        _require_opaque_id(self.source_id, "access decision source id")
        if self.policy_version < 1:
            raise KnowledgeRetrievalError("access decision policy version must be positive")
        if not 1 <= len(self.reason_codes) <= 20 or len(set(self.reason_codes)) != len(
            self.reason_codes
        ):
            raise KnowledgeRetrievalError("access decision reasons must be non-empty and unique")
        for reason in self.reason_codes:
            _require_stable_name(reason, "access decision reason")


@dataclass(frozen=True, slots=True)
class KnowledgeAccessEvaluationResult:
    """@brief 同一时刻产生的访问评估快照 / Access-evaluation snapshot produced at one instant.

    @param evaluated_at 评估时刻 / Evaluation instant.
    @param decisions 与请求顺序对应的决定 / Decisions corresponding to request order.
    """

    evaluated_at: datetime
    decisions: tuple[KnowledgeAccessDecision, ...]

    def __post_init__(self) -> None:
        """@brief 校验评估快照 / Validate the evaluation snapshot.

        @raise KnowledgeRetrievalError 时间、数量或重复非法时抛出 / Raised for invalid result.
        """
        _require_aware(self.evaluated_at, "knowledge evaluated_at")
        if not 1 <= len(self.decisions) <= 200:
            raise KnowledgeRetrievalError("access evaluation result requires one to 200 decisions")
        if len({decision.source_id for decision in self.decisions}) != len(self.decisions):
            raise KnowledgeRetrievalError("access evaluation decisions must be source-unique")


def evaluate_visibility(
    *,
    source_id: KnowledgeSourceId,
    enabled: bool,
    policy: KnowledgeVisibilityPolicy,
    agent_scope: str,
    operation: KnowledgeOperation,
    inference: InferenceIntent | None,
) -> KnowledgeAccessDecision:
    """@brief 以 deny-first 顺序生成最小充分 policy 决定 / Produce a minimal sufficient deny-first decision.

    @param source_id 来源 / Source.
    @param enabled 来源是否可执行 / Whether the source is executable.
    @param policy 当前 visibility policy / Current visibility policy.
    @param agent_scope 当前 agent scope / Current agent scope.
    @param operation 目标操作 / Target operation.
    @param inference 可空推理意图；纯本地检索为空 / Optional inference intent, absent for local retrieval.
    @return 含单个决定性 reason code 的结果 / Result carrying one decisive reason code.
    @note 评估不能缓存为执行授权；调用方执行前必须重新授权 / Evaluation is never an
        execution grant; callers must reauthorize immediately before execution.
    """
    _require_stable_name(agent_scope, "knowledge agent scope")
    if not enabled:
        return _decision(source_id, policy, PolicyEffect.DENY, "source.disabled")
    if inference is not None and inference.data_region not in policy.allowed_model_regions:
        return _decision(source_id, policy, PolicyEffect.DENY, "policy.model_region_denied")
    if (
        inference is not None
        and inference.allow_external_model_processing
        and not policy.allow_external_model_processing
    ):
        return _decision(source_id, policy, PolicyEffect.DENY, "policy.external_processing_denied")
    matching = tuple(
        grant for grant in policy.agent_grants if grant.applies_to(agent_scope, operation)
    )
    if any(grant.effect is PolicyEffect.DENY for grant in matching):
        return _decision(source_id, policy, PolicyEffect.DENY, "policy.agent_deny")
    if any(grant.effect is PolicyEffect.ALLOW for grant in matching):
        return _decision(source_id, policy, PolicyEffect.ALLOW, "policy.agent_allow")
    reason = (
        "policy.default_allow"
        if policy.default_effect is PolicyEffect.ALLOW
        else "policy.default_deny"
    )
    return _decision(source_id, policy, policy.default_effect, reason)


def _decision(
    source_id: KnowledgeSourceId,
    policy: KnowledgeVisibilityPolicy,
    effect: PolicyEffect,
    reason: str,
) -> KnowledgeAccessDecision:
    """@brief 构造单一决定原因 / Construct a decision with one decisive reason.

    @param source_id 来源 / Source.
    @param policy 决策 policy / Decision policy.
    @param effect 决策效果 / Decision effect.
    @param reason 稳定原因 / Stable reason.
    @return 最小充分决定 / Minimal sufficient decision.
    """
    return KnowledgeAccessDecision(source_id, effect, policy.policy_version, (reason,))


def _freeze_filter(value: object, *, depth: int) -> SearchFilterValue:
    """@brief 递归校验并冻结 filter JSON / Recursively validate and freeze filter JSON.

    @param value 不可信 JSON 值 / Untrusted JSON value.
    @param depth 当前深度 / Current depth.
    @return 深度不可变值 / Deeply immutable value.
    @raise KnowledgeRetrievalError 类型、数值、大小或深度非法时抛出 / Raised for invalid JSON.
    """
    if depth > 8:
        raise KnowledgeRetrievalError("knowledge search filter depth cannot exceed eight")
    if value is None or isinstance(value, (bool, str)):
        if isinstance(value, str) and len(value) > 2_000:
            raise KnowledgeRetrievalError("knowledge search filter string is too long")
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise KnowledgeRetrievalError("knowledge search filter number must be finite")
        return value
    if isinstance(value, (list, tuple)):
        if len(value) > 200:
            raise KnowledgeRetrievalError("knowledge search filter array cannot exceed 200 items")
        return tuple(_freeze_filter(item, depth=depth + 1) for item in value)
    if isinstance(value, Mapping):
        if len(value) > 50:
            raise KnowledgeRetrievalError("nested knowledge search filter cannot exceed 50 entries")
        copied: dict[str, SearchFilterValue] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key or len(key) > 100:
                raise KnowledgeRetrievalError("nested knowledge search filter key is invalid")
            copied[key] = _freeze_filter(item, depth=depth + 1)
        return MappingProxyType(copied)
    raise KnowledgeRetrievalError("knowledge search filters must contain only JSON values")


def _require_unique_ids(values: tuple[str, ...], label: str) -> None:
    """@brief 校验 ID 列表唯一且语法正确 / Validate a unique list of syntactically valid IDs.

    @param values 标识列表 / Identifier list.
    @param label 错误标签 / Error label.
    @raise KnowledgeRetrievalError 重复或标识非法时抛出 / Raised for duplicates or invalid IDs.
    """
    if len(set(values)) != len(values):
        raise KnowledgeRetrievalError(f"{label} must be unique")
    for value in values:
        _require_opaque_id(value, label)


def _require_opaque_id(value: str, label: str) -> None:
    """@brief 校验 API v2 不透明标识 / Validate an API v2 opaque identifier.

    @param value 标识 / Identifier.
    @param label 错误标签 / Error label.
    @raise KnowledgeRetrievalError 标识非法时抛出 / Raised for an invalid identifier.
    """
    if _OPAQUE_ID_PATTERN.fullmatch(value) is None:
        raise KnowledgeRetrievalError(f"{label} does not satisfy the API v2 grammar")


def _require_stable_name(value: str, label: str) -> None:
    """@brief 校验稳定名称 / Validate a stable name.

    @param value 名称 / Name.
    @param label 错误标签 / Error label.
    @raise KnowledgeRetrievalError 名称非法时抛出 / Raised for an invalid name.
    """
    if _STABLE_NAME_PATTERN.fullmatch(value) is None:
        raise KnowledgeRetrievalError(f"{label} does not satisfy the stable-name grammar")


def _require_aware(value: datetime, label: str) -> None:
    """@brief 校验带时区时间 / Validate a timezone-aware datetime.

    @param value 时间 / Datetime.
    @param label 错误标签 / Error label.
    @raise KnowledgeRetrievalError 时间 naive 时抛出 / Raised for a naive datetime.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise KnowledgeRetrievalError(f"{label} must be timezone-aware")


__all__ = [
    "HybridScore",
    "InferenceCostTier",
    "InferenceIntent",
    "InferenceQualityTier",
    "KnowledgeAccessDecision",
    "KnowledgeAccessEvaluationRequest",
    "KnowledgeAccessEvaluationResult",
    "KnowledgeCitation",
    "KnowledgeRetrievalError",
    "KnowledgeSearchHit",
    "KnowledgeSearchPlan",
    "KnowledgeSearchRequest",
    "KnowledgeSearchResult",
    "KnowledgeSearchScope",
    "KnowledgeSelection",
    "KnowledgeSelectionMode",
    "KnowledgeVersionPin",
    "SearchFilterValue",
    "SearchFilters",
    "evaluate_visibility",
]
