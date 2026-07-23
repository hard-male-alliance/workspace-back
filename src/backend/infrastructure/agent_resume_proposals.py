"""@brief Agent 与 Resume 间的同事务 Proposal 防腐层 / Same-transaction Agent-to-Resume Proposal anti-corruption layer.

本模块是两个 bounded context（限界上下文）的唯一写桥：读取精确 Resume revision，
把模型的无身份草案映射为服务端稳定 ID，预演领域操作，并在 Agent 最终事务中插入
pending Proposal。它从不改写权威 Resume。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from hashlib import sha256
from typing import Protocol, cast

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.application.ports.agent_v2 import (
    AgentProposalFailure,
    AgentResumeProposalCommand,
)
from backend.domain.agent_v2 import (
    AgentResumeContext,
    AgentResumeOperationDraft,
)
from backend.domain.platform import JsonValue, ProblemDetails
from backend.domain.principals import UserId, WorkspaceId
from backend.domain.resources import ResourceRef
from backend.domain.resumes import (
    ResumeOperation,
    preview_resume_operations,
    resume_operation_fingerprint,
)
from backend.infrastructure.persistence.models import (
    JsonObject,
    ResumeDocumentRecord,
    ResumeProposalOperationRecord,
    ResumeProposalRecord,
    ResumeRevisionRecord,
)
from backend.infrastructure.resumes import (
    decode_resume_document,
    decode_resume_operation,
    encode_resume_operation,
)

_RESOURCE_REFS_ADAPTER: TypeAdapter[tuple[ResourceRef, ...]] = TypeAdapter(
    tuple[ResourceRef, ...]
)
"""@brief Proposal evidence refs codec / Proposal evidence-reference codec."""


class _AgentProposalScope(Protocol):
    """@brief Agent UoW 已安装的最小身份作用域 / Minimal identity scope installed by the Agent UoW."""

    def require_workspace(self, workspace_id: WorkspaceId) -> None:
        """@brief 要求 Workspace 与当前事务一致 / Require the transaction Workspace."""

    def require_actor(self) -> UserId:
        """@brief 返回当前事务 actor / Return the transaction actor."""


class PostgresAgentResumeProposalBoundary:
    """@brief 在 Agent AsyncSession 中物化 Resume Proposal / Materialize Resume Proposals in the Agent AsyncSession."""

    def __init__(
        self,
        session: AsyncSession,
        scope: _AgentProposalScope,
    ) -> None:
        """@brief 绑定同一事务与密封身份 / Bind the same transaction and sealed identity.

        @param session Agent 最终事务的 Session / Session for the Agent final transaction.
        @param scope 已安装 RLS 的 Agent scope / Agent scope with RLS installed.
        """

        self._session = session
        self._scope = scope

    async def load_base(
        self,
        workspace_id: WorkspaceId,
        resume_ref: ResourceRef,
    ) -> AgentResumeContext:
        """@brief 锁定并读取精确、未删除的 Resume SIR / Lock and load an exact live Resume SIR.

        @param workspace_id 当前 Run Workspace / Current Run Workspace.
        @param resume_ref 必须含 revision 的 Resume ref / Revision-bearing Resume reference.
        @return 完整性校验后的精确上下文 / Integrity-verified exact context.
        @raise AgentProposalFailure Resume 缺失、过期或损坏时抛出 / Raised for an absent,
            stale, or corrupt Resume.
        """

        self._scope.require_workspace(workspace_id)
        revision = resume_ref.revision
        if resume_ref.resource_type != "resume" or revision is None:
            raise _proposal_failure(
                resume_ref.id,
                "agent.resume_context_invalid",
                "Agent Resume context must be an exact revision",
                422,
            )
        statement = (
            select(ResumeDocumentRecord, ResumeRevisionRecord)
            .join(
                ResumeRevisionRecord,
                (
                    ResumeRevisionRecord.workspace_id
                    == ResumeDocumentRecord.workspace_id
                )
                & (ResumeRevisionRecord.resume_id == ResumeDocumentRecord.id)
                & (
                    ResumeRevisionRecord.revision_no
                    == ResumeDocumentRecord.current_revision_no
                ),
            )
            .where(
                ResumeDocumentRecord.workspace_id == str(workspace_id),
                ResumeDocumentRecord.id == resume_ref.id,
                ResumeDocumentRecord.current_revision_no == revision,
                ResumeDocumentRecord.deleted_at.is_(None),
            )
            .with_for_update(of=ResumeDocumentRecord)
        )
        row = (await self._session.execute(statement)).one_or_none()
        if row is None:
            raise _proposal_failure(
                resume_ref.id,
                "agent.resume_context_stale",
                "Agent Resume context is absent or stale",
                409,
            )
        _, revision_record = row
        try:
            document = decode_resume_document(
                revision_record.semantic_document,
                expected_sha256=revision_record.content_hash,
            )
            context = AgentResumeContext(resume_ref, document)
        except (TypeError, ValueError) as error:
            raise _proposal_failure(
                resume_ref.id,
                "agent.resume_context_corrupt",
                "Agent Resume context failed integrity validation",
                500,
            ) from error
        if document.workspace_id != workspace_id:
            raise _proposal_failure(
                resume_ref.id,
                "agent.resume_context_stale",
                "Agent Resume context is absent or stale",
                409,
            )
        return context

    async def create(self, command: AgentResumeProposalCommand) -> ResourceRef:
        """@brief 幂等创建可审核 Proposal，不写 Resume / Idempotently create a reviewable Proposal without writing Resume.

        @param command 当前 Run、精确基础 revision 与模型草案 / Current Run, exact base
            revision, and model drafts.
        @return 稳定 Proposal ref / Stable Proposal reference.
        @raise AgentProposalFailure 草案、基础 revision 或持久状态不一致时抛出 / Raised for
            invalid drafts, a stale base, or inconsistent persisted state.
        """

        self._scope.require_workspace(command.workspace_id)
        actor_id = self._scope.require_actor()
        if actor_id != command.actor_id:
            raise PermissionError("Agent Proposal actor does not match the worker scope")
        if command.base.document.workspace_id != command.workspace_id:
            raise _proposal_failure(
                str(command.run_id),
                "agent.resume_context_stale",
                "Agent Resume context is absent or stale",
                409,
            )
        if (
            not 1 <= len(command.title) <= 300
            or command.created_at.tzinfo is None
            or command.created_at.utcoffset() is None
        ):
            raise _proposal_failure(
                str(command.run_id),
                "agent.resume_operations_invalid",
                "Model-generated Resume proposal metadata is invalid",
                422,
            )

        persisted_base = await self.load_base(
            command.workspace_id,
            command.base.resume_ref,
        )
        if persisted_base != command.base:
            raise _proposal_failure(
                str(command.run_id),
                "agent.resume_context_stale",
                "Agent Resume snapshot differs from the persisted revision",
                409,
            )
        try:
            operations = _materialize_operations(
                command.run_id,
                persisted_base,
                command.operations,
            )
            preview_resume_operations(persisted_base.document, operations)
        except (TypeError, ValueError) as error:
            raise _proposal_failure(
                str(command.run_id),
                "agent.resume_operations_invalid",
                "Model-generated Resume operations are not valid for the base revision",
                422,
            ) from error
        evidence_refs = _evidence_refs(command)
        if len(evidence_refs) > 200:
            raise _proposal_failure(
                str(command.run_id),
                "agent.resume_operations_invalid",
                "Model-generated Resume proposal metadata is invalid",
                422,
            )
        proposal_id = _stable_id("proposal", str(command.run_id))
        existing = await self._session.scalar(
            select(ResumeProposalRecord)
            .where(
                ResumeProposalRecord.workspace_id == str(command.workspace_id),
                ResumeProposalRecord.id == proposal_id,
            )
            .with_for_update()
        )
        if existing is not None:
            await self._require_matching_replay(
                existing,
                command,
                operations,
                evidence_refs,
            )
            return ResourceRef("resume_proposal", proposal_id, existing.revision)

        proposal = ResumeProposalRecord(
            id=proposal_id,
            workspace_id=str(command.workspace_id),
            resource_owner_id=str(actor_id),
            resume_id=command.base.resume_ref.id,
            agent_run_id=str(command.run_id),
            base_revision_no=cast(int, command.base.resume_ref.revision),
            title=command.title,
            status="pending",
            decision_payload=None,
            decided_by_actor_id=None,
            decided_at=None,
            expires_at=None,
            evidence_refs=_dump_resource_refs(evidence_refs),
            created_at=command.created_at,
            updated_at=command.created_at,
            revision=1,
            extensions={},
        )
        self._session.add(proposal)
        # 两张 legacy ORM 表没有 relationship，显式先 flush 父行让数据库 FK 决定顺序；
        # flush 仍在当前 Agent transaction 内，后续任一失败会整体 rollback。
        await self._session.flush((proposal,))
        for ordinal, operation in enumerate(operations):
            operation_id = str(operation.operation_id)
            self._session.add(
                ResumeProposalOperationRecord(
                    id=_stable_id("propop", f"{command.run_id}:{ordinal}"),
                    workspace_id=str(command.workspace_id),
                    resource_owner_id=str(actor_id),
                    proposal_id=proposal_id,
                    ordinal=ordinal,
                    operation_id=operation_id,
                    operation_type=operation.op,
                    payload=encode_resume_operation(operation),
                    fingerprint=resume_operation_fingerprint(operation),
                    applied_revision_no=None,
                    decision=None,
                    created_at=command.created_at,
                    updated_at=command.created_at,
                    revision=1,
                    extensions={},
                )
            )
        return ResourceRef("resume_proposal", proposal_id, 1)

    async def _require_matching_replay(
        self,
        existing: ResumeProposalRecord,
        command: AgentResumeProposalCommand,
        operations: tuple[ResumeOperation, ...],
        evidence_refs: tuple[ResourceRef, ...],
    ) -> None:
        """@brief 要求稳定 ID 重放与已存内容完全一致 / Require a stable-ID replay to match persisted content exactly.

        @param existing 已锁定 Proposal row / Locked Proposal row.
        @param command 当前物化命令 / Current materialization command.
        @param operations 已服务端验证的 operations / Server-validated operations.
        @param evidence_refs 已服务端派生的证据 refs / Server-derived evidence refs.
        @raise AgentProposalFailure 同一 Run 的稳定 ID 被不同内容复用时抛出 / Raised when
            one Run's stable ID is reused for different content.
        """

        operation_rows = (
            await self._session.scalars(
                select(ResumeProposalOperationRecord)
                .where(
                    ResumeProposalOperationRecord.workspace_id
                    == str(command.workspace_id),
                    ResumeProposalOperationRecord.proposal_id == existing.id,
                )
                .order_by(ResumeProposalOperationRecord.ordinal)
            )
        ).all()
        expected_evidence = _dump_resource_refs(evidence_refs)
        identity_matches = (
            existing.agent_run_id == str(command.run_id)
            and existing.resume_id == command.base.resume_ref.id
            and existing.base_revision_no == command.base.resume_ref.revision
            and existing.resource_owner_id == str(command.actor_id)
            and existing.title == command.title
            and existing.evidence_refs == expected_evidence
        )
        operations_match = len(operation_rows) == len(operations) and all(
            row.ordinal == ordinal
            and row.operation_id == str(operation.operation_id)
            and row.operation_type == operation.op
            and row.payload == encode_resume_operation(operation)
            and row.fingerprint == resume_operation_fingerprint(operation)
            for ordinal, (row, operation) in enumerate(
                zip(operation_rows, operations, strict=True)
            )
        )
        if not identity_matches or not operations_match:
            raise _proposal_failure(
                str(command.run_id),
                "agent.proposal_identity_conflict",
                "Agent Proposal identity is already bound to different content",
                409,
            )


class UnavailableAgentResumeProposalBoundary:
    """@brief 内存运行时拒绝伪造 durable Resume Proposal / Reject fake durable Resume Proposals in memory mode."""

    async def load_base(
        self,
        workspace_id: WorkspaceId,
        resume_ref: ResourceRef,
    ) -> AgentResumeContext:
        """@brief 以 503 拒绝内存 Resume 快照读取 / Reject in-memory Resume snapshot reads with 503.

        @param workspace_id 请求 Workspace / Requested Workspace.
        @param resume_ref 精确 Resume ref / Exact Resume reference.
        @raise AgentProposalFailure memory runtime 无 durable 跨域事务时抛出 / Raised because
            memory mode has no durable cross-context transaction.
        """

        del workspace_id
        raise _durable_runtime_failure(resume_ref.id)

    async def create(self, command: AgentResumeProposalCommand) -> ResourceRef:
        """@brief 以 503 拒绝内存 Proposal 写入 / Reject in-memory Proposal writes with 503.

        @param command 未执行的 Proposal 命令 / Proposal command that is not executed.
        @raise AgentProposalFailure memory runtime 无 durable 跨域事务时抛出 / Raised because
            memory mode has no durable cross-context transaction.
        """

        raise _durable_runtime_failure(str(command.run_id))


def _materialize_operations(
    run_id: str,
    base: AgentResumeContext,
    drafts: Sequence[AgentResumeOperationDraft],
) -> tuple[ResumeOperation, ...]:
    """@brief 派生稳定 ID、重映射新实体并解码 operation union / Derive stable IDs, remap new entities, and decode the operation union.

    @param run_id 稳定 Run identity / Stable Run identity.
    @param base 精确 Resume snapshot / Exact Resume snapshot.
    @param drafts 模型的无身份草案 / Identity-free model drafts.
    @return 完整类型化 operations / Fully typed operations.
    @raise ValueError 草案引用未知临时实体或形状不合法时抛出 / Raised for unknown temporary
        references or malformed drafts.
    """

    if not 1 <= len(drafts) <= 200:
        raise ValueError("Agent Resume proposal must contain one to 200 operations")
    payloads = [_thaw_object(draft.payload) for draft in drafts]
    existing = _resume_entity_ids(base)
    remapped: dict[str, str] = {}
    kinds: dict[str, str] = {}
    for ordinal, payload in enumerate(payloads):
        operation_type = payload.get("op")
        if operation_type == "upsert_section":
            section = _object_field(payload, "section")
            _register_new_entity(
                section,
                kind="section",
                ordinal=ordinal,
                run_id=run_id,
                existing=existing,
                remapped=remapped,
                kinds=kinds,
            )
            items = section.get("items")
            if isinstance(items, tuple):
                for item_index, item in enumerate(items):
                    if not isinstance(item, dict):
                        raise ValueError("Resume section items must be objects")
                    _register_new_entity(
                        item,
                        kind="item",
                        ordinal=ordinal * 1_000 + item_index,
                        run_id=run_id,
                        existing=existing,
                        remapped=remapped,
                        kinds=kinds,
                    )
        elif operation_type == "upsert_item":
            _register_new_entity(
                _object_field(payload, "item"),
                kind="item",
                ordinal=ordinal,
                run_id=run_id,
                existing=existing,
                remapped=remapped,
                kinds=kinds,
            )
        elif operation_type == "set_field" and payload.get("field_path") == (
            "profile",
            "contacts",
        ):
            contacts = payload.get("value")
            if isinstance(contacts, tuple):
                for contact_index, contact in enumerate(contacts):
                    if not isinstance(contact, dict):
                        raise ValueError("Resume contacts must be objects")
                    _register_new_entity(
                        contact,
                        kind="contact",
                        ordinal=ordinal * 1_000 + contact_index,
                        run_id=run_id,
                        existing=existing,
                        remapped=remapped,
                        kinds=kinds,
                    )

    operations: list[ResumeOperation] = []
    for ordinal, payload in enumerate(payloads):
        normalized = _remap_operation_payload(payload, remapped)
        normalized["operation_id"] = _stable_id(
            "operation",
            f"{run_id}:{ordinal}",
        )
        operation = decode_resume_operation(normalized)
        if (
            normalized.get("op") != operation.op
            or not _matches_canonical_subset(
                normalized,
                encode_resume_operation(operation),
            )
        ):
            raise ValueError(
                "Resume operation contains unknown, coerced, or mismatched fields"
            )
        operations.append(operation)
    return tuple(operations)


def _register_new_entity(
    entity: dict[str, JsonValue],
    *,
    kind: str,
    ordinal: int,
    run_id: str,
    existing: set[str],
    remapped: dict[str, str],
    kinds: dict[str, str],
) -> None:
    """@brief 为模型临时 ID 注册稳定服务端 ID / Register a stable server ID for a model temporary ID.

    @param entity 含 id 的嵌入 entity / Embedded entity containing an ID.
    @param kind section、item 或 contact / Section, item, or contact.
    @param ordinal 确定性位置 / Deterministic position.
    @param run_id Run identity / Run identity.
    @param existing 基础 Resume entity IDs / Base Resume entity IDs.
    @param remapped 临时到稳定 ID map / Temporary-to-stable ID map.
    @param kinds 临时 ID 的一致 kind map / Consistent-kind map for temporary IDs.
    """

    temporary = entity.get("id")
    if not isinstance(temporary, str) or not temporary:
        raise ValueError("new Resume entity requires a temporary ID")
    if temporary in existing:
        return
    prior_kind = kinds.get(temporary)
    if prior_kind is not None and prior_kind != kind:
        raise ValueError("one temporary Resume ID cannot represent multiple entity kinds")
    kinds[temporary] = kind
    remapped.setdefault(
        temporary,
        _stable_id(kind, f"{run_id}:{temporary}:{ordinal}"),
    )


def _remap_operation_payload(
    payload: dict[str, JsonValue],
    remapped: Mapping[str, str],
) -> dict[str, JsonValue]:
    """@brief 只重映射 operation 语义中的 ID 槽位 / Remap only semantic ID slots in an operation.

    @param payload 模型草案 / Model draft.
    @param remapped 已注册临时 ID / Registered temporary IDs.
    @return 不修改任意 ``value`` 文本的 payload / Payload that leaves arbitrary value text untouched.
    """

    normalized = deepcopy(payload)
    operation_type = normalized.get("op")
    if operation_type == "set_field":
        _remap_key(normalized, "entity_id", remapped)
        if normalized.get("field_path") == ("profile", "contacts"):
            contacts = normalized.get("value")
            if isinstance(contacts, tuple):
                for contact in contacts:
                    if isinstance(contact, dict):
                        _remap_key(contact, "id", remapped)
    elif operation_type == "upsert_section":
        section = _object_field(normalized, "section")
        _remap_key(section, "id", remapped)
        items = section.get("items")
        if isinstance(items, tuple):
            for item in items:
                if not isinstance(item, dict):
                    raise ValueError("Resume section items must be objects")
                _remap_key(item, "id", remapped)
        _remap_key(normalized, "after_section_id", remapped, nullable=True)
    elif operation_type == "upsert_item":
        _remap_key(normalized, "section_id", remapped)
        _remap_key(_object_field(normalized, "item"), "id", remapped)
        _remap_key(normalized, "after_item_id", remapped, nullable=True)
    elif operation_type == "remove_entity":
        _remap_key(normalized, "entity_id", remapped)
    elif operation_type == "move_entity":
        _remap_key(normalized, "entity_id", remapped)
        _remap_key(normalized, "parent_id", remapped, nullable=True)
        _remap_key(normalized, "after_id", remapped, nullable=True)
    elif operation_type != "set_template":
        raise ValueError("Agent Resume operation type is unsupported")
    return normalized


def _remap_key(
    target: dict[str, JsonValue],
    key: str,
    remapped: Mapping[str, str],
    *,
    nullable: bool = False,
) -> None:
    """@brief 重映射单个 ID 字段并保持 null 语义 / Remap one ID field while preserving null semantics.

    @param target 包含字段的对象 / Object containing the field.
    @param key 字段名 / Field name.
    @param remapped 临时 ID map / Temporary-ID map.
    @param nullable 是否允许 null / Whether null is allowed.
    """

    value = target.get(key)
    if value is None and nullable:
        return
    if not isinstance(value, str):
        raise ValueError(f"Resume operation {key} must be a string")
    target[key] = remapped.get(value, value)


def _object_field(
    payload: dict[str, JsonValue],
    key: str,
) -> dict[str, JsonValue]:
    """@brief 读取一个必需 JSON object 字段 / Read a required JSON-object field.

    @param payload 父对象 / Parent object.
    @param key 字段名 / Field name.
    @return 具体 mutable object / Concrete mutable object.
    """

    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Resume operation {key} must be an object")
    return value


def _resume_entity_ids(base: AgentResumeContext) -> set[str]:
    """@brief 收集基础 SIR 的全局 entity IDs / Collect globally addressable entity IDs from the base SIR.

    @param base 精确 Resume snapshot / Exact Resume snapshot.
    @return Resume root、contacts、sections 与 items IDs / Root, contact, section, and item IDs.
    """

    document = base.document
    return {
        str(document.meta.id),
        *(contact.id for contact in document.profile.contacts),
        *(section.id for section in document.sections),
        *(item.id for section in document.sections for item in section.items),
    }


def _evidence_refs(
    command: AgentResumeProposalCommand,
) -> tuple[ResourceRef, ...]:
    """@brief 从服务端证据生成唯一 version refs / Build unique version refs from server evidence.

    @param command Proposal command / Proposal command.
    @return 稳定去重的 Knowledge version refs / Stable deduplicated Knowledge-version refs.
    """

    seen: set[str] = set()
    refs: list[ResourceRef] = []
    for evidence in command.evidence:
        version_id = str(evidence.citation.version_id)
        if version_id in seen:
            continue
        seen.add(version_id)
        refs.append(ResourceRef("knowledge_source_version", version_id))
    return tuple(refs)


def _dump_resource_refs(value: tuple[ResourceRef, ...]) -> list[JsonObject]:
    """@brief 编码 evidence refs / Encode evidence refs.

    @param value 已验证 refs / Validated refs.
    @return PostgreSQL JSONB array / PostgreSQL JSONB array.
    """

    payload = _RESOURCE_REFS_ADAPTER.dump_python(value, mode="json")
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise TypeError("Agent Proposal evidence codec must produce an object array")
    return cast(list[JsonObject], payload)


def _stable_id(prefix: str, material: str) -> str:
    """@brief 从 Run 材料派生不透明稳定 ID / Derive an opaque stable ID from Run material.

    @param prefix 领域类型前缀 / Domain-type prefix.
    @param material 不离开服务端的确定性材料 / Deterministic server-local material.
    @return 32 hex 后缀的不透明 ID / Opaque ID with a 32-hex suffix.
    """

    digest = sha256(f"aiws:v2:agent-proposal:{prefix}:{material}".encode()).hexdigest()
    return f"{prefix}_{digest[:32]}"


def _thaw_object(value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """@brief 将冻结 provider JSON object 转为普通 dict / Thaw a frozen provider JSON object into a plain dict.

    @param value MappingProxy JSON object / MappingProxy JSON object.
    @return 可供严格 operation codec 使用的 mutable dict / Mutable dict for the strict operation codec.
    """

    return {key: _thaw_json(item) for key, item in value.items()}


def _thaw_json(value: JsonValue) -> JsonValue:
    """@brief 将冻结 provider JSON 转为普通容器 / Thaw frozen provider JSON into plain containers.

    @param value MappingProxy/tuple/scalar tree / MappingProxy, tuple, or scalar tree.
    @return PostgreSQL/Pydantic 可接受的 JSON tree / JSON tree accepted by PostgreSQL and Pydantic.
    """

    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(_thaw_json(item) for item in value)
    return value


def _matches_canonical_subset(candidate: object, canonical: object) -> bool:
    """@brief 拒绝 codec 会忽略或强制转换的模型字段 / Reject model fields the codec would ignore or coerce.

    @param candidate 重映射后的模型 JSON / Remapped model JSON.
    @param canonical 类型化 operation 的规范编码 / Canonical encoding of the typed operation.
    @return candidate 每个键和值均被规范编码忠实保留时为真 / True when every candidate key
        and value is faithfully preserved by the canonical encoding.
    @note 缺省字段可由 dataclass 默认值补齐，但未知字段、错误 discriminator 和隐式类型
        coercion 一律失败关闭。/ Dataclass defaults may fill omitted fields, while unknown fields,
        wrong discriminators, and implicit type coercion fail closed.
    """

    if isinstance(candidate, Mapping):
        if not isinstance(canonical, Mapping):
            return False
        return all(
            isinstance(key, str)
            and key in canonical
            and _matches_canonical_subset(value, canonical[key])
            for key, value in candidate.items()
        )
    if isinstance(candidate, tuple):
        if not isinstance(canonical, (list, tuple)) or len(candidate) != len(canonical):
            return False
        return all(
            _matches_canonical_subset(left, right)
            for left, right in zip(candidate, canonical, strict=True)
        )
    return type(candidate) is type(canonical) and candidate == canonical


def _durable_runtime_failure(request_id: str) -> AgentProposalFailure:
    """@brief 构造 memory runtime 的显式 503 / Build the explicit memory-runtime 503.

    @param request_id Run 或 Resume 关联 ID / Run or Resume correlation ID.
    @return 可安全持久化的 durable-runtime 问题 / Persistable durable-runtime problem.
    """

    return AgentProposalFailure(
        ProblemDetails(
            type_uri=(
                "https://api.hmalliances.org:8022/problems/"
                "service/durable_runtime_required"
            ),
            title="This operation requires the durable service runtime",
            status=503,
            code="service.durable_runtime_required",
            request_id=request_id,
            retryable=True,
        )
    )


def _proposal_failure(
    request_id: str,
    code: str,
    title: str,
    status: int,
) -> AgentProposalFailure:
    """@brief 构造不泄漏草案正文的 Proposal 失败 / Build a Proposal failure without leaking draft content.

    @param request_id Run 或 Resume 关联 ID / Run or Resume correlation ID.
    @param code 稳定问题码 / Stable problem code.
    @param title 安全标题 / Safe title.
    @param status HTTP 语义状态 / HTTP-semantic status.
    @return 可持久化边界错误 / Persistable boundary failure.
    """

    return AgentProposalFailure(
        ProblemDetails(
            type_uri="https://api.hmalliances.org:8022/problems/" + code.replace(".", "/"),
            title=title,
            status=status,
            code=code,
            request_id=request_id,
            retryable=False,
        )
    )


__all__ = [
    "PostgresAgentResumeProposalBoundary",
    "UnavailableAgentResumeProposalBoundary",
]
"""@brief Agent UoW 使用的 Proposal bridge / Proposal bridge used by the Agent UoW."""
