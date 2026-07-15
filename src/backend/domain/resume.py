"""@brief 简历领域模型与操作 / Resume domain model and operations."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any, cast

from backend.domain.common import DomainError, Problem, iso_timestamp, utc_now
from workspace_shared.tenancy import ActorScope


@dataclass(slots=True)
class ResumeRecord:
    """@brief 带版本历史的简历聚合 / Resume aggregate with revision history."""

    scope: ActorScope
    document: dict[str, Any]
    revisions: dict[int, dict[str, Any]]
    operation_ids: set[str] = field(default_factory=set)
    batch_hashes: dict[str, str] = field(default_factory=dict)
    batch_results: dict[str, dict[str, Any]] = field(default_factory=dict)
    changed_targets: dict[int, set[tuple[str, ...]]] = field(default_factory=dict)

    @property
    def id(self) -> str:
        """@brief 返回简历 ID / Return the resume ID.

        @return 不透明简历 ID / Opaque resume ID.
        """
        return str(self.document["id"])

    @property
    def revision(self) -> int:
        """@brief 返回当前 revision / Return the current revision.

        @return 当前领域版本 / Current domain revision.
        """
        return int(self.document["revision"])

    def etag(self, revision: int | None = None) -> str:
        """@brief 计算强 ETag / Compute a strong ETag.

        @param revision 指定快照版本；None 表示当前版本 / Requested snapshot revision; None means current.
        @return 包含 revision 与内容摘要的 ETag / ETag containing revision and content digest.
        """
        requested_revision = self.revision if revision is None else revision
        document = self.snapshot(requested_revision)
        digest = sha256(repr(document).encode("utf-8")).hexdigest()[:12]
        return f'"rev-{requested_revision}-{digest}"'

    def snapshot(self, revision: int | None = None) -> dict[str, Any]:
        """@brief 获取不可变快照副本 / Get a copy of an immutable snapshot.

        @param revision 指定版本；None 表示当前 / Requested revision; None means current.
        @return 简历语义中间表示副本 / Copy of the resume semantic intermediate representation.
        @raise DomainError 指定版本不存在时抛出 / Raised when a requested revision does not exist.
        """
        requested_revision = self.revision if revision is None else revision
        try:
            return deepcopy(self.revisions[requested_revision])
        except KeyError as error:
            raise DomainError(Problem("resume.revision_not_found", 404, "Resume revision was not found")) from error

    def verify_batch_idempotency(self, batch_id: str, body_hash: str) -> dict[str, Any] | None:
        """@brief 检查操作批次幂等性 / Check operation-batch idempotency.

        @param batch_id 客户端批次 ID / Client batch ID.
        @param body_hash 规范化请求摘要 / Canonical request digest.
        @return 已缓存结果；首次请求返回 None / Cached result, or None for a first request.
        @raise DomainError 同 key 不同 body 时抛出 / Raised for a key reused with a different body.
        """
        previous_hash = self.batch_hashes.get(batch_id)
        if previous_hash is None:
            return None
        if previous_hash != body_hash:
            raise DomainError(
                Problem("idempotency.key_reused", 409, "Idempotency key was reused with different input")
            )
        return deepcopy(self.batch_results[batch_id])

    def save_batch_result(self, batch_id: str, body_hash: str, result: dict[str, Any]) -> None:
        """@brief 保存幂等批次结果 / Persist an idempotent batch result.

        @param batch_id 客户端批次 ID / Client batch ID.
        @param body_hash 规范化请求摘要 / Canonical request digest.
        @param result 可重放响应 / Replayable response.
        """
        self.batch_hashes[batch_id] = body_hash
        self.batch_results[batch_id] = deepcopy(result)

    def apply_operations(
        self,
        base_revision: int,
        conflict_strategy: str,
        operations: list[dict[str, Any]],
    ) -> tuple[int, int, list[dict[str, Any]], bool]:
        """@brief 原子应用领域操作 / Atomically apply domain operations.

        @param base_revision 客户端基于的版本 / Client base revision.
        @param conflict_strategy reject 或 rebase_if_safe / reject or rebase_if_safe.
        @param operations 稳定 ID 引用的领域操作 / Domain operations referencing stable IDs.
        @return 原版本、新版本、操作结果和是否 rebased / Old revision, new revision, results, and rebase flag.
        @raise DomainError 发生冲突或操作无效时抛出 / Raised for conflicts or invalid operations.
        """
        rebased = False
        operation_targets = {_operation_target(operation) for operation in operations}
        if base_revision != self.revision:
            if conflict_strategy != "rebase_if_safe" or not self._can_rebase(base_revision, operation_targets):
                raise DomainError(
                    Problem(
                        "resume.revision_conflict",
                        412,
                        "Resume revision is stale",
                        extensions={"current_revision": self.revision},
                        retryable=True,
                    )
                )
            rebased = True
        candidate = deepcopy(self.document)
        results: list[dict[str, Any]] = []
        for operation in operations:
            operation_id = _required_operation_id(operation)
            if operation_id in self.operation_ids:
                results.append({"operation_id": operation_id, "status": "deduplicated", "problem": None})
                continue
            _apply_operation(candidate, operation)
            results.append(
                {
                    "operation_id": operation_id,
                    "status": "rebased" if rebased else "applied",
                    "problem": None,
                }
            )
        previous_revision = self.revision
        if all(result["status"] == "deduplicated" for result in results):
            return previous_revision, previous_revision, results, rebased
        candidate["revision"] = previous_revision + 1
        candidate["updated_at"] = iso_timestamp(utc_now())
        self.document = candidate
        self.revisions[self.revision] = deepcopy(candidate)
        self.operation_ids.update(_required_operation_id(operation) for operation in operations)
        self.changed_targets[self.revision] = operation_targets
        return previous_revision, self.revision, results, rebased

    def _can_rebase(self, base_revision: int, targets: set[tuple[str, ...]]) -> bool:
        """@brief 判断重放是否无冲突 / Determine whether replay is conflict-free.

        @param base_revision 客户端基于版本 / Client base revision.
        @param targets 本批次改变目标 / Targets changed by this batch.
        @return 可安全重放时为真 / True when replay is safe.
        """
        if base_revision < 1 or base_revision > self.revision:
            return False
        changed_since_base: set[tuple[str, ...]] = set()
        for revision in range(base_revision + 1, self.revision + 1):
            changed_since_base.update(self.changed_targets.get(revision, {("document",)}))
        return not any(_targets_overlap(left, right) for left in targets for right in changed_since_base)


def create_empty_document(
    scope: ActorScope,
    resume_id: str,
    title: str,
    locale: str,
    template_id: str,
    template_version: str,
    section_id: str,
    knowledge_source_id: str | None = None,
) -> dict[str, Any]:
    """@brief 创建有效的最小 SIR / Create a valid minimal SIR.

    @param scope 多租户范围 / Multi-tenant scope.
    @param resume_id 新简历 ID / New resume ID.
    @param title 简历标题 / Resume title.
    @param locale 语言区域 / Locale.
    @param template_id 模板 ID / Template ID.
    @param template_version 模板不可变版本 / Immutable template version.
    @param section_id 初始 section ID / Initial section ID.
    @param knowledge_source_id 派生 resume KnowledgeSource 的稳定 ID / Stable ID of the derived resume KnowledgeSource.
    @return 不含渲染器实现细节的 ResumeDocument / ResumeDocument without renderer implementation details.
    """
    timestamp = iso_timestamp(utc_now())
    measurement = {"value": 18.0, "unit": "mm"}
    def color(value: str) -> dict[str, str]:
        """@brief 构造 sRGB 十六进制颜色 / Construct an sRGB hexadecimal color.

        @param value 十六进制颜色字符串 / Hexadecimal color string.
        @return 契约中的颜色对象 / Contract color object.
        """
        return {"space": "srgb_hex", "value": value}
    return {
        "id": resume_id,
        "created_at": timestamp,
        "updated_at": timestamp,
        "revision": 1,
        "schema_version": "1.0",
        "workspace_id": scope.workspace_id,
        "title": title,
        "locale": locale,
        "template": {"template_id": template_id, "template_version": template_version},
        "profile": {"full_name": "未命名求职者", "contacts": [], "headline": None, "pronouns": None, "photo_asset_id": None, "summary": None},
        "sections": [
            {
                "section_id": section_id,
                "kind": "summary",
                "title": "简介",
                "visible": True,
                "content": None,
                "items": [],
                "extensions": {},
            }
        ],
        "style_intent": {
            "style_contract_version": "1.0",
            "page": {
                "size": "A4",
                "custom_width": None,
                "custom_height": None,
                "orientation": "portrait",
                "margins": {"top": measurement, "right": measurement, "bottom": measurement, "left": measurement},
                "max_pages": None,
                "show_page_numbers": False,
            },
            "typography": {
                "font_family_token": "body.default",
                "base_size_pt": 10.5,
                "line_height": 1.25,
                "heading_scale": 1.2,
                "letter_spacing_em": 0.0,
            },
            "palette": {
                "primary": color("#1F4E79"),
                "secondary": color("#4F81BD"),
                "text": color("#1A1A1A"),
                "muted_text": color("#666666"),
                "background": color("#FFFFFF"),
            },
            "density": 0.5,
            "date_format_token": "yyyy_mm",
            "bullet_style_token": "bullet.default",
            "section_layout": [],
            "template_settings": {},
            "extensions": {},
        },
        "knowledge_source_id": knowledge_source_id,
        "extensions": {},
    }


def _required_operation_id(operation: dict[str, Any]) -> str:
    """@brief 读取操作 ID / Read an operation ID.

    @param operation 操作载荷 / Operation payload.
    @return 操作 ID / Operation ID.
    @raise DomainError ID 缺失时抛出 / Raised when an ID is missing.
    """
    value = operation.get("operation_id")
    if not isinstance(value, str) or not value:
        raise DomainError(Problem("resume.invalid_operation", 422, "Operation ID is required"))
    return value


def _operation_target(operation: dict[str, Any]) -> tuple[str, ...]:
    """@brief 提取操作冲突目标 / Extract an operation conflict target.

    @param operation 操作载荷 / Operation payload.
    @return 可比较的稳定目标路径 / Comparable stable target path.
    """
    kind = str(operation.get("op", ""))
    if kind == "set_field":
        target = operation.get("target", {})
        if not isinstance(target, dict):
            return ("document",)
        return (kind, str(target.get("entity_type")), str(target.get("section_id")), str(target.get("item_id")), *map(str, operation.get("field_path", [])))
    if kind in {"set_template", "set_style_intent", "replace_document"}:
        return ("document", kind)
    return (kind, str(operation.get("section_id") or operation.get("item_id") or "document"))


def _targets_overlap(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    """@brief 判断两个稳定目标是否冲突 / Determine whether two stable targets conflict.

    @param left 第一个目标 / First target.
    @param right 第二个目标 / Second target.
    @return 两目标相同或互为前缀时为真 / True when targets match or one prefixes the other.
    """
    common_length = min(len(left), len(right))
    return left[:common_length] == right[:common_length]


def _apply_operation(document: dict[str, Any], operation: dict[str, Any]) -> None:
    """@brief 应用一个已验证的领域操作 / Apply one validated domain operation.

    @param document 候选 SIR / Candidate SIR.
    @param operation 领域操作 / Domain operation.
    @raise DomainError 目标或操作不合法时抛出 / Raised for invalid targets or operations.
    """
    operation_kind = operation.get("op")
    if operation_kind == "set_template":
        document["template"] = deepcopy(operation["template"])
        if operation.get("style_intent") is not None:
            document["style_intent"] = deepcopy(operation["style_intent"])
        return
    if operation_kind == "set_style_intent":
        document["style_intent"] = deepcopy(operation["style_intent"])
        return
    if operation_kind == "replace_document":
        replacement = deepcopy(operation["document"])
        if (
            replacement.get("id") != document.get("id")
            or replacement.get("workspace_id") != document.get("workspace_id")
        ):
            raise DomainError(Problem("resume.invalid_replace", 422, "Replacement must retain the resume scope"))
        current_source_id = document.get("knowledge_source_id")
        replacement_source_id = replacement.get("knowledge_source_id", current_source_id)
        if replacement_source_id != current_source_id:
            raise DomainError(
                Problem(
                    "resume.knowledge_source_immutable",
                    422,
                    "Replacement must retain the derived knowledge source",
                )
            )
        # ``knowledge_source_id`` has always been optional in the public SIR.  Preserve the
        # server-owned relation when an older client submits a previously valid snapshot that
        # did not include this optional field.
        replacement["knowledge_source_id"] = current_source_id
        document.clear()
        document.update(replacement)
        return
    if operation_kind == "upsert_section":
        _upsert_by_id(document["sections"], "section_id", deepcopy(operation["section"]), operation.get("after_section_id"))
        return
    if operation_kind == "remove_section":
        _remove_by_id(document["sections"], "section_id", str(operation["section_id"]))
        if not document["sections"]:
            raise DomainError(Problem("resume.last_section", 422, "A resume needs at least one section"))
        return
    if operation_kind == "move_section":
        _move_by_id(document["sections"], "section_id", str(operation["section_id"]), operation.get("after_section_id"))
        return
    if operation_kind == "upsert_item":
        section = _find_section(document, str(operation["section_id"]))
        _upsert_by_id(section["items"], _item_id_key(operation["item"]), deepcopy(operation["item"]), operation.get("after_item_id"))
        return
    if operation_kind == "remove_item":
        section = _find_section(document, str(operation["section_id"]))
        _remove_item(section["items"], str(operation["item_id"]))
        return
    if operation_kind == "move_item":
        source = _find_section(document, str(operation["from_section_id"]))
        target = _find_section(document, str(operation["to_section_id"]))
        item = _pop_item(source["items"], str(operation["item_id"]))
        _upsert_by_id(target["items"], _item_id_key(item), item, operation.get("after_item_id"))
        return
    if operation_kind == "set_field":
        entity = _find_entity(document, operation.get("target"))
        path = operation.get("field_path")
        if not isinstance(path, list) or not path or not all(isinstance(part, str) for part in path):
            raise DomainError(Problem("resume.invalid_field_path", 422, "Field path is invalid"))
        _set_path(entity, path, deepcopy(operation.get("value")))
        return
    raise DomainError(Problem("resume.unsupported_operation", 422, "Resume operation is unsupported"))


def _find_section(document: dict[str, Any], section_id: str) -> dict[str, Any]:
    """@brief 按稳定 ID 查找 section / Find a section by stable ID.

    @param document 简历 SIR / Resume SIR.
    @param section_id section ID / Section ID.
    @return section 对象 / Section object.
    @raise DomainError section 不存在时抛出 / Raised when a section is absent.
    """
    sections = document.get("sections")
    if not isinstance(sections, list):
        raise DomainError(Problem("resume.invalid_document", 422, "Resume sections are invalid"))
    for section in sections:
        if isinstance(section, dict) and section.get("section_id") == section_id:
            return cast(dict[str, Any], section)
    raise DomainError(Problem("resume.section_not_found", 422, "Resume section was not found"))


def _find_entity(document: dict[str, Any], target: object) -> dict[str, Any]:
    """@brief 按 EntityTarget 查找可修改对象 / Find a mutable object by EntityTarget.

    @param document 简历 SIR / Resume SIR.
    @param target 契约 EntityTarget / Contract EntityTarget.
    @return 可修改对象 / Mutable object.
    @raise DomainError target 不完整或不存在时抛出 / Raised for incomplete or absent targets.
    """
    if not isinstance(target, dict):
        raise DomainError(Problem("resume.invalid_target", 422, "Entity target is invalid"))
    entity_type = target.get("entity_type")
    if entity_type == "profile":
        profile = document.get("profile")
        if not isinstance(profile, dict):
            raise DomainError(Problem("resume.invalid_document", 422, "Resume profile is invalid"))
        return cast(dict[str, Any], profile)
    if entity_type == "section":
        section_id = target.get("section_id")
        if not isinstance(section_id, str):
            raise DomainError(Problem("resume.invalid_target", 422, "Section target needs section_id"))
        return _find_section(document, section_id)
    if entity_type == "item":
        section_id = target.get("section_id")
        item_id = target.get("item_id")
        if not isinstance(section_id, str) or not isinstance(item_id, str):
            raise DomainError(Problem("resume.invalid_target", 422, "Item target needs section_id and item_id"))
        items = _find_section(document, section_id).get("items")
        if not isinstance(items, list):
            raise DomainError(Problem("resume.invalid_document", 422, "Resume items are invalid"))
        for item in items:
            if isinstance(item, dict) and _item_id(item) == item_id:
                return cast(dict[str, Any], item)
        raise DomainError(Problem("resume.item_not_found", 422, "Resume item was not found"))
    raise DomainError(Problem("resume.invalid_target", 422, "Entity target type is not supported"))


def _set_path(entity: dict[str, Any], path: list[str], value: Any) -> None:
    """@brief 设置对象字段路径 / Set an object field path.

    @param entity 目标对象 / Target object.
    @param path 不允许数组索引的字段路径 / Field path without array indexes.
    @param value 新值 / New value.
    @raise DomainError 中间节点无效时抛出 / Raised for an invalid intermediate node.
    """
    cursor = entity
    for part in path[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            raise DomainError(Problem("resume.invalid_field_path", 422, "Field path crosses a non-object"))
        cursor = child
    cursor[path[-1]] = value


def _upsert_by_id(items: list[dict[str, Any]], key: str, value: dict[str, Any], after_id: object) -> None:
    """@brief 使用稳定 ID 插入或替换 / Insert or replace using a stable ID.

    @param items 有序对象列表 / Ordered object list.
    @param key ID 键 / ID key.
    @param value 新对象 / New object.
    @param after_id 插入锚点 / Insertion anchor.
    @raise DomainError 锚点不存在时抛出 / Raised when the anchor is absent.
    """
    existing_index = next((index for index, item in enumerate(items) if item.get(key) == value.get(key)), None)
    if existing_index is not None:
        items.pop(existing_index)
    insertion_index = 0 if after_id is None else _index_after(items, key, str(after_id))
    items.insert(insertion_index, value)


def _remove_by_id(items: list[dict[str, Any]], key: str, value: str) -> None:
    """@brief 移除稳定 ID 对象 / Remove an object by stable ID.

    @param items 有序对象列表 / Ordered object list.
    @param key ID 键 / ID key.
    @param value ID 值 / ID value.
    @raise DomainError 对象不存在时抛出 / Raised when the object is absent.
    """
    for index, item in enumerate(items):
        if item.get(key) == value:
            items.pop(index)
            return
    raise DomainError(Problem("resume.target_not_found", 422, "Resume target was not found"))


def _move_by_id(items: list[dict[str, Any]], key: str, value: str, after_id: object) -> None:
    """@brief 移动稳定 ID 对象 / Move an object by stable ID.

    @param items 有序对象列表 / Ordered object list.
    @param key ID 键 / ID key.
    @param value 待移动 ID / ID to move.
    @param after_id 锚点 ID / Anchor ID.
    """
    moving = next((item for item in items if item.get(key) == value), None)
    if moving is None:
        raise DomainError(Problem("resume.target_not_found", 422, "Resume target was not found"))
    items.remove(moving)
    insertion_index = 0 if after_id is None else _index_after(items, key, str(after_id))
    items.insert(insertion_index, moving)


def _index_after(items: list[dict[str, Any]], key: str, after_id: str) -> int:
    """@brief 计算锚点后的索引 / Compute the index after an anchor.

    @param items 有序对象列表 / Ordered object list.
    @param key ID 键 / ID key.
    @param after_id 锚点 ID / Anchor ID.
    @return 插入索引 / Insertion index.
    @raise DomainError 锚点不存在时抛出 / Raised when the anchor is absent.
    """
    for index, item in enumerate(items):
        if item.get(key) == after_id:
            return index + 1
    raise DomainError(Problem("resume.anchor_not_found", 422, "Resume insertion anchor was not found"))


def _item_id_key(item: dict[str, Any]) -> str:
    """@brief 推断 ResumeItem 的 ID 字段 / Infer a ResumeItem ID field.

    @param item ResumeItem 对象 / ResumeItem object.
    @return ID 字段名 / ID field name.
    @raise DomainError item 无稳定 ID 时抛出 / Raised when the item lacks a stable ID.
    """
    candidates = ("item_id", "experience_id", "education_id", "project_id", "skill_group_id", "publication_id", "award_id", "certification_id", "language_id", "volunteer_id", "custom_item_id")
    for key in candidates:
        if isinstance(item.get(key), str):
            return key
    raise DomainError(Problem("resume.invalid_item", 422, "Resume item lacks a stable ID"))


def _item_id(item: dict[str, Any]) -> str | None:
    """@brief 取得 ResumeItem 的稳定 ID / Get a ResumeItem stable ID.

    @param item ResumeItem 对象 / ResumeItem object.
    @return ID 或 None / ID or None.
    """
    try:
        return str(item[_item_id_key(item)])
    except DomainError:
        return None


def _remove_item(items: list[dict[str, Any]], item_id: str) -> None:
    """@brief 按稳定 ID 移除 item / Remove an item by stable ID.

    @param items section items / Section items.
    @param item_id item ID / Item ID.
    @raise DomainError item 不存在时抛出 / Raised when the item is absent.
    """
    for index, item in enumerate(items):
        if _item_id(item) == item_id:
            items.pop(index)
            return
    raise DomainError(Problem("resume.item_not_found", 422, "Resume item was not found"))


def _pop_item(items: list[dict[str, Any]], item_id: str) -> dict[str, Any]:
    """@brief 弹出稳定 ID item / Pop an item by stable ID.

    @param items section items / Section items.
    @param item_id item ID / Item ID.
    @return 被移除 item / Removed item.
    @raise DomainError item 不存在时抛出 / Raised when the item is absent.
    """
    for index, item in enumerate(items):
        if _item_id(item) == item_id:
            return items.pop(index)
    raise DomainError(Problem("resume.item_not_found", 422, "Resume item was not found"))
