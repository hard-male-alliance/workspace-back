"""@brief 现有 JSON Schema 契约校验器 / Validator for the existing JSON Schema contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from backend.domain.common import DomainError, Problem


class ContractValidator:
    """@brief 从权威 bundle 校验 entrypoint / Validate entrypoints from the authoritative bundle.

    @note 这不修改或复制 `contract/`；只将正式 Schema 用作输入边界。
    """

    def __init__(self, schema_path: Path, *, assert_formats: bool = False) -> None:
        """@brief 初始化契约路径 / Initialize the contract path.

        @param schema_path 严格 JSON Schema 路径 / Strict JSON Schema path.
        """
        try:
            serialized = schema_path.read_text(encoding="utf-8")
            payload = (
                load_jsonc_document(serialized)
                if schema_path.suffix == ".jsonc"
                else json.loads(serialized)
            )
        except (OSError, json.JSONDecodeError, ValueError) as error:
            raise RuntimeError("contract bundle cannot be loaded") from error
        self._schema = _require_schema_bundle(payload)
        self._format_checker = FormatChecker() if assert_formats else None

    @classmethod
    def from_json(cls, serialized: str) -> ContractValidator:
        """@brief 从已交付的 JSON 资源构造校验器 / Build a validator from a delivered JSON resource.

        @param serialized 完整 Draft 2020-12 bundle / Complete Draft 2020-12 bundle.
        @return 已验证且不依赖临时文件生命周期的校验器 / Validated validator independent of temporary-file lifetime.
        @raise RuntimeError 资源不是合法 contract bundle 时抛出 / Raised when the resource is not a valid contract bundle.
        """

        try:
            payload = json.loads(serialized)
        except json.JSONDecodeError as error:
            raise RuntimeError("contract bundle cannot be loaded") from error
        instance = cls.__new__(cls)
        instance._schema = _require_schema_bundle(payload)
        instance._format_checker = None
        return instance

    @classmethod
    def from_jsonc(cls, serialized: str, *, assert_formats: bool = True) -> ContractValidator:
        """Build a validator directly from the published comment-bearing JSONC source."""

        try:
            payload = load_jsonc_document(serialized)
        except (json.JSONDecodeError, ValueError) as error:
            raise RuntimeError("contract bundle cannot be loaded") from error
        instance = cls.__new__(cls)
        instance._schema = _require_schema_bundle(payload)
        instance._format_checker = FormatChecker() if assert_formats else None
        return instance

    def validate(self, entrypoint: str, payload: object) -> None:
        """@brief 校验一个已声明 entrypoint / Validate a declared entrypoint.

        @param entrypoint `x-entrypoints` 中的名称 / Name in `x-entrypoints`.
        @param payload 待校验 JSON 值 / JSON value to validate.
        @raise DomainError payload 不符合正式 Schema 时抛出 / Raised when payload violates the formal schema.
        """
        reference = self._schema.get("x-entrypoints", {}).get(entrypoint)
        if not isinstance(reference, str):
            raise RuntimeError(f"unknown contract entrypoint: {entrypoint}")
        validator = self._validator(reference)
        try:
            validator.validate(payload)
        except ValidationError as error:
            pointer = "/" + "/".join(str(part) for part in error.absolute_path)
            raise DomainError(
                Problem(
                    "contract.validation_failed",
                    422,
                    "Request does not satisfy the public contract",
                    detail=error.message,
                    extensions={"pointer": pointer},
                )
            ) from error

    def validate_declared(self, name: str, payload: object) -> None:
        """@brief 校验 entrypoint 或现有 definition / Validate an entrypoint or existing definition.

        @param name entrypoint 或 `$defs` 名称 / Entrypoint or `$defs` name.
        @param payload 待校验 JSON 值 / JSON value to validate.
        @note 不会为缺失的 path binding 推导新 schema。
        """
        if name in self._schema.get("x-entrypoints", {}):
            self.validate(name, payload)
            return
        self.validate_definition(name, payload)

    def validate_definition(self, definition: str, payload: object) -> None:
        """@brief 校验 bundle 中已定义但未列 entrypoint 的对象 / Validate a defined object not listed as an entrypoint.

        @param definition `$defs` 中的定义名称 / Definition name in `$defs`.
        @param payload 待校验 JSON 值 / JSON value to validate.
        @raise DomainError payload 不符合定义时抛出 / Raised when payload violates the definition.
        @note 这只复用已有正式 definition，不为缺失路径绑定创造新 contract。
        """
        if definition not in self._schema["$defs"]:
            raise RuntimeError(f"unknown contract definition: {definition}")
        validator = self._validator(f"#/$defs/{definition}")
        try:
            validator.validate(payload)
        except ValidationError as error:
            pointer = "/" + "/".join(str(part) for part in error.absolute_path)
            raise DomainError(
                Problem(
                    "contract.validation_failed",
                    422,
                    "Request does not satisfy the public contract",
                    detail=error.message,
                    extensions={"pointer": pointer},
                )
            ) from error

    def validate_reference(self, reference: str, payload: object) -> None:
        """Validate a payload against one local ``#/$defs/...`` reference."""

        prefix = "#/$defs/"
        if (
            not reference.startswith(prefix)
            or reference[len(prefix) :] not in self._schema["$defs"]
        ):
            raise RuntimeError(f"unknown contract reference: {reference}")
        try:
            self._validator(reference).validate(payload)
        except ValidationError as error:
            pointer = "/" + "/".join(str(part) for part in error.absolute_path)
            raise DomainError(
                Problem(
                    "contract.validation_failed",
                    422,
                    "Request does not satisfy the public contract",
                    detail=error.message,
                    extensions={"pointer": pointer},
                )
            ) from error

    def _validator(self, reference: str) -> Draft202012Validator:
        """Create a validator that retains the bundle root and optional format assertions."""

        return Draft202012Validator(
            {"$ref": reference, "$defs": self._schema["$defs"]},
            format_checker=self._format_checker,
        )


def load_jsonc_document(serialized: str) -> Any:
    """Parse comment-bearing JSON without accepting the wider JSON5 language.

    The v2 publication is JSONC: standard JSON plus line/block comments and trailing commas.
    A small deterministic scanner avoids loading the very large schema through the recursive
    third-party JSON5 parser, which is unstable on CPython 3.14 under repeated CI validation.
    """

    without_comments: list[str] = []
    index = 0
    in_string = False
    escaped = False
    while index < len(serialized):
        character = serialized[index]
        if in_string:
            without_comments.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            index += 1
            continue
        if character == '"':
            in_string = True
            without_comments.append(character)
            index += 1
            continue
        following = serialized[index + 1] if index + 1 < len(serialized) else ""
        if character == "/" and following == "/":
            index += 2
            while index < len(serialized) and serialized[index] not in "\r\n":
                index += 1
            continue
        if character == "/" and following == "*":
            closing = serialized.find("*/", index + 2)
            if closing < 0:
                raise ValueError("unterminated JSONC block comment")
            index = closing + 2
            continue
        without_comments.append(character)
        index += 1

    cleaned = "".join(without_comments)
    without_trailing_commas: list[str] = []
    index = 0
    in_string = False
    escaped = False
    while index < len(cleaned):
        character = cleaned[index]
        if in_string:
            without_trailing_commas.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            index += 1
            continue
        if character == '"':
            in_string = True
        if character == ",":
            lookahead = index + 1
            while lookahead < len(cleaned) and cleaned[lookahead].isspace():
                lookahead += 1
            if lookahead < len(cleaned) and cleaned[lookahead] in "}]":
                index += 1
                continue
        without_trailing_commas.append(character)
        index += 1
    return json.loads("".join(without_trailing_commas))


def _require_schema_bundle(payload: object) -> dict[str, Any]:
    """@brief 校验并返回 contract bundle 根对象 / Validate and return a contract-bundle root.

    @param payload 已解析 JSON 值 / Parsed JSON value.
    @return 含 `$defs`、可选 `x-entrypoints` 的 bundle / Bundle with `$defs` and optional `x-entrypoints`.
    @raise RuntimeError bundle 结构不完整时抛出 / Raised when the bundle structure is incomplete.
    """

    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("$defs"), dict)
        or ("x-entrypoints" in payload and not isinstance(payload.get("x-entrypoints"), dict))
    ):
        raise RuntimeError("contract bundle is malformed")
    return payload
