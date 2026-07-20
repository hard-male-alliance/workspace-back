"""@brief 现有 JSON Schema 契约校验器 / Validator for the existing JSON Schema contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, ValidationError

from backend.domain.common import DomainError, Problem


class ContractValidator:
    """@brief 从权威 bundle 校验 entrypoint / Validate entrypoints from the authoritative bundle.

    @note 这不修改或复制 `contract/`；只将正式 Schema 用作输入边界。
    """

    def __init__(self, schema_path: Path) -> None:
        """@brief 初始化契约路径 / Initialize the contract path.

        @param schema_path 严格 JSON Schema 路径 / Strict JSON Schema path.
        """
        try:
            with schema_path.open(encoding="utf-8") as stream:
                payload = json.load(stream)
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError("contract bundle cannot be loaded") from error
        self._schema = _require_schema_bundle(payload)

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
        validator = Draft202012Validator({"$ref": reference, "$defs": self._schema["$defs"]})
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
        validator = Draft202012Validator({"$ref": f"#/$defs/{definition}", "$defs": self._schema["$defs"]})
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


def _require_schema_bundle(payload: object) -> dict[str, Any]:
    """@brief 校验并返回 contract bundle 根对象 / Validate and return a contract-bundle root.

    @param payload 已解析 JSON 值 / Parsed JSON value.
    @return 含 `$defs` 与 `x-entrypoints` 的 bundle / Bundle containing `$defs` and `x-entrypoints`.
    @raise RuntimeError bundle 结构不完整时抛出 / Raised when the bundle structure is incomplete.
    """

    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("$defs"), dict)
        or not isinstance(payload.get("x-entrypoints"), dict)
    ):
        raise RuntimeError("contract bundle is malformed")
    return payload
