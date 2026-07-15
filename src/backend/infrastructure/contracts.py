"""@brief 现有 JSON Schema 契约校验器 / Validator for the existing JSON Schema contract."""

from __future__ import annotations

import json
from functools import cached_property
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
        self._schema_path = schema_path

    @cached_property
    def _schema(self) -> dict[str, Any]:
        """@brief 延迟加载完整 bundle / Lazily load the full bundle.

        @return Draft 2020-12 bundle / Draft 2020-12 bundle.
        """
        with self._schema_path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
        if not isinstance(payload, dict) or not isinstance(payload.get("$defs"), dict):
            raise RuntimeError("contract bundle is malformed")
        return payload

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
