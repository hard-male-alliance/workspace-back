"""@brief 正式契约 Schema 与已发布示例的回归测试 / Regression tests for formal schema and published examples."""

from __future__ import annotations

import json
from typing import Any

import json5
import pytest
from jsonschema import Draft202012Validator

from backend.infrastructure.contracts import ContractValidator
from conftest import CONTRACT_SCHEMA_JSONC_PATH, CONTRACT_SCHEMA_PATH


def test_jsonc_schema_is_semantically_identical_to_strict_schema(
    contract_bundle: dict[str, Any],
) -> None:
    """@brief JSONC 源和发布的严格 JSON 必须表达同一份契约 / JSONC source and strict JSON must express one contract."""

    jsonc_bundle = json5.loads(CONTRACT_SCHEMA_JSONC_PATH.read_text(encoding="utf-8"))
    assert jsonc_bundle == contract_bundle


def test_contract_bundle_is_a_valid_draft_2020_12_schema(
    contract_bundle: dict[str, Any],
) -> None:
    """@brief 正式 Bundle 应通过 Draft 2020-12 元模式校验 / Formal bundle must pass the Draft 2020-12 metaschema."""

    Draft202012Validator.check_schema(contract_bundle)
    entrypoints = contract_bundle["x-entrypoints"]
    definitions = contract_bundle["$defs"]
    assert isinstance(entrypoints, dict)
    assert isinstance(definitions, dict)
    for name, reference in entrypoints.items():
        assert reference == f"#/$defs/{name}"
        assert name in definitions


@pytest.mark.parametrize(
    ("example_name", "entrypoint"),
    (
        ("resume_operation_batch", "ResumeOperationBatch"),
        ("agent_run_request", "AgentRunRequest"),
        ("interview_create_request", "InterviewSessionCreateRequest"),
    ),
)
def test_published_examples_validate_against_declared_entrypoints(
    example_name: str,
    entrypoint: str,
    contract_examples: dict[str, Any],
    contract_validator: ContractValidator,
) -> None:
    """@brief 已发布示例必须满足其声明的正式入口 / Each published example must satisfy its declared formal entrypoint.

    @param example_name 示例对象名称 / Example object name.
    @param entrypoint 对应正式 Schema 入口 / Corresponding formal schema entrypoint.
    @param contract_examples 已解析示例集合 / Parsed example collection.
    @param contract_validator 权威 Schema 验证器 / Authoritative schema validator.
    """

    assert example_name in contract_examples
    contract_validator.validate(entrypoint, contract_examples[example_name])


def test_strict_schema_file_contains_no_jsonc_syntax() -> None:
    """@brief 运行时使用的严格 Schema 必须可由标准 JSON 解析器读取 / Runtime strict schema must be readable by a standard JSON parser."""

    payload = json.loads(CONTRACT_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert payload["$schema"] == "https://json-schema.org/draft/2020-12/schema"
