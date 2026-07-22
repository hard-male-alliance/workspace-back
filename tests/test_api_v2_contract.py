"""API v2 publication gates and the first safe parallel-read slice."""

from __future__ import annotations

import re
from typing import Any

from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator

from backend.api.oauth_metadata import (
    AUTHORIZATION_ENDPOINT,
    JWKS_URI,
    OPENID_CONFIGURATION_PATH,
    PROTECTED_RESOURCE_METADATA_PATH,
    RESOURCE_SCOPES,
    REVOCATION_ENDPOINT,
    SUPPORTED_SCOPES,
    TOKEN_ENDPOINT,
    USERINFO_ENDPOINT,
)
from backend.api.v2 import (
    PROTECTED_RESOURCE_METADATA_URL,
    PUBLIC_ORIGIN,
    TEST_RESOURCE_SERVER_ORIGIN,
)
from backend.infrastructure.contracts import ContractValidator, load_jsonc_document
from backend.package_resources import read_contract_schema_text
from conftest import PROJECT_ROOT

V2_DIRECTORY = PROJECT_ROOT / "workspace-shared-docs" / "contracts" / "v2"


def _v2_schema() -> dict[str, Any]:
    payload = load_jsonc_document(
        (V2_DIRECTORY / "schema.jsonc").read_text(encoding="utf-8")
    )
    assert isinstance(payload, dict)
    return payload


def test_v2_publication_files_and_fixed_origins_are_frozen() -> None:
    assert {path.name for path in V2_DIRECTORY.iterdir() if path.is_file()} == {
        "contract.md",
        "diff.md",
        "examples.jsonc",
        "schema.jsonc",
    }
    assert PUBLIC_ORIGIN == "https://api.hmalliances.org:8022"
    assert TEST_RESOURCE_SERVER_ORIGIN == "http://dev.hmalliances.org:9000"
    assert PROTECTED_RESOURCE_METADATA_URL == (
        "https://api.hmalliances.org:8022/.well-known/oauth-protected-resource"
    )


def test_v2_schema_and_every_published_example_validate_directly_from_jsonc() -> None:
    schema = _v2_schema()
    Draft202012Validator.check_schema(schema)
    serialized = (V2_DIRECTORY / "schema.jsonc").read_text(encoding="utf-8")
    validator = ContractValidator.from_jsonc(serialized)
    examples = load_jsonc_document(
        (V2_DIRECTORY / "examples.jsonc").read_text(encoding="utf-8")
    )
    validator.validate_definition("ExampleCatalog", examples)
    names: set[str] = set()
    for case in examples["cases"]:
        assert case["name"] not in names
        names.add(case["name"])
        validator.validate_reference(case["schema_ref"], case["payload"])


def test_every_json_schema_named_by_the_v2_route_table_exists() -> None:
    schema_names = set(_v2_schema()["$defs"])
    contract = (V2_DIRECTORY / "contract.md").read_text(encoding="utf-8")
    route_rows = re.findall(
        r"^\| (?:GET|POST|PATCH|DELETE) \| `(/api/v2[^`]*)` \| ([^|]+) \|$",
        contract,
        flags=re.MULTILINE,
    )
    assert route_rows
    mentioned: set[str] = set()
    for _, binding in route_rows:
        mentioned.update(re.findall(r"`([A-Z][A-Za-z0-9]+)`", binding))
    assert mentioned
    assert mentioned <= schema_names


def test_packaged_v2_contract_is_the_published_jsonc_source() -> None:
    packaged = load_jsonc_document(read_contract_schema_text("v2"))
    assert packaged == _v2_schema()


def test_public_v2_template_routes_use_the_canonical_shape(backend_client: TestClient) -> None:
    collection = backend_client.get("/api/v2/resume-templates")
    assert collection.status_code == 200
    payload = collection.json()
    ContractValidator.from_jsonc(read_contract_schema_text("v2")).validate_definition(
        "TemplateList", payload
    )
    assert "total_estimate" not in payload["page"]

    item = payload["items"][0]
    detail = backend_client.get(
        f"/api/v2/resume-templates/{item['id']}", params={"version": item["version"]}
    )
    assert detail.status_code == 200
    assert detail.json() == item

    not_found = backend_client.get(
        "/api/v2/resume-templates/tpl_missing", params={"version": "1.0"}
    )
    assert not_found.status_code == 404
    ContractValidator.from_jsonc(read_contract_schema_text("v2")).validate_definition(
        "ProblemDetails", not_found.json()
    )

    invalid = backend_client.get(f"/api/v2/resume-templates/{item['id']}")
    assert invalid.status_code == 422
    ContractValidator.from_jsonc(read_contract_schema_text("v2")).validate_definition(
        "ProblemDetails", invalid.json()
    )


def test_openapi_contains_only_the_v2_routes_implemented_to_standard(
    backend_client: TestClient,
) -> None:
    paths = backend_client.get("/openapi.json").json()["paths"]
    v2_methods = {
        (method.upper(), path)
        for path, operations in paths.items()
        if path.startswith("/api/v2/")
        for method in operations
    }
    assert v2_methods == {
        ("GET", "/api/v2/resume-templates"),
        ("GET", "/api/v2/resume-templates/{template_id}"),
    }
    assert (
        paths["/api/v2/resume-templates"]["get"]["x-contract-response"]
        == "TemplateList"
    )
    assert (
        paths["/api/v2/resume-templates/{template_id}"]["get"]["x-contract-response"]
        == "TemplateManifest"
    )


def test_non_public_v2_paths_never_fall_back_to_v1_mock_identity(
    backend_client: TestClient,
) -> None:
    missing_request_id = backend_client.get("/api/v2/me")
    assert missing_request_id.status_code == 400
    assert missing_request_id.json()["code"] == "http.request_id_required"

    unauthenticated = backend_client.get(
        "/api/v2/me", headers={"X-Request-Id": "req-api-v2-boundary"}
    )
    assert unauthenticated.status_code == 401
    assert unauthenticated.json()["code"] == "oauth.invalid_token"
    assert unauthenticated.headers["www-authenticate"] == (
        f'Bearer resource_metadata="{PROTECTED_RESOURCE_METADATA_URL}"'
    )
    ContractValidator.from_jsonc(read_contract_schema_text("v2")).validate_definition(
        "ProblemDetails", unauthenticated.json()
    )


def test_public_openid_configuration_only_advertises_the_frozen_secure_flow(
    backend_client: TestClient,
) -> None:
    response = backend_client.get(OPENID_CONFIGURATION_PATH)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.headers["cache-control"] == "public, max-age=300"
    assert response.json() == {
        "issuer": PUBLIC_ORIGIN,
        "authorization_endpoint": AUTHORIZATION_ENDPOINT,
        "token_endpoint": TOKEN_ENDPOINT,
        "revocation_endpoint": REVOCATION_ENDPOINT,
        "jwks_uri": JWKS_URI,
        "userinfo_endpoint": USERINFO_ENDPOINT,
        "scopes_supported": list(SUPPORTED_SCOPES),
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "revocation_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
        "claims_supported": [
            "sub",
            "iss",
            "aud",
            "exp",
            "iat",
            "auth_time",
            "nonce",
            "name",
            "locale",
        ],
        "request_parameter_supported": False,
        "request_uri_parameter_supported": False,
        "claims_parameter_supported": False,
    }
    serialized = response.text
    assert "implicit" not in serialized
    assert "password" not in serialized
    assert "client_credentials" not in serialized
    assert '"plain"' not in serialized


def test_public_protected_resource_metadata_matches_the_bearer_challenge(
    backend_client: TestClient,
) -> None:
    response = backend_client.get(PROTECTED_RESOURCE_METADATA_PATH)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.headers["cache-control"] == "public, max-age=300"
    assert response.json() == {
        "resource": PUBLIC_ORIGIN,
        "authorization_servers": [PUBLIC_ORIGIN],
        "scopes_supported": list(RESOURCE_SCOPES),
        "bearer_methods_supported": ["header"],
    }
    assert PROTECTED_RESOURCE_METADATA_URL == (
        f"{PUBLIC_ORIGIN}{PROTECTED_RESOURCE_METADATA_PATH}"
    )
