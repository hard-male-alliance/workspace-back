"""@brief API V2 HTTP 语义内核测试 / Tests for the API V2 HTTP semantics kernel."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.api.v2_http import (
    CursorCodec,
    JsonValue,
    decode_contract_json,
    list_response,
    require_strong_if_match,
    resource_response_headers,
    strong_etag,
    token_principal_from_claims,
    validate_idempotency_key,
)
from backend.api.v2_transport import replayable_json
from backend.domain.common import DomainError
from backend.domain.oauth import ACCESS_TOKEN_USER_ID_CLAIM
from backend.domain.principals import (
    ClientId,
    Scope,
    Subject,
    TokenPrincipal,
    UserId,
    WorkspaceId,
)


class RecordingValidator:
    """@brief 记录 definition 调用的契约替身 / Contract test double recording definition calls."""

    def __init__(self) -> None:
        """@brief 初始化空调用记录 / Initialize an empty call record."""

        self.calls: list[tuple[str, object]] = []

    def validate_definition(self, definition: str, payload: object) -> None:
        """@brief 记录一次校验 / Record one validation.

        @param definition definition 名称 / Definition name.
        @param payload JSON payload / JSON payload.
        @return 无返回值 / No return value.
        """

        self.calls.append((definition, payload))


def _problem(error: pytest.ExceptionInfo[DomainError]) -> tuple[str, int]:
    """@brief 提取稳定错误码与状态 / Extract a stable error code and status.

    @param error 捕获的领域错误 / Captured domain error.
    @return `(code,status)` / `(code,status)`.
    """

    return error.value.problem.code, error.value.problem.status


def _principal(
    subject: str = "usr_01",
    client_id: str = "web",
    scopes: frozenset[str] = frozenset({"workspace.read"}),
) -> TokenPrincipal:
    """@brief 构建测试 principal / Build a test principal.

    @param subject token subject / Token subject.
    @param client_id OAuth client ID / OAuth client ID.
    @param scopes OAuth scopes / OAuth scopes.
    @return 不可变 principal / Immutable principal.
    """

    return TokenPrincipal(
        UserId("usr_local_01"),
        Subject(subject),
        ClientId(client_id),
        frozenset(Scope(scope) for scope in scopes),
    )


def test_verified_claims_are_narrowed_to_an_immutable_principal() -> None:
    """@brief claims 只投影授权字段且 scopes 去顺序 / Claims project only authorization fields and scopes ignore order."""

    principal = token_principal_from_claims(
        {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_local_01",
            "sub": "usr_01",
            "client_id": "web",
            "scope": "workspace.read profile",
            "jti": "ignored-after-verification",
        }
    )

    assert principal == _principal(scopes=frozenset({"workspace.read", "profile"}))
    assert principal.scopes == frozenset({Scope("profile"), Scope("workspace.read")})
    with pytest.raises(AttributeError):
        principal.subject = Subject("usr_other")  # type: ignore[misc]


@pytest.mark.parametrize(
    "claims",
    [
        {},
        {ACCESS_TOKEN_USER_ID_CLAIM: "", "sub": "usr_01", "client_id": "web", "scope": "profile"},
        {
            ACCESS_TOKEN_USER_ID_CLAIM: " usr_local",
            "sub": "usr_01",
            "client_id": "web",
            "scope": "profile",
        },
        {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_local",
            "sub": "",
            "client_id": "web",
            "scope": "profile",
        },
        {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_local",
            "sub": " usr_01",
            "client_id": "web",
            "scope": "profile",
        },
        {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_local",
            "sub": "usr_01",
            "client_id": "",
            "scope": "profile",
        },
        {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_local",
            "sub": "usr_01",
            "client_id": "web ",
            "scope": "profile",
        },
        {ACCESS_TOKEN_USER_ID_CLAIM: "usr_local", "sub": "usr_01", "client_id": "web", "scope": ""},
        {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_local",
            "sub": "usr_01",
            "client_id": "web",
            "scope": "profile  workspace.read",
        },
        {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_local",
            "sub": "usr_01",
            "client_id": "web",
            "scope": "profile profile",
        },
        {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_local",
            "sub": "usr_01",
            "client_id": "web",
            "scope": "bad\\scope",
        },
        {
            ACCESS_TOKEN_USER_ID_CLAIM: "usr_local",
            "sub": "usr_01",
            "client_id": "web",
            "scope": ["profile"],
        },
    ],
)
def test_invalid_principal_claims_fail_closed(claims: dict[str, object]) -> None:
    """@brief 缺失或歧义 claims 统一失败关闭 / Missing or ambiguous claims uniformly fail closed.

    @param claims 不可信 claims / Untrusted claims.
    """

    with pytest.raises(DomainError) as captured:
        token_principal_from_claims(claims)
    assert _problem(captured) == ("oauth.invalid_token", 401)


@pytest.mark.parametrize("length", [16, 128])
def test_idempotency_key_accepts_exact_boundaries(length: int) -> None:
    """@brief 幂等键边界长度有效 / Idempotency-key boundary lengths are valid.

    @param length 测试长度 / Tested length.
    """

    value = "a" * (length - 4) + "._~-"
    assert validate_idempotency_key(value) == value


@pytest.mark.parametrize(
    "value",
    [None, "a" * 15, "a" * 129, "contains space 000", "非ASCII-幂等键-000000"],
)
def test_idempotency_key_rejects_missing_length_and_alphabet_violations(
    value: str | None,
) -> None:
    """@brief 幂等键不被 trim 或宽松解码 / Idempotency keys are not trimmed or loosely decoded.

    @param value 原始 header / Raw header.
    """

    with pytest.raises(DomainError) as captured:
        validate_idempotency_key(value)
    expected = "http.idempotency_key_required" if value is None else "http.invalid_idempotency_key"
    assert _problem(captured) == (expected, 400)


def test_json_body_is_strictly_decoded_then_validated_by_definition() -> None:
    """@brief 严格解码成功后才调用权威 definition / The authoritative definition runs only after strict decoding succeeds."""

    validator = RecordingValidator()
    payload = decode_contract_json(
        raw_body=b'{"name":"Klee","nested":[1,true,null]}',
        content_type="APPLICATION/JSON",
        method="POST",
        max_body_bytes=1024,
        max_depth=3,
        validator=validator,
        definition="CreateWorkspaceRequest",
    )

    assert payload == {"name": "Klee", "nested": [1, True, None]}
    assert validator.calls == [("CreateWorkspaceRequest", payload)]


def test_patch_requires_merge_patch_and_other_methods_require_plain_json() -> None:
    """@brief PATCH 与普通 JSON 媒体类型不可混用 / PATCH and ordinary JSON media types cannot be interchanged."""

    validator = RecordingValidator()
    decoded = decode_contract_json(
        raw_body=b'{"name":null}',
        content_type="application/merge-patch+json",
        method="patch",
        max_body_bytes=64,
        max_depth=1,
        validator=validator,
        definition="UpdateWorkspaceRequest",
    )
    assert decoded == {"name": None}

    for method, media_type in [
        ("PATCH", "application/json"),
        ("POST", "application/merge-patch+json"),
        ("POST", "application/json; charset=utf-8"),
    ]:
        with pytest.raises(DomainError) as captured:
            decode_contract_json(
                raw_body=b"{}",
                content_type=media_type,
                method=method,
                max_body_bytes=64,
                max_depth=1,
                validator=validator,
                definition="Request",
            )
        assert _problem(captured) == ("http.unsupported_media_type", 415)


def test_raw_size_and_depth_fail_with_413_before_contract_validation() -> None:
    """@brief 原始大小与深度门禁先于完整解析和契约 / Raw-size and depth gates precede full parsing and contract validation."""

    validator = RecordingValidator()
    with pytest.raises(DomainError) as oversized:
        decode_contract_json(
            raw_body=b'{"x":1}',
            content_type=None,
            method="POST",
            max_body_bytes=6,
            max_depth=1,
            validator=validator,
            definition="Request",
        )
    assert _problem(oversized) == ("http.payload_too_large", 413)

    with pytest.raises(DomainError) as too_deep:
        decode_contract_json(
            raw_body=b'{"x":[{"braces":"[[["}]}',
            content_type="application/json",
            method="POST",
            max_body_bytes=128,
            max_depth=2,
            validator=validator,
            definition="Request",
        )
    assert _problem(too_deep) == ("http.payload_too_large", 413)
    assert validator.calls == []


@pytest.mark.parametrize(
    "raw_body",
    [
        b'{"duplicate":1,"duplicate":2}',
        b'{"number":NaN}',
        b'{"number":Infinity}',
        b'{"trailing":true,}',
        b'\xef\xbb\xbf{"bom":true}',
        b"\xff",
        b"",
    ],
)
def test_non_interoperable_or_malformed_json_is_rejected(raw_body: bytes) -> None:
    """@brief 重复键、扩展数字与非 UTF-8 均不是严格 JSON / Duplicate keys, numeric extensions, and non-UTF-8 are not strict JSON.

    @param raw_body 不合法 JSON bytes / Invalid JSON bytes.
    """

    validator = RecordingValidator()
    with pytest.raises(DomainError) as captured:
        decode_contract_json(
            raw_body=raw_body,
            content_type="application/json",
            method="POST",
            max_body_bytes=1024,
            max_depth=8,
            validator=validator,
            definition="Request",
        )
    assert _problem(captured) == ("http.invalid_json", 400)
    assert validator.calls == []


def test_strong_etag_hashes_the_canonical_representation_not_revision() -> None:
    """@brief 强 ETag 对表示 canonicalize，且不是 revision 别名 / Strong ETags canonicalize representations and are not revision aliases."""

    first = strong_etag({"name": "Klee", "revision": 7})
    reordered = strong_etag({"revision": 7, "name": "Klee"})
    changed = strong_etag({"name": "Alice", "revision": 7})

    assert first == reordered
    assert first.startswith('"sha256-') and first.endswith('"')
    assert first not in {'"7"', '"revision-7"'}
    assert changed != first


def test_if_match_accepts_only_one_matching_strong_validator() -> None:
    """@brief If-Match 缺失、弱、多值与 stale 状态彼此区分 / Missing, weak, multiple, and stale If-Match states are distinguished."""

    current = strong_etag({"revision": 2, "name": "Klee"})
    assert require_strong_if_match(f"  {current}\t", current_etag=current) == current

    cases = [
        (None, "http.precondition_failed", 412),
        (f"W/{current}", "http.precondition_failed", 412),
        (f'{current}, "other"', "http.precondition_failed", 412),
        ("*", "http.precondition_failed", 412),
        ('"stale"', "http.precondition_failed", 412),
    ]
    for candidate, code, status in cases:
        with pytest.raises(DomainError) as captured:
            require_strong_if_match(candidate, current_etag=current)
        assert _problem(captured) == (code, status)


def test_cursor_round_trip_and_context_binding() -> None:
    """@brief cursor 绑定 principal、Workspace、filter 与稳定排序 / A cursor binds principal, Workspace, filters, and stable ordering."""

    current_time = [datetime(2026, 7, 23, 1, 0, tzinfo=UTC)]
    codec = CursorCodec(
        b"c" * 32,
        lifetime=timedelta(minutes=5),
        clock=lambda: current_time[0],
    )
    principal = _principal(scopes=frozenset({"workspace.read", "profile"}))
    workspace_id = WorkspaceId("wsp_01")
    filters: dict[str, JsonValue] = {"kind": "render", "terminal": False}
    sort = ("created_at:desc", "id:asc")
    cursor = codec.encode(
        {"created_at": "2026-07-23T00:00:00Z", "id": "job_02"},
        principal=principal,
        workspace_id=workspace_id,
        filters=filters,
        sort=sort,
    )

    assert codec.decode(
        cursor,
        principal=principal,
        workspace_id=workspace_id,
        filters=filters,
        sort=sort,
    ) == {"created_at": "2026-07-23T00:00:00Z", "id": "job_02"}

    replay_contexts = [
        (
            _principal("usr_other", scopes=frozenset({"workspace.read", "profile"})),
            workspace_id,
            filters,
            sort,
        ),
        (
            _principal(client_id="electron", scopes=frozenset({"workspace.read", "profile"})),
            workspace_id,
            filters,
            sort,
        ),
        (_principal(scopes=frozenset({"workspace.read"})), workspace_id, filters, sort),
        (principal, WorkspaceId("wsp_other"), filters, sort),
        (principal, workspace_id, {"kind": "ingestion", "terminal": False}, sort),
        (principal, workspace_id, filters, ("created_at:asc", "id:asc")),
    ]
    for other_principal, other_workspace, other_filters, other_sort in replay_contexts:
        with pytest.raises(DomainError) as captured:
            codec.decode(
                cursor,
                principal=other_principal,
                workspace_id=other_workspace,
                filters=other_filters,
                sort=other_sort,
            )
        assert _problem(captured) == ("http.cursor_invalid", 400)


def test_cursor_tampering_and_expiry_fail_closed() -> None:
    """@brief cursor 任一位篡改或到期均统一失败 / Any cursor tampering or expiry uniformly fails closed."""

    current_time = [datetime(2026, 7, 23, 1, 0, tzinfo=UTC)]
    codec = CursorCodec(b"s" * 32, lifetime=timedelta(seconds=30), clock=lambda: current_time[0])
    principal = _principal()
    cursor = codec.encode(
        ["job_01", 42],
        principal=principal,
        workspace_id=None,
        filters={},
        sort=("id:asc",),
    )
    replacement = "A" if cursor[-1] != "A" else "B"
    tampered = cursor[:-1] + replacement

    with pytest.raises(DomainError) as altered:
        codec.decode(
            tampered,
            principal=principal,
            workspace_id=None,
            filters={},
            sort=("id:asc",),
        )
    assert _problem(altered) == ("http.cursor_invalid", 400)

    current_time[0] += timedelta(seconds=30)
    with pytest.raises(DomainError) as expired:
        codec.decode(
            cursor,
            principal=principal,
            workspace_id=None,
            filters={},
            sort=("id:asc",),
        )
    assert _problem(expired) == ("http.cursor_invalid", 400)


def test_cursor_transport_limit_matches_the_published_page_schema() -> None:
    """@brief Cursor codec 与公开 2048 字符查询上限保持一致 / Keep the codec aligned with the public 2048-character query limit.

    @return 无返回值 / No return value.
    """
    now = datetime(2026, 7, 23, 1, 0, tzinfo=UTC)
    codec = CursorCodec(b"k" * 32, clock=lambda: now)

    with pytest.raises(DomainError) as captured:
        codec.decode(
            "A" * 2049,
            principal=_principal(),
            workspace_id=WorkspaceId("wsp_cursor_limit"),
            filters={},
            sort=("id",),
        )

    assert _problem(captured) == ("http.cursor_invalid", 400)

    with pytest.raises(ValueError, match="transport limit"):
        codec.encode(
            "x" * 2048,
            principal=_principal(),
            workspace_id=WorkspaceId("wsp_cursor_limit"),
            filters={},
            sort=("id",),
        )


def test_resource_headers_and_list_page_preserve_v2_invariants() -> None:
    """@brief 资源 header 与 page helper 固化 V2 不变量 / Resource-header and page helpers preserve V2 invariants."""

    resource: dict[str, JsonValue] = {"id": "wsp_01", "revision": 1}
    headers = resource_response_headers(
        resource,
        request_id="req_01",
        location="/api/v2/workspaces/wsp_01",
    )
    assert headers == {
        "ETag": strong_etag(resource),
        "X-Request-Id": "req_01",
        "Location": "/api/v2/workspaces/wsp_01",
    }
    assert list_response([resource], next_cursor="signed.cursor") == {
        "items": [resource],
        "page": {"next_cursor": "signed.cursor", "has_more": True},
    }
    assert list_response([], next_cursor=None)["page"] == {
        "next_cursor": None,
        "has_more": False,
    }
    with pytest.raises(ValueError):
        list_response([], next_cursor="")


def test_json_response_budget_is_enforced_before_idempotency_persistence() -> None:
    """@brief 无界应用结果不能进入 response 或幂等 receipt / Keep unbounded results out of responses and idempotency receipts.

    @return 无返回值 / No return value.
    """
    with pytest.raises(DomainError) as captured:
        replayable_json(
            {"payload": "x" * 64},
            status_code=200,
            max_response_bytes=32,
        )

    assert _problem(captured) == ("http.response_too_large", 500)
