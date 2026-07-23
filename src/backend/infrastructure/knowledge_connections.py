"""@brief Knowledge Connection provider 与加密凭据 vault / Knowledge Connection providers and encrypted credential vault.

Provider registry 是闭合 allowlist。所有 OAuth/device/API-token HTTP 调用都使用精确 endpoint、
有限 timeout、禁用环境代理并拒绝 redirect。凭据只以 AES-256-GCM 密文持久化，AAD 绑定
Workspace、actor、provider、reference 与记录版本；旧 key 可读、新写始终使用 active key。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, Protocol, cast
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx
import sqlalchemy as sa
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import CursorResult, RowMapping

from backend.application.ports.knowledge import (
    ConnectionAuthorizationLaunch,
    ProvisionedConnectionCredential,
)
from backend.application.ports.knowledge_worker import (
    KnowledgeWorkerClaim,
    KnowledgeWorkerTerminalFailure,
)
from backend.application.ports.v2_idempotency import IdempotencyPreparationId
from backend.domain.connections import (
    ConnectionAuthorizationFlow,
    ConnectionId,
    ConnectionOwnership,
    ConnectionProvider,
    CredentialReference,
    ProviderSessionReference,
    SecretValue,
)
from backend.infrastructure.persistence.database import AsyncDatabase

_VAULT_SCHEMA_VERSION: Final[int] = 1
"""@brief 凭据密文 AAD schema 版本 / Credential-ciphertext AAD schema version."""

_metadata = sa.MetaData(schema="knowledge")
"""@brief 独立 Core metadata，避免修改共享 ORM registry / Standalone Core metadata avoiding the shared ORM registry."""

connection_credentials = sa.Table(
    "connection_credentials",
    _metadata,
    sa.Column("reference", sa.String(160), primary_key=True),
    sa.Column("workspace_id", sa.String(128), nullable=False),
    sa.Column("created_by", sa.String(128), nullable=False),
    sa.Column("connection_id", sa.String(160), nullable=False),
    sa.Column("provider", sa.String(101), nullable=False),
    sa.Column("auth_method", sa.String(16), nullable=False),
    sa.Column("operation_id", sa.String(160), nullable=False),
    sa.Column("secret_fingerprint", sa.String(64), nullable=False),
    sa.Column("key_id", sa.String(64), nullable=True),
    sa.Column("nonce", sa.LargeBinary(), nullable=True),
    sa.Column("ciphertext", sa.LargeBinary(), nullable=True),
    sa.Column("scopes", JSONB(), nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("validated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("orphan_after", sa.DateTime(timezone=True), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
)
"""@brief 加密 Connection credential vault 表 / Encrypted Connection credential-vault table."""

connection_provider_sessions = sa.Table(
    "connection_provider_sessions",
    _metadata,
    sa.Column("reference", sa.String(160), primary_key=True),
    sa.Column("workspace_id", sa.String(128), nullable=False),
    sa.Column("created_by", sa.String(128), nullable=False),
    sa.Column("provider", sa.String(101), nullable=False),
    sa.Column("flow", sa.String(16), nullable=False),
    sa.Column("state_sha256", sa.String(64), nullable=False),
    sa.Column("key_id", sa.String(64), nullable=True),
    sa.Column("nonce", sa.LargeBinary(), nullable=True),
    sa.Column("ciphertext", sa.LargeBinary(), nullable=True),
    sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
)
"""@brief OAuth/device 私有事务表 / Private OAuth/device transaction table."""


class ConnectionProviderError(RuntimeError):
    """@brief 可安全归类但不暴露 secret 的 provider 错误 / Provider error safe to classify without exposing secrets."""


class ConnectionProviderNotConfigured(ConnectionProviderError):
    """@brief provider 不在显式 registry / Provider absent from the explicit registry."""


class ConnectionCredentialRejected(ConnectionProviderError):
    """@brief provider 确定性拒绝 credential / Provider deterministically rejected a credential."""


class ConnectionProviderUnavailable(ConnectionProviderError):
    """@brief provider 暂时不可用或响应无效 / Provider is unavailable or returned an invalid response."""


class ConnectionSecretStoreError(ConnectionProviderError):
    """@brief vault 完整性或幂等冲突 / Vault integrity or idempotency conflict."""


class UnavailableConnectionAdapter:
    """@brief 无 durable vault/provider 时的显式关闭端口 / Explicitly closed port without a durable vault/provider.

    @note 该 adapter 只用于已校验 provider allowlist 为空的运行时；它不会
        生成临时 token 或伪造 credential reference。 / This adapter is used only when
        the validated provider allowlist is empty; it never manufactures temporary tokens or
        credential references.
    """

    async def begin(
        self,
        ownership: ConnectionOwnership,
        provider: ConnectionProvider,
        flow: ConnectionAuthorizationFlow,
        requested_scopes: tuple[str, ...],
        state: SecretValue,
    ) -> ConnectionAuthorizationLaunch:
        """@brief 拒绝未配置的 OAuth/device flow / Reject an unconfigured OAuth/device flow.

        @param ownership 请求 Workspace 与 actor / Requested Workspace and actor.
        @param provider 未配置 provider / Unconfigured provider.
        @param flow 请求 flow / Requested flow.
        @param requested_scopes 请求 scopes / Requested scopes.
        @param state 未持久 OAuth state / Unpersisted OAuth state.
        @raise ConnectionProviderNotConfigured 始终抛出 / Always raised.
        """

        del ownership, provider, flow, requested_scopes, state
        raise ConnectionProviderNotConfigured("connection provider is not configured")

    async def provision_api_token(
        self,
        ownership: ConnectionOwnership,
        connection_id: ConnectionId,
        provider: ConnectionProvider,
        token: SecretValue,
        *,
        operation_id: IdempotencyPreparationId,
    ) -> ProvisionedConnectionCredential:
        """@brief 拒绝未配置的 API token / Reject an API token for an unconfigured provider.

        @param ownership 请求 Workspace 与 actor / Requested Workspace and actor.
        @param connection_id 待创建 Connection / Connection being prepared.
        @param provider 未配置 provider / Unconfigured provider.
        @param token 仅在调用内存活的 secret / Call-scoped secret.
        @param operation_id 准备阶段幂等 ID / Preparation idempotency ID.
        @raise ConnectionProviderNotConfigured 始终抛出 / Always raised.
        """

        del ownership, connection_id, provider, token, operation_id
        raise ConnectionProviderNotConfigured("connection provider is not configured")


@dataclass(frozen=True, slots=True)
class ApiTokenValidation:
    """@brief provider API-token 在线验证配置 / Provider API-token online-validation settings.

    @param endpoint 固定 HTTPS validation endpoint / Fixed HTTPS validation endpoint.
    @param method GET 或 POST / GET or POST.
    @param authorization_scheme Authorization scheme / Authorization scheme.
    @param scopes_field 响应 JSON 中 scope 字段 / Scope field in the response JSON.
    """

    endpoint: str
    method: Literal["GET", "POST"] = "GET"
    authorization_scheme: str = "Bearer"
    scopes_field: str = "scopes"

    def __post_init__(self) -> None:
        """@brief 校验 validation 配置 / Validate the validation configuration."""

        _require_exact_https_endpoint(self.endpoint, "API-token validation endpoint")
        if self.method not in {"GET", "POST"}:
            raise ValueError("API-token validation method must be GET or POST")
        if (
            not self.authorization_scheme
            or not self.authorization_scheme.isascii()
            or any(character.isspace() for character in self.authorization_scheme)
        ):
            raise ValueError("API-token authorization scheme is invalid")
        _require_json_field(self.scopes_field, "API-token scopes field")


@dataclass(frozen=True, slots=True)
class ConnectionProviderDefinition:
    """@brief registry 中一个生产 provider 的完整能力 / Complete capabilities for one production provider."""

    provider: ConnectionProvider
    client_id: str
    authorization_endpoint: str | None
    token_endpoint: str | None
    device_authorization_endpoint: str | None
    redirect_uri: str | None
    allowed_scopes: frozenset[str]
    api_token_validation: ApiTokenValidation | None
    revocation_endpoint: str | None = None

    def __post_init__(self) -> None:
        """@brief 拒绝部分配置和开放 scope / Reject partial configuration and open-ended scopes."""

        if not self.client_id or len(self.client_id) > 512:
            raise ValueError("connection provider client_id is invalid")
        for endpoint, label in (
            (self.authorization_endpoint, "authorization endpoint"),
            (self.token_endpoint, "token endpoint"),
            (self.device_authorization_endpoint, "device authorization endpoint"),
            (self.revocation_endpoint, "revocation endpoint"),
        ):
            if endpoint is not None:
                _require_exact_https_endpoint(endpoint, label)
        if self.authorization_endpoint is not None and (
            self.token_endpoint is None or self.redirect_uri is None
        ):
            raise ValueError("browser OAuth requires token endpoint and redirect URI")
        if self.redirect_uri is not None:
            _require_redirect_uri(self.redirect_uri)
        if self.device_authorization_endpoint is not None and self.token_endpoint is None:
            raise ValueError("device authorization requires a token endpoint")
        if not self.allowed_scopes or len(self.allowed_scopes) > 100:
            raise ValueError("connection provider requires a bounded non-empty scope allowlist")
        if any(
            not scope or scope.strip() != scope or len(scope) > 200 for scope in self.allowed_scopes
        ):
            raise ValueError("connection provider contains an invalid allowed scope")


class ConnectionProviderRegistry:
    """@brief exact-match provider allowlist / Exact-match provider allowlist."""

    def __init__(self, providers: Sequence[ConnectionProviderDefinition]) -> None:
        """@brief 建立无重复 registry / Build a duplicate-free registry."""

        entries = {definition.provider.value: definition for definition in providers}
        if len(entries) != len(providers):
            raise ValueError("connection provider registry contains duplicate providers")
        self._entries = entries

    def require(
        self,
        provider: ConnectionProvider,
        *,
        flow: ConnectionAuthorizationFlow | None = None,
        api_token: bool = False,
    ) -> ConnectionProviderDefinition:
        """@brief 查找并验证请求能力 / Look up a provider and validate the requested capability."""

        definition = self._entries.get(provider.value)
        if definition is None:
            raise ConnectionProviderNotConfigured("connection provider is not configured")
        if flow is ConnectionAuthorizationFlow.BROWSER_REDIRECT:
            if definition.authorization_endpoint is None or definition.redirect_uri is None:
                raise ConnectionProviderNotConfigured("provider browser OAuth is not configured")
        elif flow is ConnectionAuthorizationFlow.DEVICE_CODE:
            if definition.device_authorization_endpoint is None:
                raise ConnectionProviderNotConfigured(
                    "provider device authorization is not configured"
                )
        if api_token and definition.api_token_validation is None:
            raise ConnectionProviderNotConfigured("provider API-token validation is not configured")
        return definition

    @staticmethod
    def require_scopes(
        definition: ConnectionProviderDefinition,
        scopes: Sequence[str],
    ) -> tuple[str, ...]:
        """@brief 规范化且限制 requested/granted scopes / Normalize and constrain requested or granted scopes."""

        normalized = tuple(dict.fromkeys(scopes))
        if len(normalized) != len(scopes) or not set(normalized) <= definition.allowed_scopes:
            raise ConnectionCredentialRejected("provider scopes exceed the configured allowlist")
        return normalized


@dataclass(frozen=True, slots=True)
class ConnectionVaultKey:
    """@brief 一把 AES-256-GCM vault key / One AES-256-GCM vault key."""

    key_id: str
    key: bytes = field(repr=False)

    def __post_init__(self) -> None:
        """@brief 校验 key ID 与 256-bit material / Validate key ID and 256-bit material."""

        if (
            not 3 <= len(self.key_id) <= 63
            or not self.key_id.isascii()
            or any(not (character.isalnum() or character in "_.-") for character in self.key_id)
            or len(self.key) != 32
        ):
            raise ValueError("connection vault key must have a safe ID and 32-byte material")


@dataclass(frozen=True, slots=True)
class SealedConnectionSecret:
    """@brief AES-GCM sealed secret / AES-GCM sealed secret."""

    key_id: str
    nonce: bytes = field(repr=False)
    ciphertext: bytes = field(repr=False)


class ConnectionSecretKeyring:
    """@brief 支持 rotation overlap 的 AES-GCM keyring / AES-GCM keyring supporting rotation overlap."""

    def __init__(self, active_key_id: str, keys: Sequence[ConnectionVaultKey]) -> None:
        """@brief 绑定 active write key 和所有 read keys / Bind the active write key and all read keys."""

        self._keys = {item.key_id: item.key for item in keys}
        if len(self._keys) != len(keys) or active_key_id not in self._keys:
            raise ValueError("connection vault active key is absent or key IDs are duplicated")
        self._active_key_id = active_key_id

    @property
    def active_key_id(self) -> str:
        """@brief 返回当前写 key ID / Return the current write-key ID."""

        return self._active_key_id

    def seal(self, plaintext: bytes, *, aad: bytes) -> SealedConnectionSecret:
        """@brief 用随机 96-bit nonce 加密 / Encrypt with a random 96-bit nonce."""

        if not plaintext or not aad:
            raise ValueError("connection vault encryption requires plaintext and AAD")
        nonce = secrets.token_bytes(12)
        ciphertext = AESGCM(self._keys[self._active_key_id]).encrypt(nonce, plaintext, aad)
        return SealedConnectionSecret(self._active_key_id, nonce, ciphertext)

    def open(self, sealed: SealedConnectionSecret, *, aad: bytes) -> bytes:
        """@brief 用记录 key 解密并校验 AAD / Decrypt with the record key and authenticate AAD."""

        key = self._keys.get(sealed.key_id)
        if key is None:
            raise ConnectionSecretStoreError("connection vault key is unavailable")
        try:
            return AESGCM(key).decrypt(sealed.nonce, sealed.ciphertext, aad)
        except InvalidTag as error:
            raise ConnectionSecretStoreError(
                "connection vault ciphertext authentication failed"
            ) from error

    def shares_key_material_with(self, other: ConnectionSecretKeyring) -> bool:
        """@brief 检测两个用途域是否复用 AES material / Detect AES-material reuse across purpose domains.

        @param other 另一个用途分离 keyring / Keyring for another purpose domain.
        @return 任一 key bytes 相同时为真 / True when any key bytes are equal.
        @note 比较不暴露 key 或可逆指纹 / Comparison exposes neither keys nor reversible fingerprints.
        """

        return any(
            hmac.compare_digest(left, right)
            for left in self._keys.values()
            for right in other._keys.values()
        )


@dataclass(frozen=True, slots=True)
class ProviderSessionSecret:
    """@brief 解密后的短生命周期 provider transaction / Decrypted short-lived provider transaction."""

    reference: ProviderSessionReference
    ownership: ConnectionOwnership
    provider: ConnectionProvider
    flow: ConnectionAuthorizationFlow
    expires_at: datetime
    payload: Mapping[str, object] = field(repr=False)


@dataclass(frozen=True, slots=True)
class ConnectionCreatorSecretErasure:
    """@brief 一轮 creator 私密材料擦除结果 / Result of one creator-secret erasure batch.

    @param credentials_cleared 本轮清除的 credential 数 / Credentials cleared in this batch.
    @param provider_sessions_cleared 本轮清除的未终态 provider session 数 / Non-terminal
        provider sessions cleared in this batch.
    @param has_more 提交本轮后是否仍存在待处理私密材料 / Whether private material remains
        after this batch is committed.
    """

    credentials_cleared: int
    provider_sessions_cleared: int
    has_more: bool


class ConnectionCredentialVault(Protocol):
    """@brief provider adapter 所需 durable secret store / Durable secret store required by provider adapters."""

    async def save_provider_session(
        self,
        session: ProviderSessionSecret,
        *,
        state: SecretValue,
    ) -> None:
        """@brief 加密保存 provider transaction / Encrypt and persist a provider transaction."""

    async def stage_api_token(
        self,
        ownership: ConnectionOwnership,
        connection_id: ConnectionId,
        provider: ConnectionProvider,
        token: SecretValue,
        scopes: tuple[str, ...],
        *,
        operation_id: IdempotencyPreparationId,
        validated_at: datetime,
    ) -> ProvisionedConnectionCredential:
        """@brief 幂等保存已在线验证 token / Idempotently persist an online-validated token."""


class PostgresConnectionCredentialVault:
    """@brief PostgreSQL AES-GCM credential/provider-session vault / PostgreSQL AES-GCM credential/provider-session vault."""

    def __init__(
        self,
        database: AsyncDatabase,
        provider_session_keyring: ConnectionSecretKeyring,
        credential_keyring: ConnectionSecretKeyring,
        *,
        fingerprint_key: bytes,
        reference_key: bytes,
        orphan_grace: timedelta = timedelta(hours=24),
    ) -> None:
        """@brief 注入数据库、独立 AES/HMAC domains 与 orphan grace / Inject database, independent AES/HMAC domains, and orphan grace.

        @param database lifespan-owned PostgreSQL runtime / Lifespan-owned PostgreSQL runtime.
        @param provider_session_keyring 短期 OAuth/device 事务 keyring / Short-lived OAuth/device transaction keyring.
        @param credential_keyring 长期 Connection credential keyring / Long-lived Connection-credential keyring.
        @param fingerprint_key credential 指纹 HMAC key / Credential-fingerprint HMAC key.
        @param reference_key credential reference HMAC key / Credential-reference HMAC key.
        @param orphan_grace prepare 孤儿的保留窗口 / Grace window for prepared orphans.
        """

        if len(fingerprint_key) < 32 or len(reference_key) < 32:
            raise ValueError("connection vault HMAC keys must contain at least 32 bytes")
        if hmac.compare_digest(fingerprint_key, reference_key):
            raise ValueError("connection vault fingerprint and reference keys must be independent")
        if provider_session_keyring.shares_key_material_with(credential_keyring):
            raise ValueError(
                "provider-session and credential keyrings must not reuse AES key material"
            )
        if orphan_grace < timedelta(minutes=5) or orphan_grace > timedelta(days=7):
            raise ValueError("connection vault orphan grace must be five minutes to seven days")
        self._database = database
        self._provider_session_keyring = provider_session_keyring
        self._credential_keyring = credential_keyring
        self._fingerprint_key = fingerprint_key
        self._reference_key = reference_key
        self._orphan_grace = orphan_grace

    async def save_provider_session(
        self,
        session: ProviderSessionSecret,
        *,
        state: SecretValue,
    ) -> None:
        """@brief 加密插入一次 provider transaction / Encrypt and insert one provider transaction."""

        now = datetime.now(UTC)
        if session.expires_at <= now:
            raise ConnectionSecretStoreError("provider session is already expired")
        state_raw = state.reveal_to_secret_adapter()
        state_sha256 = hashlib.sha256(state_raw.encode()).hexdigest()
        payload = json.dumps(session.payload, separators=(",", ":"), sort_keys=True).encode()
        aad = _session_aad(session)
        sealed = self._provider_session_keyring.seal(payload, aad=aad)
        async with self._database.new_session() as database_session:
            async with database_session.begin():
                await self._database.install_v2_request_scope(
                    database_session,
                    actor_id=str(session.ownership.created_by),
                    workspace_id=str(session.ownership.workspace_id),
                )
                statement = sa.dialects.postgresql.insert(connection_provider_sessions).values(
                    reference=str(session.reference),
                    workspace_id=str(session.ownership.workspace_id),
                    created_by=str(session.ownership.created_by),
                    provider=session.provider.value,
                    flow=session.flow.value,
                    state_sha256=state_sha256,
                    key_id=sealed.key_id,
                    nonce=sealed.nonce,
                    ciphertext=sealed.ciphertext,
                    expires_at=session.expires_at,
                    status="pending",
                    created_at=now,
                    updated_at=now,
                )
                result = await database_session.execute(
                    statement.on_conflict_do_nothing(index_elements=["reference"])
                )
                if cast(CursorResult[Any], result).rowcount != 1:
                    raise ConnectionSecretStoreError("provider session reference already exists")

    async def load_provider_session(
        self,
        ownership: ConnectionOwnership,
        reference: ProviderSessionReference,
    ) -> ProviderSessionSecret:
        """@brief 读取、认证并按需 lazy-rotate provider transaction / Load, authenticate, and lazily rotate a provider transaction."""

        async with self._database.new_session() as database_session:
            async with database_session.begin():
                await self._database.install_v2_request_scope(
                    database_session,
                    actor_id=str(ownership.created_by),
                    workspace_id=str(ownership.workspace_id),
                )
                row = (
                    (
                        await database_session.execute(
                            sa.select(connection_provider_sessions)
                            .where(
                                connection_provider_sessions.c.reference == str(reference),
                                connection_provider_sessions.c.workspace_id
                                == str(ownership.workspace_id),
                                connection_provider_sessions.c.created_by
                                == str(ownership.created_by),
                            )
                            .with_for_update()
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None or row["status"] != "pending":
                    raise ConnectionSecretStoreError("provider session is unavailable")
                session = ProviderSessionSecret(
                    reference,
                    ownership,
                    ConnectionProvider(str(row["provider"])),
                    ConnectionAuthorizationFlow(str(row["flow"])),
                    cast(datetime, row["expires_at"]),
                    {},
                )
                if session.expires_at <= datetime.now(UTC):
                    await database_session.execute(
                        sa.update(connection_provider_sessions)
                        .where(connection_provider_sessions.c.reference == str(reference))
                        .values(
                            status="expired",
                            key_id=None,
                            nonce=None,
                            ciphertext=None,
                            updated_at=datetime.now(UTC),
                        )
                    )
                    raise ConnectionSecretStoreError("provider session is expired")
                sealed = _sealed_from_row(row)
                plaintext = self._provider_session_keyring.open(sealed, aad=_session_aad(session))
                try:
                    decoded = json.loads(plaintext)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ConnectionSecretStoreError(
                        "provider session payload is invalid"
                    ) from error
                if not isinstance(decoded, dict):
                    raise ConnectionSecretStoreError("provider session payload is not an object")
                if sealed.key_id != self._provider_session_keyring.active_key_id:
                    rotated = self._provider_session_keyring.seal(
                        plaintext, aad=_session_aad(session)
                    )
                    await database_session.execute(
                        sa.update(connection_provider_sessions)
                        .where(connection_provider_sessions.c.reference == str(reference))
                        .values(
                            key_id=rotated.key_id,
                            nonce=rotated.nonce,
                            ciphertext=rotated.ciphertext,
                            updated_at=datetime.now(UTC),
                        )
                    )
                return ProviderSessionSecret(
                    session.reference,
                    session.ownership,
                    session.provider,
                    session.flow,
                    session.expires_at,
                    cast(dict[str, object], decoded),
                )

    async def stage_api_token(
        self,
        ownership: ConnectionOwnership,
        connection_id: ConnectionId,
        provider: ConnectionProvider,
        token: SecretValue,
        scopes: tuple[str, ...],
        *,
        operation_id: IdempotencyPreparationId,
        validated_at: datetime,
    ) -> ProvisionedConnectionCredential:
        """@brief 以 stable operation ID 幂等暂存 credential / Stage a credential idempotently by stable operation ID."""

        operation = str(operation_id)
        reference = CredentialReference(
            "credential_"
            + hmac.new(
                self._reference_key,
                _framed(
                    str(ownership.workspace_id),
                    str(ownership.created_by),
                    provider.value,
                    operation,
                ),
                hashlib.sha256,
            ).hexdigest()[:40]
        )
        fingerprint = token.keyed_fingerprint(
            self._fingerprint_key,
            context=_framed(
                "connection-api-token-v1",
                str(ownership.workspace_id),
                provider.value,
                operation,
            ),
        )
        raw_token = token.reveal_to_secret_adapter().encode()
        aad = _credential_aad(
            reference,
            ownership,
            connection_id,
            provider,
            "api_token",
        )
        sealed = self._credential_keyring.seal(raw_token, aad=aad)
        now = datetime.now(UTC)
        async with self._database.new_session() as database_session:
            async with database_session.begin():
                await self._database.install_v2_request_scope(
                    database_session,
                    actor_id=str(ownership.created_by),
                    workspace_id=str(ownership.workspace_id),
                )
                insert = sa.dialects.postgresql.insert(connection_credentials).values(
                    reference=str(reference),
                    workspace_id=str(ownership.workspace_id),
                    created_by=str(ownership.created_by),
                    connection_id=str(connection_id),
                    provider=provider.value,
                    auth_method="api_token",
                    operation_id=operation,
                    secret_fingerprint=fingerprint,
                    key_id=sealed.key_id,
                    nonce=sealed.nonce,
                    ciphertext=sealed.ciphertext,
                    scopes=list(scopes),
                    status="staged",
                    validated_at=validated_at,
                    orphan_after=now + self._orphan_grace,
                    created_at=now,
                    updated_at=now,
                )
                await database_session.execute(
                    insert.on_conflict_do_nothing(
                        index_elements=[
                            "workspace_id",
                            "created_by",
                            "provider",
                            "operation_id",
                        ]
                    )
                )
                row = (
                    (
                        await database_session.execute(
                            sa.select(connection_credentials).where(
                                connection_credentials.c.workspace_id
                                == str(ownership.workspace_id),
                                connection_credentials.c.created_by == str(ownership.created_by),
                                connection_credentials.c.provider == provider.value,
                                connection_credentials.c.operation_id == operation,
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
                if (
                    row["reference"] != str(reference)
                    or row["connection_id"] != str(connection_id)
                    or not hmac.compare_digest(str(row["secret_fingerprint"]), fingerprint)
                    or tuple(_string_list(row["scopes"])) != scopes
                ):
                    raise ConnectionSecretStoreError(
                        "connection credential operation was reused with different input"
                    )
                return ProvisionedConnectionCredential(
                    reference,
                    scopes,
                    cast(datetime, row["validated_at"]),
                )

    async def reveal(
        self,
        ownership: ConnectionOwnership,
        reference: CredentialReference,
    ) -> SecretValue:
        """@brief 为专用 worker 解密 credential 并 lazy-rotate / Decrypt a credential for a dedicated worker and lazily rotate it."""

        async with self._database.new_session() as database_session:
            async with database_session.begin():
                await self._database.install_v2_request_scope(
                    database_session,
                    actor_id=str(ownership.created_by),
                    workspace_id=str(ownership.workspace_id),
                )
                row = (
                    (
                        await database_session.execute(
                            sa.select(connection_credentials)
                            .where(
                                connection_credentials.c.reference == str(reference),
                                connection_credentials.c.workspace_id
                                == str(ownership.workspace_id),
                                connection_credentials.c.created_by == str(ownership.created_by),
                                connection_credentials.c.status.in_(
                                    ("staged", "active", "revoking")
                                ),
                            )
                            .with_for_update()
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise ConnectionSecretStoreError("connection credential is unavailable")
                connection_id = ConnectionId(str(row["connection_id"]))
                provider = ConnectionProvider(str(row["provider"]))
                sealed = _sealed_from_row(row)
                aad = _credential_aad(
                    reference,
                    ownership,
                    connection_id,
                    provider,
                    str(row["auth_method"]),
                )
                plaintext = self._credential_keyring.open(sealed, aad=aad)
                if sealed.key_id != self._credential_keyring.active_key_id:
                    rotated = self._credential_keyring.seal(plaintext, aad=aad)
                    await database_session.execute(
                        sa.update(connection_credentials)
                        .where(connection_credentials.c.reference == str(reference))
                        .values(
                            key_id=rotated.key_id,
                            nonce=rotated.nonce,
                            ciphertext=rotated.ciphertext,
                            updated_at=datetime.now(UTC),
                        )
                    )
                try:
                    return SecretValue(plaintext.decode("utf-8"))
                except UnicodeDecodeError as error:
                    raise ConnectionSecretStoreError(
                        "connection credential encoding is invalid"
                    ) from error

    async def mark_revoked(
        self,
        ownership: ConnectionOwnership,
        reference: CredentialReference,
    ) -> bool:
        """@brief 原子终止 credential 并立即擦除密文 / Atomically terminate a credential and erase ciphertext immediately."""

        async with self._database.new_session() as database_session:
            async with database_session.begin():
                await self._database.install_v2_request_scope(
                    database_session,
                    actor_id=str(ownership.created_by),
                    workspace_id=str(ownership.workspace_id),
                )
                result = await database_session.execute(
                    sa.update(connection_credentials)
                    .where(
                        connection_credentials.c.reference == str(reference),
                        connection_credentials.c.workspace_id == str(ownership.workspace_id),
                        connection_credentials.c.created_by == str(ownership.created_by),
                        connection_credentials.c.status != "revoked",
                    )
                    .values(
                        status="revoked",
                        key_id=None,
                        nonce=None,
                        ciphertext=None,
                        updated_at=datetime.now(UTC),
                    )
                )
                return cast(CursorResult[Any], result).rowcount == 1

    async def credential_status(
        self,
        ownership: ConnectionOwnership,
        reference: CredentialReference,
    ) -> str | None:
        """@brief 读取 scoped credential lifecycle 而不解密 / Read a scoped credential lifecycle without decrypting."""

        async with self._database.new_session() as database_session:
            async with database_session.begin():
                await self._database.install_v2_request_scope(
                    database_session,
                    actor_id=str(ownership.created_by),
                    workspace_id=str(ownership.workspace_id),
                )
                value = await database_session.scalar(
                    sa.select(connection_credentials.c.status).where(
                        connection_credentials.c.reference == str(reference),
                        connection_credentials.c.workspace_id == str(ownership.workspace_id),
                        connection_credentials.c.created_by == str(ownership.created_by),
                    )
                )
        return value if isinstance(value, str) else None

    async def erase_created_by(
        self,
        ownership: ConnectionOwnership,
        *,
        limit: int = 1_000,
    ) -> ConnectionCreatorSecretErasure:
        """@brief 有界清除一个 creator 的全部非终态私密材料 / Clear all non-terminal private material for one creator.

        @param ownership 精确 actor+Workspace RLS scope / Exact actor-and-Workspace RLS scope.
        @param limit 单事务跨两类记录的最大总行数 / Maximum total rows across both record
            types in one transaction.
        @return 本轮计数及提交后显式 remaining 状态 / Batch counts and explicit remaining
            state after the updates.
        @note account-deletion worker 应重复调用直到 ``has_more`` 为 false；provider 远端撤销应在 crypto
            erasure 前由普通逐 Connection revoke saga 完成 / An account-deletion worker may
            repeat until ``has_more`` is false; provider-side revocation should precede cryptographic
            erasure through the regular per-Connection revocation saga.
        """

        if not 1 <= limit <= 1_000:
            raise ValueError("connection secret erasure limit must be 1 to 1000")
        async with self._database.new_session() as database_session:
            async with database_session.begin():
                await self._database.install_v2_request_scope(
                    database_session,
                    actor_id=str(ownership.created_by),
                    workspace_id=str(ownership.workspace_id),
                )
                credential_candidates = (
                    sa.select(connection_credentials.c.reference)
                    .where(
                        connection_credentials.c.workspace_id == str(ownership.workspace_id),
                        connection_credentials.c.created_by == str(ownership.created_by),
                        connection_credentials.c.status != "revoked",
                    )
                    .order_by(connection_credentials.c.reference)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
                credential_references = tuple(
                    (await database_session.scalars(credential_candidates)).all()
                )
                credential_count = 0
                if credential_references:
                    credential_result = await database_session.scalars(
                        sa.update(connection_credentials)
                        .where(
                            connection_credentials.c.reference.in_(credential_references),
                            connection_credentials.c.workspace_id == str(ownership.workspace_id),
                            connection_credentials.c.created_by == str(ownership.created_by),
                            connection_credentials.c.status != "revoked",
                        )
                        .values(
                            status="revoked",
                            key_id=None,
                            nonce=None,
                            ciphertext=None,
                            updated_at=datetime.now(UTC),
                        )
                        .returning(connection_credentials.c.reference)
                    )
                    credential_count = len(credential_result.all())

                session_budget = limit - credential_count
                session_count = 0
                if session_budget > 0:
                    provider_session_candidates = (
                        sa.select(connection_provider_sessions.c.reference)
                        .where(
                            connection_provider_sessions.c.workspace_id
                            == str(ownership.workspace_id),
                            connection_provider_sessions.c.created_by == str(ownership.created_by),
                            connection_provider_sessions.c.status == "pending",
                        )
                        .order_by(connection_provider_sessions.c.reference)
                        .limit(session_budget)
                        .with_for_update(skip_locked=True)
                    )
                    provider_session_references = tuple(
                        (await database_session.scalars(provider_session_candidates)).all()
                    )
                    if provider_session_references:
                        provider_session_result = await database_session.scalars(
                            sa.update(connection_provider_sessions)
                            .where(
                                connection_provider_sessions.c.reference.in_(
                                    provider_session_references
                                ),
                                connection_provider_sessions.c.workspace_id
                                == str(ownership.workspace_id),
                                connection_provider_sessions.c.created_by
                                == str(ownership.created_by),
                                connection_provider_sessions.c.status == "pending",
                            )
                            .values(
                                status="expired",
                                key_id=None,
                                nonce=None,
                                ciphertext=None,
                                updated_at=datetime.now(UTC),
                            )
                            .returning(connection_provider_sessions.c.reference)
                        )
                        session_count = len(provider_session_result.all())

                credential_remaining = sa.exists(
                    sa.select(sa.literal(1))
                    .select_from(connection_credentials)
                    .where(
                        connection_credentials.c.workspace_id == str(ownership.workspace_id),
                        connection_credentials.c.created_by == str(ownership.created_by),
                        connection_credentials.c.status != "revoked",
                    )
                )
                provider_session_remaining = sa.exists(
                    sa.select(sa.literal(1))
                    .select_from(connection_provider_sessions)
                    .where(
                        connection_provider_sessions.c.workspace_id == str(ownership.workspace_id),
                        connection_provider_sessions.c.created_by == str(ownership.created_by),
                        connection_provider_sessions.c.status == "pending",
                    )
                )
                has_more = bool(
                    await database_session.scalar(
                        sa.select(sa.or_(credential_remaining, provider_session_remaining))
                    )
                )
                return ConnectionCreatorSecretErasure(
                    credentials_cleared=credential_count,
                    provider_sessions_cleared=session_count,
                    has_more=has_more,
                )

    async def reconcile_orphans(self, ownership: ConnectionOwnership, *, limit: int = 100) -> int:
        """@brief 删除 prepare 成功但 Connection 永久未提交的密文 / Delete ciphertext orphaned by a permanently failed final commit."""

        if not 1 <= limit <= 1_000:
            raise ValueError("connection orphan reconciliation limit must be 1 to 1000")
        now = datetime.now(UTC)
        async with self._database.new_session() as database_session:
            async with database_session.begin():
                await self._database.install_v2_request_scope(
                    database_session,
                    actor_id=str(ownership.created_by),
                    workspace_id=str(ownership.workspace_id),
                )
                connections = sa.table(
                    "connections",
                    sa.column("workspace_id"),
                    sa.column("credential_reference"),
                    schema="knowledge",
                )
                referenced = sa.exists(
                    sa.select(sa.literal(1))
                    .select_from(connections)
                    .where(
                        connections.c.workspace_id == connection_credentials.c.workspace_id,
                        connections.c.credential_reference == connection_credentials.c.reference,
                    )
                )
                candidates = (
                    sa.select(connection_credentials.c.reference)
                    .where(
                        connection_credentials.c.workspace_id == str(ownership.workspace_id),
                        connection_credentials.c.created_by == str(ownership.created_by),
                        connection_credentials.c.status == "staged",
                        connection_credentials.c.orphan_after <= now,
                        ~referenced,
                    )
                    .order_by(
                        connection_credentials.c.orphan_after, connection_credentials.c.reference
                    )
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                    .cte("orphaned_credentials")
                )
                result = await database_session.execute(
                    sa.delete(connection_credentials).where(
                        connection_credentials.c.reference.in_(sa.select(candidates.c.reference))
                    )
                )
                return int(cast(CursorResult[Any], result).rowcount or 0)


class ProviderConnectionAdapter:
    """@brief registry 驱动的 OAuth/device gateway 与 API-token broker / Registry-driven OAuth/device gateway and API-token broker."""

    def __init__(
        self,
        registry: ConnectionProviderRegistry,
        vault: ConnectionCredentialVault,
        *,
        connect_timeout_ms: int,
        read_timeout_ms: int,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """@brief 注入 registry、vault 和严格 HTTP policy / Inject registry, vault, and strict HTTP policy."""

        if not 100 <= connect_timeout_ms <= 60_000 or not 100 <= read_timeout_ms <= 120_000:
            raise ValueError("connection provider timeouts are outside safe bounds")
        self._registry = registry
        self._vault = vault
        self._timeout = httpx.Timeout(
            read_timeout_ms / 1_000,
            connect=connect_timeout_ms / 1_000,
            write=connect_timeout_ms / 1_000,
            pool=connect_timeout_ms / 1_000,
        )
        self._transport = transport

    async def begin(
        self,
        ownership: ConnectionOwnership,
        provider: ConnectionProvider,
        flow: ConnectionAuthorizationFlow,
        requested_scopes: tuple[str, ...],
        state: SecretValue,
    ) -> ConnectionAuthorizationLaunch:
        """@brief 启动带 PKCE S256/state 的 browser 或 device flow / Start a browser or device flow with PKCE S256 and state."""

        definition = self._registry.require(provider, flow=flow)
        scopes = self._registry.require_scopes(definition, requested_scopes)
        verifier = _pkce_verifier()
        challenge = _base64url(hashlib.sha256(verifier.encode()).digest())
        reference = ProviderSessionReference(f"provider_session_{secrets.token_urlsafe(24)}")
        now = datetime.now(UTC)
        state_raw = state.reveal_to_secret_adapter()
        if flow is ConnectionAuthorizationFlow.BROWSER_REDIRECT:
            if definition.authorization_endpoint is None or definition.redirect_uri is None:
                raise ConnectionProviderNotConfigured("provider browser OAuth is not configured")
            expires_at = now + timedelta(minutes=10)
            authorization_url = _authorization_url(
                definition,
                scopes,
                state_raw,
                challenge,
            )
            session = ProviderSessionSecret(
                reference,
                ownership,
                provider,
                flow,
                expires_at,
                {
                    "code_verifier": verifier,
                    "redirect_uri": definition.redirect_uri,
                    "requested_scopes": list(scopes),
                },
            )
            await self._vault.save_provider_session(session, state=state)
            return ConnectionAuthorizationLaunch(
                reference,
                expires_at,
                authorization_url=authorization_url,
            )

        endpoint = definition.device_authorization_endpoint
        if endpoint is None:
            raise ConnectionProviderNotConfigured("provider device authorization is not configured")
        response = await self._request(
            "POST",
            endpoint,
            data={"client_id": definition.client_id, "scope": " ".join(scopes)},
        )
        payload = _json_object(response)
        device_code = _required_response_text(payload, "device_code", maximum=8_192)
        user_code = _required_response_text(payload, "user_code", maximum=100)
        verification_uri = _verification_uri(payload)
        expires_in = _bounded_response_int(payload, "expires_in", minimum=60, maximum=1_800)
        interval = _bounded_response_int(payload, "interval", minimum=1, maximum=120, default=5)
        expires_at = now + timedelta(seconds=expires_in)
        session = ProviderSessionSecret(
            reference,
            ownership,
            provider,
            flow,
            expires_at,
            {
                "device_code": device_code,
                "code_verifier": verifier,
                "requested_scopes": list(scopes),
            },
        )
        await self._vault.save_provider_session(session, state=state)
        return ConnectionAuthorizationLaunch(
            reference,
            expires_at,
            verification_uri=verification_uri,
            user_code=user_code,
            poll_interval_ms=interval * 1_000,
        )

    async def provision_api_token(
        self,
        ownership: ConnectionOwnership,
        connection_id: ConnectionId,
        provider: ConnectionProvider,
        token: SecretValue,
        *,
        operation_id: IdempotencyPreparationId,
    ) -> ProvisionedConnectionCredential:
        """@brief 在线验证 token 后以 stable operation ID 加密暂存 / Validate a token online, then encrypt and stage it by stable operation ID."""

        definition = self._registry.require(provider, api_token=True)
        validation = definition.api_token_validation
        if validation is None:
            raise ConnectionProviderNotConfigured("provider API-token validation is not configured")
        raw_token = token.reveal_to_secret_adapter()
        response = await self._request(
            validation.method,
            validation.endpoint,
            headers={"Authorization": f"{validation.authorization_scheme} {raw_token}"},
        )
        payload = _json_object(response)
        scopes = _response_scopes(payload.get(validation.scopes_field))
        allowed = self._registry.require_scopes(definition, scopes)
        validated_at = datetime.now(UTC)
        return await self._vault.stage_api_token(
            ownership,
            connection_id,
            provider,
            token,
            allowed,
            operation_id=operation_id,
            validated_at=validated_at,
        )

    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        headers: Mapping[str, str] | None = None,
        data: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """@brief 执行禁代理、禁 redirect、有界响应的 provider 请求 / Execute a no-proxy, no-redirect, bounded provider request."""

        try:
            async with httpx.AsyncClient(
                timeout=self._timeout,
                transport=self._transport,
                trust_env=False,
                follow_redirects=False,
                max_redirects=0,
            ) as client:
                async with client.stream(
                    method,
                    endpoint,
                    headers={"Accept": "application/json", **dict(headers or {})},
                    data=data,
                ) as response:
                    if 300 <= response.status_code < 400:
                        raise ConnectionProviderUnavailable("provider redirect was rejected")
                    body = await response.aread()
                    if len(body) > 1_048_576:
                        raise ConnectionProviderUnavailable("provider response exceeded one MiB")
        except ConnectionProviderError:
            raise
        except (httpx.TimeoutException, httpx.NetworkError, httpx.ProtocolError) as error:
            raise ConnectionProviderUnavailable("connection provider is unavailable") from error
        if response.status_code in {400, 401, 403}:
            raise ConnectionCredentialRejected("connection provider rejected the credential")
        if response.status_code < 200 or response.status_code >= 300:
            raise ConnectionProviderUnavailable(
                "connection provider returned an unsuccessful status"
            )
        return response


class ProviderCredentialRevoker:
    """@brief RFC 7009 风格远端撤销后立即 crypto-erasure 的 worker adapter / Worker adapter performing RFC-7009-style remote revocation followed by immediate cryptographic erasure."""

    def __init__(
        self,
        registry: ConnectionProviderRegistry,
        vault: PostgresConnectionCredentialVault,
        *,
        connect_timeout_ms: int,
        read_timeout_ms: int,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """@brief 绑定显式 provider registry 与 vault / Bind the explicit provider registry and vault."""

        if not 100 <= connect_timeout_ms <= 60_000 or not 100 <= read_timeout_ms <= 120_000:
            raise ValueError("credential revocation timeouts are outside safe bounds")
        self._registry = registry
        self._vault = vault
        self._timeout = httpx.Timeout(
            read_timeout_ms / 1_000,
            connect=connect_timeout_ms / 1_000,
            write=connect_timeout_ms / 1_000,
            pool=connect_timeout_ms / 1_000,
        )
        self._transport = transport

    async def revoke(
        self,
        claim: KnowledgeWorkerClaim,
        *,
        operation_id: str,
    ) -> None:
        """@brief 可重放地先撤销远端 token、再清除本地密文 / Replayably revoke the remote token before clearing local ciphertext."""

        del operation_id
        provider = claim.connection_provider
        reference = claim.credential_reference
        if provider is None or reference is None:
            raise KnowledgeWorkerTerminalFailure("connection.revocation_claim_invalid")
        ownership = ConnectionOwnership(claim.workspace_id, claim.actor_id)
        status = await self._vault.credential_status(ownership, reference)
        if status == "revoked":
            return
        if status is None:
            raise KnowledgeWorkerTerminalFailure("connection.credential_unavailable")
        token = await self._vault.reveal(ownership, reference)
        definition = self._registry.require(provider)
        if definition.revocation_endpoint is not None:
            raw_token = token.reveal_to_secret_adapter()
            try:
                async with httpx.AsyncClient(
                    timeout=self._timeout,
                    transport=self._transport,
                    trust_env=False,
                    follow_redirects=False,
                    max_redirects=0,
                ) as client:
                    async with client.stream(
                        "POST",
                        definition.revocation_endpoint,
                        headers={
                            "Accept": "application/json",
                            "Content-Type": "application/x-www-form-urlencoded",
                        },
                        data={"token": raw_token, "client_id": definition.client_id},
                    ) as response:
                        if 300 <= response.status_code < 400:
                            raise ConnectionProviderUnavailable("provider redirect was rejected")
                        body = await response.aread()
                        if len(body) > 1_048_576:
                            raise ConnectionProviderUnavailable(
                                "provider response exceeded one MiB"
                            )
            except ConnectionProviderError:
                raise
            except (httpx.TimeoutException, httpx.NetworkError, httpx.ProtocolError) as error:
                raise ConnectionProviderUnavailable("connection provider is unavailable") from error
            if not 200 <= response.status_code < 300:
                raise ConnectionProviderUnavailable("provider token revocation failed")
        await self._vault.mark_revoked(ownership, reference)


def _authorization_url(
    definition: ConnectionProviderDefinition,
    scopes: tuple[str, ...],
    state: str,
    challenge: str,
) -> str:
    """@brief 构造不含 verifier 的 exact OAuth URL / Build an exact OAuth URL without the verifier."""

    endpoint = cast(str, definition.authorization_endpoint)
    redirect_uri = cast(str, definition.redirect_uri)
    parsed = urlsplit(endpoint)
    query = urlencode(
        {
            "response_type": "code",
            "client_id": definition.client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(scopes),
            "state": state,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
    )
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _pkce_verifier() -> str:
    """@brief 生成 RFC 7636 范围内 256-bit verifier / Generate a 256-bit verifier within RFC 7636 bounds."""

    verifier = secrets.token_urlsafe(32)
    if not 43 <= len(verifier) <= 128:
        raise RuntimeError("generated PKCE verifier is outside RFC 7636 bounds")
    return verifier


def _session_aad(session: ProviderSessionSecret) -> bytes:
    """@brief 构造 provider-session AAD / Build provider-session AAD."""

    return _framed(
        "connection-provider-session-v1",
        str(_VAULT_SCHEMA_VERSION),
        str(session.reference),
        str(session.ownership.workspace_id),
        str(session.ownership.created_by),
        session.provider.value,
        session.flow.value,
    )


def _credential_aad(
    reference: CredentialReference,
    ownership: ConnectionOwnership,
    connection_id: ConnectionId,
    provider: ConnectionProvider,
    auth_method: str,
) -> bytes:
    """@brief 构造 credential AAD / Build credential AAD."""

    return _framed(
        "connection-credential-v1",
        str(_VAULT_SCHEMA_VERSION),
        str(reference),
        str(ownership.workspace_id),
        str(ownership.created_by),
        str(connection_id),
        provider.value,
        auth_method,
    )


def _framed(*parts: str) -> bytes:
    """@brief 长度前缀编码 HMAC/AAD fields / Length-prefix HMAC/AAD fields."""

    encoded = bytearray()
    for part in parts:
        raw = part.encode("utf-8")
        encoded.extend(len(raw).to_bytes(4, "big"))
        encoded.extend(raw)
    return bytes(encoded)


def _sealed_from_row(row: Mapping[str, Any] | RowMapping) -> SealedConnectionSecret:
    """@brief 从非终态数据库行恢复 sealed value / Restore a sealed value from a nonterminal database row."""

    key_id = row.get("key_id")
    nonce = row.get("nonce")
    ciphertext = row.get("ciphertext")
    if (
        not isinstance(key_id, str)
        or not isinstance(nonce, bytes)
        or not isinstance(ciphertext, bytes)
    ):
        raise ConnectionSecretStoreError("connection vault row has no encrypted payload")
    return SealedConnectionSecret(key_id, nonce, ciphertext)


def _require_exact_https_endpoint(value: str, label: str) -> None:
    """@brief 验证固定无 query/fragment HTTPS endpoint / Validate a fixed HTTPS endpoint without query or fragment."""

    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{label} must be an exact credential-free HTTPS URL")


def _require_redirect_uri(value: str) -> None:
    """@brief 验证 exact OAuth redirect URI / Validate an exact OAuth redirect URI."""

    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("connection provider redirect URI must be exact HTTPS")


def _require_json_field(value: str, label: str) -> None:
    """@brief 验证简单 JSON object field 名 / Validate a simple JSON-object field name."""

    if not value or len(value) > 100 or not value.isascii() or not value.replace("_", "").isalnum():
        raise ValueError(f"{label} is invalid")


def _json_object(response: httpx.Response) -> dict[str, Any]:
    """@brief 读取有界 provider JSON object / Read a bounded provider JSON object."""

    try:
        payload = response.json()
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ConnectionProviderUnavailable("provider response is not valid JSON") from error
    if not isinstance(payload, dict) or len(payload) > 100:
        raise ConnectionProviderUnavailable("provider response is not a bounded JSON object")
    return cast(dict[str, Any], payload)


def _required_response_text(payload: Mapping[str, Any], key: str, *, maximum: int) -> str:
    """@brief 读取 provider 必需文本字段 / Read a required provider text field."""

    value = payload.get(key)
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ConnectionProviderUnavailable("provider response is missing a required field")
    return value


def _verification_uri(payload: Mapping[str, Any]) -> str:
    """@brief 验证 device verification URI / Validate a device verification URI."""

    value = payload.get("verification_uri")
    if value is None:
        value = payload.get("verification_url")
    uri = value if isinstance(value, str) else ""
    try:
        _require_exact_https_endpoint(uri, "device verification URI")
    except ValueError as error:
        raise ConnectionProviderUnavailable(
            "provider returned an invalid verification URI"
        ) from error
    return uri


def _bounded_response_int(
    payload: Mapping[str, Any],
    key: str,
    *,
    minimum: int,
    maximum: int,
    default: int | None = None,
) -> int:
    """@brief 读取非 bool 有界整数 / Read a bounded non-boolean integer."""

    value = payload.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ConnectionProviderUnavailable("provider response contains an invalid integer")
    return cast(int, value)


def _response_scopes(value: object) -> tuple[str, ...]:
    """@brief 接受空格字符串或字符串数组 scopes / Accept space-delimited or string-array scopes."""

    if isinstance(value, str):
        scopes = tuple(item for item in value.split(" ") if item)
    elif isinstance(value, list) and all(isinstance(item, str) for item in value):
        scopes = tuple(cast(list[str], value))
    else:
        raise ConnectionProviderUnavailable("provider validation response contains no scopes")
    if not scopes or len(scopes) > 100 or len(set(scopes)) != len(scopes):
        raise ConnectionProviderUnavailable("provider validation response scopes are invalid")
    return scopes


def _string_list(value: object) -> list[str]:
    """@brief 验证数据库 JSON 字符串数组 / Validate a database JSON string array."""

    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConnectionSecretStoreError("connection credential scopes are invalid")
    return cast(list[str], value)


def _base64url(value: bytes) -> str:
    """@brief 无 padding base64url / Unpadded base64url."""

    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


__all__ = [
    "ApiTokenValidation",
    "ConnectionCreatorSecretErasure",
    "ConnectionCredentialRejected",
    "ConnectionProviderDefinition",
    "ConnectionProviderError",
    "ConnectionProviderNotConfigured",
    "ConnectionProviderRegistry",
    "ConnectionProviderUnavailable",
    "ConnectionSecretKeyring",
    "ConnectionSecretStoreError",
    "ConnectionVaultKey",
    "PostgresConnectionCredentialVault",
    "ProviderConnectionAdapter",
    "ProviderCredentialRevoker",
    "ProviderSessionSecret",
    "SealedConnectionSecret",
    "UnavailableConnectionAdapter",
    "connection_credentials",
    "connection_provider_sessions",
]
