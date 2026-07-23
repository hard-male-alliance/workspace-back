"""Persistent RSA signing keys and strict RFC 9068-style JWT processing."""

from __future__ import annotations

import base64
import json
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from backend.api.constants import PUBLIC_ORIGIN
from backend.domain.oauth import ACCESS_TOKEN_USER_ID_CLAIM, OAuthTokenValidationError
from workspace_shared.ids import new_opaque_id


class OAuthTokenSigner:
    """Sign with the first configured RSA key and verify against the full rotation set."""

    def __init__(self, private_keys: tuple[rsa.RSAPrivateKey, ...]) -> None:
        if not private_keys:
            raise ValueError("at least one OAuth signing key is required")
        self._keys = {self._kid(key): key for key in private_keys}
        self._active_kid = self._kid(private_keys[0])

    @classmethod
    def from_paths(
        cls,
        paths: tuple[Path, ...],
        *,
        runtime_root: Path,
        allow_generate: bool,
    ) -> OAuthTokenSigner:
        """Load rotation keys, generating only the first development key when explicitly allowed."""

        resolved = tuple(path if path.is_absolute() else runtime_root / path for path in paths)
        keys: list[rsa.RSAPrivateKey] = []
        for index, path in enumerate(resolved):
            if not path.exists() and allow_generate and index == 0:
                _generate_private_key_file(path)
            keys.append(_load_private_key_file(path))
        return cls(tuple(keys))

    @property
    def jwks(self) -> dict[str, list[dict[str, str]]]:
        """Return public-only JWK material for every active rotation key."""

        return {
            "keys": [
                {
                    "kty": "RSA",
                    "use": "sig",
                    "alg": "RS256",
                    "kid": kid,
                    "n": _uint_b64(public.public_numbers().n),
                    "e": _uint_b64(public.public_numbers().e),
                }
                for kid, private in self._keys.items()
                for public in (private.public_key(),)
            ]
        }

    def issue_access_token(
        self,
        *,
        user_id: str,
        subject: str,
        client_id: str,
        scopes: tuple[str, ...],
        lifetime_seconds: int,
        now: datetime | None = None,
    ) -> tuple[str, datetime, str]:
        """Issue a signed access token with the mandatory Resource Server claims."""

        issued_at = (now or datetime.now(UTC)).replace(microsecond=0)
        expires_at = issued_at + timedelta(seconds=lifetime_seconds)
        jti = new_opaque_id("jti")
        token = self._sign(
            {
                "iss": PUBLIC_ORIGIN,
                "sub": subject,
                ACCESS_TOKEN_USER_ID_CLAIM: user_id,
                "aud": PUBLIC_ORIGIN,
                "exp": int(expires_at.timestamp()),
                "nbf": int(issued_at.timestamp()),
                "iat": int(issued_at.timestamp()),
                "jti": jti,
                "client_id": client_id,
                "scope": " ".join(scopes),
            },
            token_type="at+jwt",
        )
        return token, expires_at, jti

    def issue_id_token(
        self,
        *,
        subject: str,
        client_id: str,
        nonce: str,
        lifetime_seconds: int,
        auth_time: datetime,
        now: datetime | None = None,
    ) -> str:
        """Issue the OIDC ID Token bound to the original authorization nonce."""

        issued_at = (now or datetime.now(UTC)).replace(microsecond=0)
        return self._sign(
            {
                "iss": PUBLIC_ORIGIN,
                "sub": subject,
                "aud": client_id,
                "exp": int((issued_at + timedelta(seconds=lifetime_seconds)).timestamp()),
                "iat": int(issued_at.timestamp()),
                "auth_time": int(auth_time.timestamp()),
                "nonce": nonce,
            },
            token_type="JWT",
        )

    def verify_access_token(
        self,
        token: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Verify signature, type, issuer, audience, time and required access-token claims."""

        claims = self._verify(token, expected_type="at+jwt")
        current = int((now or datetime.now(UTC)).timestamp())
        required_strings = (
            "sub",
            ACCESS_TOKEN_USER_ID_CLAIM,
            "jti",
            "client_id",
            "scope",
        )
        if any(
            not isinstance(claims.get(name), str) or not claims[name] for name in required_strings
        ):
            raise OAuthTokenValidationError("access token is missing a required claim")
        if claims.get("iss") != PUBLIC_ORIGIN or claims.get("aud") != PUBLIC_ORIGIN:
            raise OAuthTokenValidationError("access token issuer or audience is invalid")
        for name in ("exp", "nbf", "iat"):
            if isinstance(claims.get(name), bool) or not isinstance(claims.get(name), int):
                raise OAuthTokenValidationError("access token time claim is invalid")
        if claims["exp"] <= current or claims["nbf"] > current + 30 or claims["iat"] > current + 30:
            raise OAuthTokenValidationError("access token is outside its validity window")
        if claims["iat"] > claims["exp"]:
            raise OAuthTokenValidationError("access token validity window is invalid")
        return claims

    def _sign(self, claims: dict[str, Any], *, token_type: str) -> str:
        header = {"alg": "RS256", "kid": self._active_kid, "typ": token_type}
        signing_input = f"{_json_b64(header)}.{_json_b64(claims)}".encode("ascii")
        signature = self._keys[self._active_kid].sign(
            signing_input,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return f"{signing_input.decode('ascii')}.{_bytes_b64(signature)}"

    def _verify(self, token: str, *, expected_type: str) -> dict[str, Any]:
        if len(token) > 8192:
            raise OAuthTokenValidationError("token is too large")
        parts = token.split(".")
        if len(parts) != 3:
            raise OAuthTokenValidationError("token is malformed")
        try:
            header = json.loads(_decode_b64(parts[0]))
            claims = json.loads(_decode_b64(parts[1]))
            signature = _decode_b64(parts[2])
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise OAuthTokenValidationError("token is malformed") from error
        if not isinstance(header, dict) or not isinstance(claims, dict):
            raise OAuthTokenValidationError("token is malformed")
        if header.get("alg") != "RS256" or header.get("typ") != expected_type:
            raise OAuthTokenValidationError("token algorithm or type is invalid")
        kid = header.get("kid")
        if not isinstance(kid, str) or (key := self._keys.get(kid)) is None:
            raise OAuthTokenValidationError("token signing key is unknown")
        try:
            key.public_key().verify(
                signature,
                f"{parts[0]}.{parts[1]}".encode("ascii"),
                padding.PKCS1v15(),
                hashes.SHA256(),
            )
        except InvalidSignature as error:
            raise OAuthTokenValidationError("token signature is invalid") from error
        return claims

    @staticmethod
    def _kid(key: rsa.RSAPrivateKey) -> str:
        public_der = key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        digest = hashes.Hash(hashes.SHA256())
        digest.update(public_der)
        return _bytes_b64(digest.finalize()[:18])


def _generate_private_key_file(path: Path) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    payload = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
    except BaseException:
        try:
            path.unlink(missing_ok=True)
        finally:
            raise


def _load_private_key_file(path: Path) -> rsa.RSAPrivateKey:
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise RuntimeError("OAuth signing private key is not provisioned") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise RuntimeError("OAuth signing private key must be a regular non-symlink file")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RuntimeError(
            "OAuth signing private key permissions must not allow group/other access"
        )
    loaded = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(loaded, rsa.RSAPrivateKey) or loaded.key_size < 2048:
        raise RuntimeError("OAuth signing private key must be RSA with at least 2048 bits")
    return loaded


def _json_b64(value: dict[str, Any]) -> str:
    return _bytes_b64(json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8"))


def _bytes_b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _decode_b64(value: str) -> bytes:
    if not value or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        for character in value
    ):
        raise ValueError("invalid base64url")
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)


def _uint_b64(value: int) -> str:
    return _bytes_b64(value.to_bytes((value.bit_length() + 7) // 8, "big"))


__all__ = ["OAuthTokenSigner", "OAuthTokenValidationError"]
