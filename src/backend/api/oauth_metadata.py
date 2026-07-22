"""Public OAuth 2.0 and OpenID Connect discovery metadata."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.api.v2 import PUBLIC_ORIGIN

OPENID_CONFIGURATION_PATH = "/.well-known/openid-configuration"
PROTECTED_RESOURCE_METADATA_PATH = "/.well-known/oauth-protected-resource"

AUTHORIZATION_ENDPOINT = f"{PUBLIC_ORIGIN}/oauth/authorize"
TOKEN_ENDPOINT = f"{PUBLIC_ORIGIN}/oauth/token"
REVOCATION_ENDPOINT = f"{PUBLIC_ORIGIN}/oauth/revoke"
JWKS_URI = f"{PUBLIC_ORIGIN}/oauth/jwks"
USERINFO_ENDPOINT = f"{PUBLIC_ORIGIN}/userinfo"

# Keep this list tied to scopes already published by the v2 contract examples. New scopes may be
# added only when their authorization policy and protected routes are implemented together.
SUPPORTED_SCOPES = (
    "openid",
    "profile",
    "offline_access",
    "workspace.read",
    "resume.read",
    "resume.write",
    "resume.render",
)
RESOURCE_SCOPES = tuple(scope for scope in SUPPORTED_SCOPES if "." in scope)

router_oauth_metadata = APIRouter()


def is_public_oauth_metadata_path(path: str) -> bool:
    """Return whether ``path`` is an exact unauthenticated discovery endpoint."""

    return path in {OPENID_CONFIGURATION_PATH, PROTECTED_RESOURCE_METADATA_PATH}


def _metadata_response(payload: dict[str, Any]) -> JSONResponse:
    """Return stable public metadata with an explicit, short cache lifetime."""

    return JSONResponse(payload, headers={"Cache-Control": "public, max-age=300"})


@router_oauth_metadata.get(OPENID_CONFIGURATION_PATH, include_in_schema=False)
async def openid_configuration() -> JSONResponse:
    """Publish the fixed v2 Authorization Server and OIDC capabilities."""

    return _metadata_response(
        {
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
    )


@router_oauth_metadata.get(PROTECTED_RESOURCE_METADATA_PATH, include_in_schema=False)
async def protected_resource_metadata() -> JSONResponse:
    """Publish the fixed v2 Resource Server identity and Bearer-token capabilities."""

    return _metadata_response(
        {
            "resource": PUBLIC_ORIGIN,
            "authorization_servers": [PUBLIC_ORIGIN],
            "scopes_supported": list(RESOURCE_SCOPES),
            "bearer_methods_supported": ["header"],
        }
    )


__all__ = [
    "AUTHORIZATION_ENDPOINT",
    "JWKS_URI",
    "OPENID_CONFIGURATION_PATH",
    "PROTECTED_RESOURCE_METADATA_PATH",
    "RESOURCE_SCOPES",
    "REVOCATION_ENDPOINT",
    "SUPPORTED_SCOPES",
    "TOKEN_ENDPOINT",
    "USERINFO_ENDPOINT",
    "is_public_oauth_metadata_path",
    "router_oauth_metadata",
]
