"""Frozen API v2 deployment identifiers shared without importing route composition."""

PUBLIC_ORIGIN = "https://api.hmalliances.org:8022"
TEST_RESOURCE_SERVER_ORIGIN = "http://dev.hmalliances.org:9000"
PROTECTED_RESOURCE_METADATA_URL = f"{PUBLIC_ORIGIN}/.well-known/oauth-protected-resource"

__all__ = [
    "PROTECTED_RESOURCE_METADATA_URL",
    "PUBLIC_ORIGIN",
    "TEST_RESOURCE_SERVER_ORIGIN",
]
