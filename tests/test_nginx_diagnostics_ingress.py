"""@brief API V2 公网 Nginx 边界测试 / API V2 public Nginx boundary tests."""

from __future__ import annotations

import re
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
"""@brief 项目根目录 / Repository project root."""

_NGINX_CONFIG = _PROJECT_ROOT / "deploy" / "nginx" / "ai-job-workspace.conf"
"""@brief 公网入口 Nginx 配置 / Public-ingress Nginx configuration."""

_PRIVATE_ASSERTION_HEADERS = (
    "X-AIWS-Identity-Version",
    "X-AIWS-Actor-Id",
    "X-AIWS-Workspace-Id",
    "X-AIWS-Resource-Owner-Id",
    "X-AIWS-Auth-Timestamp",
    "X-AIWS-Identity-Signature",
    "X-AIWS-Dashboard-Token",
    "X-AIWS-Dashboard-Operator-Token",
    "X-Dashboard-Operator-Token",
    "X-Mock-Actor-Id",
    "X-Mock-Workspace-Id",
    "X-Mock-Resource-Owner-Id",
)
"""@brief 公网请求不得注入的私有断言头 / Private assertion headers public callers cannot inject."""


def _configuration() -> str:
    """@brief 读取 Nginx 公网入口配置 / Read the public-ingress Nginx configuration.

    @return UTF-8 Nginx 配置文本 / UTF-8 Nginx configuration text.
    """
    return _NGINX_CONFIG.read_text(encoding="utf-8")


def _block(configuration: str, header: str) -> str:
    """@brief 提取一个具名 Nginx 块 / Extract one named Nginx block.

    @param configuration 完整 Nginx 配置 / Complete Nginx configuration.
    @param header 左花括号前的唯一块头 / Unique block header before the opening brace.
    @return 不含外层花括号的块文本 / Block text without the outer braces.
    """
    marker = f"{header} {{"
    start = configuration.index(marker)
    opening = configuration.index("{", start)
    depth = 0
    for position in range(opening, len(configuration)):
        character = configuration[position]
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return configuration[opening + 1 : position]
    raise AssertionError(f"Nginx 块未闭合：{header}")


def test_public_ingress_rejects_legacy_diagnostics() -> None:
    """@brief 公网 V2 入口拒绝旧诊断协议 / Public V2 ingress rejects the legacy diagnostics protocol.

    @return 无返回值；旧诊断被重新公开时测试失败 / No return; fails if legacy diagnostics is exposed again.
    """
    configuration = _configuration()
    public_server = _block(configuration, "server")

    assert "aiws_diagnostics_per_ip" not in configuration
    assert "/api/v1/diagnostics" not in public_server
    assert "/api/v1/frontend-diagnostics/batches" not in public_server
    assert "location /api/v1/ { return 404; }" in public_server


def test_v2_product_ingress_overwrites_forwarded_metadata_and_bounds_requests() -> None:
    """@brief V2 产品入口覆盖转发元数据并设置有限资源预算 / V2 product ingress overwrites forwarding metadata and sets finite resource budgets.

    @return 无返回值；代理信任或资源边界漂移时测试失败 / No return; fails when proxy trust or resource boundaries drift.
    """
    configuration = _configuration()
    product_api = _block(configuration, "location /api/v2/")
    expected_proxy_directives = (
        "client_max_body_size 16m;",
        "proxy_pass http://aiws_backend_v2;",
        "proxy_http_version 1.1;",
        "proxy_set_header Host api.hmalliances.org:8022;",
        'proxy_set_header Forwarded "";',
        "proxy_set_header X-Forwarded-For $remote_addr;",
        "proxy_set_header X-Forwarded-Host api.hmalliances.org:8022;",
        "proxy_set_header X-Forwarded-Proto https;",
        "proxy_set_header X-Request-Id $request_id;",
        'proxy_set_header Connection "";',
        "proxy_request_buffering on;",
        "proxy_buffering on;",
        "proxy_cache off;",
        "proxy_connect_timeout 3s;",
        "proxy_read_timeout 120s;",
        "proxy_send_timeout 120s;",
    )
    for directive in expected_proxy_directives:
        assert directive in product_api

    assert "$proxy_add_x_forwarded_for" not in product_api
    assert "proxy_set_header Upgrade" not in product_api
    assert "3600s" not in product_api


def test_v2_product_ingress_strips_private_identity_assertions() -> None:
    """@brief V2 产品入口清除旧版私有身份断言 / V2 product ingress strips legacy private identity assertions.

    @return 无返回值；私有身份头可由公网注入时测试失败 / No return; fails if private identity headers can be injected publicly.
    """
    configuration = _configuration()
    product_api = _block(configuration, "location /api/v2/")
    for header in _PRIVATE_ASSERTION_HEADERS:
        values = re.findall(
            rf"^\s*proxy_set_header\s+{re.escape(header)}\s+([^;]+);",
            product_api,
            flags=re.MULTILINE,
        )
        assert values == ['""'], f"{header} 必须被清空而不是转发。"
