"""@brief 前端诊断入口 Nginx 防护的静态测试 / Static tests for Nginx frontend-diagnostics protection."""

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


def test_diagnostics_rate_limit_uses_the_direct_peer_address() -> None:
    """@brief 诊断限流不得信任调用方提供的转发地址 / Diagnostics rate limiting must not trust caller-supplied forwarding addresses.

    @return 无返回值；zone 键或预算不安全时测试失败 / No return; fails for an unsafe zone key or budget.
    """
    configuration = _configuration()
    zone_directives = tuple(
        line.strip()
        for line in configuration.splitlines()
        if line.strip().startswith("limit_req_zone ")
        and "zone=aiws_diagnostics_per_ip:" in line
    )

    assert zone_directives == (
        "limit_req_zone $binary_remote_addr zone=aiws_diagnostics_per_ip:10m rate=5r/s;",
    )
    normalized_directive = zone_directives[0].lower()
    assert "forwarded" not in normalized_directive
    assert "$http_" not in normalized_directive


def test_diagnostics_exact_path_has_bounded_post_ingestion() -> None:
    """@brief 精确诊断路径应限制方法、正文和突发速率 / The exact diagnostics path bounds methods, body, and burst rate.

    @return 无返回值；任一入口预算缺失时测试失败 / No return; fails when an ingress budget is missing.
    """
    configuration = _configuration()
    diagnostics = _block(configuration, "location = /api/v1/diagnostics")
    method_guard = _block(diagnostics, "limit_except POST OPTIONS")

    assert configuration.count("location = /api/v1/diagnostics {") == 1
    assert "client_max_body_size 64k;" in diagnostics
    assert "limit_req zone=aiws_diagnostics_per_ip burst=20 nodelay;" in diagnostics
    assert "limit_req_status 429;" in diagnostics
    assert "deny all;" in method_guard
    assert "limit_except POST OPTIONS" in diagnostics


def test_diagnostics_uses_finite_post_semantics_and_strips_private_assertions() -> None:
    """@brief 诊断入口应限制短 POST 资源并清除内部认证 / Diagnostics bound finite POST resources and strip internal authentication.

    @return 无返回值；代理或认证边界漂移时测试失败 / No return; fails when proxy or authentication boundaries drift.
    """
    configuration = _configuration()
    diagnostics = _block(configuration, "location = /api/v1/diagnostics")
    expected_proxy_directives = (
        "proxy_pass http://aiws_identity_proxy;",
        "proxy_http_version 1.1;",
        "proxy_set_header Host $host;",
        "proxy_set_header X-Real-IP $remote_addr;",
        "proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;",
        "proxy_set_header X-Forwarded-Host $host;",
        "proxy_set_header X-Forwarded-Proto $scheme;",
        "proxy_set_header X-Request-Id $request_id;",
        'proxy_set_header Connection "";',
        "proxy_request_buffering on;",
        "proxy_buffering on;",
        "proxy_cache off;",
        "gzip off;",
        "proxy_connect_timeout 3s;",
        "proxy_read_timeout 10s;",
        "proxy_send_timeout 10s;",
    )
    for directive in expected_proxy_directives:
        assert directive in diagnostics

    assert "proxy_pass http://aiws_backend" not in diagnostics
    assert "proxy_set_header Upgrade" not in diagnostics
    assert "3600s" not in diagnostics
    for header in _PRIVATE_ASSERTION_HEADERS:
        values = re.findall(
            rf"^\s*proxy_set_header\s+{re.escape(header)}\s+([^;]+);",
            diagnostics,
            flags=re.MULTILINE,
        )
        assert values == ['""'], f"{header} 必须被清空而不是转发。"
