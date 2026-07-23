"""@brief Knowledge 外部来源网络边界 / Knowledge external-source network boundary.

该模块把 URL 规范化、显式 hostname allowlist、全地址 DNS 校验、连接对端核验和
redirect 重验放进同一个不可绕过的策略对象。``ResolvedSourceTarget`` 是一次连接的
短生命周期 DNS pin；调用方不得跨请求缓存它，也不得仅校验 DNS 返回的第一个地址。
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address, ip_address
from typing import Any, Protocol
from urllib.parse import SplitResult, urljoin, urlsplit, urlunsplit

import httpx

from backend.domain.knowledge_sources import GitSourceInput, UrlSourceInput

type IpAddress = IPv4Address | IPv6Address
"""@brief 已解析的 IP 地址联合 / Resolved IP-address union."""


class SourceNetworkRejected(ValueError):
    """@brief 来源目标未通过 SSRF 策略 / Source target rejected by the SSRF policy."""


@dataclass(frozen=True, slots=True)
class FetchedSourceBody:
    """@brief 经 DNS pin 与 peer 校验取得的有界响应 / Bounded response obtained through DNS pinning and peer verification."""

    content: bytes
    final_url: str
    media_type: str


class SourceFetchUnavailable(RuntimeError):
    """@brief 已允许来源暂时无法抓取 / An allowed source is temporarily unavailable."""


class SourceDnsResolver(Protocol):
    """@brief 可替换且返回全部地址的 DNS resolver / Replaceable DNS resolver returning all addresses."""

    async def resolve(self, hostname: str, port: int) -> tuple[IpAddress, ...]:
        """@brief 解析一次连接的全部地址 / Resolve every address for one connection.

        @param hostname 已规范化 ASCII hostname / Canonical ASCII hostname.
        @param port 已验证端口 / Validated port.
        @return 去重、稳定排序的 IP 地址 / Deduplicated, stably ordered IP addresses.
        """


class SystemSourceDnsResolver:
    """@brief 使用系统 async getaddrinfo 的生产 resolver / Production resolver using async getaddrinfo."""

    async def resolve(self, hostname: str, port: int) -> tuple[IpAddress, ...]:
        """@brief 在线程安全 event loop API 中解析 A/AAAA / Resolve A/AAAA through the event-loop API."""

        loop = asyncio.get_running_loop()
        try:
            answers = await loop.getaddrinfo(
                hostname,
                port,
                family=socket.AF_UNSPEC,
                type=socket.SOCK_STREAM,
                proto=socket.IPPROTO_TCP,
            )
        except socket.gaierror as error:
            raise SourceNetworkRejected("source hostname could not be resolved") from error
        addresses = {
            ip_address(str(sockaddr[0]).split("%", maxsplit=1)[0])
            for _family, _type, _protocol, _canonical, sockaddr in answers
        }
        if not addresses:
            raise SourceNetworkRejected("source hostname resolved to no addresses")
        return tuple(sorted(addresses, key=lambda item: (item.version, int(item))))


@dataclass(frozen=True, slots=True)
class SourceNetworkPolicy:
    """@brief 显式来源网络 allowlist / Explicit source-network allowlist.

    @param allowed_schemes 允许的 URL schemes / Allowed URL schemes.
    @param allowed_ports 允许的显式或默认 ports / Allowed explicit or default ports.
    @param allowed_host_patterns 精确 hostname 或 ``*.example.com`` 子域规则 / Exact hostnames
        or ``*.example.com`` subdomain rules.
    @param maximum_redirects 单次抓取的 redirect 上限 / Redirect cap for one fetch.
    @param allow_https_downgrade 是否允许 HTTPS redirect 到 HTTP / Whether HTTPS may redirect to HTTP.
    """

    allowed_schemes: frozenset[str]
    allowed_ports: frozenset[int]
    allowed_host_patterns: tuple[str, ...]
    maximum_redirects: int = 5
    allow_https_downgrade: bool = False

    def __post_init__(self) -> None:
        """@brief 校验 policy 本身为闭合 allowlist / Validate the policy as a closed allowlist."""

        if not self.allowed_schemes or not self.allowed_schemes <= {"http", "https"}:
            raise ValueError("source network schemes must be a non-empty HTTP(S) allowlist")
        if not self.allowed_ports or any(
            isinstance(port, bool) or not 1 <= port <= 65_535 for port in self.allowed_ports
        ):
            raise ValueError("source network ports must be a non-empty valid allowlist")
        if not self.allowed_host_patterns:
            raise ValueError("source network hostname allowlist must not be empty")
        canonical_patterns = tuple(_canonical_host_pattern(item) for item in self.allowed_host_patterns)
        if len(set(canonical_patterns)) != len(canonical_patterns):
            raise ValueError("source network hostname allowlist contains duplicates")
        if not 0 <= self.maximum_redirects <= 20:
            raise ValueError("source network redirect limit must be between zero and 20")
        object.__setattr__(self, "allowed_host_patterns", canonical_patterns)


@dataclass(frozen=True, slots=True)
class ResolvedSourceTarget:
    """@brief 一次连接使用的规范 URL 与 DNS pin / Canonical URL and DNS pin for one connection.

    @param url 已规范化 URL / Canonical URL.
    @param scheme 已验证 scheme / Validated scheme.
    @param hostname ASCII hostname / ASCII hostname.
    @param port 有效目标端口 / Effective target port.
    @param addresses 本次连接必须选择的全部已验证地址 / All validated addresses from which
        this connection must choose.
    """

    url: str
    scheme: str
    hostname: str
    port: int
    addresses: tuple[IpAddress, ...]

    def require_connected_peer(self, peer: str) -> IpAddress:
        """@brief 校验实际 socket peer 属于本次 DNS pin / Validate the socket peer against this DNS pin.

        @param peer 实际连接对端 IP，可含 IPv6 zone suffix / Connected peer IP, optionally with an IPv6 zone suffix.
        @return 规范 IP / Canonical IP address.
        @raise SourceNetworkRejected 对端非法、受保护或不在本次解析结果时抛出 / Raised
            when the peer is invalid, protected, or absent from this resolution.
        """

        try:
            address = ip_address(peer.split("%", maxsplit=1)[0])
        except ValueError as error:
            raise SourceNetworkRejected("connected source peer is not an IP address") from error
        _require_public_address(address)
        if address not in self.addresses:
            raise SourceNetworkRejected("connected source peer does not match the current DNS pin")
        return address


class StrictSourceNetworkGuard:
    """@brief 每次解析、连接与 redirect 都可复用的 SSRF guard / Reusable SSRF guard for every resolution, connection, and redirect."""

    def __init__(
        self,
        policy: SourceNetworkPolicy,
        *,
        resolver: SourceDnsResolver | None = None,
    ) -> None:
        """@brief 绑定显式 policy 与 resolver / Bind an explicit policy and resolver.

        @param policy 闭合 allowlist / Closed allowlist.
        @param resolver 测试可替换 resolver / Test-replaceable resolver.
        """

        self._policy = policy
        self._resolver = resolver or SystemSourceDnsResolver()

    async def validate(self, source_input: UrlSourceInput | GitSourceInput) -> None:
        """@brief 验证登记时的初始 URL / Validate a source's initial URL at registration."""

        target = source_input.url if isinstance(source_input, UrlSourceInput) else source_input.clone_url
        await self.resolve(target)

    async def resolve(self, url: str) -> ResolvedSourceTarget:
        """@brief 为一次实际连接重新解析并校验全部地址 / Re-resolve and validate every address for one connection.

        @param url 即将连接的 URL / URL about to be connected.
        @return 不得跨连接复用的 DNS pin / DNS pin that must not be reused across connections.
        """

        parsed, scheme, hostname, port = self._parse(url)
        try:
            literal = ip_address(hostname)
        except ValueError:
            addresses = await self._resolver.resolve(hostname, port)
        else:
            addresses = (literal,)
        if not addresses:
            raise SourceNetworkRejected("source hostname resolved to no addresses")
        if len(set(addresses)) != len(addresses):
            raise SourceNetworkRejected("source resolver returned duplicate addresses")
        for address in addresses:
            _require_public_address(address)
        canonical_netloc = f"[{hostname}]" if ":" in hostname else hostname
        default_port = 443 if scheme == "https" else 80
        if port != default_port:
            canonical_netloc = f"{canonical_netloc}:{port}"
        canonical_url = urlunsplit(
            (scheme, canonical_netloc, parsed.path or "/", parsed.query, "")
        )
        return ResolvedSourceTarget(canonical_url, scheme, hostname, port, addresses)

    async def resolve_redirect(
        self,
        previous: ResolvedSourceTarget,
        redirect_url: str,
        *,
        redirect_count: int,
    ) -> ResolvedSourceTarget:
        """@brief 对 redirect 目标执行完整重新校验 / Fully revalidate a redirect target.

        @param previous 上一跳已验证目标 / Previous validated target.
        @param redirect_url 已解析为绝对 URL 的 Location / Location resolved to an absolute URL.
        @param redirect_count 包含当前跳的 redirect 数 / Redirect count including this hop.
        @return 新连接专属 DNS pin / A DNS pin dedicated to the new connection.
        """

        if not 1 <= redirect_count <= self._policy.maximum_redirects:
            raise SourceNetworkRejected("source redirect limit was exceeded")
        redirected = await self.resolve(redirect_url)
        if (
            previous.scheme == "https"
            and redirected.scheme == "http"
            and not self._policy.allow_https_downgrade
        ):
            raise SourceNetworkRejected("source redirect cannot downgrade HTTPS to HTTP")
        return redirected

    def _parse(self, url: str) -> tuple[SplitResult, str, str, int]:
        """@brief 规范化 URL 并应用 scheme/port/host allowlist / Canonicalize a URL and apply its allowlists."""

        if not url or len(url) > 8_192 or any(ord(character) < 32 for character in url):
            raise SourceNetworkRejected("source URL is empty, oversized, or contains controls")
        parsed = urlsplit(url)
        scheme = parsed.scheme.lower()
        if scheme not in self._policy.allowed_schemes:
            raise SourceNetworkRejected("source URL scheme is not allowed")
        if parsed.username is not None or parsed.password is not None:
            raise SourceNetworkRejected("source URL cannot contain userinfo")
        if parsed.fragment:
            raise SourceNetworkRejected("source URL cannot contain a fragment")
        if not parsed.hostname:
            raise SourceNetworkRejected("source URL requires a hostname")
        hostname = _canonical_hostname(parsed.hostname)
        if not _host_allowed(hostname, self._policy.allowed_host_patterns):
            raise SourceNetworkRejected("source hostname is not allowed")
        try:
            port = parsed.port or (443 if scheme == "https" else 80)
        except ValueError as error:
            raise SourceNetworkRejected("source URL port is invalid") from error
        if port not in self._policy.allowed_ports:
            raise SourceNetworkRejected("source URL port is not allowed")
        return parsed, scheme, hostname, port


class PinnedHttpSourceFetcher:
    """@brief 直接连接已验证 IP、保留 Host/SNI 并核验 socket peer / Connect to a validated IP while preserving Host/SNI and checking the socket peer."""

    def __init__(
        self,
        guard: StrictSourceNetworkGuard,
        *,
        maximum_body_bytes: int,
        connect_timeout_ms: int,
        read_timeout_ms: int,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """@brief 注入同一不可绕过 guard 与 HTTP 预算 / Inject the same non-bypassable guard and HTTP budgets."""

        if not 1 <= maximum_body_bytes <= 1_073_741_824:
            raise ValueError("source fetch body bound must be one byte to one GiB")
        if not 100 <= connect_timeout_ms <= 60_000 or not 100 <= read_timeout_ms <= 300_000:
            raise ValueError("source fetch timeouts are outside safe bounds")
        self._guard = guard
        self._maximum_body_bytes = maximum_body_bytes
        self._timeout = httpx.Timeout(
            read_timeout_ms / 1_000,
            connect=connect_timeout_ms / 1_000,
            write=connect_timeout_ms / 1_000,
            pool=connect_timeout_ms / 1_000,
        )
        self._transport = transport

    async def fetch(self, url: str) -> FetchedSourceBody:
        """@brief 每跳重新解析、每连接锁定 IP 并限制解码后正文 / Re-resolve every hop, pin every connection, and bound decoded body bytes."""

        target = await self._guard.resolve(url)
        redirect_count = 0
        while True:
            response, content = await self._one_hop(target)
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                if location is None or not location or len(location) > 8_192:
                    raise SourceNetworkRejected("source redirect has no valid Location")
                redirect_count += 1
                target = await self._guard.resolve_redirect(
                    target,
                    urljoin(target.url, location),
                    redirect_count=redirect_count,
                )
                continue
            if not 200 <= response.status_code < 300:
                raise SourceFetchUnavailable("source returned an unsuccessful status")
            media_type = response.headers.get("content-type", "application/octet-stream")
            media_type = media_type.partition(";")[0].strip().lower()
            if not media_type or len(media_type) > 200:
                media_type = "application/octet-stream"
            return FetchedSourceBody(content, target.url, media_type)

    async def _one_hop(self, target: ResolvedSourceTarget) -> tuple[httpx.Response, bytes]:
        """@brief 尝试当前 pin 内地址且不跟随 redirect / Try addresses in the current pin without following redirects."""

        last_error: Exception | None = None
        for address in target.addresses:
            try:
                return await self._request_address(target, address)
            except SourceNetworkRejected:
                raise
            except (httpx.TimeoutException, httpx.NetworkError, httpx.ProtocolError) as error:
                last_error = error
        raise SourceFetchUnavailable("source could not be reached at its validated addresses") from last_error

    async def _request_address(
        self,
        target: ResolvedSourceTarget,
        address: IpAddress,
    ) -> tuple[httpx.Response, bytes]:
        """@brief 对一个 pin 地址发 GET 并验证实际 peer / GET one pinned address and verify its actual peer."""

        request_url = _pinned_url(target, address)
        headers = {
            "Accept": "text/plain, text/markdown, text/html, application/pdf, "
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document, "
            "application/xml, application/atom+xml, application/rss+xml;q=0.9, */*;q=0.1",
            "Host": _host_header(target),
            "User-Agent": "HM-Alliances-Knowledge-Fetcher/2",
        }
        extensions: dict[str, Any] = {"sni_hostname": target.hostname}
        async with httpx.AsyncClient(
            timeout=self._timeout,
            transport=self._transport,
            trust_env=False,
            follow_redirects=False,
            http2=False,
        ) as client:
            async with client.stream(
                "GET",
                request_url,
                headers=headers,
                extensions=extensions,
            ) as response:
                _verify_response_peer(response, target)
                declared = response.headers.get("content-length")
                if declared is not None:
                    try:
                        declared_size = int(declared)
                    except ValueError as error:
                        raise SourceNetworkRejected("source Content-Length is invalid") from error
                    if declared_size < 0 or declared_size > self._maximum_body_bytes:
                        raise SourceNetworkRejected("source response exceeds the configured bound")
                body = bytearray()
                async for chunk in response.aiter_bytes(1024 * 1024):
                    body.extend(chunk)
                    if len(body) > self._maximum_body_bytes:
                        raise SourceNetworkRejected("source response exceeds the configured bound")
                return response, bytes(body)


def _pinned_url(target: ResolvedSourceTarget, address: IpAddress) -> str:
    """@brief 将连接 authority 换为 pin IP 而保持 path/query / Replace connection authority with a pinned IP while preserving path/query."""

    parsed = urlsplit(target.url)
    host = f"[{address.compressed}]" if address.version == 6 else address.compressed
    default_port = 443 if target.scheme == "https" else 80
    authority = host if target.port == default_port else f"{host}:{target.port}"
    return urlunsplit((target.scheme, authority, parsed.path, parsed.query, ""))


def _host_header(target: ResolvedSourceTarget) -> str:
    """@brief 构造原始 hostname 的 RFC Host header / Build the RFC Host header for the original hostname."""

    host = f"[{target.hostname}]" if ":" in target.hostname else target.hostname
    default_port = 443 if target.scheme == "https" else 80
    return host if target.port == default_port else f"{host}:{target.port}"


def _verify_response_peer(response: httpx.Response, target: ResolvedSourceTarget) -> None:
    """@brief 从 transport stream 读取实际对端并对 DNS pin 复验 / Read the transport peer and recheck it against the DNS pin."""

    stream = response.extensions.get("network_stream")
    if stream is None or not hasattr(stream, "get_extra_info"):
        raise SourceNetworkRejected("source transport did not expose its connected peer")
    peer = stream.get_extra_info("server_addr")
    if (
        not isinstance(peer, tuple)
        or not peer
        or not isinstance(peer[0], str)
    ):
        raise SourceNetworkRejected("source transport returned an invalid connected peer")
    target.require_connected_peer(peer[0])


def _canonical_hostname(hostname: str) -> str:
    """@brief 把 hostname 规范为无尾点 IDNA ASCII / Canonicalize a hostname to IDNA ASCII without a trailing dot."""

    candidate = hostname.rstrip(".").lower()
    if not candidate or "%" in candidate or len(candidate) > 253:
        raise SourceNetworkRejected("source hostname is invalid")
    try:
        literal = ip_address(candidate)
    except ValueError:
        try:
            ascii_host = candidate.encode("idna").decode("ascii")
        except UnicodeError as error:
            raise SourceNetworkRejected("source hostname IDNA encoding failed") from error
        labels = ascii_host.split(".")
        if any(
            not label
            or len(label) > 63
            or label.startswith("-")
            or label.endswith("-")
            for label in labels
        ):
            raise SourceNetworkRejected("source hostname labels are invalid") from None
        return ascii_host
    return literal.compressed


def _canonical_host_pattern(pattern: str) -> str:
    """@brief 规范化精确或 wildcard hostname 规则 / Canonicalize an exact or wildcard hostname rule."""

    if pattern.startswith("*."):
        suffix = _canonical_hostname(pattern[2:])
        try:
            ip_address(suffix)
        except ValueError:
            return f"*.{suffix}"
        raise ValueError("source hostname wildcard cannot target an IP literal")
    return _canonical_hostname(pattern)


def _host_allowed(hostname: str, patterns: Sequence[str]) -> bool:
    """@brief 精确匹配 hostname 或一个以上子域 label / Match an exact hostname or one-or-more subdomain labels."""

    for pattern in patterns:
        if not pattern.startswith("*."):
            if hostname == pattern:
                return True
            continue
        suffix = pattern[1:]
        if hostname.endswith(suffix) and hostname != suffix[1:]:
            return True
    return False


def _require_public_address(address: IpAddress) -> None:
    """@brief 拒绝所有非全球可路由地址，包括 metadata / Reject every non-global address, including metadata."""

    if (
        not address.is_global
        or address.is_loopback
        or address.is_private
        or address.is_link_local
        or address.is_multicast
        or address.is_unspecified
        or address.is_reserved
    ):
        raise SourceNetworkRejected("source hostname resolves to a protected address")


__all__ = [
    "FetchedSourceBody",
    "PinnedHttpSourceFetcher",
    "ResolvedSourceTarget",
    "SourceDnsResolver",
    "SourceFetchUnavailable",
    "SourceNetworkPolicy",
    "SourceNetworkRejected",
    "StrictSourceNetworkGuard",
    "SystemSourceDnsResolver",
]
