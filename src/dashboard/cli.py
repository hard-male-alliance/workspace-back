"""Dashboard 的无界面 CLI 适配器（command-line adapter）。"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from .composition import DashboardApplication, create_dashboard_application
from .errors import (
    DashboardConfigurationError,
    DashboardDataError,
    DashboardError,
    DashboardUnavailableError,
)


def main(
    argv: Sequence[str] | None = None,
    *,
    application: DashboardApplication | None = None,
) -> int:
    """@brief 执行 Dashboard CLI 的同步进程入口（process entrypoint）。

    @param argv: 不含程序名的参数序列；省略时读取 sys.argv。
    @param application: 可注入的已组合应用，便于测试或复用外部资源。
    @return: 0 表示成功，2 表示用户输入或配置错误，1 表示未预期失败。
    """

    try:
        return asyncio.run(async_main(argv, application=application))
    except KeyboardInterrupt:
        return 130


async def async_main(
    argv: Sequence[str] | None = None,
    *,
    application: DashboardApplication | None = None,
) -> int:
    """@brief 执行 Dashboard CLI 的异步入口，供嵌入式调用方复用。

    @param argv: 不含程序名的参数序列；省略时读取 sys.argv。
    @param application: 可注入的已组合应用，调用方保留其资源所有权。
    @return: 与 main 相同的进程退出码语义。
    """

    parser = _build_parser()
    try:
        arguments = parser.parse_args(list(argv) if argv is not None else None)
    except SystemExit as error:
        return int(error.code) if isinstance(error.code, int) else 2

    owns_application = False
    resolved_application: DashboardApplication | None = None
    exit_code = 0
    try:
        start_at = _parse_datetime(arguments.start_at, "--start-at")
        end_at = _parse_datetime(arguments.end_at, "--end-at")
        owns_application = application is None
        resolved_application = (
            create_dashboard_application(config_path=arguments.config)
            if application is None
            else application
        )
        scope = resolved_application.scope_for_local_operator(
            arguments.workspace_id,
            requested_actor_id=arguments.actor_id,
        )

        if arguments.command == "overview":
            overview = await resolved_application.service.overview(
                scope,
                start_at=start_at,
                end_at=end_at,
                service=arguments.service,
                max_samples=arguments.max_samples,
            )
            _write_overview(overview.to_dict(), arguments.output)
        elif arguments.command == "services":
            services = await resolved_application.service.list_services(
                scope,
                start_at=start_at,
                end_at=end_at,
                max_samples=arguments.max_samples,
            )
            _write_services(
                {
                    "scope": scope.to_dict(),
                    "items": [summary.to_dict() for summary in services],
                },
                arguments.output,
            )
        else:
            raise ValueError(f"未知命令：{arguments.command}")
    except (DashboardConfigurationError, ValueError) as error:
        print(f"dashboard: {error}", file=sys.stderr)
        exit_code = 2
    except (DashboardDataError, DashboardUnavailableError):
        print("dashboard: Dashboard 读模型当前不可用。", file=sys.stderr)
        exit_code = 1
    except DashboardError as error:
        print(f"dashboard: {error}", file=sys.stderr)
        exit_code = 2
    except Exception:  # pragma: no cover - 仅进程级故障边界
        print("dashboard: 未预期错误；请查看受控日志。", file=sys.stderr)
        exit_code = 1
    finally:
        if owns_application and resolved_application is not None:
            try:
                await resolved_application.aclose()
            except Exception:  # pragma: no cover - 仅资源释放的进程级边界
                print("dashboard: 关闭资源失败；请查看受控日志。", file=sys.stderr)
                if exit_code == 0:
                    exit_code = 1

    return exit_code


def _build_parser() -> argparse.ArgumentParser:
    """构造只读、无副作用的 argparse 解析器。"""

    parser = argparse.ArgumentParser(
        prog="dashboard",
        description="以受控本地 operator 身份查询 AI Job Workspace 的可观测性摘要。",
    )
    parser.add_argument(
        "--config",
        default="config.jsonc",
        help="共享根配置文件路径（默认：config.jsonc）。",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    overview = subparsers.add_parser("overview", help="查看工作区总览。")
    _add_query_arguments(overview, include_service=True)

    services = subparsers.add_parser("services", help="列出工作区内的服务摘要。")
    _add_query_arguments(services, include_service=False)
    return parser


def _add_query_arguments(
    parser: argparse.ArgumentParser,
    *,
    include_service: bool,
) -> None:
    """为一个只读子命令添加共享查询选项。"""

    parser.add_argument("--workspace-id", required=True, help="必须读取的工作区标识。")
    parser.add_argument(
        "--actor-id",
        default=None,
        help="可选审计 operator 标识；提供时必须等于 dashboard.access.operator_id。",
    )
    parser.add_argument("--start-at", default=None, help="RFC 3339 窗口起点。")
    parser.add_argument("--end-at", default=None, help="RFC 3339 窗口终点。")
    parser.add_argument("--max-samples", type=int, default=None, help="可选更严格样本上限。")
    parser.add_argument(
        "--output",
        choices=("json", "table"),
        default="json",
        help="输出格式（默认：json）。",
    )
    if include_service:
        parser.add_argument("--service", default=None, help="可选稳定服务名过滤。")


def _parse_datetime(value: str | None, option_name: str) -> datetime | None:
    """解析 RFC 3339 CLI 时间，明确拒绝无时区输入。"""

    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{option_name} 必须是 RFC 3339 时间戳。") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{option_name} 必须包含时区，例如 2026-01-01T00:00:00Z。")
    return parsed.astimezone(UTC)


def _write_overview(payload: dict[str, object], output: str) -> None:
    """以 JSON 或紧凑表格写出完整总览。"""

    if output == "json":
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
        return

    scope = payload["scope"]
    workspace_id = scope.get("workspace_id", "") if isinstance(scope, Mapping) else ""
    print(f"工作区: {workspace_id}")
    print(f"健康状态: {payload['health']}")
    print(f"请求数: {payload['request_count']}")
    print(f"错误数: {payload['error_count']}")
    print(f"错误率: {_format_optional(payload['error_rate'])}")
    print(f"可用性: {_format_optional(payload['availability'])}")
    print()
    _write_service_table(payload["services"])


def _write_services(payload: dict[str, object], output: str) -> None:
    """以 JSON 或紧凑表格写出服务列表。"""

    if output == "json":
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
        return
    _write_service_table(payload["items"])


def _write_service_table(items: object) -> None:
    """写出无额外依赖的固定列宽服务表格。"""

    rows = items if isinstance(items, list) else []
    headers = ("服务", "状态", "请求", "错误", "错误率", "p95 延迟(ms)", "饱和度")
    rendered = [
        (
            str(row.get("service", "")),
            str(row.get("health", "")),
            str(row.get("request_count", "")),
            str(row.get("error_count", "")),
            _format_optional(row.get("error_rate")),
            _format_optional(row.get("latency_p95_ms")),
            _format_optional(row.get("saturation")),
        )
        for row in rows
        if isinstance(row, dict)
    ]
    widths = [len(header) for header in headers]
    for row in rendered:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def render(row: tuple[str, ...]) -> str:
        return "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    print(render(headers))
    print(render(tuple("-" * width for width in widths)))
    for row in rendered:
        print(render(row))


def _format_optional(value: object) -> str:
    """稳定格式化 API 可能为 null 的数值。"""

    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


if __name__ == "__main__":  # pragma: no cover - 标准 console entrypoint
    raise SystemExit(main())


__all__ = ["async_main", "main"]
