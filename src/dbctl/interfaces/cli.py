"""@brief dbctl 命令行适配器 / Command-line adapter for dbctl."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from dbctl.application.errors import ApplicationError
from dbctl.application.migrate import MigrationRevision
from dbctl.application.provision import BootstrapAccessMode, build_bootstrap_plan
from dbctl.application.prune_telemetry import PruneMode, PruneRequest
from dbctl.composition import compose_dbctl
from dbctl.domain.errors import DomainError
from dbctl.domain.retention import (
    DEFAULT_LOCK_TIMEOUT_MS,
    DEFAULT_PRUNE_BATCH_SIZE,
    DEFAULT_PRUNE_MAX_BATCHES,
    DEFAULT_STATEMENT_TIMEOUT_MS,
    MAX_LOCK_TIMEOUT_MS,
    MAX_PRUNE_BATCH_SIZE,
    MAX_PRUNE_MAX_BATCHES,
    MAX_STATEMENT_TIMEOUT_MS,
    PruneLimits,
)
from dbctl.domain.roles import LoginRole
from dbctl.interfaces.presenters import (
    render_bootstrap_plan,
    render_prune_outcome,
    render_shell_policy,
)


def build_parser() -> argparse.ArgumentParser:
    """@brief 创建稳定的 dbctl 参数契约 / Build the stable dbctl argument contract.

    @return 已配置 argparse parser / Configured argparse parser.
    @note CLI 不接受 DSN 或密码参数 / The CLI accepts no DSN or password arguments.
    """

    parser = argparse.ArgumentParser(
        prog="dbctl",
        description="安全管理 AI Job Workspace PostgreSQL bootstrap 与 psql shell。",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="本地私密运行配置路径（默认：config.jsonc）。",
    )
    parser.add_argument(
        "--dbinit",
        type=Path,
        default=None,
        help="数据库初始化声明路径（默认：dbinit.jsonc）。",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="生成并可选执行幂等 PostgreSQL role、database、schema、权限计划。",
    )
    _add_path_overrides(bootstrap)
    bootstrap.add_argument(
        "--dry-run",
        action="store_true",
        help="仅输出脱敏 SQL 计划，不连接 PostgreSQL、不执行 SQL。",
    )
    bootstrap.add_argument(
        "--access-mode",
        choices=tuple(mode.value for mode in BootstrapAccessMode),
        default=BootstrapAccessMode.AUTO.value,
        help="管理权限方式：auto 优先本机 POSIX sudo，否则终端提示 PostgreSQL 管理密码。",
    )

    migrate = subparsers.add_parser(
        "migrate",
        help="显式使用 migrator DSN 执行 Alembic migration；后端启动从不自动执行。",
    )
    _add_path_overrides(migrate)
    migrate.add_argument(
        "--revision",
        default="head",
        help="目标 Alembic revision（默认：head）。",
    )

    prune = subparsers.add_parser(
        "prune-telemetry",
        help="按 observability.retention_days 受限清理过期遥测；默认仅 dry-run。",
    )
    _add_path_overrides(prune)
    execution_mode = prune.add_mutually_exclusive_group()
    execution_mode.add_argument(
        "--dry-run",
        action="store_true",
        help="显式预览清理边界；这是默认行为，不连接数据库也不执行 SQL。",
    )
    execution_mode.add_argument(
        "--apply",
        action="store_true",
        help="明确允许删除；仅此选项会使用 migrator DSN 连接 PostgreSQL。",
    )
    prune.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_PRUNE_BATCH_SIZE,
        help=f"单批删除上限（默认：{DEFAULT_PRUNE_BATCH_SIZE}，最大：{MAX_PRUNE_BATCH_SIZE}）。",
    )
    prune.add_argument(
        "--max-batches",
        type=int,
        default=DEFAULT_PRUNE_MAX_BATCHES,
        help=f"本次最多提交的删除短事务数（默认：{DEFAULT_PRUNE_MAX_BATCHES}，最大：{MAX_PRUNE_MAX_BATCHES}）。",
    )
    prune.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=DEFAULT_STATEMENT_TIMEOUT_MS,
        help=(
            "每个清理/计数 SQL 的事务本地超时毫秒数（默认："
            f"{DEFAULT_STATEMENT_TIMEOUT_MS}，最大：{MAX_STATEMENT_TIMEOUT_MS}）。"
        ),
    )
    prune.add_argument(
        "--lock-timeout-ms",
        type=int,
        default=DEFAULT_LOCK_TIMEOUT_MS,
        help=(
            "每个清理事务的锁等待超时毫秒数（默认："
            f"{DEFAULT_LOCK_TIMEOUT_MS}，最大：{MAX_LOCK_TIMEOUT_MS}）。"
        ),
    )

    shell = subparsers.add_parser(
        "shell",
        help="使用 config.jsonc 中所选登录身份启动 psql，不交互询问数据库密码。",
    )
    _add_path_overrides(shell)
    shell.add_argument(
        "--role",
        choices=tuple(role.value for role in LoginRole),
        default=LoginRole.APP.value,
        help="psql 身份（默认：app）。owner 是 NOLOGIN，故不可选。",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 解析终端输入并调用一个应用用例 / Parse terminal input and invoke one application use case.

    @param argv 可选参数序列；None 读取进程命令行 / Optional arguments; None reads process arguments.
    @return 成功为 0、可展示错误为 2；shell 返回 psql 状态。
    / Zero on success, two for displayable failures; shell returns psql status.
    """

    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        application = compose_dbctl(
            arguments.config,
            dbinit_path=arguments.dbinit,
            initialize_config=arguments.command == "bootstrap",
        )
        if arguments.command == "bootstrap":
            plan = build_bootstrap_plan(application.settings)
            if arguments.dry_run:
                print(render_bootstrap_plan(plan))
                return 0
            result = application.bootstrap.execute(
                plan,
                access_mode=BootstrapAccessMode(arguments.access_mode),
            )
            database_status = "已创建" if result.database_created else "已存在"
            print(
                "dbctl bootstrap 完成：目标数据库"
                f"{database_status}；执行了 {result.executed_statement_count} 条计划 SQL。"
            )
            return 0
        if arguments.command == "migrate":
            revision = MigrationRevision(arguments.revision)
            application.migration.execute(
                application.settings.connections.migrator,
                revision,
                application.settings,
            )
            print(f"dbctl migrate 完成：已升级至 {revision.value}。")
            return 0
        if arguments.command == "prune-telemetry":
            outcome = application.prune.execute(
                PruneRequest(
                    policy=application.settings.retention,
                    limits=PruneLimits(
                        batch_size=arguments.batch_size,
                        max_batches=arguments.max_batches,
                        statement_timeout_ms=arguments.statement_timeout_ms,
                        lock_timeout_ms=arguments.lock_timeout_ms,
                    ),
                    mode=PruneMode.APPLY if arguments.apply else PruneMode.DRY_RUN,
                )
            )
            print(render_prune_outcome(outcome))
            return 0
        if arguments.command == "shell":
            login = application.settings.connections.login_for(LoginRole(arguments.role))
            print(render_shell_policy(login), file=sys.stderr)
            return application.shell.execute(login)
        parser.error("未知 dbctl 子命令。")
    except (ApplicationError, DomainError) as error:
        print(f"dbctl: {error}", file=sys.stderr)
        return 2
    except OSError:
        print("dbctl: 无法启动所需的本地可执行程序。", file=sys.stderr)
        return 2
    return 2


def _add_path_overrides(parser: argparse.ArgumentParser) -> None:
    """@brief 支持把全局路径参数写在子命令后 / Support global path options after a subcommand.

    @param parser 子命令 parser / Subcommand parser.
    @return 无返回值 / No return value.
    """

    parser.add_argument(
        "--config",
        type=Path,
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--dbinit",
        type=Path,
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )


__all__ = ["build_parser", "main"]
