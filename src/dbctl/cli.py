"""@brief dbctl 命令行接口 / Command-line interface for dbctl."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from .composition import DbctlComposition
from .config import DatabaseRole
from .errors import DbctlError
from .retention import (
    DEFAULT_TELEMETRY_PRUNE_BATCH_SIZE,
    DEFAULT_TELEMETRY_PRUNE_MAX_BATCHES,
    DEFAULT_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS,
    MAX_TELEMETRY_PRUNE_BATCH_SIZE,
    MAX_TELEMETRY_PRUNE_MAX_BATCHES,
    MAX_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS,
)
from .shell import PasswordPolicy


def build_parser() -> argparse.ArgumentParser:
    """@brief 创建 dbctl 参数解析器 / Create the dbctl argument parser.

    @return 已配置的 argparse parser / Configured argparse parser.
    @note CLI 不接受 DSN 或密码参数；所有敏感配置均从环境变量读取。
    / The CLI accepts no DSN or password argument; all sensitive configuration is read from environment variables.
    """
    parser = argparse.ArgumentParser(
        prog="workspace-dbctl",
        description="安全管理 AI Job Workspace PostgreSQL bootstrap 与 psql shell。",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.jsonc"),
        help="本地私密运行配置路径（默认：config.jsonc）。",
    )
    parser.add_argument(
        "--dbinit",
        type=Path,
        default=Path("dbinit.jsonc"),
        help="数据库初始化声明路径（默认：dbinit.jsonc）。",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="生成并可选执行幂等 PostgreSQL role、database、schema、权限计划。",
    )
    _add_subcommand_config_override(bootstrap)
    _add_subcommand_dbinit_override(bootstrap)
    bootstrap.add_argument(
        "--dry-run",
        action="store_true",
        help="仅输出脱敏 SQL 计划，不连接 PostgreSQL、不执行 SQL。",
    )

    migrate = subparsers.add_parser(
        "migrate",
        help="显式使用 migrator DSN 执行 Alembic migration；后端启动从不自动执行。",
    )
    _add_subcommand_config_override(migrate)
    _add_subcommand_dbinit_override(migrate)
    migrate.add_argument(
        "--revision",
        default="head",
        help="目标 Alembic revision（默认：head）。",
    )

    prune_telemetry = subparsers.add_parser(
        "prune-telemetry",
        help="按 observability.retention_days 受限清理过期遥测；默认仅 dry-run。",
    )
    _add_subcommand_config_override(prune_telemetry)
    _add_subcommand_dbinit_override(prune_telemetry)
    execution_mode = prune_telemetry.add_mutually_exclusive_group()
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
    prune_telemetry.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_TELEMETRY_PRUNE_BATCH_SIZE,
        help=(
            "单批删除上限（默认："
            f"{DEFAULT_TELEMETRY_PRUNE_BATCH_SIZE}，最大：{MAX_TELEMETRY_PRUNE_BATCH_SIZE}）。"
        ),
    )
    prune_telemetry.add_argument(
        "--max-batches",
        type=int,
        default=DEFAULT_TELEMETRY_PRUNE_MAX_BATCHES,
        help=(
            "本次最多提交的删除短事务数（默认："
            f"{DEFAULT_TELEMETRY_PRUNE_MAX_BATCHES}，最大：{MAX_TELEMETRY_PRUNE_MAX_BATCHES}）。"
        ),
    )
    prune_telemetry.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=DEFAULT_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS,
        help=(
            "每个清理/计数 SQL 的事务本地超时毫秒数（默认："
            f"{DEFAULT_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS}，最大："
            f"{MAX_TELEMETRY_PRUNE_STATEMENT_TIMEOUT_MS}）。"
        ),
    )
    shell = subparsers.add_parser(
        "shell",
        help="直接 exec 到以合适登录身份连接的 psql。",
    )
    _add_subcommand_config_override(shell)
    _add_subcommand_dbinit_override(shell)
    shell.add_argument(
        "--role",
        choices=(DatabaseRole.APP.value, DatabaseRole.MIGRATOR.value, DatabaseRole.DASHBOARD.value),
        default=DatabaseRole.APP.value,
        help="psql 身份（默认：app）。owner 是 NOLOGIN，故不可选。",
    )
    shell.add_argument(
        "--password-source",
        choices=tuple(policy.value for policy in PasswordPolicy),
        default=PasswordPolicy.AUTO.value,
        help=(
            "认证策略：auto 优先 .pgpass 后提示；pgpass 强制 .pgpass；"
            "prompt 强制提示；environment 仅在显式请求时使用 PGPASSWORD。"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 运行 dbctl CLI / Run the dbctl CLI.

    @param argv 可选参数序列；``None`` 时读取进程命令行。
    / Optional argument sequence; reads process command line when ``None``.
    @return 成功为 ``0``，可展示配置/执行错误为 ``2``。
    / ``0`` on success and ``2`` for displayable configuration/execution errors.
    @note ``shell`` 成功时会由 ``os.execvp`` 替换进程，通常不会返回。
    / On success ``shell`` replaces the process through ``os.execvp`` and normally does not return.
    """
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        composition = DbctlComposition.from_config_path(
            arguments.config,
            dbinit_path=arguments.dbinit,
        )
        if arguments.command == "bootstrap":
            plan = composition.build_bootstrap_plan()
            if arguments.dry_run:
                print(plan.render_dry_run())
                return 0
            result = composition.execute_bootstrap(plan)
            database_status = "已创建" if result.database_created else "已存在"
            print(
                "dbctl bootstrap 完成：目标数据库"
                f"{database_status}；执行了 {result.executed_statement_count} 条计划 SQL。"
            )
            return 0

        if arguments.command == "shell":
            prepared = composition.prepare_shell(
                DatabaseRole(arguments.role),
                password_policy=PasswordPolicy(arguments.password_source),
            )
            print(f"dbctl shell：{prepared.policy_message()}", file=sys.stderr)
            composition.exec_prepared_shell(prepared)
            return 0
        if arguments.command == "migrate":
            composition.execute_migration(arguments.revision)
            print(f"dbctl migrate 完成：已升级至 {arguments.revision}。")
            return 0
        if arguments.command == "prune-telemetry":
            prune_result = composition.prune_telemetry(
                apply=arguments.apply,
                batch_size=arguments.batch_size,
                max_batches=arguments.max_batches,
                statement_timeout_ms=arguments.statement_timeout_ms,
            )
            print(prune_result.render_operator_summary())
            return 0
        parser.error("未知 dbctl 子命令。")
    except DbctlError as error:
        print(f"dbctl: {error}", file=sys.stderr)
        return 2
    except OSError:
        print("dbctl: 无法启动所需的本地可执行程序。", file=sys.stderr)
        return 2
    return 2


def _add_subcommand_config_override(parser: argparse.ArgumentParser) -> None:
    """@brief 支持把 --config 写在子命令后 / Support placing --config after a subcommand.

    @param parser 子命令 parser / Subcommand parser.
    @return 无返回值 / No return value.
    """
    parser.add_argument(
        "--config",
        type=Path,
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )


def _add_subcommand_dbinit_override(parser: argparse.ArgumentParser) -> None:
    """@brief 支持把 --dbinit 写在子命令后 / Support placing --dbinit after a subcommand.

    @param parser 子命令 parser / Subcommand parser.
    @return 无返回值 / No return value.
    """
    parser.add_argument(
        "--dbinit",
        type=Path,
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
