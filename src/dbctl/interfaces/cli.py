"""@brief 面向数据库运维者的 dbctl 命令行适配器 / Operator-facing dbctl CLI adapter."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import TextIO

from dbctl.application.errors import ApplicationError
from dbctl.application.migrate import MigrationRevision, MigrationService
from dbctl.application.open_shell import OpenShellService
from dbctl.application.progress import (
    OperationName,
    ProgressState,
    ProgressUpdate,
    publish_progress,
)
from dbctl.application.provision import BootstrapAccessMode, BootstrapService, build_bootstrap_plan
from dbctl.application.prune_telemetry import PruneMode, PruneRequest, PruneTelemetryService
from dbctl.composition import (
    compose_bootstrap,
    compose_migration,
    compose_prune_telemetry,
    compose_shell,
)
from dbctl.domain.database import DbctlSettings
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
from dbctl.interfaces.console import (
    OperatorConsole,
    render_bootstrap_plan,
    render_bootstrap_result,
    render_migration_result,
    render_prune_outcome,
)

_ROOT_EPILOG = """推荐工作流：
  1. dbctl bootstrap --dry-run
  2. dbctl bootstrap
  3. dbctl migrate --revision head
  4. dbctl shell --role app

主结果写入 stdout；进度、警告和安全 traceback 写入 stderr。
"""
"""@brief 根帮助中的可发现工作流 / Discoverable workflow in root help."""


def build_parser() -> argparse.ArgumentParser:
    """@brief 创建可发现且稳定的 dbctl 参数契约 / Build a discoverable and stable argument contract.

    @return 已配置 argparse parser / Configured argparse parser.
    @note CLI 不接受 DSN 或密码参数 / The CLI accepts no DSN or password arguments.
    """

    parser = argparse.ArgumentParser(
        prog="dbctl",
        description=(
            "显式管理 AI Job Workspace 的 PostgreSQL bootstrap、schema migration、"
            "遥测保留清理与受控 psql shell。"
        ),
        epilog=_ROOT_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        suggest_on_error=True,
    )
    _add_global_options(parser)
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="收敛 PostgreSQL role、database、schema 与最小权限。",
        description=(
            "生成或补全本地私密配置，并以五个幂等阶段收敛 PostgreSQL role、database、"
            "schema 与权限。--dry-run 只禁止数据库连接和 SQL 执行，本地配置初始化仍会发生。"
        ),
        epilog=(
            "示例：\n"
            "  dbctl bootstrap --dry-run\n"
            "  dbctl bootstrap --access-mode prompt\n\n"
            "成功后运行：dbctl migrate --revision head"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_global_overrides(bootstrap)
    bootstrap.add_argument(
        "--dry-run",
        action="store_true",
        help="输出脱敏 SQL 计划；不连接 PostgreSQL，但仍可初始化本地私密配置。",
    )
    bootstrap.add_argument(
        "--access-mode",
        choices=tuple(mode.value for mode in BootstrapAccessMode),
        default=BootstrapAccessMode.AUTO.value,
        help=(
            "管理权限方式：auto 优先 loopback POSIX sudo，否则安全提示 PostgreSQL 管理密码；"
            "默认：auto。"
        ),
    )

    migrate = subparsers.add_parser(
        "migrate",
        help="使用 migrator 身份显式执行 Alembic schema upgrade。",
        description=(
            "从私密配置读取 migrator DSN，并显式执行 Alembic upgrade；后端启动从不自动迁移。"
        ),
        epilog=(
            "示例：\n"
            "  dbctl migrate --revision head\n"
            "  dbctl migrate --revision +1\n\n"
            "执行前请确认处于受控变更窗口，并已完成 dbctl bootstrap。"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_global_overrides(migrate)
    migrate.add_argument(
        "--revision",
        default="head",
        help="目标 Alembic revision（默认：head；也接受 revision id 或相对表达式）。",
    )

    prune = subparsers.add_parser(
        "prune-telemetry",
        help="按 retention policy 预览或分批删除过期遥测。",
        description=(
            "使用本轮固定 UTC cutoff 清理过期遥测。默认 dry-run 不连接数据库；只有显式"
            " --apply 才会逐批提交删除短事务。"
        ),
        epilog=(
            "示例：\n"
            "  dbctl prune-telemetry --dry-run\n"
            "  dbctl prune-telemetry --apply --batch-size 1000 --max-batches 10\n\n"
            "中途失败时，已完成批次保持提交；请按错误中的“运维影响”判断重试。"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_global_overrides(prune)
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
        help=(
            f"本次最多提交的删除短事务数（默认：{DEFAULT_PRUNE_MAX_BATCHES}，"
            f"最大：{MAX_PRUNE_MAX_BATCHES}）。"
        ),
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
        help="以配置中的受限登录身份启动交互式 psql。",
        description=(
            "从私密配置选择 app、migrator 或 dashboard 身份，通过临时 0600 PGPASSFILE"
            " 启动 psql；密码不进入 argv，也不再次交互询问。"
        ),
        epilog=(
            "示例：\n"
            "  dbctl shell\n"
            "  dbctl shell --role migrator\n"
            "  dbctl shell --role dashboard\n\n"
            "psql 的退出状态会原样归一化并由 dbctl 返回。"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_global_overrides(shell)
    shell.add_argument(
        "--role",
        choices=tuple(role.value for role in LoginRole),
        default=LoginRole.APP.value,
        help="psql 身份（默认：app）。owner 是 NOLOGIN，故不可选。",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 使用进程标准流执行 dbctl / Run dbctl using process standard streams.

    @param argv 可选参数序列；None 读取进程命令行 / Optional arguments; None reads process arguments.
    @return 成功为 0、运行时可展示错误为 2、未预期错误为 1、取消为 130；shell 返回 psql 状态。
    / Zero on success, two for displayable runtime failures, one for unexpected failures, 130 for
    cancellation; shell returns the psql status.
    """

    return run(argv, stdout=sys.stdout, stderr=sys.stderr)


def run(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    """@brief 通过可注入流执行一个 dbctl 用例 / Execute one dbctl use case with injectable streams.

    @param argv 不含程序名的可选参数 / Optional arguments excluding the program name.
    @param stdout 主结果输出流 / Primary-result output stream.
    @param stderr 进度与诊断输出流 / Progress and diagnostic output stream.
    @return 进程语义退出码 / Process-semantic exit code.
    """

    parser = build_parser()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        arguments = parser.parse_args(argv)
    command: str = arguments.command
    console = OperatorConsole(
        stderr,
        quiet=arguments.quiet,
        no_color=arguments.no_color,
    )
    console.announce(
        command,
        mode=_command_mode(arguments),
        config_path=arguments.config,
        dbinit_path=arguments.dbinit,
    )
    try:
        if command == "bootstrap":
            bootstrap_settings, bootstrap_service = compose_bootstrap(
                arguments.config,
                dbinit_path=arguments.dbinit,
                progress=console,
            )
            return _run_bootstrap(
                arguments,
                bootstrap_settings,
                bootstrap_service,
                console,
                stdout,
            )
        if command == "migrate":
            migration_settings, migration_service = compose_migration(
                arguments.config,
                dbinit_path=arguments.dbinit,
                progress=console,
            )
            return _run_migration(
                arguments,
                migration_settings,
                migration_service,
                stdout,
            )
        if command == "prune-telemetry":
            prune_settings, prune_service = compose_prune_telemetry(
                arguments.config,
                dbinit_path=arguments.dbinit,
                progress=console,
            )
            return _run_prune(arguments, prune_settings, prune_service, stdout)
        if command == "shell":
            shell_settings, shell_service = compose_shell(
                arguments.config,
                dbinit_path=arguments.dbinit,
                progress=console,
            )
            return _run_shell(arguments, shell_settings, shell_service)
        parser.error("未知 dbctl 子命令。")
    except KeyboardInterrupt as error:
        console.cancelled(command, error)
        return 130
    except (ApplicationError, DomainError) as error:
        console.failure(command, error, exit_code=2)
        return 2
    except OSError as error:
        console.failure(command, error, exit_code=2)
        return 2
    except Exception as error:  # pragma: no cover - 由专门故障注入测试覆盖输出契约。
        console.failure(command, error, exit_code=1)
        return 1
    return 2


def _run_bootstrap(
    arguments: argparse.Namespace,
    settings: DbctlSettings,
    service: BootstrapService,
    console: OperatorConsole,
    stdout: TextIO,
) -> int:
    """@brief 预览或执行 bootstrap 计划 / Preview or execute the bootstrap plan.

    @param arguments 已解析参数 / Parsed arguments.
    @param settings 已验证领域设置 / Validated domain settings.
    @param service 仅 bootstrap 用例 / Bootstrap-only use case.
    @param console 操作者进度终端 / Operator progress console.
    @param stdout 主结果流 / Primary-result stream.
    @return 成功状态 0 / Success status zero.
    """

    publish_progress(
        console,
        ProgressUpdate(
            operation=OperationName.BOOTSTRAP,
            state=ProgressState.STARTED,
            message="构建幂等最小权限计划",
            detail="仅使用已验证领域设置；尚未连接 PostgreSQL",
        ),
    )
    plan = build_bootstrap_plan(settings)
    statement_count = sum(len(stage.statements) for stage in plan.stages)
    publish_progress(
        console,
        ProgressUpdate(
            operation=OperationName.BOOTSTRAP,
            state=ProgressState.SUCCEEDED,
            message="幂等最小权限计划已构建",
            detail=f"阶段={len(plan.stages)}；计划 SQL={statement_count} 条",
        ),
    )
    if arguments.dry_run:
        _write_result(stdout, render_bootstrap_plan(plan))
        return 0
    result = service.execute(
        plan,
        access_mode=BootstrapAccessMode(arguments.access_mode),
    )
    _write_result(stdout, render_bootstrap_result(result))
    return 0


def _run_migration(
    arguments: argparse.Namespace,
    settings: DbctlSettings,
    service: MigrationService,
    stdout: TextIO,
) -> int:
    """@brief 执行一次显式 Alembic upgrade / Execute one explicit Alembic upgrade.

    @param arguments 已解析参数 / Parsed arguments.
    @param settings 已验证领域设置 / Validated domain settings.
    @param service 仅 migration 用例 / Migration-only use case.
    @param stdout 主结果流 / Primary-result stream.
    @return 成功状态 0 / Success status zero.
    """

    revision = MigrationRevision(arguments.revision)
    login = settings.connections.migrator
    service.execute(login, revision, settings)
    _write_result(stdout, render_migration_result(revision))
    return 0


def _run_prune(
    arguments: argparse.Namespace,
    settings: DbctlSettings,
    service: PruneTelemetryService,
    stdout: TextIO,
) -> int:
    """@brief 预览或执行一次有界遥测清理 / Preview or apply one bounded telemetry prune.

    @param arguments 已解析参数 / Parsed arguments.
    @param settings 已验证领域设置 / Validated domain settings.
    @param service 仅遥测清理用例 / Telemetry-pruning-only use case.
    @param stdout 主结果流 / Primary-result stream.
    @return 成功状态 0 / Success status zero.
    """

    outcome = service.execute(
        PruneRequest(
            policy=settings.retention,
            limits=PruneLimits(
                batch_size=arguments.batch_size,
                max_batches=arguments.max_batches,
                statement_timeout_ms=arguments.statement_timeout_ms,
                lock_timeout_ms=arguments.lock_timeout_ms,
            ),
            mode=PruneMode.APPLY if arguments.apply else PruneMode.DRY_RUN,
        )
    )
    _write_result(stdout, render_prune_outcome(outcome))
    return 0


def _run_shell(
    arguments: argparse.Namespace,
    settings: DbctlSettings,
    service: OpenShellService,
) -> int:
    """@brief 以选定登录身份启动 psql / Launch psql with the selected login identity.

    @param arguments 已解析参数 / Parsed arguments.
    @param settings 已验证领域设置 / Validated domain settings.
    @param service 仅 shell 用例 / Shell-only use case.
    @return 规范化的 psql 状态 / Normalized psql status.
    """

    login = settings.connections.login_for(LoginRole(arguments.role))
    return service.execute(login)


def _add_global_options(parser: argparse.ArgumentParser) -> None:
    """@brief 添加真正的根级通用选项 / Add canonical root-level global options.

    @param parser 根 parser / Root parser.
    @return 无返回值 / No return value.
    """

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
        help="数据库目标声明路径（默认：dbinit.jsonc；缺失时可读取内置资源）。",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="抑制正常进度；主结果、警告和错误仍会输出。",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="禁用 ANSI 颜色；NO_COLOR 或 TERM=dumb 也会自动禁用。",
    )


def _add_global_overrides(parser: argparse.ArgumentParser) -> None:
    """@brief 允许通用选项自然地写在子命令后 / Allow global options naturally after a subcommand.

    @param parser 子命令 parser / Subcommand parser.
    @return 无返回值 / No return value.
    """

    parser.add_argument("--config", type=Path, default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    parser.add_argument("--dbinit", type=Path, default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    parser.add_argument(
        "--quiet", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )


def _command_mode(arguments: argparse.Namespace) -> str:
    """@brief 在 I/O 前描述当前命令模式 / Describe the command mode before I/O.

    @param arguments 已解析参数 / Parsed arguments.
    @return 不含 secret 的简短模式 / Concise secret-free mode.
    """

    if arguments.command == "bootstrap":
        effect = "SQL dry-run（本地配置仍可初始化）" if arguments.dry_run else "执行数据库变更"
        return f"{effect}；access={arguments.access_mode}"
    if arguments.command == "migrate":
        return f"执行 schema upgrade；revision={arguments.revision}"
    if arguments.command == "prune-telemetry":
        return "执行有界删除（apply）" if arguments.apply else "dry-run（不连接数据库）"
    if arguments.command == "shell":
        return f"交互式 psql；role={arguments.role}"
    return str(arguments.command)


def _write_result(stdout: TextIO, value: str) -> None:
    """@brief 向 stdout 写入恰好一个结尾换行的主结果 / Write a primary result with exactly one final newline.

    @param stdout 主结果流 / Primary-result stream.
    @param value 待写出的非空结果 / Non-empty result to write.
    @return 无返回值 / No return value.
    """

    stdout.write(value.rstrip("\n") + "\n")


__all__ = ["build_parser", "main", "run"]
