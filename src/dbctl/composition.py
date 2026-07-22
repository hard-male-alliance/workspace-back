"""@brief 按命令装配单一 dbctl 能力 / Compose one dbctl command capability at a time."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from dbctl.application.migrate import MigrationService
from dbctl.application.open_shell import OpenShellService
from dbctl.application.progress import ProgressSink
from dbctl.application.provision import BootstrapService
from dbctl.application.prune_telemetry import PruneTelemetryService
from dbctl.domain.database import DbctlSettings
from dbctl.infrastructure.alembic import AlembicMigrationAdapter
from dbctl.infrastructure.configuration import DbctlConfigStore
from dbctl.infrastructure.postgres.psql import LocalPsqlBootstrapRunnerFactory
from dbctl.infrastructure.postgres.shell import PsqlShellAdapter
from dbctl.infrastructure.postgres.telemetry import PsycopgTelemetryRetentionAdapter


def compose_bootstrap(
    config_path: Path | str | None = None,
    *,
    dbinit_path: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
    progress: ProgressSink | None = None,
) -> tuple[DbctlSettings, BootstrapService]:
    """@brief 初始化配置并只装配 bootstrap 用例 / Initialize config and compose only bootstrap.

    @param config_path 私密运行配置路径 / Private runtime-configuration path.
    @param dbinit_path 非秘密数据库目标状态路径 / Non-secret database target-state path.
    @param environ psql 子进程基础环境；默认当前环境 / Base psql environment; defaults to current process.
    @param progress 可选同步操作者进度端口 / Optional synchronous operator-progress port.
    @return 已验证设置与 bootstrap 用例 / Validated settings and the bootstrap use case.
    @note 包括 dry-run 在内，只有本函数调用 ``initialize`` 并获准创建或补全凭据。
    / Only this function calls ``initialize`` and may create or complete credentials, including for dry-run.
    """

    settings = DbctlConfigStore(config_path, dbinit_path, progress=progress).initialize()
    process_environment = os.environ if environ is None else environ
    service = BootstrapService(
        LocalPsqlBootstrapRunnerFactory(environ=process_environment),
        progress=progress,
    )
    return settings, service


def compose_migration(
    config_path: Path | str | None = None,
    *,
    dbinit_path: Path | str | None = None,
    progress: ProgressSink | None = None,
) -> tuple[DbctlSettings, MigrationService]:
    """@brief 只读配置并只装配 migration 用例 / Load config read-only and compose only migration.

    @param config_path 私密运行配置路径 / Private runtime-configuration path.
    @param dbinit_path 非秘密数据库目标状态路径 / Non-secret database target-state path.
    @param progress 可选同步操作者进度端口 / Optional synchronous operator-progress port.
    @return 已验证设置与 migration 用例 / Validated settings and the migration use case.
    """

    settings = _load_settings(config_path, dbinit_path=dbinit_path, progress=progress)
    return settings, MigrationService(AlembicMigrationAdapter(), progress=progress)


def compose_prune_telemetry(
    config_path: Path | str | None = None,
    *,
    dbinit_path: Path | str | None = None,
    progress: ProgressSink | None = None,
) -> tuple[DbctlSettings, PruneTelemetryService]:
    """@brief 只读配置并只装配遥测清理用例 / Load config read-only and compose only telemetry pruning.

    @param config_path 私密运行配置路径 / Private runtime-configuration path.
    @param dbinit_path 非秘密数据库目标状态路径 / Non-secret database target-state path.
    @param progress 可选同步操作者进度端口 / Optional synchronous operator-progress port.
    @return 已验证设置与遥测清理用例 / Validated settings and the telemetry-pruning use case.
    """

    settings = _load_settings(config_path, dbinit_path=dbinit_path, progress=progress)
    adapter = PsycopgTelemetryRetentionAdapter(
        settings.connections.migrator,
        owner_role=settings.blueprint.roles.owner,
        observability_schema=settings.blueprint.observability_schema,
    )
    return settings, PruneTelemetryService(adapter, progress=progress)


def compose_shell(
    config_path: Path | str | None = None,
    *,
    dbinit_path: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
    progress: ProgressSink | None = None,
) -> tuple[DbctlSettings, OpenShellService]:
    """@brief 只读配置并只装配交互 shell 用例 / Load config read-only and compose only the shell.

    @param config_path 私密运行配置路径 / Private runtime-configuration path.
    @param dbinit_path 非秘密数据库目标状态路径 / Non-secret database target-state path.
    @param environ psql 子进程基础环境；默认当前环境 / Base psql environment; defaults to current process.
    @param progress 可选同步操作者进度端口 / Optional synchronous operator-progress port.
    @return 已验证设置与 shell 用例 / Validated settings and the shell use case.
    """

    settings = _load_settings(config_path, dbinit_path=dbinit_path, progress=progress)
    process_environment = os.environ if environ is None else environ
    return settings, OpenShellService(
        PsqlShellAdapter(process_environment),
        progress=progress,
    )


def _load_settings(
    config_path: Path | str | None,
    *,
    dbinit_path: Path | str | None,
    progress: ProgressSink | None,
) -> DbctlSettings:
    """@brief 为非 bootstrap 命令只读加载设置 / Load settings read-only for non-bootstrap commands.

    @param config_path 私密运行配置路径 / Private runtime-configuration path.
    @param dbinit_path 非秘密数据库目标状态路径 / Non-secret database target-state path.
    @param progress 可选同步操作者进度端口 / Optional synchronous operator-progress port.
    @return 完整验证且未写入磁盘的设置 / Fully validated settings without disk writes.
    """

    return DbctlConfigStore(config_path, dbinit_path, progress=progress).load()


__all__ = [
    "compose_bootstrap",
    "compose_migration",
    "compose_prune_telemetry",
    "compose_shell",
]
