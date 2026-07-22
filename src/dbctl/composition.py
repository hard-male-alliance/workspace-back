"""@brief dbctl 唯一组合根 / Sole composition root for dbctl."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class DbctlApplication:
    """@brief 已装配但尚未执行 I/O 的 dbctl 应用 / Composed dbctl application before use-case I/O.

    @param settings 已验证领域设置 / Validated domain settings.
    @param bootstrap 数据库 bootstrap 用例 / Database-bootstrap use case.
    @param migration Alembic migration 用例 / Alembic-migration use case.
    @param prune 遥测保留清理用例 / Telemetry-retention use case.
    @param shell 交互式 psql 用例 / Interactive-psql use case.
    """

    settings: DbctlSettings
    bootstrap: BootstrapService
    migration: MigrationService
    prune: PruneTelemetryService
    shell: OpenShellService


def compose_dbctl(
    config_path: Path | str | None = None,
    *,
    dbinit_path: Path | str | None = None,
    initialize_config: bool = False,
    environ: Mapping[str, str] | None = None,
    progress: ProgressSink | None = None,
) -> DbctlApplication:
    """@brief 从配置装配四个独立用例 / Compose four independent use cases from configuration.

    @param config_path 私密运行配置路径 / Private runtime-configuration path.
    @param dbinit_path 非秘密数据库目标状态路径 / Non-secret database target-state path.
    @param initialize_config 仅 bootstrap 可授权的凭证初始化 / Credential initialization authorized only for bootstrap.
    @param environ psql 子进程基础环境；默认当前环境 / Base psql environment; defaults to current process.
    @param progress 可选同步操作者进度端口 / Optional synchronous operator-progress port.
    @return 不启动 subprocess/网络连接的 DbctlApplication / DbctlApplication without starting subprocesses or connections.
    """

    store = DbctlConfigStore(config_path, dbinit_path, progress=progress)
    settings = store.initialize() if initialize_config else store.load()
    process_environment = os.environ if environ is None else environ
    telemetry_adapter = PsycopgTelemetryRetentionAdapter(
        settings.connections.migrator,
        owner_role=settings.blueprint.roles.owner,
        observability_schema=settings.blueprint.observability_schema,
    )
    return DbctlApplication(
        settings=settings,
        bootstrap=BootstrapService(
            LocalPsqlBootstrapRunnerFactory(environ=process_environment),
            progress=progress,
        ),
        migration=MigrationService(AlembicMigrationAdapter(), progress=progress),
        prune=PruneTelemetryService(telemetry_adapter, progress=progress),
        shell=OpenShellService(PsqlShellAdapter(process_environment), progress=progress),
    )


__all__ = ["DbctlApplication", "compose_dbctl"]
