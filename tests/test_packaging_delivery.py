"""@brief wheel 资源与 console entrypoint 交付契约 / Wheel-resource and console-entrypoint delivery contracts."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from importlib import import_module
from pathlib import Path
from zipfile import ZipFile

import pytest

from conftest import PROJECT_ROOT


def test_every_declared_console_target_is_importable_and_callable() -> None:
    """@brief 所有 console target 必须解析到可调用对象 / Every console target resolves to a callable.

    @return 无返回值 / No return value.
    """

    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    scripts: dict[str, str] = project["project"]["scripts"]
    for command, target in scripts.items():
        module_name, separator, object_path = target.partition(":")
        assert separator and module_name and object_path, f"{command}: 非法 target {target!r}"
        resolved: object = import_module(module_name)
        for attribute in object_path.split("."):
            resolved = getattr(resolved, attribute)
        assert callable(resolved), f"{command}: {target} 不是 callable"


def test_built_wheel_contains_layered_dbctl_and_all_resources(tmp_path: Path) -> None:
    """@brief wheel 必须交付分层 dbctl 及全部资源 / The wheel ships layered dbctl and every resource.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """

    output_directory = tmp_path / "dist"
    build = subprocess.run(
        ("uv", "build", "--wheel", "--out-dir", str(output_directory)),
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert build.returncode == 0, build.stdout + build.stderr
    wheels = tuple(output_directory.glob("*.whl"))
    assert len(wheels) == 1
    wheel = wheels[0]

    expected_entries = {
        "backend/resources/ai-job-workspace.contract.schema.json",
        "backend/resources/api-v2.schema.jsonc",
        "dbctl/resources/example.jsonc",
        "dbctl/resources/dbinit.jsonc",
        "dbctl/resources/alembic/versions/20260721_0007_protect_alembic_version_table.py",
    }
    for layer in ("domain", "application", "infrastructure", "interfaces"):
        for source in (PROJECT_ROOT / "src" / "dbctl" / layer).rglob("*.py"):
            expected_entries.add(source.relative_to(PROJECT_ROOT / "src").as_posix())
    for source in (PROJECT_ROOT / "alembic").rglob("*"):
        if source.is_file() and "__pycache__" not in source.parts and source.suffix != ".pyc":
            relative = source.relative_to(PROJECT_ROOT).as_posix()
            expected_entries.add(f"dbctl/resources/{relative}")

    with ZipFile(wheel) as archive:
        archived_entries = frozenset(archive.namelist())
    missing = expected_entries - archived_entries
    assert not missing, "wheel 缺少 dbctl 资源：\n" + "\n".join(sorted(missing))
    unexpected_bytecode = {
        name
        for name in archived_entries
        if name.startswith("dbctl/resources/")
        and ("/__pycache__/" in name or name.endswith((".pyc", ".pyo")))
    }
    assert not unexpected_bytecode

    runtime_directory = tmp_path / "runtime"
    runtime_directory.mkdir()
    verification_program = """
from pathlib import Path
import sys

wheel = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(wheel))

import dbctl
from backend.app import create_app
from backend.config import BackendSettings
from backend.infrastructure.contracts import ContractValidator
from backend.package_resources import read_contract_schema_text
from dbctl.infrastructure.configuration import DbctlConfigStore
from dbctl.infrastructure.resources import alembic_script_location, read_default_text
from fastapi.testclient import TestClient

assert str(wheel) in str(dbctl.__file__)
ContractValidator.from_json(read_contract_schema_text())
ContractValidator.from_jsonc(read_contract_schema_text('v2'))
assert 'database_administration' in read_default_text('dbinit.jsonc')
with alembic_script_location() as scripts:
    assert (scripts / 'env.py').is_file()
    assert (scripts / 'versions' / '20260721_0006_observability_signal_envelope.py').is_file()
    assert (scripts / 'versions' / '20260721_0007_protect_alembic_version_table.py').is_file()

settings = DbctlConfigStore().initialize()
assert settings.blueprint.database.value == 'ai_job_workspace'
assert Path('config.jsonc').is_file()
backend_settings = BackendSettings.from_file(Path('config.jsonc'))
with TestClient(create_app(backend_settings)) as client:
    assert client.get('/_internal/healthz').json() == {'status': 'ok'}
assert Path('data/logs/backend.jsonl').is_file()
"""
    verification = subprocess.run(
        (sys.executable, "-c", verification_program, str(wheel)),
        cwd=runtime_directory,
        check=False,
        capture_output=True,
        text=True,
    )
    assert verification.returncode == 0, verification.stdout + verification.stderr


def test_explicit_missing_paths_do_not_fall_back_to_packaged_defaults(tmp_path: Path) -> None:
    """@brief 显式缺失路径不得被内置资源掩盖 / Bundled resources must not mask explicit missing paths.

    @param tmp_path pytest 临时目录 / pytest temporary directory.
    @return 无返回值 / No return value.
    """

    from dbctl.application.errors import DbctlConfigurationError
    from dbctl.infrastructure.configuration import DbctlConfigStore

    config_case = tmp_path / "config-case"
    config_case.mkdir()
    explicit_config = config_case / "runtime.jsonc"
    explicit_dbinit = config_case / "database.jsonc"
    explicit_dbinit.write_text(
        (PROJECT_ROOT / "dbinit.jsonc").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    with pytest.raises(DbctlConfigurationError, match=r"配置文件不存在"):
        DbctlConfigStore(explicit_config, explicit_dbinit).load()
    assert not explicit_config.exists()

    dbinit_case = tmp_path / "dbinit-case"
    dbinit_case.mkdir()
    explicit_config = dbinit_case / "runtime.jsonc"
    explicit_config.write_text(
        (PROJECT_ROOT / "example.jsonc").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    explicit_dbinit = dbinit_case / "database.jsonc"
    with pytest.raises(DbctlConfigurationError, match="dbinit文件不存在"):
        DbctlConfigStore(explicit_config, explicit_dbinit).load()
    assert not explicit_dbinit.exists()
