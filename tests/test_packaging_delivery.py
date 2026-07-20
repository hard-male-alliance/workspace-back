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


def test_built_wheel_contains_and_can_read_all_dbctl_resources(tmp_path: Path) -> None:
    """@brief wheel 必须含全部 dbctl 资源并可脱离源码树读取 / The wheel ships and reads every dbctl resource outside the checkout.

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
        "dbctl/resources/example.jsonc",
        "dbctl/resources/dbinit.jsonc",
    }
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
from dbctl.config import DbctlConfigurationService
from dbctl.package_resources import alembic_script_location, read_default_text
from fastapi.testclient import TestClient

assert str(wheel) in str(dbctl.__file__)
ContractValidator.from_json(read_contract_schema_text())
assert 'database_administration' in read_default_text('dbinit.jsonc')
with alembic_script_location() as scripts:
    assert (scripts / 'env.py').is_file()
    assert (scripts / 'versions' / '20260721_0006_observability_signal_envelope.py').is_file()

settings = DbctlConfigurationService().load()
assert settings.administration.database_name == 'ai_job_workspace'
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

    from dbctl.config import DbctlConfigurationService
    from dbctl.errors import DbctlConfigurationError

    config_case = tmp_path / "config-case"
    config_case.mkdir()
    explicit_config = config_case / "runtime.jsonc"
    explicit_dbinit = config_case / "database.jsonc"
    explicit_dbinit.write_text(
        (PROJECT_ROOT / "dbinit.jsonc").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    with pytest.raises(DbctlConfigurationError, match=r"config\.jsonc 不存在"):
        DbctlConfigurationService(explicit_config, explicit_dbinit).load()
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
        DbctlConfigurationService(explicit_config, explicit_dbinit).load()
    assert not explicit_dbinit.exists()
