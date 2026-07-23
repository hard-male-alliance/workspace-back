"""@brief Linux 子进程的无特权纵深隔离 / Unprivileged defense-in-depth confinement for Linux child processes.

生产模式以 Landlock 文件系统 allowlist 和 libseccomp syscall denylist 为必需边界。
Bubblewrap 仅在当前运行环境真实 probe 成功时作为额外 mount-namespace 层；它不是
生产可用性的前提，也不会要求容器增加 capability 或关闭默认 seccomp。
"""

from __future__ import annotations

import ctypes
import ctypes.util
import errno
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any

_PR_SET_NO_NEW_PRIVS = 38
"""@brief Linux ``PR_SET_NO_NEW_PRIVS`` 操作码 / Linux ``PR_SET_NO_NEW_PRIVS`` operation."""

_LANDLOCK_CREATE_RULESET_VERSION = 1
"""@brief 查询 Landlock ABI 的 flag / Flag used to query the Landlock ABI."""

_LANDLOCK_RULE_PATH_BENEATH = 1
"""@brief Landlock path-beneath rule 类型 / Landlock path-beneath rule type."""

_LANDLOCK_ACCESS_FS_EXECUTE = 1 << 0
"""@brief 文件执行权限位 / File-execution access bit."""

_LANDLOCK_ACCESS_FS_WRITE_FILE = 1 << 1
"""@brief 文件写权限位 / File-write access bit."""

_LANDLOCK_ACCESS_FS_READ_FILE = 1 << 2
"""@brief 文件读权限位 / File-read access bit."""

_LANDLOCK_ACCESS_FS_READ_DIR = 1 << 3
"""@brief 目录读取权限位 / Directory-read access bit."""

_LANDLOCK_ACCESS_FS_REMOVE_DIR = 1 << 4
"""@brief 删除目录权限位 / Directory-removal access bit."""

_LANDLOCK_ACCESS_FS_REMOVE_FILE = 1 << 5
"""@brief 删除文件权限位 / File-removal access bit."""

_LANDLOCK_ACCESS_FS_MAKE_CHAR = 1 << 6
"""@brief 创建设备节点权限位 / Character-device creation access bit."""

_LANDLOCK_ACCESS_FS_MAKE_DIR = 1 << 7
"""@brief 创建目录权限位 / Directory-creation access bit."""

_LANDLOCK_ACCESS_FS_MAKE_REG = 1 << 8
"""@brief 创建普通文件权限位 / Regular-file creation access bit."""

_LANDLOCK_ACCESS_FS_MAKE_SOCK = 1 << 9
"""@brief 创建 Unix socket 节点权限位 / Unix-socket-node creation access bit."""

_LANDLOCK_ACCESS_FS_MAKE_FIFO = 1 << 10
"""@brief 创建 FIFO 权限位 / FIFO-creation access bit."""

_LANDLOCK_ACCESS_FS_MAKE_BLOCK = 1 << 11
"""@brief 创建块设备权限位 / Block-device creation access bit."""

_LANDLOCK_ACCESS_FS_MAKE_SYM = 1 << 12
"""@brief 创建符号链接权限位 / Symbolic-link creation access bit."""

_LANDLOCK_ACCESS_FS_REFER = 1 << 13
"""@brief 跨目录 reparent 权限位 / Cross-directory reparent access bit."""

_LANDLOCK_ACCESS_FS_TRUNCATE = 1 << 14
"""@brief 文件 truncate 权限位 / File-truncation access bit."""

_MINIMUM_LANDLOCK_ABI = 3
"""@brief 可约束 truncate 的最低 Landlock ABI / Minimum Landlock ABI that controls truncate."""

_SCMP_ACT_ALLOW = 0x7FFF0000
"""@brief libseccomp allow action / libseccomp allow action."""

_SCMP_ACT_ERRNO = 0x00050000
"""@brief libseccomp errno action base / libseccomp errno-action base."""

_REQUIRED_BLOCKED_SYSCALLS = (
    "socket",
    "ptrace",
    "process_vm_readv",
    "process_vm_writev",
    "kill",
    "tgkill",
)
"""@brief 强隔离必须能解析并阻断的 syscall / Syscalls required for strong confinement."""

_OPTIONAL_BLOCKED_SYSCALLS = (
    "socketcall",
    "socketpair",
    "connect",
    "bind",
    "listen",
    "accept",
    "accept4",
    "sendto",
    "recvfrom",
    "sendmsg",
    "recvmsg",
    "shutdown",
    "getsockname",
    "getpeername",
    "setsockopt",
    "getsockopt",
    "pidfd_getfd",
    "pidfd_open",
    "pidfd_send_signal",
    "tkill",
    "rt_sigqueueinfo",
    "rt_tgsigqueueinfo",
    "kcmp",
    "mount",
    "umount2",
    "pivot_root",
    "move_mount",
    "open_tree",
    "fsopen",
    "fsmount",
    "fspick",
    "mount_setattr",
    "unshare",
    "setns",
    "bpf",
    "perf_event_open",
    "userfaultfd",
    "io_uring_setup",
    "io_uring_enter",
    "io_uring_register",
    "add_key",
    "request_key",
    "keyctl",
)
"""@brief 存在时一并阻断的逃逸与外部交互 syscall / Escape and external-interaction syscalls blocked when present."""


class ProcessConfinementUnavailable(RuntimeError):
    """@brief 当前 runtime 无法提供生产级子进程隔离 / Production-grade child confinement is unavailable."""


class ProcessConfinementMode(StrEnum):
    """@brief 子进程隔离强度 / Child-process confinement strength."""

    STRONG = "strong"
    """@brief Landlock + libseccomp 必需 / Landlock plus libseccomp is mandatory."""

    DEVELOPMENT = "development"
    """@brief 仅用于 development/test 的 rlimit fallback / Rlimit fallback for development/test only."""


@dataclass(frozen=True, slots=True)
class ProcessConfinementPlan:
    """@brief 已完成 capability probe 的进程隔离计划 / Capability-probed process-confinement plan.

    @param mode 必需隔离强度 / Required confinement strength.
    @param bubblewrap 可用的额外 Bubblewrap 层；不可用时为空 / Optional proven Bubblewrap layer.
    """

    mode: ProcessConfinementMode
    """@brief 强隔离或明确的研发 fallback / Strong confinement or explicit development fallback."""

    bubblewrap: str | None
    """@brief 已真实 probe 的 Bubblewrap 绝对路径 / Real-probed absolute Bubblewrap path."""


class _LandlockRulesetAttr(ctypes.Structure):
    """@brief ``landlock_ruleset_attr`` 的 ctypes 表达 / ctypes representation of ``landlock_ruleset_attr``."""

    _fields_ = [("handled_access_fs", ctypes.c_uint64)]


class _LandlockPathBeneathAttr(ctypes.Structure):
    """@brief ``landlock_path_beneath_attr`` 的 ctypes 表达 / ctypes representation of ``landlock_path_beneath_attr``."""

    _fields_ = [
        ("allowed_access", ctypes.c_uint64),
        ("parent_fd", ctypes.c_int32),
    ]


def confinement_plan_for(environment: str) -> ProcessConfinementPlan:
    """@brief 为部署环境选择一次性 probe 后的隔离计划 / Select a capability-probed confinement plan for an environment.

    @param environment 已验证的部署环境 / Validated deployment environment.
    @return 不提升宿主或容器权限的隔离计划 / Plan that does not elevate host or container privileges.
    @raise ProcessConfinementUnavailable staging/production 缺少强边界 / Strong boundary is unavailable in staging/production.
    @raise ValueError 环境名不受支持 / Environment name is unsupported.
    """

    if environment in {"development", "test"}:
        return ProcessConfinementPlan(ProcessConfinementMode.DEVELOPMENT, None)
    if environment not in {"staging", "production"}:
        raise ValueError("process confinement environment is unsupported")
    if not _strong_confinement_probe():
        raise ProcessConfinementUnavailable(
            "production child processing requires usable Landlock and libseccomp"
        )
    bubblewrap = shutil.which("bwrap")
    proven_bubblewrap = (
        str(Path(bubblewrap).resolve())
        if bubblewrap is not None and _bubblewrap_probe(str(Path(bubblewrap).resolve()))
        else None
    )
    return ProcessConfinementPlan(ProcessConfinementMode.STRONG, proven_bubblewrap)


def clear_confinement_probe_cache() -> None:
    """@brief 清除 capability probe 缓存供测试使用 / Clear capability-probe caches for tests.

    @return 无返回值 / No return value.
    """

    _strong_confinement_probe.cache_clear()
    _bubblewrap_probe.cache_clear()


def python_runtime_read_paths() -> tuple[Path, ...]:
    """@brief 返回 Python child 导入所需的最小只读 roots / Return minimal read-only roots needed by a Python child.

    @return 去重且存在的绝对目录 / Deduplicated existing absolute directories.
    """

    candidates = (
        Path(sys.prefix),
        Path(sys.base_prefix),
        Path(__file__).resolve().parents[2],
        Path("/usr"),
        Path("/lib"),
        Path("/lib64"),
        Path("/etc/ld.so.cache"),
    )
    return _existing_paths(candidates)


def apply_strong_confinement(
    *,
    read_only_paths: Sequence[Path],
    read_write_paths: Sequence[Path] = (),
) -> None:
    """@brief 不可逆地应用 Landlock deny-by-default 与 libseccomp / Irreversibly apply deny-by-default Landlock and libseccomp.

    @param read_only_paths child 可读/执行但不可写的路径 / Paths the child may read or execute but not write.
    @param read_write_paths child 可完整操作的私有路径 / Private paths the child may fully operate on.
    @return 无返回值 / No return value.
    @raise ProcessConfinementUnavailable 任一 kernel/library 边界无法完整安装 / Any kernel or library boundary cannot be fully installed.
    @note 调用方必须在接触不可信 bytes 前调用；限制自动继承到 ``execve`` 后的程序及后代。
        / Call before touching untrusted bytes; restrictions survive ``execve`` and propagate to descendants.
    """

    if sys.platform != "linux" or os.name != "posix":
        raise ProcessConfinementUnavailable("strong process confinement requires Linux")
    libc = _load_libc()
    seccomp = _load_libseccomp()
    syscall_numbers = {
        name: _resolve_syscall(seccomp, name, required=True)
        for name in (
            "landlock_create_ruleset",
            "landlock_add_rule",
            "landlock_restrict_self",
        )
    }
    abi = _landlock_abi(libc, syscall_numbers["landlock_create_ruleset"])
    if abi < _MINIMUM_LANDLOCK_ABI:
        raise ProcessConfinementUnavailable(
            f"Landlock ABI {_MINIMUM_LANDLOCK_ABI} or newer is required"
        )
    handled_access = _landlock_handled_access(abi)
    ruleset_fd = _create_landlock_ruleset(
        libc,
        syscall_numbers["landlock_create_ruleset"],
        handled_access,
    )
    try:
        for path in _existing_paths(read_only_paths):
            _add_landlock_path_rule(
                libc,
                syscall_numbers["landlock_add_rule"],
                ruleset_fd,
                path,
                _read_access_for(path),
            )
        for path in _existing_paths(read_write_paths):
            _add_landlock_path_rule(
                libc,
                syscall_numbers["landlock_add_rule"],
                ruleset_fd,
                path,
                handled_access,
            )
        _set_no_new_privileges(libc)
        result = libc.syscall(
            syscall_numbers["landlock_restrict_self"],
            ruleset_fd,
            0,
        )
        if result != 0:
            _raise_errno("landlock_restrict_self")
    finally:
        os.close(ruleset_fd)
    _install_seccomp_filter(seccomp)


@lru_cache(maxsize=1)
def _strong_confinement_probe() -> bool:
    """@brief 在隔离 Python child 中真实验证强边界 / Verify the strong boundary in an isolated Python child.

    @return Landlock 文件与 libseccomp syscall 拒绝均生效时为真 / True only when Landlock and seccomp denials both work.
    """

    if sys.platform != "linux" or os.name != "posix":
        return False
    with tempfile.TemporaryDirectory(prefix="aiws-confinement-probe-") as directory:
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-B",
                    "-m",
                    "backend.infrastructure.process_confinement",
                    "--probe",
                    directory,
                ],
                cwd=directory,
                env={
                    "HOME": directory,
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                    "PATH": os.defpath,
                    "PYTHONHASHSEED": "0",
                    "TMPDIR": directory,
                },
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=5,
                start_new_session=True,
            )
        except (OSError, subprocess.SubprocessError):
            return False
    return completed.returncode == 0


@lru_cache(maxsize=8)
def _bubblewrap_probe(binary: str) -> bool:
    """@brief 真实执行 Bubblewrap 的 namespace/mount capability probe / Execute a real Bubblewrap namespace/mount capability probe.

    @param binary Bubblewrap 绝对路径 / Absolute Bubblewrap path.
    @return 当前 runtime 允许所需操作时为真 / True when the current runtime permits required operations.
    """

    try:
        completed = subprocess.run(
            [
                binary,
                "--die-with-parent",
                "--new-session",
                "--unshare-user",
                "--unshare-ipc",
                "--unshare-pid",
                "--unshare-net",
                "--unshare-uts",
                "--ro-bind",
                "/usr",
                "/usr",
                "--ro-bind-try",
                "/lib",
                "/lib",
                "--ro-bind-try",
                "/lib64",
                "/lib64",
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "/usr/bin/true",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=5,
            start_new_session=True,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0


def _existing_paths(paths: Sequence[Path]) -> tuple[Path, ...]:
    """@brief 规范化、去重并过滤不存在路径 / Resolve, deduplicate, and filter missing paths.

    @param paths 候选路径 / Candidate paths.
    @return 稳定排序的存在路径 / Stably sorted existing paths.
    """

    existing: set[Path] = set()
    for path in paths:
        try:
            resolved = path.resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        existing.add(resolved)
    return tuple(sorted(existing, key=str))


def _load_libc() -> Any:
    """@brief 加载当前进程 libc / Load the current process libc.

    @return 启用 errno 的 ctypes library / ctypes library with errno enabled.
    @raise ProcessConfinementUnavailable libc 无法加载 / libc cannot be loaded.
    """

    try:
        libc = ctypes.CDLL(None, use_errno=True)
    except OSError as error:
        raise ProcessConfinementUnavailable("libc is unavailable") from error
    libc.syscall.restype = ctypes.c_long
    libc.prctl.restype = ctypes.c_int
    return libc


def _load_libseccomp() -> Any:
    """@brief 加载 libseccomp ABI / Load the libseccomp ABI.

    @return 已声明核心函数签名的 ctypes library / ctypes library with core signatures declared.
    @raise ProcessConfinementUnavailable libseccomp 缺失 / libseccomp is unavailable.
    """

    library_name = ctypes.util.find_library("seccomp") or "libseccomp.so.2"
    try:
        library = ctypes.CDLL(library_name, use_errno=True)
    except OSError as error:
        raise ProcessConfinementUnavailable("libseccomp is unavailable") from error
    library.seccomp_init.argtypes = [ctypes.c_uint32]
    library.seccomp_init.restype = ctypes.c_void_p
    library.seccomp_release.argtypes = [ctypes.c_void_p]
    library.seccomp_release.restype = None
    library.seccomp_rule_add.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_int,
        ctypes.c_uint,
    ]
    library.seccomp_rule_add.restype = ctypes.c_int
    library.seccomp_load.argtypes = [ctypes.c_void_p]
    library.seccomp_load.restype = ctypes.c_int
    library.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    library.seccomp_syscall_resolve_name.restype = ctypes.c_int
    return library


def _resolve_syscall(library: Any, name: str, *, required: bool) -> int:
    """@brief 通过 libseccomp 解析本架构 syscall / Resolve an architecture syscall through libseccomp.

    @param library libseccomp handle / libseccomp handle.
    @param name syscall 名 / Syscall name.
    @param required 缺失时是否 fail closed / Whether absence must fail closed.
    @return syscall number；optional 缺失时为负一 / Syscall number, or minus one when optional and absent.
    @raise ProcessConfinementUnavailable 必需 syscall 无法解析 / A required syscall cannot be resolved.
    """

    number = int(library.seccomp_syscall_resolve_name(name.encode("ascii")))
    if number < 0 and required:
        raise ProcessConfinementUnavailable(f"required syscall {name} cannot be resolved")
    return number


def _landlock_abi(libc: Any, syscall_number: int) -> int:
    """@brief 查询 kernel Landlock ABI / Query the kernel Landlock ABI.

    @param libc libc handle / libc handle.
    @param syscall_number ``landlock_create_ruleset`` number / ``landlock_create_ruleset`` number.
    @return 正 ABI 版本 / Positive ABI version.
    @raise ProcessConfinementUnavailable kernel/容器不允许 Landlock / Kernel or container disallows Landlock.
    """

    ctypes.set_errno(0)
    result = int(
        libc.syscall(
            syscall_number,
            ctypes.c_void_p(),
            0,
            _LANDLOCK_CREATE_RULESET_VERSION,
        )
    )
    if result < 0:
        _raise_errno("landlock_create_ruleset(version)")
    return result


def _landlock_handled_access(abi: int) -> int:
    """@brief 按 ABI 返回兼容的 filesystem access mask / Return an ABI-compatible filesystem access mask.

    @param abi kernel Landlock ABI / Kernel Landlock ABI.
    @return deny-by-default 处理位 / Access bits handled deny-by-default.
    """

    access = (
        _LANDLOCK_ACCESS_FS_EXECUTE
        | _LANDLOCK_ACCESS_FS_WRITE_FILE
        | _LANDLOCK_ACCESS_FS_READ_FILE
        | _LANDLOCK_ACCESS_FS_READ_DIR
        | _LANDLOCK_ACCESS_FS_REMOVE_DIR
        | _LANDLOCK_ACCESS_FS_REMOVE_FILE
        | _LANDLOCK_ACCESS_FS_MAKE_CHAR
        | _LANDLOCK_ACCESS_FS_MAKE_DIR
        | _LANDLOCK_ACCESS_FS_MAKE_REG
        | _LANDLOCK_ACCESS_FS_MAKE_SOCK
        | _LANDLOCK_ACCESS_FS_MAKE_FIFO
        | _LANDLOCK_ACCESS_FS_MAKE_BLOCK
        | _LANDLOCK_ACCESS_FS_MAKE_SYM
    )
    if abi >= 2:
        access |= _LANDLOCK_ACCESS_FS_REFER
    if abi >= 3:
        access |= _LANDLOCK_ACCESS_FS_TRUNCATE
    return access


def _create_landlock_ruleset(libc: Any, syscall_number: int, handled_access: int) -> int:
    """@brief 创建 filesystem ruleset / Create a filesystem ruleset.

    @param libc libc handle / libc handle.
    @param syscall_number ``landlock_create_ruleset`` number / Syscall number.
    @param handled_access 被 deny-by-default 的 access / Access handled deny-by-default.
    @return ruleset descriptor / Ruleset descriptor.
    @raise ProcessConfinementUnavailable ruleset 创建失败 / Ruleset creation fails.
    """

    attributes = _LandlockRulesetAttr(handled_access_fs=handled_access)
    ctypes.set_errno(0)
    descriptor = int(
        libc.syscall(
            syscall_number,
            ctypes.byref(attributes),
            ctypes.sizeof(attributes),
            0,
        )
    )
    if descriptor < 0:
        _raise_errno("landlock_create_ruleset")
    return descriptor


def _read_access_for(path: Path) -> int:
    """@brief 为文件或目录选择只读 Landlock 权限 / Select read-only Landlock access for a file or directory.

    @param path 已存在的规范路径 / Existing canonical path.
    @return 与 inode 类型兼容的 access mask / Access mask compatible with the inode type.
    """

    if path.is_dir():
        return (
            _LANDLOCK_ACCESS_FS_EXECUTE
            | _LANDLOCK_ACCESS_FS_READ_FILE
            | _LANDLOCK_ACCESS_FS_READ_DIR
        )
    return _LANDLOCK_ACCESS_FS_EXECUTE | _LANDLOCK_ACCESS_FS_READ_FILE


def _add_landlock_path_rule(
    libc: Any,
    syscall_number: int,
    ruleset_fd: int,
    path: Path,
    allowed_access: int,
) -> None:
    """@brief 向 ruleset 添加一个 path-beneath allow rule / Add a path-beneath allow rule to a ruleset.

    @param libc libc handle / libc handle.
    @param syscall_number ``landlock_add_rule`` number / Syscall number.
    @param ruleset_fd ruleset descriptor / Ruleset descriptor.
    @param path 规则根路径 / Rule root path.
    @param allowed_access 允许权限 / Allowed access bits.
    @return 无返回值 / No return value.
    @raise ProcessConfinementUnavailable rule 无法安装 / Rule cannot be installed.
    """

    flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_CLOEXEC
    try:
        path_fd = os.open(path, flags)
    except OSError as error:
        raise ProcessConfinementUnavailable(f"cannot open confinement path {path}") from error
    try:
        attributes = _LandlockPathBeneathAttr(
            allowed_access=allowed_access,
            parent_fd=path_fd,
        )
        ctypes.set_errno(0)
        result = libc.syscall(
            syscall_number,
            ruleset_fd,
            _LANDLOCK_RULE_PATH_BENEATH,
            ctypes.byref(attributes),
            0,
        )
        if result != 0:
            _raise_errno(f"landlock_add_rule({path})")
    finally:
        os.close(path_fd)


def _set_no_new_privileges(libc: Any) -> None:
    """@brief 设置不可逆 ``no_new_privs`` / Set irreversible ``no_new_privs``.

    @param libc libc handle / libc handle.
    @return 无返回值 / No return value.
    @raise ProcessConfinementUnavailable ``prctl`` 失败 / ``prctl`` fails.
    """

    ctypes.set_errno(0)
    if libc.prctl(_PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
        _raise_errno("prctl(PR_SET_NO_NEW_PRIVS)")


def _install_seccomp_filter(library: Any) -> None:
    """@brief 安装 syscall denylist 并保持默认 allow / Install a syscall denylist with default allow.

    @param library libseccomp handle / libseccomp handle.
    @return 无返回值 / No return value.
    @raise ProcessConfinementUnavailable filter 不能完整加载 / Filter cannot be fully loaded.
    """

    context = library.seccomp_init(_SCMP_ACT_ALLOW)
    if not context:
        raise ProcessConfinementUnavailable("seccomp_init failed")
    try:
        action = _SCMP_ACT_ERRNO | errno.EPERM
        for name in _REQUIRED_BLOCKED_SYSCALLS:
            syscall_number = _resolve_syscall(library, name, required=True)
            result = int(library.seccomp_rule_add(context, action, syscall_number, 0))
            if result != 0:
                raise ProcessConfinementUnavailable(
                    f"seccomp cannot block required syscall {name}: errno {-result}"
                )
        for name in _OPTIONAL_BLOCKED_SYSCALLS:
            syscall_number = _resolve_syscall(library, name, required=False)
            if syscall_number < 0:
                continue
            result = int(library.seccomp_rule_add(context, action, syscall_number, 0))
            if result != 0:
                raise ProcessConfinementUnavailable(
                    f"seccomp cannot block syscall {name}: errno {-result}"
                )
        result = int(library.seccomp_load(context))
        if result != 0:
            raise ProcessConfinementUnavailable(
                f"seccomp_load failed: errno {-result}"
            )
    finally:
        library.seccomp_release(context)


def _raise_errno(operation: str) -> None:
    """@brief 将当前 ctypes errno 转为稳定 capability 错误 / Convert current ctypes errno into a stable capability error.

    @param operation 失败操作名 / Failed operation name.
    @return 不返回 / Does not return.
    @raise ProcessConfinementUnavailable 总是抛出 / Always raised.
    """

    error_number = ctypes.get_errno()
    detail = os.strerror(error_number) if error_number else "unknown error"
    raise ProcessConfinementUnavailable(f"{operation} failed: {detail}")


def _probe_denied(call: Any) -> bool:
    """@brief 判断调用是否被权限边界拒绝 / Check whether a call is denied by the permission boundary.

    @param call 无参数 probe callable / Zero-argument probe callable.
    @return 仅在 EACCES/EPERM 时为真 / True only for EACCES or EPERM.
    """

    try:
        resource = call()
    except OSError as error:
        return error.errno in {errno.EACCES, errno.EPERM}
    if hasattr(resource, "close"):
        resource.close()
    return False


def _probe_main(denied_directory: Path) -> int:
    """@brief 在 child 内验证 allow/deny 行为 / Verify allow and deny behavior inside a child.

    @param denied_directory 不在 allowlist 的父进程临时目录 / Parent temporary directory absent from the allowlist.
    @return 零表示文件与 syscall 边界均真实生效 / Zero only when filesystem and syscall boundaries actually work.
    """

    allowed_file = Path(os.__file__).resolve()
    libc = _load_libc()
    seccomp = _load_libseccomp()
    sensitive_syscalls = {
        name: _resolve_syscall(seccomp, name, required=True)
        for name in ("ptrace", "process_vm_readv", "process_vm_writev", "kill")
    }
    try:
        apply_strong_confinement(read_only_paths=python_runtime_read_paths())
        allowed_file.read_bytes()
    except (OSError, ProcessConfinementUnavailable):
        return 1
    filesystem_read_denied = _probe_denied(lambda: Path("/etc/passwd").open("rb"))
    filesystem_write_denied = _probe_denied(
        lambda: (denied_directory / "must-not-exist").open("xb")
    )
    network_denied = _probe_denied(lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    sensitive_syscalls_denied = all(
        _raw_syscall_denied(
            libc,
            syscall_number,
            os.getppid() if name == "kill" else 0,
        )
        for name, syscall_number in sensitive_syscalls.items()
    )
    return (
        0
        if filesystem_read_denied
        and filesystem_write_denied
        and network_denied
        and sensitive_syscalls_denied
        else 1
    )


def _raw_syscall_denied(libc: Any, syscall_number: int, first_argument: int = 0) -> bool:
    """@brief 验证敏感 syscall 由 seccomp 返回 EPERM / Verify seccomp returns EPERM for a sensitive syscall.

    @param libc libc handle / libc handle.
    @param syscall_number 待调用 syscall number / Syscall number to invoke.
    @param first_argument 第一个 syscall 参数 / First syscall argument.
    @return 调用被 EPERM 拒绝时为真 / True when the call is denied with EPERM.
    """

    ctypes.set_errno(0)
    result = int(libc.syscall(syscall_number, first_argument, 0, 0, 0, 0, 0))
    return result == -1 and ctypes.get_errno() == errno.EPERM


def main(arguments: Sequence[str] | None = None) -> int:
    """@brief 运行隔离 capability probe / Run the confinement capability probe.

    @param arguments 测试可注入 argv / Test-injectable argv.
    @return probe 退出码 / Probe exit code.
    """

    argv = list(sys.argv[1:] if arguments is None else arguments)
    if len(argv) != 2 or argv[0] != "--probe":
        return 64
    return _probe_main(Path(argv[1]))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ProcessConfinementMode",
    "ProcessConfinementPlan",
    "ProcessConfinementUnavailable",
    "apply_strong_confinement",
    "clear_confinement_probe_cache",
    "confinement_plan_for",
    "python_runtime_read_paths",
]
"""@brief 公开的强类型 confinement API / Public typed confinement API."""
