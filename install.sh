#!/usr/bin/env bash

set -Eeuo pipefail

## @brief 安装脚本所在的仓库根目录 / Repository root containing this installer.
readonly REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

## @brief GUI 依赖安装策略：auto、enabled 或 disabled / GUI dependency policy: auto, enabled, or disabled.
GUI_POLICY="auto"

case "${1:-}" in
  -h | --help)
    cat <<'EOF'
用法：./install.sh [--gui|--headless]

创建或复用仓库根目录的 .venv，并以 editable 模式安装项目及 dev 依赖。
默认自动检测桌面会话；非 headless 环境同时安装 gui 依赖。

  --gui       强制安装 dev 和 gui 依赖
  --headless  只安装 dev 依赖

脚本不会执行 git add、git commit 或 git push。
EOF
    exit 0
    ;;
  --gui)
    GUI_POLICY="enabled"
    ;;
  --headless)
    GUI_POLICY="disabled"
    ;;
  "")
    ;;
  *)
    printf '错误：不支持参数 %q；请使用 --help。\n' "$1" >&2
    exit 2
    ;;
esac

cd -- "$REPO_ROOT"

if ! command -v uv >/dev/null 2>&1; then
  printf '错误：找不到 uv；请先安装 uv 并确保它位于 PATH。\n' >&2
  exit 1
fi

if [[ ! -f pyproject.toml ]]; then
  printf '错误：%s 中缺少 pyproject.toml。\n' "$REPO_ROOT" >&2
  exit 1
fi

if [[ -f .gitmodules ]]; then
  printf '初始化父仓库固定的 submodule revision\n'
  git submodule update --init --recursive
fi

if [[ "$GUI_POLICY" == "auto" ]]; then
  ## @brief 当前操作系统内核名称 / Current operating-system kernel name.
  readonly KERNEL_NAME="$(uname -s)"

  if [[ -n "${DISPLAY:-}" || -n "${WAYLAND_DISPLAY:-}" || -n "${MIR_SOCKET:-}" ]]; then
    GUI_POLICY="enabled"
  elif [[ "${XDG_SESSION_TYPE:-}" =~ ^(x11|wayland|mir)$ ]]; then
    GUI_POLICY="enabled"
  elif [[ "$KERNEL_NAME" =~ ^(Darwin|MINGW|MSYS|CYGWIN) ]] \
    && [[ -z "${SSH_CONNECTION:-}" && -z "${SSH_TTY:-}" ]]; then
    GUI_POLICY="enabled"
  else
    GUI_POLICY="disabled"
  fi
fi

## @brief 项目虚拟环境目录 / Project virtual-environment directory.
readonly VENV_PATH="$REPO_ROOT/.venv"

if [[ -e "$VENV_PATH" && ! -f "$VENV_PATH/pyvenv.cfg" ]]; then
  printf '错误：%s 已存在但不是虚拟环境；不会覆盖。\n' "$VENV_PATH" >&2
  exit 1
fi

if [[ ! -d "$VENV_PATH" ]]; then
  printf '使用 Python 3.14 创建虚拟环境：%s\n' "$VENV_PATH"
  uv venv --python 3.14 "$VENV_PATH"
else
  printf '复用现有虚拟环境：%s\n' "$VENV_PATH"
fi

if [[ -x "$VENV_PATH/bin/python" ]]; then
  ## @brief POSIX 虚拟环境 Python 解释器 / Python interpreter in a POSIX virtual environment.
  readonly VENV_PYTHON="$VENV_PATH/bin/python"
  ## @brief POSIX 虚拟环境激活命令 / POSIX virtual-environment activation command.
  readonly ACTIVATE_COMMAND="source .venv/bin/activate"
elif [[ -x "$VENV_PATH/Scripts/python.exe" ]]; then
  ## @brief Windows Bash 虚拟环境 Python 解释器 / Python interpreter in a Windows Bash virtual environment.
  readonly VENV_PYTHON="$VENV_PATH/Scripts/python.exe"
  ## @brief Windows Bash 虚拟环境激活命令 / Windows Bash virtual-environment activation command.
  readonly ACTIVATE_COMMAND="source .venv/Scripts/activate"
else
  printf '错误：虚拟环境中找不到 Python 解释器：%s。\n' "$VENV_PATH" >&2
  exit 1
fi

if ! "$VENV_PYTHON" -c 'import sys; raise SystemExit(sys.version_info < (3, 14))'; then
  printf '错误：现有 .venv 的 Python 版本低于 3.14；请移走该环境后重新运行。\n' >&2
  exit 1
fi

if [[ "$GUI_POLICY" == "enabled" ]]; then
  printf '检测到非 headless 环境：安装 editable dev + gui 依赖\n'
  uv pip install --python "$VENV_PYTHON" --strict --editable '.[dev,gui]'
else
  printf '检测到 headless 环境：安装 editable dev 依赖\n'
  uv pip install --python "$VENV_PYTHON" --strict --editable '.[dev]'
fi

printf '安装完成。激活命令：%s\n' "$ACTIVATE_COMMAND"
