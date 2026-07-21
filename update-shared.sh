#!/usr/bin/env bash

set -Eeuo pipefail

## @brief 脚本所在的父仓库根目录 / Parent repository root containing this script.
readonly REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

case "${1:-}" in
  -h | --help)
    cat <<'EOF'
用法：./update-shared.sh

初始化所有 submodule，并将它们更新到各自配置的远端分支最新 revision。
脚本不会拉取父仓库，也不会执行 git add、git commit 或 git push；更新后的
gitlink 必须手动审阅、测试和提交。

为避免覆盖本地工作，父仓库或任何已初始化 submodule 存在未提交变更时会停止。
EOF
    exit 0
    ;;
  "")
    ;;
  *)
    printf '错误：不支持参数 %q；请使用 --help。\n' "$1" >&2
    exit 2
    ;;
esac

cd -- "$REPO_ROOT"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  printf '错误：%s 不是 Git 工作区。\n' "$REPO_ROOT" >&2
  exit 1
fi

## @brief Git 识别的父仓库规范路径 / Canonical parent-repository path reported by Git.
readonly GIT_TOPLEVEL="$(git rev-parse --show-toplevel)"

if [[ "$GIT_TOPLEVEL" != "$REPO_ROOT" ]]; then
  printf '错误：脚本必须位于父仓库根目录；当前根目录是 %s。\n' "$GIT_TOPLEVEL" >&2
  exit 1
fi

if [[ -n "$(git status --porcelain=v1 --untracked-files=normal)" ]]; then
  printf '错误：父仓库存在未提交变更；请先提交或暂存到 stash。\n' >&2
  git status --short >&2
  exit 1
fi

if ! git submodule foreach --quiet --recursive '
  if test -n "$(git status --porcelain=v1 --untracked-files=normal)"; then
    printf "错误：submodule %s 存在未提交变更。\n" "$displaypath" >&2
    git status --short >&2
    exit 1
  fi
'; then
  exit 1
fi

printf '同步 submodule URL 并初始化父仓库固定的 revision\n'
git submodule sync --recursive
git submodule update --init --recursive

printf '更新 submodule 到各自配置的远端分支最新 revision\n'
git submodule update --remote --recursive

printf '更新完成，当前 submodule 状态：\n'
git submodule status --recursive

printf '父仓库中待审阅的 gitlink 更新：\n'
git status --short
git diff --submodule=log
