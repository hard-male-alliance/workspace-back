"""@brief 后端 CLI 入口 / Backend CLI entry point."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

import uvicorn

from backend.app import config_path, create_app
from backend.config import BackendSettings


def build_parser() -> argparse.ArgumentParser:
    """@brief 构造 backend 进程的无副作用参数解析器 / Build the side-effect-free backend process parser.

    @return 仅描述配置路径的 argparse 解析器。

    @note backend 不提供 migration、bootstrap 或 dashboard 子命令；这些职责分别属于
    ``workspace-dbctl`` 与 ``workspace-dashboard``，从命令行边界上避免分布式单体式耦合。
    """
    parser = argparse.ArgumentParser(
        prog="workspace-backend",
        description="启动仅供可信反向代理访问的 AI Job Workspace FastAPI 后端。",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=config_path(),
        help="根 JSONC 配置路径（默认：AIWS_CONFIG 或 config.jsonc）。",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """@brief 启动仅绑定内部地址的 Uvicorn / Start Uvicorn bound only to an internal address.

    @param argv 可选命令行参数；``None`` 时读取进程参数 / Optional command-line arguments; reads process arguments when ``None``.
    @return Uvicorn 正常停止时为 ``0``。
    @note 生产环境应在 Nginx 后运行；本进程不假设客户端可以直连内部端口。
    """
    arguments = build_parser().parse_args(argv)
    settings = BackendSettings.from_file(arguments.config)
    uvicorn.run(
        create_app(settings),
        host=settings.network.bind_host,
        port=settings.network.bind_port,
        proxy_headers=False,
        log_config=None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
