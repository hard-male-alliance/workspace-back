"""@brief `dbctl` console 与模块入口 / ``dbctl`` console and module entry point."""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
