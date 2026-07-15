"""@brief ``python -m dbctl`` 入口 / Entry point for ``python -m dbctl``."""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
