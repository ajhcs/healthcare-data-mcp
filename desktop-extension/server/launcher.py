"""Launcher used inside the Claude Desktop MCPB package."""

from __future__ import annotations

import sys
from pathlib import Path


def _add_bundle_paths() -> None:
    bundle_root = Path(__file__).resolve().parents[1]
    vendored_lib = bundle_root / "server" / "lib"

    if vendored_lib.exists():
        sys.path.insert(0, str(vendored_lib))

    sys.path.insert(0, str(bundle_root))


def main() -> None:
    _add_bundle_paths()

    from servers._launcher import main as launcher_main

    launcher_main()


if __name__ == "__main__":
    main()
