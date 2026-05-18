"""Export backend/demo/xyz/*.xlsx for Metadata upload parity testing."""
from __future__ import annotations

import os
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1]


def main() -> int:
    sys.path.insert(0, str(_BACKEND))
    os.chdir(_BACKEND)
    from script_env import load_backend_env

    load_backend_env()

    from demo.xyz_generator import export_demo_workbooks

    out = _BACKEND / "demo" / "xyz"
    paths = export_demo_workbooks(str(out))
    for label, path in paths.items():
        print(f"{label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
