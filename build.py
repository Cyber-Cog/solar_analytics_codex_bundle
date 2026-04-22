"""
Mirror frontend assets into public/ for Vercel static hosting.
"""

from __future__ import annotations

import shutil
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = ROOT_DIR / "frontend"
PUBLIC_DIR = ROOT_DIR / "public"


def main() -> None:
    if not FRONTEND_DIR.is_dir():
        raise SystemExit("frontend/ directory not found")

    if PUBLIC_DIR.exists():
        shutil.rmtree(PUBLIC_DIR)

    shutil.copytree(FRONTEND_DIR, PUBLIC_DIR)
    print(f"Copied {FRONTEND_DIR} -> {PUBLIC_DIR}")


if __name__ == "__main__":
    main()

