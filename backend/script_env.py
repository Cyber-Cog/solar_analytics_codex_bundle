"""
Load backend/.env before importing database.py (same rules as main.py).
CLI scripts must call load_backend_env() first so DATABASE_URL is set.
"""

from __future__ import annotations

import os


def load_backend_env() -> None:
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(backend_dir, ".env")
    if not os.path.isfile(env_path):
        return
    try:
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                if key and key not in os.environ:
                    os.environ[key] = val.strip()
    except OSError:
        pass
