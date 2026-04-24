"""UAT: verify Analytics Lab data path (equipment + signals) for one plant. No FastAPI."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

_BACKEND = Path(__file__).resolve().parents[1]


def load_env() -> None:
    p = _BACKEND / ".env"
    if not p.is_file():
        print("MISSING backend/.env")
        sys.exit(1)
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ[k.strip()] = v.strip()


def host_of(url: str) -> str:
    u = urlparse(url.replace("postgresql+psycopg2", "postgresql", 1) if "postgresql+" in url else url)
    return (u.hostname or "") + ":" + str(u.port or 5432) + "/" + (u.path or "").lstrip("/").split("?", 1)[0]


def main() -> int:
    load_env()
    os.environ.setdefault("DB_STATEMENT_TIMEOUT_MS", "600000")
    sys.path.insert(0, str(_BACKEND))
    os.chdir(_BACKEND)

    import importlib

    import database

    importlib.reload(database)
    from sqlalchemy import text
    from database import read_engine, DATABASE_URL

    read_url = os.environ.get("DATABASE_URL_READ", "").strip() or DATABASE_URL
    print("WRITE host/db:", host_of(DATABASE_URL))
    print("READ  host/db:", host_of(read_url))
    if host_of(read_url) != host_of(DATABASE_URL):
        print("WARNING: READ and WRITE point at different DBs — Analytics uses READ pool.")

    plant = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
    if not plant:
        with read_engine.connect() as c:
            rows = c.execute(
                text("SELECT plant_id, name FROM plants WHERE status = 'Active' ORDER BY plant_id LIMIT 8")
            ).fetchall()
        print("Usage: python scripts/uat_analytics_lab.py <plant_id>")
        print("Sample plants:", [dict(r._mapping) for r in rows])
        return 2

    print("--- plant_id:", plant, "---")

    with read_engine.connect() as c:
        n_raw = c.execute(
            text("SELECT COUNT(*) FROM raw_data_generic WHERE plant_id = :p"), {"p": plant}
        ).scalar()
        n_sig = c.execute(
            text(
                "SELECT COUNT(DISTINCT signal) FROM raw_data_generic WHERE plant_id = :p "
                "AND equipment_id IN (SELECT DISTINCT inverter_id FROM plant_architecture "
                "WHERE plant_id = :p AND inverter_id IS NOT NULL LIMIT 500)"
            ),
            {"p": plant},
        ).scalar()
        inv_arch = c.execute(
            text(
                "SELECT COUNT(DISTINCT inverter_id) FROM plant_architecture "
                "WHERE plant_id = :p AND inverter_id IS NOT NULL"
            ),
            {"p": plant},
        ).scalar()
        inv_pe = c.execute(
            text(
                "SELECT COUNT(DISTINCT equipment_id) FROM plant_equipment "
                "WHERE plant_id = :p AND LOWER(TRIM(equipment_level::text)) = 'inverter'"
            ),
            {"p": plant},
        ).scalar()
        sample = c.execute(
            text(
                "SELECT DISTINCT signal FROM raw_data_generic WHERE plant_id = :p "
                "AND equipment_id IN (SELECT DISTINCT inverter_id FROM plant_architecture "
                "WHERE plant_id = :p AND inverter_id IS NOT NULL) LIMIT 15"
            ),
            {"p": plant},
        ).fetchall()

    print("raw_data_generic rows (plant):", int(n_raw or 0))
    print("plant_architecture distinct inverter_id:", int(inv_arch or 0))
    print("plant_equipment inverter rows:", int(inv_pe or 0))
    print("distinct signals (via arch inverter ids):", int(n_sig or 0))
    print("sample signals:", [r[0] for r in sample])

    if int(n_sig or 0) == 0 and int(n_raw or 0) > 0:
        print(
            "DIAGNOSIS: raw data exists but no signals for architecture inverter_ids — "
            "likely equipment_id mismatch in raw_data_generic vs plant_architecture."
        )
    if int(n_sig or 0) == 0 and int(n_raw or 0) == 0:
        print("DIAGNOSIS: no raw_data_generic for this plant_id string — wrong plant_id or empty DB.")
    return 0 if int(n_sig or 0) > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
