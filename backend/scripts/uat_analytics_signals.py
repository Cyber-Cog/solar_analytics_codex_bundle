"""UAT: print raw_data / architecture / signals-subquery stats for a plant (default NTPCNOKHRA)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1]


def _load_env() -> None:
    p = _BACKEND / ".env"
    if not p.is_file():
        print("No backend/.env")
        sys.exit(1)
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ[k.strip()] = v.strip()


def main() -> int:
    _load_env()
    sys.path.insert(0, str(_BACKEND))
    os.chdir(_BACKEND)

    from sqlalchemy import text

    from database import SessionLocal

    plant = (sys.argv[1] if len(sys.argv) > 1 else "NTPCNOKHRA").strip()
    db = SessionLocal()
    try:
        c1 = db.execute(
            text("SELECT COUNT(*) FROM raw_data_generic WHERE plant_id = :p"), {"p": plant}
        ).scalar()
        c2 = db.execute(
            text("SELECT COUNT(DISTINCT signal) FROM raw_data_generic WHERE plant_id = :p"),
            {"p": plant},
        ).scalar()
        c3 = db.execute(
            text(
                "SELECT COUNT(*) FROM plant_architecture "
                "WHERE plant_id = :p AND inverter_id IS NOT NULL"
            ),
            {"p": plant},
        ).scalar()
        c4 = db.execute(
            text(
                "SELECT COUNT(*) FROM plant_equipment "
                "WHERE plant_id = :p AND LOWER(TRIM(equipment_level::text)) = 'inverter'"
            ),
            {"p": plant},
        ).scalar()
        rows = db.execute(
            text(
                "SELECT DISTINCT signal FROM raw_data_generic WHERE plant_id=:p ORDER BY 1 LIMIT 15"
            ),
            {"p": plant},
        ).fetchall()
        print("plant_id:", plant)
        print("raw_data_generic rows:", c1)
        print("distinct signals (whole plant):", c2)
        print("plant_architecture rows with inverter_id:", c3)
        print("plant_equipment inverter rows:", c4)
        print("sample signals:", [r[0] for r in rows])

        subq = (
            "SELECT equipment_id FROM plant_equipment "
            "WHERE plant_id = :plant_id AND LOWER(TRIM(equipment_level::text)) = 'inverter' "
            "AND equipment_id IS NOT NULL "
            "UNION SELECT inverter_id FROM plant_architecture "
            "WHERE plant_id = :plant_id AND inverter_id IS NOT NULL"
        )
        sql = (
            "SELECT COUNT(DISTINCT r.signal) FROM raw_data_generic r "
            "WHERE r.plant_id = :plant_id AND r.equipment_id IN ( " + subq + " )"
        )
        c5 = db.execute(text(sql), {"plant_id": plant}).scalar()
        print("distinct signals where equipment_id in (PE inverter UNION arch inverter):", c5)
        return 0
    except Exception as exc:
        print("ERROR", exc)
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
