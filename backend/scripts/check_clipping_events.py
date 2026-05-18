"""Print power_clip (and other CD) events for a plant and date range. Run from backend/."""
from __future__ import annotations

import os
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1]


def _load_env() -> None:
    p = _BACKEND / ".env"
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ[k.strip()] = v.strip()


def main() -> None:
    import argparse

    _load_env()
    sys.path.insert(0, str(_BACKEND))
    os.chdir(_BACKEND)

    from sqlalchemy import text  # noqa: E402

    from database import SessionLocal  # noqa: E402
    from engine.clipping_derating import run_clipping_derating  # noqa: E402

    ap = argparse.ArgumentParser()
    ap.add_argument("--plant", default="Tiger")
    ap.add_argument("--from", dest="d0", default="2026-03-09")
    ap.add_argument("--to", dest="d1", default="2026-03-09")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        r = db.execute(
            text(
                """
                SELECT MIN(timestamp)::date AS dmin, MAX(timestamp)::date AS dmax
                FROM raw_data_generic
                WHERE plant_id = :p AND signal = 'ac_power'
                """
            ),
            {"p": args.plant},
        ).fetchone()
        print(f"{args.plant} AC date span in DB: {r[0]} .. {r[1]}")

        inv, _, meta = run_clipping_derating(db, args.plant, args.d0, args.d1)
    finally:
        db.close()

    clips = [x for x in inv if (x.get("power_clip_points") or 0) > 0]
    print(f"Range {args.d0} .. {args.d1}")
    print(f"  Inverters with any CD issue: {len(inv)}")
    print(f"  Inverters with POWER_CLIP:  {len(clips)}")
    if meta.get("reason"):
        print(f"  meta.reason: {meta['reason']}")
    print(f"  Skipped (coverage/calibration): {len(meta.get('skipped') or [])}")

    if not clips:
        print("  No clipping events (power_clip_points = 0 for all kept inverters).")
        return

    for c in sorted(clips, key=lambda x: -(x.get("power_clip_points") or 0)):
        print(
            f"  {c.get('inverter_id')}: "
            f"power_clip_points={c.get('power_clip_points')} "
            f"loss_clip_kwh={c.get('loss_power_clipping_kwh')} "
            f"static_pts={c.get('static_derate_points')} "
            f"dynamic_pts={c.get('dynamic_derate_points')} "
            f"dominant={c.get('dominant_kind')}"
        )


if __name__ == "__main__":
    main()
