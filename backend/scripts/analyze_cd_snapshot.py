"""Snapshot raw AC + GTI around a wall-clock instant; align with clipping_derating math.

Run from backend/:  python scripts/analyze_cd_snapshot.py --plant Tiger --date 2026-03-09

Loads backend/.env first (same pattern as run_precompute_once.py).
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
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


def _window(center: datetime, pad_min: int) -> tuple[str, str]:
    lo = center - timedelta(minutes=pad_min)
    hi = center + timedelta(minutes=pad_min)
    return lo.strftime("%Y-%m-%d %H:%M:%S"), hi.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    _load_env()
    sys.path.insert(0, str(_BACKEND))
    os.chdir(_BACKEND)

    import numpy as np  # noqa: E402
    import pandas as pd  # noqa: E402
    from sqlalchemy import text  # noqa: E402

    from database import SessionLocal  # noqa: E402
    import engine.clipping_derating as cd  # noqa: E402
    from engine.irr_join import merge_irradiance_onto_ac  # noqa: E402

    p = argparse.ArgumentParser()
    p.add_argument("--plant", default="Tiger")
    p.add_argument("--date", default="2026-03-09")
    p.add_argument("--center", default="2026-03-09 13:00:00")
    p.add_argument("--pad-min", type=int, default=8)
    args = p.parse_args()

    day = args.date[:10]
    center = datetime.strptime(args.center[:19], "%Y-%m-%d %H:%M:%S")
    w0, w1 = _window(center, args.pad_min)

    db = SessionLocal()
    try:
        _, _, meta = cd.run_clipping_derating(db, args.plant, day, day)
        print("=== run_clipping_derating", args.plant, day, "===")
        if meta.get("reason"):
            print("meta.reason:", meta["reason"])
        print("skipped inverters:", len(meta.get("skipped", [])))
        for s in (meta.get("skipped") or [])[:15]:
            print(" ", s)

        rows = db.execute(
            text(
                """
            SELECT equipment_id AS inverter_id, value AS ac_kw, timestamp
            FROM raw_data_generic
            WHERE plant_id = :p
              AND LOWER(TRIM(equipment_level::text)) = 'inverter'
              AND signal = 'ac_power'
              AND timestamp >= :f AND timestamp <= :t
            ORDER BY inverter_id, timestamp
            """
            ),
            {"p": args.plant, "f": w0, "t": w1},
        ).fetchall()

        irr_rows = db.execute(
            text(
                """
            SELECT timestamp, signal, value
            FROM raw_data_generic
            WHERE plant_id = :p
              AND LOWER(TRIM(equipment_level::text)) IN ('plant', 'wms')
              AND signal IN ('gti', 'irradiance', 'ghi')
              AND timestamp >= :f AND timestamp <= :t
            ORDER BY timestamp, signal
            """
            ),
            {"p": args.plant, "f": w0, "t": w1},
        ).fetchall()
    finally:
        db.close()

    if not irr_rows:
        print("\nNo irradiance rows in window — cannot align GTI.")
        return
    if not rows:
        print("\nNo inverter ac_power rows in window.")
        return

    df_irr_long = pd.DataFrame(irr_rows, columns=["timestamp", "signal", "value"])
    df_irr_long["value"] = pd.to_numeric(df_irr_long["value"], errors="coerce")
    df_irr_long = df_irr_long.dropna(subset=["value", "timestamp"])

    df = pd.DataFrame(rows, columns=["inverter_id", "ac_kw", "timestamp"])
    df["ac_kw"] = pd.to_numeric(df["ac_kw"], errors="coerce")
    df["ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["ts"])
    df = merge_irradiance_onto_ac(
        df,
        ts_col="timestamp",
        df_irr=df_irr_long,
        value_col="value",
        priority_mode="clipping",
        out_col="gti",
    )
    df["gti"] = pd.to_numeric(df["gti"], errors="coerce")
    df = df.dropna(subset=["gti"])

    inv_meta = meta.get("inverters") or {}
    print(f"\n=== samples in window {w0} .. {w1} (center {args.center}) ===")
    print(
        "tunables: RATED_HIT=%s HEALTHY_AC<=%s*rated NOISE=%s*rated GTI_HEALTHY=[%s,%s] HOUR=[%s,%s) persist>=%s"
        % (
            cd.CD_RATED_HIT_RATIO,
            cd.CD_HEALTHY_MAX_OF_RATED,
            cd.CD_NOISE_FRAC_OF_RATED,
            cd.CD_HEALTHY_GTI_MIN,
            cd.CD_HEALTHY_GTI_MAX,
            cd.CD_HOUR_START,
            cd.CD_HOUR_END,
            cd.CD_PERSIST_MIN_SAMPLES,
        )
    )
    print(
        "Note: table below is pre-persistence. Final POWER_CLIP also needs "
        f">={cd.CD_PERSIST_MIN_SAMPLES} consecutive samples with clip mask true."
    )

    for inv_id, g in df.groupby("inverter_id"):
        m = inv_meta.get(inv_id) or {}
        rated = float(m.get("rated_ac_kw") or 0.0)
        k = m.get("k_factor")
        skipped = m.get("skipped")
        if not rated:
            print(inv_id, "no rated in meta — skip row-level")
            continue
        if skipped or k is None:
            print(f"\n[{inv_id}] SKIPPED by engine: {m.get('skip_reason')} rated={rated}")
            continue
        k = float(k)
        noise = cd.CD_NOISE_FRAC_OF_RATED * rated
        hit = cd.CD_RATED_HIT_RATIO * rated
        g2 = g.sort_values("ts").copy()
        for _, r in g2.iterrows():
            ac = float(r["ac_kw"])
            gti = float(r["gti"])
            pv = k * gti
            gap = max(0.0, pv - ac)
            hr = int(r["ts"].hour)
            in_hours = cd.CD_HOUR_START <= hr <= cd.CD_HOUR_END - 1
            valid = (
                np.isfinite(ac)
                and np.isfinite(gti)
                and gti > cd.CD_GTI_FLOOR_W_M2
                and in_hours
            )
            min_active = max(cd.CD_MIN_ACTIVE_FRAC * rated, cd.CD_MIN_ACTIVE_ABS_KW)
            active = valid and ac >= min_active
            at_cap = active and ac >= hit
            gap_real = active and gap > noise
            healthy_band = (
                cd.CD_HEALTHY_GTI_MIN <= gti <= cd.CD_HEALTHY_GTI_MAX
                and ac >= min_active
                and ac <= cd.CD_HEALTHY_MAX_OF_RATED * rated
            )
            print(
                f"  {inv_id} @ {r['timestamp']}  AC={ac:.2f}  GTI={gti:.1f}  "
                f"k={k:.5f}  Pvir={pv:.2f}  gap={gap:.3f}  noise={noise:.3f}  "
                f"active={active} at_cap={at_cap} gap>noise={gap_real}  "
                f"would_healthy_calib={healthy_band}"
            )


if __name__ == "__main__":
    main()
