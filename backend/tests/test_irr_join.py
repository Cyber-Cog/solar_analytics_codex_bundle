"""Tests for merge_asof irradiance ↔ AC alignment."""
from __future__ import annotations

import pandas as pd

from engine.irr_join import merge_irradiance_onto_ac


def test_merge_asof_15min_offset_clipping_priority():
    """AC on :00 / :15 grid; GTI on :03 / :18 — exact map would drop rows."""
    ac = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-03-09 10:00:00", "2026-03-09 10:15:00"]),
            "inverter_id": ["A", "A"],
            "ac_kw": [10.0, 20.0],
        }
    )
    irr = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-03-09 10:03:00", "2026-03-09 10:18:00"]),
            "signal": ["gti", "gti"],
            "value": [500.0, 600.0],
        }
    )
    out = merge_irradiance_onto_ac(
        ac,
        ts_col="timestamp",
        df_irr=irr,
        value_col="value",
        priority_mode="clipping",
        out_col="gti",
        tolerance_min=12.0,
    )
    assert out["gti"].notna().all()
    assert abs(float(out.loc[0, "gti"]) - 500.0) < 0.01
    assert abs(float(out.loc[1, "gti"]) - 600.0) < 0.01


def test_standard_priority_prefers_irradiance():
    ac = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-03-09 12:00:00"]),
            "ac_kw": [0.0],
        }
    )
    irr = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-03-09 12:01:00", "2026-03-09 12:01:00"]),
            "signal": ["gti", "irradiance"],
            "irradiance": [999.0, 100.0],
        }
    )
    out = merge_irradiance_onto_ac(
        ac,
        ts_col="timestamp",
        df_irr=irr,
        value_col="irradiance",
        priority_mode="standard",
        out_col="irradiance",
        tolerance_min=12.0,
    )
    assert abs(float(out.loc[0, "irradiance"]) - 100.0) < 0.01


def test_outside_tolerance_nan():
    ac = pd.DataFrame({"timestamp": pd.to_datetime(["2026-03-09 12:00:00"]), "ac_kw": [1.0]})
    irr = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-03-09 14:00:00"]),
            "signal": ["gti"],
            "value": [500.0],
        }
    )
    out = merge_irradiance_onto_ac(
        ac,
        ts_col="timestamp",
        df_irr=irr,
        value_col="value",
        priority_mode="clipping",
        out_col="gti",
        tolerance_min=5.0,
    )
    assert pd.isna(out.loc[0, "gti"])
