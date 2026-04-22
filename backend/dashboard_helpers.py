"""
Pure helpers for dashboard date range and WMS-style insolation math.
Used by routers/dashboard.py and unit tests (no DATABASE_URL required).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Tuple

# WMS / GTI insolation: assumes **1-minute** samples (same as dashboard WMS KPIs).
# kWh/m² = Σ (W/m²) × (1/60 h) / 1000 = Σ P / 60000
WMS_INSOLATION_SUM_DIVISOR = 60000.0


def resolve_dashboard_date_range(
    date_from: Optional[str], date_to: Optional[str]
) -> Tuple[str, str]:
    """
    Return (from, to) as YYYY-MM-DD.
    Empty strings must not fall through to the 7-day default (UI sends '' when unset).
    If only one bound is set, use that day for both ends.
    """
    def norm(s: Optional[str]) -> Optional[str]:
        if s is None:
            return None
        t = str(s).strip()
        if not t:
            return None
        return t[:10] if len(t) >= 10 else t

    df = norm(date_from)
    dt = norm(date_to)
    if df and not dt:
        return df, df
    if dt and not df:
        return dt, dt
    if df and dt:
        return df, dt
    today = date.today()
    return str(today - timedelta(days=7)), str(today)


def gti_insolation_kwh_m2_from_sums(gti_sum: float, irradiance_sum: float) -> float:
    """
    Tilt-plane insolation (kWh/m²) from summed W/m² samples over the period.
    If there is no `gti` sum but legacy `irradiance` exists, use irradiance sum (same as WMS KPI block).
    """
    gs = float(gti_sum or 0.0)
    ir = float(irradiance_sum or 0.0)
    if gs == 0.0 and ir > 0.0:
        gs = ir
    return gs / WMS_INSOLATION_SUM_DIVISOR
