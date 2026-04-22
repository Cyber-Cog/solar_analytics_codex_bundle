"""
Pure math for Soiling analytics: PR smoothing, slopes, loss from PR steps.
"""

from __future__ import annotations

import statistics
from typing import List, Optional, Sequence, Tuple

# Ignore single-day PR drops larger than this (percentage points) when accumulating loss.
DELTA_PP_CAP = 15.0


def moving_median(values: Sequence[float], window: int = 3) -> List[float]:
    if not values:
        return []
    n = len(values)
    half = max(0, window // 2)
    out: List[float] = []
    for i in range(n):
        lo = max(0, i - half)
        hi = min(n, i + half + 1)
        chunk = sorted(values[lo:hi])
        mid = len(chunk) // 2
        if len(chunk) % 2:
            out.append(chunk[mid])
        else:
            out.append((chunk[mid - 1] + chunk[mid]) / 2.0)
    return out


def linreg_slope_per_step(y: Sequence[float]) -> Optional[float]:
    """Least-squares slope with x = 0..n-1 (change in y per one step / day)."""
    n = len(y)
    if n < 2:
        return None
    xs = list(range(n))
    x_mean = (n - 1) / 2.0
    y_mean = sum(y) / n
    num = sum((x - x_mean) * (yi - y_mean) for x, yi in zip(xs, y))
    den = sum((x - x_mean) ** 2 for x in xs)
    if abs(den) < 1e-18:
        return None
    return num / den


def median_consecutive_delta(y: Sequence[float]) -> Optional[float]:
    if len(y) < 2:
        return None
    deltas = [float(y[i]) - float(y[i - 1]) for i in range(1, len(y))]
    return float(statistics.median(deltas))


def soiling_loss_kwh_from_pr_steps(
    pr_series: Sequence[float], e_ref_kwh_daily: Sequence[float], delta_cap: float = DELTA_PP_CAP
) -> float:
    """
    For each day transition, attribute loss from negative PR steps to reference energy
    of the target day (index i). pr_series and e_ref_kwh_daily must align by day index.
    """
    if len(pr_series) != len(e_ref_kwh_daily):
        raise ValueError("pr_series and e_ref_kwh_daily length mismatch")
    loss = 0.0
    for i in range(1, len(pr_series)):
        drop_pp = -(float(pr_series[i]) - float(pr_series[i - 1]))
        if drop_pp > 0:
            drop_pp = min(drop_pp, delta_cap)
            loss += (drop_pp / 100.0) * float(e_ref_kwh_daily[i])
    return loss


def ratio_trend_stats(
    ratios: Sequence[float], window: int = 3
) -> Tuple[Optional[float], Optional[float], List[float]]:
    """Returns (median_delta, linreg_slope, smoothed_ratios)."""
    if not ratios:
        return None, None, []
    r = [float(x) for x in ratios]
    smooth = moving_median(r, window) if len(r) >= window else r
    return median_consecutive_delta(smooth), linreg_slope_per_step(smooth), smooth
