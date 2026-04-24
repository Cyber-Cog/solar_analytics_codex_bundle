"""
Lightweight snapshot / API timing metrics (in-process counters + structured logs).

For multi-worker deployments, treat as per-process hints; aggregate in log stack
if needed. Zero external cost.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from typing import Optional

_lock = threading.Lock()
_counters: Counter[str] = Counter()

log = logging.getLogger("solar.snapshot_perf")


def record_snapshot(kind: str, hit: bool) -> None:
    """kind: ds_summary | unified_feed | loss_bridge"""
    key = f"{kind}:{'hit' if hit else 'miss'}"
    with _lock:
        _counters[key] += 1


def record_compute_ms(kind: str, elapsed_ms: float, extra: Optional[str] = None) -> None:
    msg = "compute_ms kind=%s elapsed_ms=%.1f" % (kind, elapsed_ms)
    if extra:
        msg += " " + extra
    log.info(msg)


def snapshot_ratio_log() -> None:
    """Emit current hit/miss counters (call from worker periodically if desired)."""
    with _lock:
        snap = dict(_counters)
    if snap:
        log.info("snapshot_counters %s", snap)


def reset_counters() -> None:
    with _lock:
        _counters.clear()


class Timer:
    def __init__(self, kind: str, extra: Optional[str] = None):
        self.kind = kind
        self.extra = extra
        self._t0 = 0.0

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *args):
        ms = (time.perf_counter() - self._t0) * 1000.0
        record_compute_ms(self.kind, ms, self.extra)
