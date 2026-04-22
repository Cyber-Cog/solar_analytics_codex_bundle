"""
Dashboard response cache.

Defaults to an in-process TTL dict (works out of the box, single worker).
If REDIS_URL is set, reads/writes go to Redis instead so multiple uvicorn /
gunicorn workers stay consistent.

Public API kept unchanged so call sites in dashboard/analytics/faults routers
do not need to change.
"""

from __future__ import annotations

import os
import json
import time
import threading
import logging
from typing import Any, Optional

log = logging.getLogger(__name__)

_TTL_SECONDS = 180  # default TTL for dashboard responses

# ── In-process fallback ──────────────────────────────────────────────────────
_lock = threading.Lock()
_store: dict[str, tuple[Any, float]] = {}

# ── Optional Redis backend (set REDIS_URL=redis://localhost:6379/0) ─────────
_redis_client = None
_redis_url = os.environ.get("REDIS_URL", "").strip()
if _redis_url:
    try:
        import redis  # type: ignore

        _redis_client = redis.Redis.from_url(_redis_url, decode_responses=True, socket_timeout=1.5)
        # Probe once so misconfiguration fails loudly in the log, not at every call.
        _redis_client.ping()
        log.info("dashboard_cache: using Redis backend at %s", _redis_url)
    except Exception as exc:
        log.warning("dashboard_cache: REDIS_URL set but unavailable (%s); falling back to in-process cache", exc)
        _redis_client = None


def _key(prefix: str, plant_id: str, date_from: str, date_to: str) -> str:
    return f"{prefix}:{plant_id}:{date_from or ''}:{date_to or ''}"


# ── Redis-aware helpers (used by both public API and internal callers) ──────
def _redis_get(key: str) -> Any:
    try:
        raw = _redis_client.get(key)  # type: ignore[union-attr]
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as exc:
        log.debug("dashboard_cache: redis GET %s failed: %s", key, exc)
        return None


def _redis_set(key: str, value: Any, ttl: int) -> None:
    try:
        _redis_client.set(key, json.dumps(value, default=str), ex=max(1, int(ttl)))  # type: ignore[union-attr]
    except Exception as exc:
        log.debug("dashboard_cache: redis SET %s failed: %s", key, exc)


def _inproc_get(key: str) -> Any:
    with _lock:
        entry = _store.get(key)
        if entry is None:
            return None
        val, expiry = entry
        if time.time() > expiry:
            del _store[key]
            return None
        return val


def _inproc_set(key: str, value: Any, ttl: int) -> None:
    with _lock:
        _store[key] = (value, time.time() + ttl)


# ── Public API (backwards compatible) ────────────────────────────────────────
def get(prefix: str, plant_id: str, date_from: str, date_to: str) -> Any:
    k = _key(prefix, plant_id, date_from, date_to)
    if _redis_client is not None:
        return _redis_get(k)
    return _inproc_get(k)


def set(prefix: str, plant_id: str, date_from: str, date_to: str, value: Any) -> None:
    k = _key(prefix, plant_id, date_from, date_to)
    if _redis_client is not None:
        _redis_set(k, value, _TTL_SECONDS)
    else:
        _inproc_set(k, value, _TTL_SECONDS)


def invalidate_plant(plant_id: str) -> None:
    """Remove all cached entries for this plant (call after raw data upload)."""
    if _redis_client is not None:
        try:
            # SCAN + DEL for keys containing :plant_id:
            pattern = f"*:{plant_id}:*"
            batch: list[str] = []
            for k in _redis_client.scan_iter(match=pattern, count=500):  # type: ignore[union-attr]
                batch.append(k)
                if len(batch) >= 500:
                    _redis_client.delete(*batch)  # type: ignore[union-attr]
                    batch.clear()
            if batch:
                _redis_client.delete(*batch)  # type: ignore[union-attr]
            return
        except Exception as exc:
            log.debug("dashboard_cache: redis invalidate failed: %s", exc)
    with _lock:
        to_del = [k for k in _store if f":{plant_id}:" in k]
        for k in to_del:
            del _store[k]


def get_any(key: str, ttl_seconds: int = 120) -> Any:
    """Get cached value by arbitrary key; None if missing or expired."""
    if _redis_client is not None:
        return _redis_get(key)
    return _inproc_get(key)


def set_any(key: str, value: Any, ttl_seconds: int = 120) -> None:
    if _redis_client is not None:
        _redis_set(key, value, ttl_seconds)
    else:
        _inproc_set(key, value, ttl_seconds)
