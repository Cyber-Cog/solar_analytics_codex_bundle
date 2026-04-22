"""
Build recurring fault episodes from fault_diagnostics rows.

This module is intentionally sidecar-only:
- It never changes DS detection math.
- It reads CONFIRMED_DS rows already written in fault_diagnostics.
- It writes compact episode/day metadata used for fast recurrence lookups.
"""

from __future__ import annotations

import os
from datetime import date
from typing import Iterable

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from models import FaultEpisode, FaultEpisodeDay


ALGO_VERSION = os.getenv("DS_ALGO_VERSION", "ds_v_current")


def _as_datestr(v) -> str:
    return str(v)[:10]


def _is_consecutive(prev_day: str, cur_day: str) -> bool:
    return (date.fromisoformat(cur_day) - date.fromisoformat(prev_day)).days <= 1


def rebuild_fault_episodes_for_scbs(db: Session, plant_id: str, scb_ids: Iterable[str]) -> None:
    """
    Rebuild episodes for the provided SCBs from existing fault_diagnostics rows.
    Safe to call repeatedly (delete + rebuild for affected SCBs only).
    """
    ids = sorted({str(s).strip() for s in scb_ids if str(s).strip()})
    if not ids:
        return

    # Delete existing sidecar metadata for these SCBs.
    db.execute(
        sa_text("DELETE FROM fault_episode_days WHERE plant_id=:p AND scb_id = ANY(:ids)"),
        {"p": plant_id, "ids": ids},
    )
    db.execute(
        sa_text("DELETE FROM fault_episodes WHERE plant_id=:p AND scb_id = ANY(:ids)"),
        {"p": plant_id, "ids": ids},
    )
    db.flush()

    # Pull per-day DS signals from canonical fault_diagnostics table.
    rows = db.execute(
        sa_text(
            """
            SELECT scb_id,
                   SUBSTR(timestamp, 1, 10) AS day,
                   MIN(timestamp) AS first_ts,
                   MAX(timestamp) AS last_ts,
                   COUNT(*) AS confirmed_points,
                   MAX(COALESCE(missing_strings, 0)) AS severity
            FROM fault_diagnostics
            WHERE plant_id = :p
              AND scb_id = ANY(:ids)
              AND fault_status = 'CONFIRMED_DS'
            GROUP BY scb_id, SUBSTR(timestamp, 1, 10)
            ORDER BY scb_id, day
            """
        ),
        {"p": plant_id, "ids": ids},
    ).fetchall()
    if not rows:
        db.commit()
        return

    grouped: dict[str, list] = {}
    for r in rows:
        grouped.setdefault(r[0], []).append(r)

    ep_batch: list[dict] = []
    ep_day_batch: list[dict] = []
    for scb_id, day_rows in grouped.items():
        # Each row: scb_id, day, first_ts, last_ts, confirmed_points, severity
        start_idx = 0
        n = len(day_rows)
        while start_idx < n:
            end_idx = start_idx + 1
            while end_idx < n and _is_consecutive(day_rows[end_idx - 1][1], day_rows[end_idx][1]):
                end_idx += 1

            block = day_rows[start_idx:end_idx]
            start_day = _as_datestr(block[0][1])
            end_day = _as_datestr(block[-1][1])
            start_ts = str(block[0][2])
            last_ts = str(block[-1][3])
            max_ms = max(int(b[5] or 0) for b in block)
            days_active = len(block)

            is_latest_block = end_idx == n
            status = "open" if is_latest_block else "closed"
            end_ts = None if is_latest_block else last_ts
            episode_id = f"{plant_id}:{scb_id}:DS:{start_day}"

            ep_batch.append(
                {
                    "episode_id": episode_id,
                    "plant_id": plant_id,
                    "scb_id": scb_id,
                    "fault_type": "DS",
                    "start_date": start_day,
                    "last_seen_date": end_day,
                    "start_ts": start_ts,
                    "last_seen_ts": last_ts,
                    "end_ts": end_ts,
                    "status": status,
                    "days_active": days_active,
                    "max_missing_strings": max_ms,
                    "algorithm_version": ALGO_VERSION,
                }
            )

            for b in block:
                ep_day_batch.append(
                    {
                        "episode_id": episode_id,
                        "plant_id": plant_id,
                        "scb_id": scb_id,
                        "day": _as_datestr(b[1]),
                        "present_flag": True,
                        "severity": int(b[5] or 0),
                        "confirmed_points": int(b[4] or 0),
                    }
                )

            start_idx = end_idx

    if ep_batch:
        db.bulk_insert_mappings(FaultEpisode, ep_batch)
    if ep_day_batch:
        db.bulk_insert_mappings(FaultEpisodeDay, ep_day_batch)
    db.commit()
