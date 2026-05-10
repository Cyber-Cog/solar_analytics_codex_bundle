"""
Admin precompute: canonical module ids, labels, and normalization.

Used by module_precompute, jobs worker, and /api/admin/precompute/* routes.
"""

from __future__ import annotations

import json
from typing import Any, Dict, FrozenSet, List, Optional, Sequence

# Stable ids stored on precompute_jobs.job_spec_json as {"modules": [...]}
ALL_PRECOMPUTE_MODULE_IDS: FrozenSet[str] = frozenset(
    {
        "ds_summary",
        "ds_status",
        "unified",
        "loss_bridge",
        "pl",
        "is",
        "gb",
        "comm",
        "cd",
    }
)

# Human-readable catalog for Admin UI (order within group is preserved).
PRECOMPUTE_MODULE_CATALOG: List[Dict[str, str]] = [
    {
        "id": "ds_summary",
        "label": "Disconnected strings — summary",
        "group": "snapshots",
        "description": "DS summary JSON (KPIs, energy, top SCBs). Fast.",
    },
    {
        "id": "ds_status",
        "label": "Disconnected strings — SCB status table",
        "group": "snapshots",
        "description": "Heavy table/heatmap payload for the DS diagnostics page.",
    },
    {
        "id": "unified",
        "label": "Unified fault overview",
        "group": "snapshots",
        "description": "Unified feed snapshot + SQL category totals (overview cards).",
    },
    {
        "id": "loss_bridge",
        "label": "Loss Analysis — bridge",
        "group": "snapshots",
        "description": "Loss bridge snapshot for the Loss Analysis page.",
    },
    {
        "id": "pl",
        "label": "Power limitation (PL)",
        "group": "fault_engines",
        "description": "Runs PL engine and saves the tab cache to the database.",
    },
    {
        "id": "is",
        "label": "Inverter shutdown (IS)",
        "group": "fault_engines",
        "description": "Runs IS engine and saves the tab cache.",
    },
    {
        "id": "gb",
        "label": "Grid breakdown (GB)",
        "group": "fault_engines",
        "description": "Runs GB engine and saves the tab cache.",
    },
    {
        "id": "comm",
        "label": "Communication issue",
        "group": "fault_engines",
        "description": "Runs communication engine and saves the tab cache.",
    },
    {
        "id": "cd",
        "label": "Clipping & derating",
        "group": "fault_engines",
        "description": "Runs clipping/derating engine and saves the tab cache (includes timelines).",
    },
]

_ALIAS_TO_ID = {
    "power_limitation": "pl",
    "inverter_shutdown": "is",
    "grid_breakdown": "gb",
    "communication": "comm",
    "communication_issue": "comm",
    "clipping_derating": "cd",
    "clipping": "cd",
    "derating": "cd",
}


def normalize_precompute_module_ids(modules: Sequence[str]) -> List[str]:
    """Return deduped canonical ids; raises ValueError on unknown tokens."""
    out: List[str] = []
    seen: set[str] = set()
    unknown: List[str] = []
    for raw in modules:
        k = str(raw).strip().lower().replace("-", "_")
        if not k:
            continue
        if k in ("all", "*", "full", "everything"):
            return sorted(ALL_PRECOMPUTE_MODULE_IDS)
        k = _ALIAS_TO_ID.get(k, k)
        if k not in ALL_PRECOMPUTE_MODULE_IDS:
            unknown.append(str(raw))
            continue
        if k not in seen:
            seen.add(k)
            out.append(k)
    if unknown:
        raise ValueError(
            "Unknown module id(s): "
            + ", ".join(unknown)
            + ". Valid: "
            + ", ".join(sorted(ALL_PRECOMPUTE_MODULE_IDS))
        )
    return out


def job_spec_for_modules_list(modules: Optional[Sequence[str]]) -> Optional[Dict[str, Any]]:
    """
    Build JSON-serializable job spec for the queue.

    None / empty / full set -> None (worker runs everything; backward compatible).
    """
    if modules is None:
        return None
    seq = [str(x) for x in modules if str(x).strip()]
    if not seq:
        return None
    normed = normalize_precompute_module_ids(seq)
    if set(normed) == set(ALL_PRECOMPUTE_MODULE_IDS):
        return None
    return {"modules": normed}


def parse_job_spec_json(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw or not str(raw).strip():
        return None
    try:
        d = json.loads(raw)
    except Exception:
        return None
    return d if isinstance(d, dict) else None


def modules_for_worker(job_spec: Optional[Dict[str, Any]]) -> Optional[FrozenSet[str]]:
    """
    None -> run all modules.

    Otherwise return frozenset of canonical ids (validated).
    """
    if not job_spec:
        return None
    raw = job_spec.get("modules")
    if raw is None:
        return None
    if not isinstance(raw, list):
        return None
    try:
        normed = normalize_precompute_module_ids([str(x) for x in raw])
    except ValueError:
        return None
    if not normed:
        return None
    if set(normed) == set(ALL_PRECOMPUTE_MODULE_IDS):
        return None
    return frozenset(normed)
