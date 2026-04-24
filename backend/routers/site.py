"""
Public site configuration (no secrets) — appearance defaults for all clients.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from database import get_read_db
from models import FaultCache

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/site", tags=["Site"])

SITE_APPEARANCE_KEY = "site:appearance:v1"

DEFAULT_APPEARANCE: dict[str, Any] = {
    "org_default_theme": "photon",
    "updated_at": None,
}

ALLOWED_ORG_THEMES = frozenset(
    {
        "photon",
        "dark_ocean",
        "dark_ink",
        "dark_forest",
        "light_paper",
        "light_air",
        "light_sand",
        "vikram",
        "dark",
        "light",
    }
)


def _normalize_theme_id(raw: Optional[str]) -> str:
    t = (raw or "").strip() or "photon"
    if t == "dark":
        return "dark_ocean"
    if t == "light":
        return "light_paper"
    if t not in ALLOWED_ORG_THEMES:
        return "photon"
    return t


def _load_payload(db: Session) -> dict[str, Any]:
    row = (
        db.query(FaultCache)
        .filter(FaultCache.cache_key == SITE_APPEARANCE_KEY)
        .first()
    )
    if not row or not row.payload:
        return dict(DEFAULT_APPEARANCE)
    try:
        data = json.loads(row.payload)
        if not isinstance(data, dict):
            return dict(DEFAULT_APPEARANCE)
        out = dict(DEFAULT_APPEARANCE)
        out["org_default_theme"] = _normalize_theme_id(
            data.get("org_default_theme")
        )
        out["updated_at"] = data.get("updated_at")
        return out
    except Exception as exc:
        log.debug("site appearance parse skipped: %s", exc)
        return dict(DEFAULT_APPEARANCE)


@router.get("/appearance")
def get_site_appearance(db: Session = Depends(get_read_db)):
    """Organization default theme for new sessions (backward compatible JSON)."""
    data = _load_payload(db)
    data = dict(data)
    data["org_default_theme"] = _normalize_theme_id(data.get("org_default_theme"))
    return data
