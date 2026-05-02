"""
backend/models.py
==================
SQLAlchemy ORM models — one class per database table.

Tables match schema.sql exactly. An additional `users` and `plants`
table is added here to support authentication and multi-plant selection.
"""

from sqlalchemy import (
    Column, Integer, BigInteger, String, Float, Text,
    DateTime, Boolean, ForeignKey, UniqueConstraint, Index,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


# ── Users ─────────────────────────────────────────────────────────────────────
class User(Base):
    __tablename__ = "users"

    id            = Column(Integer, primary_key=True, index=True)
    email         = Column(String, unique=True, index=True, nullable=False)
    full_name     = Column(String, nullable=True)
    hashed_password = Column(String, nullable=False)
    is_active     = Column(Boolean, default=True)
    is_admin      = Column(Boolean, default=False)
    allowed_plants = Column(String, nullable=True) # comma separated IDs
    created_at    = Column(DateTime, server_default=func.now())

    plants        = relationship("Plant", back_populates="owner")


# ── Plants ─────────────────────────────────────────────────────────────────────
class Plant(Base):
    __tablename__ = "plants"

    id              = Column(Integer, primary_key=True, index=True)
    plant_id        = Column(String, unique=True, index=True, nullable=False)  # e.g. PLANT-WMS-01
    name            = Column(String, nullable=False)
    technology      = Column(String, default="Solar PV")
    location        = Column(String, nullable=True)
    capacity_mwp    = Column(Float, nullable=True)
    cod_date        = Column(String, nullable=True)   # Commercial Operation Date
    ppa_tariff      = Column(Float, nullable=True)
    status          = Column(String, default="Active")
    plant_type      = Column(String, default="SCB")  # SCB | MPPT (Tiger is MPPT)
    owner_id        = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at      = Column(DateTime, server_default=func.now())

    owner           = relationship("User", back_populates="plants")


# ── Raw Data Generic ───────────────────────────────────────────────────────────
class RawDataGeneric(Base):
    __tablename__ = "raw_data_generic"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    plant_id        = Column(String, nullable=False, index=True)
    timestamp       = Column(String, nullable=False, index=True)
    timestamp_ts    = Column(DateTime(timezone=True), nullable=True)
    equipment_level = Column(String, nullable=False)
    equipment_id    = Column(String, nullable=False, index=True)
    signal          = Column(String, nullable=False)
    value           = Column(Float, nullable=True)
    source          = Column(String, default="excel_upload")

    __table_args__ = (
        Index("idx_raw_data_generic_composite", "plant_id", "signal", "timestamp"),
    )


# ── DC Hierarchy Derived ───────────────────────────────────────────────────────
class DCHierarchyDerived(Base):
    __tablename__ = "dc_hierarchy_derived"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    plant_id        = Column(String, nullable=False, index=True)
    timestamp       = Column(String, nullable=False, index=True)
    equipment_level = Column(String, nullable=False)
    equipment_id    = Column(String, nullable=False, index=True)
    signal          = Column(String, nullable=False)
    value           = Column(Float, nullable=True)
    source          = Column(String, default="derived")


# ── Plant Architecture ─────────────────────────────────────────────────────────
class PlantArchitecture(Base):
    __tablename__ = "plant_architecture"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    plant_id            = Column(String, nullable=False)
    inverter_id         = Column(String, nullable=False, index=True)
    scb_id              = Column(String, nullable=False, index=True)
    string_id           = Column(String, nullable=False)
    modules_per_string  = Column(Integer, nullable=True)
    strings_per_scb     = Column(Integer, nullable=True)
    scbs_per_inverter   = Column(Integer, nullable=True)
    dc_capacity_kw      = Column(Float, nullable=True)
    spare_flag          = Column(Boolean, default=False, nullable=True)  # if True: exclude from DS, grey in UI

    __table_args__ = (
        UniqueConstraint("plant_id", "inverter_id", "scb_id", "string_id"),
    )


# ── Equipment Specs ────────────────────────────────────────────────────────────
class EquipmentSpec(Base):
    __tablename__ = "equipment_specs"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    plant_id        = Column(String, nullable=False, index=True)
    equipment_id    = Column(String, nullable=False, index=True)
    equipment_type  = Column(String, nullable=False)  # 'inverter' | 'module'
    manufacturer    = Column(String, nullable=True)
    model           = Column(String, nullable=True)
    rated_power     = Column(Float, nullable=True)
    imp             = Column(Float, nullable=True)
    vmp             = Column(Float, nullable=True)
    isc             = Column(Float, nullable=True)
    voc             = Column(Float, nullable=True)
    target_efficiency = Column(Float, default=98.5)

    # ── Inverter-specific (optional) ─────────────────────────────────────────
    ac_capacity_kw     = Column(Float, nullable=True)
    dc_capacity_kwp    = Column(Float, nullable=True)
    rated_efficiency   = Column(Float, nullable=True)   # Euro/rated efficiency %
    mppt_voltage_min   = Column(Float, nullable=True)
    mppt_voltage_max   = Column(Float, nullable=True)
    voltage_limit      = Column(Float, nullable=True)
    current_set_point   = Column(Float, nullable=True)
    spec_sheet_path    = Column(String, nullable=True)   # relative path for attachment
    # Loss Analysis (inverter): % of expected energy lost to degradation; temp coeff as positive fraction per °C above 25 (e.g. 0.004)
    degradation_loss_pct = Column(Float, nullable=True)
    temp_coefficient_per_deg = Column(Float, nullable=True)

    # ── Module-specific (optional) ────────────────────────────────────────────
    impp                   = Column(Float, nullable=True)
    vmpp                   = Column(Float, nullable=True)
    pmax                   = Column(Float, nullable=True)
    degradation_year1_pct   = Column(Float, nullable=True)
    degradation_year2_pct   = Column(Float, nullable=True)
    degradation_annual_pct  = Column(Float, nullable=True)  # annual after year 2
    module_efficiency_pct   = Column(Float, nullable=True)
    alpha_stc              = Column(Float, nullable=True)
    beta_stc                = Column(Float, nullable=True)
    gamma_stc               = Column(Float, nullable=True)
    alpha_noct              = Column(Float, nullable=True)
    beta_noct                = Column(Float, nullable=True)
    gamma_noct               = Column(Float, nullable=True)



# ── Support Tickets ────────────────────────────────────────────────────────────
class SupportTicket(Base):
    __tablename__ = "support_tickets"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    user_email  = Column(String, nullable=False)
    plant_id    = Column(String, nullable=True)
    subject     = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    status      = Column(String, default="open")
    created_at  = Column(DateTime, server_default=func.now())


# ── Fault Diagnostics (Disconnected String) ───────────────────────────────────
class FaultDiagnostics(Base):
    __tablename__ = "fault_diagnostics"

    id                     = Column(Integer, primary_key=True, autoincrement=True)
    timestamp              = Column(String, nullable=False, index=True)
    plant_id               = Column(String, nullable=False, index=True)
    inverter_id            = Column(String, nullable=False, index=True)
    scb_id                 = Column(String, nullable=False, index=True)
    
    virtual_string_current = Column(Float, nullable=True)
    expected_current       = Column(Float, nullable=True)
    missing_current        = Column(Float, nullable=True)
    missing_strings        = Column(Integer, nullable=True)
    
    power_loss_kw          = Column(Float, nullable=True)
    energy_loss_kwh        = Column(Float, nullable=True)
    fault_status           = Column(String, nullable=False)  # NORMAL, POTENTIAL_DS, CONFIRMED_DS


# ── Fault / analytics result cache (faster fetches) ─────────────────────────────
class FaultEpisode(Base):
    __tablename__ = "fault_episodes"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    episode_id          = Column(String, nullable=False, unique=True, index=True)
    plant_id            = Column(String, nullable=False, index=True)
    scb_id              = Column(String, nullable=False, index=True)
    fault_type          = Column(String, nullable=False, default="DS")
    start_date          = Column(String, nullable=False, index=True)  # YYYY-MM-DD
    last_seen_date      = Column(String, nullable=False, index=True)  # YYYY-MM-DD
    start_ts            = Column(String, nullable=False)
    last_seen_ts        = Column(String, nullable=False)
    end_ts              = Column(String, nullable=True)
    status              = Column(String, nullable=False, default="open")  # open | closed
    days_active         = Column(Integer, nullable=False, default=1)
    max_missing_strings = Column(Integer, nullable=False, default=0)
    algorithm_version   = Column(String, nullable=False, default="ds_v_current")
    created_at          = Column(DateTime, server_default=func.now())
    updated_at          = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("idx_fault_episodes_plant_scb_status", "plant_id", "scb_id", "status"),
    )


class FaultEpisodeDay(Base):
    __tablename__ = "fault_episode_days"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    episode_id       = Column(String, nullable=False, index=True)
    plant_id         = Column(String, nullable=False, index=True)
    scb_id           = Column(String, nullable=False, index=True)
    day              = Column(String, nullable=False, index=True)  # YYYY-MM-DD
    present_flag     = Column(Boolean, nullable=False, default=True)
    severity         = Column(Integer, nullable=False, default=0)   # max missing_strings for that day
    confirmed_points = Column(Integer, nullable=False, default=0)   # confirmed timestamps count for that day
    created_at       = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("episode_id", "day", name="uq_fault_episode_day"),
        Index("idx_fault_episode_days_plant_scb_day", "plant_id", "scb_id", "day"),
    )


class FaultEvent(Base):
    __tablename__ = "fault_events"

    id                   = Column(BigInteger, primary_key=True, autoincrement=True)
    plant_id             = Column(String, nullable=False, index=True)
    inverter_id          = Column(String, nullable=True, index=True)
    equipment_level      = Column(String, nullable=False)  # mppt | scb | inverter
    equipment_id         = Column(String, nullable=False, index=True)
    fault_type           = Column(String, nullable=False, default="DS")
    start_time           = Column(DateTime(timezone=True), nullable=False, index=True)
    end_time             = Column(DateTime(timezone=True), nullable=True, index=True)
    duration_minutes     = Column(Float, nullable=True)
    status               = Column(String, nullable=False, default="closed")
    severity             = Column(String, nullable=True)
    detection_confidence = Column(Float, nullable=True)
    missing_strings      = Column(Integer, nullable=True)
    start_reason         = Column(Text, nullable=True)
    close_reason         = Column(Text, nullable=True)
    created_at           = Column(DateTime, server_default=func.now())
    updated_at           = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("idx_fault_events_lookup", "plant_id", "equipment_id", "fault_type", "start_time", "end_time"),
        Index("idx_fault_events_inverter_range", "plant_id", "inverter_id", "start_time", "end_time"),
    )


class FaultCache(Base):
    __tablename__ = "fault_cache"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    cache_key  = Column(String(512), unique=True, nullable=False, index=True)
    payload    = Column(Text, nullable=False)   # JSON
    created_at = Column(DateTime, server_default=func.now())


# ── Materialized unique equipment IDs per plant per level ─────────────────────
# Populated on upload; replaces the 80s+ DISTINCT scan in the Analytics Lab
class PlantEquipment(Base):
    __tablename__ = "plant_equipment"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    plant_id       = Column(String, nullable=False, index=True)
    equipment_level = Column(String, nullable=False)
    equipment_id   = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("plant_id", "equipment_level", "equipment_id"),
    )


# ── Pre-computed raw data stats per plant (avoids full table scan in summary) ──
class RawDataStats(Base):
    __tablename__ = "raw_data_stats"

    plant_id    = Column(String, primary_key=True)
    total_rows  = Column(Integer, default=0)
    min_ts      = Column(String, nullable=True)
    max_ts      = Column(String, nullable=True)
    levels_json = Column(Text, nullable=True)   # JSON: {"inverter": 4, "scb": 20}
    updated_at  = Column(DateTime, server_default=func.now(), onupdate=func.now())


# ── SCB Fault Review (engineer review / sign-off) ─────────────────────────────
class ScbFaultReview(Base):
    """
    Stores a single engineer review per SCB per plant + date range.
    review_status: 'valid_fault' | 'other_fault' | 'no_fault'
    Upserted (update on same plant_id+scb_id+date_from+date_to).
    """
    __tablename__ = "scb_fault_reviews"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    plant_id      = Column(String, nullable=False, index=True)
    scb_id        = Column(String, nullable=False, index=True)
    date_from     = Column(String, nullable=False)
    date_to       = Column(String, nullable=False)
    review_status = Column(String, nullable=False)   # valid_fault | other_fault | no_fault
    remarks       = Column(Text, nullable=True)
    reviewed_by   = Column(String, nullable=True)    # user email
    reviewed_at   = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("plant_id", "scb_id", "date_from", "date_to", name="uq_scb_review"),
    )


# ── Precomputed JSON payloads for PL / IS / GB tabs (survives API restarts) ───
class FaultRuntimeSnapshot(Base):
    """
    Stores last computed tab payloads for raw-data-derived fault analyses.
    Invalidated when raw data is uploaded for the plant (see metadata router).
    """

    __tablename__ = "fault_runtime_snapshot"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(String, nullable=False, index=True)
    date_from = Column(String, nullable=False)
    date_to = Column(String, nullable=False)
    kind = Column(String(32), nullable=False)  # pl_page | is_tab | gb_tab
    payload_json = Column(Text, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("plant_id", "date_from", "date_to", "kind", name="uq_fault_runtime_snapshot"),
        Index("idx_fault_rt_snap_plant_updated", "plant_id", "updated_at"),
    )


# ── Module snapshots (invalid when raw_data_stats.updated_at is newer) ────────
# Populated after raw-data ingest (background) and on first API miss. Keeps
# heavy aggregates off the request path. Apply schema only to the DB in DATABASE_URL
# (e.g. AWS RDS), not a shared local Postgres used by another app.


class DsSummarySnapshot(Base):
    """Cached JSON for `/api/faults/ds-summary` per plant + date range."""

    __tablename__ = "ds_summary_snapshot"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(String, nullable=False, index=True)
    date_from = Column(String(32), nullable=False, default="")
    date_to = Column(String(32), nullable=False, default="")
    payload_json = Column(Text, nullable=False)
    computed_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("plant_id", "date_from", "date_to", name="uq_ds_summary_snapshot"),
    )


class DsStatusSnapshot(Base):
    """Cached JSON for `/api/faults/ds-scb-status` per plant + date range."""

    __tablename__ = "ds_status_snapshot"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(String, nullable=False, index=True)
    date_from = Column(String(32), nullable=False, default="")
    date_to = Column(String(32), nullable=False, default="")
    payload_json = Column(Text, nullable=False)
    computed_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("plant_id", "date_from", "date_to", name="uq_ds_status_snapshot"),
    )


class UnifiedFaultSnapshot(Base):
    """Cached JSON for `/api/faults/unified-feed` (categories + rows + totals)."""

    __tablename__ = "unified_fault_snapshot"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(String, nullable=False, index=True)
    date_from = Column(String(32), nullable=False)
    date_to = Column(String(32), nullable=False)
    payload_json = Column(Text, nullable=False)
    computed_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("plant_id", "date_from", "date_to", name="uq_unified_fault_snapshot"),
    )


class LossAnalysisSnapshot(Base):
    """Cached JSON for loss-analysis `/bridge` (one scope + optional equipment)."""

    __tablename__ = "loss_analysis_snapshot"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(String, nullable=False, index=True)
    date_from = Column(String(32), nullable=False)
    date_to = Column(String(32), nullable=False)
    scope = Column(String(16), nullable=False, default="plant")
    equipment_id = Column(String(512), nullable=False, default="")
    payload_json = Column(Text, nullable=False)
    computed_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "plant_id", "date_from", "date_to", "scope", "equipment_id",
            name="uq_loss_analysis_snapshot",
        ),
    )


class UnifiedFeedCategoryTotal(Base):
    """
    Narrow SQL-friendly copy of unified-feed category tiles (loss_mwh, fault_count per category_id).
    Filled by module precompute alongside unified_fault_snapshot JSON; use for reporting / BI
    without parsing payload_json. Detail rows stay in the JSON snapshot only.
    """

    __tablename__ = "unified_feed_category_totals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(String, nullable=False, index=True)
    date_from = Column(String(32), nullable=False)
    date_to = Column(String(32), nullable=False)
    category_id = Column(String(32), nullable=False)
    loss_mwh = Column(Float, nullable=False, default=0.0)
    fault_count = Column(Integer, nullable=False, default=0)
    computed_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "plant_id", "date_from", "date_to", "category_id",
            name="uq_unified_feed_category_totals",
        ),
        Index("idx_ufct_plant_computed", "plant_id", "computed_at"),
    )


class PlantComputeStatus(Base):
    """Last background module precompute for a plant (for Metadata UI status)."""

    __tablename__ = "plant_compute_status"

    plant_id = Column(String, primary_key=True)
    status = Column(String(32), nullable=False, default="idle")  # idle|running|done|error
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    last_range_json = Column(Text, nullable=True)  # JSON: {"date_from","date_to"}


class PrecomputeJob(Base):
    """
    Durable queue for module snapshot recompute (`python -m jobs.precompute_runner`).
    Pending rows for the same plant are merged (expanded date range) on enqueue.
    """

    __tablename__ = "precompute_jobs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    plant_id = Column(String, nullable=False, index=True)
    date_from = Column(String(32), nullable=False)
    date_to = Column(String(32), nullable=False)
    status = Column(String(16), nullable=False, default="pending")  # pending|running|done|failed
    attempts = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=5)
    worker_id = Column(String(64), nullable=True)
    locked_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("idx_precompute_jobs_status_created", "status", "created_at"),
        Index("idx_precompute_jobs_plant_status", "plant_id", "status"),
    )
