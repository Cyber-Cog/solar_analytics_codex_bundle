"""
backend/schemas.py
==================
Pydantic request/response models.
All API endpoints use these for validation and serialization.
"""

from pydantic import BaseModel
from typing import Optional, List, Any
from datetime import datetime


# ── Auth ──────────────────────────────────────────────────────────────────────
class UserCreate(BaseModel):
    email: str
    full_name: Optional[str] = None
    password: str
    is_admin: Optional[bool] = False
    allowed_plants: Optional[str] = None


class UserUpdate(BaseModel):
    email: Optional[str] = None
    full_name: Optional[str] = None
    password: Optional[str] = None
    is_active: Optional[bool] = None
    is_admin: Optional[bool] = None
    allowed_plants: Optional[str] = None


class UserResponse(BaseModel):
    id: int
    email: str
    full_name: Optional[str]
    is_active: bool
    is_admin: bool
    allowed_plants: Optional[str]

    class Config:
        from_attributes = True


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


# ── Plants ────────────────────────────────────────────────────────────────────
class PlantCreate(BaseModel):
    plant_id: str
    name: str
    technology: Optional[str] = "Solar PV"
    location: Optional[str] = None
    capacity_mwp: Optional[float] = None
    cod_date: Optional[str] = None
    ppa_tariff: Optional[float] = None
    status: Optional[str] = "Active"
    plant_type: Optional[str] = "SCB"


class PlantResponse(BaseModel):
    id: int
    plant_id: str
    name: str
    technology: Optional[str]
    location: Optional[str]
    capacity_mwp: Optional[float]
    cod_date: Optional[str]
    ppa_tariff: Optional[float]
    status: Optional[str]
    plant_type: Optional[str] = "SCB"

    class Config:
        from_attributes = True


# ── Dashboard ─────────────────────────────────────────────────────────────────
class StationDetails(BaseModel):
    plant_id: str
    name: str
    technology: str
    status: str
    capacity_mwp: Optional[float]
    cod_date: Optional[str]
    ppa_tariff: Optional[float]
    plant_age_years: Optional[float]
    location: Optional[str]


class EnergyDataPoint(BaseModel):
    date: str
    actual_kwh: Optional[float]
    target_kwh: Optional[float]
    actual_mwh: Optional[float] = None
    target_mwh: Optional[float] = None


class WeatherDataPoint(BaseModel):
    timestamp: str
    ghi: Optional[float]
    gti: Optional[float]
    ambient_temp: Optional[float]
    module_temp: Optional[float]
    wind_speed: Optional[float]


class KPIData(BaseModel):
    energy_export_kwh: Optional[float]
    net_generation_kwh: Optional[float]
    energy_export_mwh: Optional[float] = None
    net_generation_mwh: Optional[float] = None
    total_inverter_generation_mwh: Optional[float] = None
    active_power_kw: Optional[float]
    peak_power_kw: Optional[float]
    performance_ratio: Optional[float]
    plant_load_factor: Optional[float]
    total_inverter_generation_kwh: Optional[float]
    insolation_kwh_m2: Optional[float] = None  # period insolation for PR reference


class WMSKPIData(BaseModel):
    # Insolation kWh/m²: Σ(signal W/m²) / 60000 for 1‑minute samples (GHI vs GTI sums separate)
    gti: Optional[float] = None
    ghi: Optional[float] = None
    # Mean W/m² over all timestamps (AVG); tilt uses `gti`, else `irradiance` if no gti rows
    irradiance_tilt: Optional[float] = None
    irradiance_horizontal: Optional[float] = None  # AVG(ghi)
    ambient_temp: Optional[float] = None
    module_temp: Optional[float] = None
    wind_speed: Optional[float] = None
    # Σ mm over range for common rain / precipitation signal names (when present in raw WMS rows)
    rainfall_mm: Optional[float] = None


class InverterRow(BaseModel):
    inverter_id: str
    dc_power_kw: Optional[float]
    ac_power_kw: Optional[float]
    generation_kwh: Optional[float] = None
    dc_capacity_kwp: Optional[float] = None
    efficiency_pct: Optional[float]
    yield_kwh_kwp: Optional[float]
    pr_pct: Optional[float]
    plf_pct: Optional[float] = None  # energy / (dc_kWp × 12 h × days) × 100


class PowerVsGTIPoint(BaseModel):
    timestamp: str
    active_power_kw: Optional[float]
    gti: Optional[float]


class LossWaterfallInput(BaseModel):
    plant_capacity_kwp: float
    irradiance_loss_pct: float = 0.0
    curtailment_pct: float = 0.0
    inverter_loss_pct: float = 0.0
    grid_loss_pct: float = 0.0
    soiling_loss_pct: float = 0.0


class LossWaterfallPoint(BaseModel):
    category: str
    value: float
    cumulative: float


# ── Analytics ─────────────────────────────────────────────────────────────────
class EquipmentListResponse(BaseModel):
    equipment_ids: List[str]
    total: int


class TimeseriesPoint(BaseModel):
    timestamp: str
    equipment_id: str
    signal: str
    value: Optional[float]


class TimeseriesResponse(BaseModel):
    data: List[TimeseriesPoint]
    availability_pct: float
    date_range: dict


class NormalizedTimeseriesPoint(BaseModel):
    timestamp: str
    equipment_id: str
    normalized_value: Optional[float]
    raw_value: Optional[float]
    irradiance: Optional[float]


# ── Metadata ──────────────────────────────────────────────────────────────────
class ArchitectureRow(BaseModel):
    id: Optional[int]
    plant_id: str
    inverter_id: str
    scb_id: str
    string_id: str
    modules_per_string: Optional[int]
    strings_per_scb: Optional[int]
    scbs_per_inverter: Optional[int]
    dc_capacity_kw: Optional[float]

    class Config:
        from_attributes = True


class EquipmentSpecRow(BaseModel):
    id: Optional[int]
    plant_id: str
    equipment_id: str
    equipment_type: str
    manufacturer: Optional[str]
    model: Optional[str]
    rated_power: Optional[float]
    imp: Optional[float]
    vmp: Optional[float]
    isc: Optional[float]
    voc: Optional[float]
    target_efficiency: Optional[float] = 98.5

    # Inverter-specific (optional)
    ac_capacity_kw: Optional[float] = None
    dc_capacity_kwp: Optional[float] = None
    rated_efficiency: Optional[float] = None
    mppt_voltage_min: Optional[float] = None
    mppt_voltage_max: Optional[float] = None
    voltage_limit: Optional[float] = None
    current_set_point: Optional[float] = None
    spec_sheet_path: Optional[str] = None
    degradation_loss_pct: Optional[float] = None
    temp_coefficient_per_deg: Optional[float] = None

    # Module-specific (optional)
    impp: Optional[float] = None
    vmpp: Optional[float] = None
    pmax: Optional[float] = None
    degradation_year1_pct: Optional[float] = None
    degradation_year2_pct: Optional[float] = None
    degradation_annual_pct: Optional[float] = None
    module_efficiency_pct: Optional[float] = None
    alpha_stc: Optional[float] = None
    beta_stc: Optional[float] = None
    gamma_stc: Optional[float] = None
    alpha_noct: Optional[float] = None
    beta_noct: Optional[float] = None
    gamma_noct: Optional[float] = None

    class Config:
        from_attributes = True


# ── Tickets ───────────────────────────────────────────────────────────────────
class TicketCreate(BaseModel):
    subject: str
    description: str
    plant_id: Optional[str] = None
    user_email: str
    recipient_emails: Optional[List[str]] = None


class TicketResponse(BaseModel):
    id: int
    subject: str
    status: str
    created_at: Optional[datetime]

    class Config:
        from_attributes = True


# ── Generic ───────────────────────────────────────────────────────────────────
class MessageResponse(BaseModel):
    message: str
    success: bool = True
