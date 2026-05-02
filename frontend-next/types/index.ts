// TypeScript types mirroring backend Pydantic schemas

export interface LoginRequest {
  email: string;
  password: string;
}

export interface UserResponse {
  id: number;
  email: string;
  full_name: string;
  is_active: boolean;
  is_admin: boolean;
  allowed_plants: string[];
}

export interface TokenResponse {
  access_token: string;
  token_type: string;
  user: UserResponse;
}

export interface PlantResponse {
  id: number;
  plant_id: string;
  name: string;
  technology?: string;
  location?: string;
  capacity_mwp?: number;
  cod_date?: string;
  ppa_tariff?: number;
  status?: string;
  plant_type?: string;
}

export interface KPIData {
  current_power_kw?: number;
  today_energy_kwh?: number;
  today_yield?: number;
  today_irradiation?: number;
  performance_ratio?: number;
  cuf?: number;
  availability?: number;
  plant_status?: string;
}

export interface InverterRow {
  inverter_id: string;
  power_kw?: number;
  energy_kwh?: number;
  efficiency?: number;
  dc_voltage?: number;
  dc_current?: number;
  status?: string;
}

export interface TimeseriesPoint {
  timestamp: string;
  equipment_id: string;
  signal: string;
  value: number;
}

export interface TimeseriesResponse {
  data: TimeseriesPoint[];
  availability_pct?: number;
  date_range?: { from: string; to: string };
}

export interface FaultDiagnostic {
  timestamp: string;
  inverter_id: string;
  scb_id: string;
  virtual_string_current?: number;
  expected_current?: number;
  missing_current?: number;
  missing_strings?: number;
  power_loss_kw?: number;
  energy_loss_kwh?: number;
  fault_status: "NORMAL" | "CONFIRMED_DS";
}

export interface FaultEvent {
  id: number;
  plant_id: string;
  inverter_id?: string;
  equipment_id: string;
  equipment_level: string;
  fault_type: string;
  start_time: string;
  end_time?: string;
  duration_minutes?: number;
  status: string;
  severity?: string;
  missing_strings?: number;
}

export interface LossWaterfallPoint {
  label: string;
  value: number;
  percent_of_reference?: number;
}

export interface EquipmentListResponse {
  equipment_ids: string[];
  total: number;
}

export interface ArchitectureRow {
  inverter_id: string;
  scb_id: string;
  strings_per_scb?: number;
  spare_flag?: boolean;
  string_id?: string;
}

export interface TicketResponse {
  id: number;
  plant_id: string;
  title: string;
  description?: string;
  status: string;
  priority?: string;
  created_at: string;
  resolved_at?: string;
}

export interface MessageResponse {
  message: string;
  success: boolean;
}

export interface DashboardData {
  station?: Record<string, unknown>;
  kpi?: KPIData;
  inverters?: InverterRow[];
  energy?: unknown[];
  weather?: unknown[];
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
}
