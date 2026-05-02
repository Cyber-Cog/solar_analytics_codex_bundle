/**
 * Typed API client for the Solar Analytics FastAPI backend.
 * All calls go through /api/* and /auth/* which are proxied by next.config.ts.
 * Bearer token is read from localStorage (client) or passed explicitly (server).
 */

import type {
  LoginRequest,
  TokenResponse,
  PlantResponse,
  TimeseriesResponse,
  FaultDiagnostic,
  FaultEvent,
  EquipmentListResponse,
  ArchitectureRow,
  TicketResponse,
  MessageResponse,
  DashboardData,
} from "@/types";

// --------------------------------------------------------------------------
// Core fetcher
// --------------------------------------------------------------------------

class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}

function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem("solar_token");
}

async function apiFetch<T>(
  path: string,
  options: RequestInit & { token?: string } = {}
): Promise<T> {
  const { token: explicitToken, ...fetchOptions } = options;
  const token = explicitToken ?? getToken();

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(fetchOptions.headers as Record<string, string>),
  };
  if (token) headers["Authorization"] = `Bearer ${token}`;

  const res = await fetch(path, { ...fetchOptions, headers });

  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      msg = body.detail ?? body.message ?? msg;
    } catch { /* ignore */ }
    throw new ApiError(res.status, msg);
  }

  // 204 No Content
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

function post<T>(path: string, body: unknown, opts: RequestInit = {}) {
  return apiFetch<T>(path, { method: "POST", body: JSON.stringify(body), ...opts });
}

function put<T>(path: string, body: unknown, opts: RequestInit = {}) {
  return apiFetch<T>(path, { method: "PUT", body: JSON.stringify(body), ...opts });
}

function del<T>(path: string, opts: RequestInit = {}) {
  return apiFetch<T>(path, { method: "DELETE", ...opts });
}

function get<T>(path: string, params?: Record<string, string | number | boolean | undefined | null>, opts: RequestInit = {}) {
  const url = new URL(path, typeof window !== "undefined" ? window.location.origin : "http://localhost:3000");
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
    }
  }
  return apiFetch<T>(url.pathname + url.search, opts);
}

// --------------------------------------------------------------------------
// Auth
// --------------------------------------------------------------------------
export const Auth = {
  login: (req: LoginRequest) => post<TokenResponse>("/auth/login", req),
  me: () => get<{ id: number; email: string; full_name: string; is_admin: boolean; allowed_plants: string[] }>("/auth/me"),
  saveSession: (res: TokenResponse) => {
    localStorage.setItem("solar_token", res.access_token);
    localStorage.setItem("solar_user", JSON.stringify(res.user));
  },
  clearSession: () => {
    localStorage.removeItem("solar_token");
    localStorage.removeItem("solar_user");
  },
  getUser: () => {
    try {
      const u = localStorage.getItem("solar_user");
      return u ? JSON.parse(u) : null;
    } catch { return null; }
  },
};

// --------------------------------------------------------------------------
// Plants
// --------------------------------------------------------------------------
export const Plants = {
  list: () => get<PlantResponse[]>("/api/plants"),
  get:  (id: string) => get<PlantResponse>(`/api/plants/${id}`),
  update: (id: string, data: Partial<PlantResponse>) => put<PlantResponse>(`/api/plants/${id}`, data),
};

// --------------------------------------------------------------------------
// Dashboard
// --------------------------------------------------------------------------
export const Dashboard = {
  summary: (plantId: string) =>
    get<DashboardData>("/api/dashboard/summary", { plant_id: plantId }),
  inverters: (plantId: string) =>
    get<unknown>("/api/dashboard/inverters", { plant_id: plantId }),
  weather: (plantId: string, date?: string) =>
    get<unknown>("/api/dashboard/weather", { plant_id: plantId, date }),
  unifiedFeed: (plantId: string) =>
    get<unknown>("/api/dashboard/unified-feed", { plant_id: plantId }),
};

// --------------------------------------------------------------------------
// Analytics
// --------------------------------------------------------------------------
export const Analytics = {
  equipmentList: (plantId: string, level: string) =>
    get<EquipmentListResponse>("/api/analytics/equipment-list", { plant_id: plantId, equipment_level: level }),
  timeseries: (params: {
    plant_id: string;
    equipment_id?: string;
    signal: string;
    date_from: string;
    date_to: string;
    equipment_level?: string;
    resolution?: string;
  }) => get<TimeseriesResponse>("/api/analytics/timeseries", params),
  parameters: (plantId: string) =>
    get<unknown>("/api/analytics/parameters", { plant_id: plantId }),
};

// --------------------------------------------------------------------------
// Faults / DS Diagnostics
// --------------------------------------------------------------------------
export const Faults = {
  diagnostics: (params: {
    plant_id: string;
    date_from?: string;
    date_to?: string;
    fault_status?: string;
    inverter_id?: string;
  }) => get<FaultDiagnostic[]>("/api/faults/diagnostics", params),

  dsStatus: (plantId: string) =>
    get<unknown>("/api/faults/ds-status", { plant_id: plantId }),

  faultEvents: (params: { plant_id: string; date_from?: string; date_to?: string }) =>
    get<FaultEvent[]>("/api/faults/events", params),

  unifiedFeed: (plantId: string) =>
    get<unknown>("/api/faults/unified-feed", { plant_id: plantId }),

  filterSummary: (plantId: string) =>
    get<unknown>("/api/faults/filter-summary", { plant_id: plantId }),
};

// --------------------------------------------------------------------------
// Loss Analysis
// --------------------------------------------------------------------------
export const LossAnalysis = {
  waterfall: (params: { plant_id: string; date_from: string; date_to: string }) =>
    get<unknown>("/api/loss-analysis/waterfall", params),
  summary: (params: { plant_id: string; date_from: string; date_to: string }) =>
    get<unknown>("/api/loss-analysis/summary", params),
};

// --------------------------------------------------------------------------
// Metadata / Architecture
// --------------------------------------------------------------------------
export const Metadata = {
  architecture: (plantId: string) =>
    get<ArchitectureRow[]>("/api/metadata/architecture", { plant_id: plantId }),
  specs: (plantId: string) =>
    get<unknown>("/api/metadata/specs", { plant_id: plantId }),
  inverterSpecs: (plantId: string) =>
    get<unknown>("/api/metadata/inverter-specs", { plant_id: plantId }),
};

// --------------------------------------------------------------------------
// Tickets
// --------------------------------------------------------------------------
export const Tickets = {
  list: (plantId: string) =>
    get<TicketResponse[]>("/api/tickets", { plant_id: plantId }),
  create: (data: { plant_id: string; title: string; description?: string; priority?: string }) =>
    post<TicketResponse>("/api/tickets", data),
  update: (id: number, data: Partial<TicketResponse>) =>
    put<TicketResponse>(`/api/tickets/${id}`, data),
};

// --------------------------------------------------------------------------
// Reports
// --------------------------------------------------------------------------
export const Reports = {
  generate: (params: { plant_id: string; report_type: string; date_from: string; date_to: string }) =>
    get<unknown>("/api/reports/generate", params),
  list: (plantId: string) =>
    get<unknown[]>("/api/reports", { plant_id: plantId }),
};

// --------------------------------------------------------------------------
// Admin
// --------------------------------------------------------------------------
export const Admin = {
  users: () => get<unknown[]>("/api/admin/users"),
  createUser: (data: unknown) => post<unknown>("/api/admin/users", data),
  updateUser: (id: number, data: unknown) => put<unknown>(`/api/admin/users/${id}`, data),
  deleteUser: (id: number) => del<MessageResponse>(`/api/admin/users/${id}`),
  plants: () => get<PlantResponse[]>("/api/admin/plants"),
  updatePlant: (plantId: string, data: Partial<PlantResponse>) =>
    put<PlantResponse>(`/api/admin/plants/${plantId}`, data),
  deletePlant: (plantId: string) => del<MessageResponse>(`/api/admin/plants/${plantId}`),
  perfStatus: () => get<unknown>("/api/admin/perf/timescale-status"),
};

// --------------------------------------------------------------------------
// Site / Appearance
// --------------------------------------------------------------------------
export const Site = {
  appearance: () => get<unknown>("/api/site/appearance"),
  updateAppearance: (data: unknown) => put<unknown>("/api/site/appearance", data),
};

export { ApiError };
