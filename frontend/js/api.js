// frontend/js/api.js
// API client - all calls to the FastAPI backend

// Use same-origin backend by default so UI and API always point to the same server instance.
// Override with localStorage 'solar_api_base' only when explicitly needed.
const API_BASE = localStorage.getItem('solar_api_base') || window.location.origin;

// ── Global loading progress bar ──────────────────────────────────────────────
(function () {
  // Loading bar is disabled by default to keep the UX calm.
  // Set localStorage.solar_show_api_bar = '1' to re-enable for debugging.
  if (localStorage.getItem('solar_show_api_bar') !== '1') {
    window._apiLoadingShow = function () {};
    window._apiLoadingHide = function () {};
    return;
  }
  const style = document.createElement('style');
  style.textContent = `
    #_api_bar { position:fixed; top:0; left:0; right:0; height:3px; z-index:99999;
      background:linear-gradient(90deg,#22c55e,#06b6d4,#6366f1);
      transform:scaleX(0); transform-origin:left;
      transition:transform 0.2s ease; pointer-events:none; }
    #_api_bar.active { transition:transform 0.4s ease; }
  `;
  document.head.appendChild(style);
  const bar = document.createElement('div');
  bar.id = '_api_bar';
  document.body.appendChild(bar);
  let _pending = 0, _pct = 0, _raf = null;
  function _animTo(target) {
    if (_raf) cancelAnimationFrame(_raf);
    _raf = requestAnimationFrame(() => {
      _pct = target;
      bar.style.transform = `scaleX(${_pct / 100})`;
    });
  }
  window._apiLoadingShow = function () {
    _pending++;
    bar.classList.add('active');
    _animTo(Math.min(80, _pct + 15 + Math.random() * 20));
  };
  window._apiLoadingHide = function () {
    if (--_pending <= 0) {
      _pending = 0;
      _animTo(100);
      setTimeout(() => { _animTo(0); bar.classList.remove('active'); }, 380);
    }
  };
})();

// ── Token storage ─────────────────────────────────────────────────────────────
const getToken  = () => localStorage.getItem('solar_token');
const setToken  = (t) => localStorage.setItem('solar_token', t);
const clearToken = () => localStorage.removeItem('solar_token');
const getUser   = () => { try { return JSON.parse(localStorage.getItem('solar_user')||'null'); } catch{ return null; } };
const setUser   = (u) => localStorage.setItem('solar_user', JSON.stringify(u));
const _inflightGet = new Map();

function allowLegacyUnifiedFeedFallback() {
  if (localStorage.getItem('solar_allow_unified_fallback') === '1') return true;
  const host = String(window.location.hostname || '').toLowerCase();
  return host === 'localhost' || host === '127.0.0.1';
}

// ── Route-scoped AbortControllers ────────────────────────────────────────────
// Every call that passes `{ signal }` will be cancellable individually. For the
// common case where a page just wants to abort *everything* in flight when the
// user navigates away, we also keep a per-route controller that app.js can
// rotate on each route change.
let _routeAbortController = (typeof AbortController !== 'undefined') ? new AbortController() : null;

function _currentRouteSignal() {
  return _routeAbortController ? _routeAbortController.signal : undefined;
}

/** Called by app.js on route changes — aborts every in-flight request that was
 *  started without its own explicit signal. */
window.__abortRouteRequests = function abortRouteRequests() {
  try {
    if (_routeAbortController) _routeAbortController.abort('route change');
  } catch (e) { /* noop */ }
  _routeAbortController = (typeof AbortController !== 'undefined') ? new AbortController() : null;
  // In-flight GET dedupe map points at promises that are now rejected — purge
  // so the next page gets a fresh request instead of replaying the old one.
  _inflightGet.clear();
};

// ── Base fetch ────────────────────────────────────────────────────────────────
async function apiFetch(path, options = {}) {
  const method = String(options.method || 'GET').toUpperCase();
  const token = getToken();
  const inflightKey = (method === 'GET' && !options.body && !options.signal)
    ? `${path}::${token || ''}` : null;
  if (inflightKey && _inflightGet.has(inflightKey)) return _inflightGet.get(inflightKey);

  const signal = options.signal || _currentRouteSignal();

  const req = (async () => {
  window._apiLoadingShow && window._apiLoadingShow();
  try {
    const isFormData = typeof FormData !== 'undefined' && options.body instanceof FormData;
    const customHeaders = { ...(options.headers || {}) };
    const hasContentType = Object.keys(customHeaders).some((k) => k.toLowerCase() === 'content-type');
    const baseHeaders = {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...customHeaders,
    };
    // Let the browser set multipart boundaries for FormData uploads.
    if (!isFormData && !hasContentType) {
      baseHeaders['Content-Type'] = 'application/json';
    }

    const res = await fetch(API_BASE + path, {
      headers: baseHeaders,
      ...options,
      signal,
    });
    if (res.status === 401 && !path.startsWith('/auth/')) { clearToken(); console.error('401 ON', path); }
    if (!res.ok) {
      let err = null;
      try {
        err = await res.json();
      } catch (_) {
        const txt = await res.text().catch(() => '');
        err = { detail: txt || res.statusText };
      }
      let msg = err.detail ?? err.message ?? res.statusText;
      if (Array.isArray(msg)) {
        msg = msg.map((x) => (x && typeof x === 'object' && (x.msg || x.message)) ? (x.msg || x.message) : JSON.stringify(x)).join('; ');
      } else if (msg && typeof msg === 'object') {
        msg = JSON.stringify(msg);
      }
      const e = new Error(String(msg || 'API Error'));
      e.status = res.status;
      throw e;
    }
    return res.json();
  } finally {
    window._apiLoadingHide && window._apiLoadingHide();
  }
  })();

  if (inflightKey) {
    _inflightGet.set(inflightKey, req);
    req.finally(() => _inflightGet.delete(inflightKey));
  }
  return req;
}

/** Try paths in order; on 404 only, attempt the next (Loss Analysis primary + dashboard alias). */
async function apiFetchFirstOk(paths, options = {}) {
  let lastErr = null;
  for (let i = 0; i < paths.length; i++) {
    try {
      return await apiFetch(paths[i], options);
    } catch (e) {
      lastErr = e;
      if (e && e.status === 404 && i < paths.length - 1) continue;
      throw e;
    }
  }
  throw lastErr || new Error('API Error');
}

// ── Auth ──────────────────────────────────────────────────────────────────────
const Auth = {
  login:  (email, password) => apiFetch('/auth/login',  { method:'POST', body: JSON.stringify({ email, password }) }),
  signup: (email, fullName, password) => apiFetch('/auth/signup', { method:'POST', body: JSON.stringify({ email, full_name: fullName, password }) }),
  me:     () => apiFetch('/auth/me'),
};

// ── Plants ────────────────────────────────────────────────────────────────────
const Plants = {
  list:   () => apiFetch('/api/plants'),
  create: (data) => apiFetch('/api/plants', { method:'POST', body: JSON.stringify(data) }),
};

// ── Dashboard ─────────────────────────────────────────────────────────────────
const Dashboard = {
  /** @param opts {{ lite?: boolean }} lite=true → omit target_generation in bundle (faster; use targetGeneration() after) */
  bundle:            (plantId, from, to, opts) => {
    const q = new URLSearchParams({
      plant_id: plantId || '',
      date_from: from || '',
      date_to: to || '',
    });
    if (opts && opts.lite) q.set('include_target_generation', '0');
    return apiFetch(`/api/dashboard/bundle?${q.toString()}`);
  },
  targetGeneration:  (plantId, from, to) =>
    apiFetch(`/api/dashboard/target-generation?plant_id=${encodeURIComponent(plantId || '')}&date_from=${encodeURIComponent(from || '')}&date_to=${encodeURIComponent(to || '')}`),
  stationDetails:    (plantId) => apiFetch(`/api/dashboard/station-details?plant_id=${encodeURIComponent(plantId || '')}`),
  energy:            (plantId, from, to) =>
    apiFetch(`/api/dashboard/energy?plant_id=${encodeURIComponent(plantId || '')}&date_from=${encodeURIComponent(from || '')}&date_to=${encodeURIComponent(to || '')}`),
  weather:           (plantId, from, to) =>
    apiFetch(`/api/dashboard/weather?plant_id=${encodeURIComponent(plantId || '')}&date_from=${encodeURIComponent(from || '')}&date_to=${encodeURIComponent(to || '')}`),
  kpis:              (plantId, from, to) =>
    apiFetch(`/api/dashboard/kpis?plant_id=${encodeURIComponent(plantId || '')}&date_from=${encodeURIComponent(from || '')}&date_to=${encodeURIComponent(to || '')}`),
  wmsKpis:           (plantId, from, to) =>
    apiFetch(`/api/dashboard/wms-kpis?plant_id=${encodeURIComponent(plantId || '')}&date_from=${encodeURIComponent(from || '')}&date_to=${encodeURIComponent(to || '')}`),
  inverterPerf:      (plantId, from, to) =>
    apiFetch(`/api/dashboard/inverter-performance?plant_id=${encodeURIComponent(plantId || '')}&date_from=${encodeURIComponent(from || '')}&date_to=${encodeURIComponent(to || '')}`),
  powerVsGti:        (plantId, from, to) =>
    apiFetch(`/api/dashboard/power-vs-gti?plant_id=${encodeURIComponent(plantId || '')}&date_from=${encodeURIComponent(from || '')}&date_to=${encodeURIComponent(to || '')}`),
  lossWaterfall:     (data) => apiFetch('/api/dashboard/loss-waterfall', { method:'POST', body: JSON.stringify(data) }),
};

// ── Loss Analysis (energy bridge vs Fault Diagnostics) ─────────────────────────
const LossAnalysis = {
  // Dashboard alias first: some gateways only forward `/api/dashboard/*` to the API.
  options: (plantId) =>
    apiFetchFirstOk([
      `/api/dashboard/loss-analysis/options?plant_id=${encodeURIComponent(plantId || '')}`,
      `/api/loss-analysis/options?plant_id=${encodeURIComponent(plantId || '')}`,
    ]),
  bridge: (plantId, dateFrom, dateTo, scope, equipmentId) => {
    const q = new URLSearchParams({ plant_id: plantId || '', scope: scope || 'plant' });
    if (dateFrom) q.set('date_from', dateFrom);
    if (dateTo) q.set('date_to', dateTo);
    if (equipmentId) q.set('equipment_id', equipmentId);
    const qs = q.toString();
    return apiFetchFirstOk([
      `/api/dashboard/loss-analysis/bridge?${qs}`,
      `/api/loss-analysis/bridge?${qs}`,
    ]);
  },
};

// ── Analytics ─────────────────────────────────────────────────────────────────
const Analytics = {
  equipment:  (level, plantId) =>
    apiFetch(`/api/analytics/equipment?level=${encodeURIComponent(level)}&plant_id=${encodeURIComponent(plantId)}`),
  signals:    (level, plantId) =>
    apiFetch(`/api/analytics/signals?level=${encodeURIComponent(level)}&plant_id=${encodeURIComponent(plantId)}`),
  timeseries: (ids, signals, plantId, from, to, normalize=false, dcSource='raw', level='inverter') =>
    apiFetch(
      `/api/analytics/timeseries?equipment_ids=${encodeURIComponent(ids)}&signals=${encodeURIComponent(signals)}` +
      `&plant_id=${encodeURIComponent(plantId)}&date_from=${encodeURIComponent(from || '')}&date_to=${encodeURIComponent(to || '')}` +
      `&normalize=${normalize}&dc_source=${encodeURIComponent(dcSource || 'raw')}&level=${encodeURIComponent(level || 'inverter')}`
    ),
};

// ── Metadata ──────────────────────────────────────────────────────────────────
const Metadata = {
  architectureCompact: (plantId) => apiFetch(`/api/metadata/architecture/compact?plant_id=${plantId}`),
  architecture:      (plantId, limit, offset) => apiFetch(`/api/metadata/architecture?plant_id=${plantId || ''}&limit=${limit || 10000}&offset=${offset || 0}`),
  addArchitecture:   (data) => apiFetch('/api/metadata/architecture', { method:'POST', body: JSON.stringify(data) }),
  equipment:         (plantId) => apiFetch(`/api/metadata/equipment${plantId?`?plant_id=${plantId}`:''}`),
  addEquipment:      (data) => apiFetch('/api/metadata/equipment', { method:'POST', body: JSON.stringify(data) }),
  deleteEquipment:   (specId) => apiFetch(`/api/metadata/equipment/${specId}`, { method:'DELETE' }),
  uploadSpecSheet:   (specId, file) => {
    const f = new FormData(); f.append('file', file);
    return apiFetch(`/api/metadata/equipment/${specId}/spec-sheet`, { method:'POST', body: f });
  },
  specSheetUrl:      (specId) => `${API_BASE}/api/metadata/equipment/${specId}/spec-sheet?token=${getToken()}`,
  rawSummary:        (plantId) => apiFetch(`/api/metadata/raw-data-summary${plantId?`?plant_id=${plantId}`:''}`),
  reindexRawEquipment: (plantId) =>
    apiFetch(`/api/metadata/reindex-raw-equipment?plant_id=${encodeURIComponent(plantId)}`, { method: 'POST' }),
  scbMetadata:       (plantId, scbId) => apiFetch(`/api/metadata/scb-metadata?plant_id=${plantId}&scb_id=${scbId}`),
  rawDataPreview:    (plantId, opts = {}) => {
    const q = new URLSearchParams({ plant_id: plantId, limit: String(opts.limit || 200) });
    if (opts.equipmentLevel) q.set('equipment_level', opts.equipmentLevel);
    if (opts.equipmentId) q.set('equipment_id', opts.equipmentId);
    if (opts.signal) q.set('signal', opts.signal);
    if (opts.dateFrom) q.set('date_from', opts.dateFrom);
    if (opts.dateTo) q.set('date_to', opts.dateTo);
    return apiFetch(`/api/metadata/raw-data-preview?${q.toString()}`);
  },
  analyzeRawData:    (file) => {
    const f = new FormData(); f.append('file', file);
    return apiFetch('/api/metadata/upload-raw-data-analyze', { method:'POST', body: f });
  },
  uploadMappedRawData: (plantId, mapping, file) => {
    const f = new FormData(); 
    f.append('plant_id', plantId);
    f.append('mapping', JSON.stringify(mapping));
    f.append('file', file);
    return apiFetch('/api/metadata/upload-raw-data-mapped', { method:'POST', body: f });
  },
  templateArch:      () => `${API_BASE}/api/metadata/template/architecture?token=${getToken()}`,
  templateEquipment: () => `${API_BASE}/api/metadata/template/equipment?token=${getToken()}`,
  templateRawData:   () => `${API_BASE}/api/metadata/template/raw-data?token=${getToken()}`,
  uploadGenericRawData: (plantId, file) => {
    const f = new FormData();
    f.append('plant_id', plantId);
    f.append('file', file);
    return apiFetch('/api/metadata/upload-raw-data-generic', { method: 'POST', body: f });
  },
  runFaultComputation: (plantId, dateFrom, dateTo) => {
    const q = new URLSearchParams({ plant_id: plantId });
    if (dateFrom) q.set('date_from', dateFrom);
    if (dateTo) q.set('date_to', dateTo);
    return apiFetch(`/api/metadata/run-fault-computation?${q.toString()}`, { method: 'POST' });
  },
};

// ── Tickets ───────────────────────────────────────────────────────────────────
const Tickets = {
  raise: (subject, description, plantId, recipientEmails = []) => apiFetch('/api/tickets', {
    method: 'POST',
    body: JSON.stringify({
      subject,
      description,
      plant_id: plantId,
      user_email: getUser()?.email || '',
      recipient_emails: recipientEmails,
    }),
  }),
};

/** Query fragment for fault date range (plant_id added by caller). */
function _faultDateQs(from, to) {
  let s = '';
  if (from) s += `&date_from=${encodeURIComponent(from)}`;
  if (to) s += `&date_to=${encodeURIComponent(to)}`;
  return s;
}

/**
 * Builds the same JSON shape as GET /api/faults/unified-feed using existing endpoints.
 * Used when the unified route is missing (404) — e.g. API not restarted after deploy.
 */
async function buildUnifiedFeedClientSide(plantId, from, to) {
  const MAX_ROWS = 500;
  const pid = encodeURIComponent(plantId || '');
  const dq = _faultDateQs(from, to);
  const [dsSummary, dsPack, plPage, isSummary, isInv, gbSummary, gbEv, commSummary, commEv, soilingPlant, soilingRank, invEff] = await Promise.all([
    apiFetch(`/api/faults/ds-summary?plant_id=${pid}${from && to ? `&date_from=${encodeURIComponent(from)}&date_to=${encodeURIComponent(to)}` : ''}`).catch(() => ({})),
    apiFetch(`/api/faults/ds-scb-status?plant_id=${pid}${dq}`).catch(() => ({ data: [] })),
    apiFetch(`/api/faults/pl-page?plant_id=${pid}${dq}`).catch(() => ({ summary: {}, inverter_status: { data: [] } })),
    apiFetch(`/api/faults/is-summary?plant_id=${pid}${dq}`).catch(() => ({})),
    apiFetch(`/api/faults/is-inverter-status?plant_id=${pid}${dq}`).catch(() => ({ data: [] })),
    apiFetch(`/api/faults/gb-summary?plant_id=${pid}${dq}`).catch(() => ({})),
    apiFetch(`/api/faults/gb-events?plant_id=${pid}${dq}`).catch(() => ({ data: [] })),
    apiFetch(`/api/faults/comm-summary?plant_id=${pid}${dq}`).catch(() => ({})),
    apiFetch(`/api/faults/comm-events?plant_id=${pid}${dq}`).catch(() => ({ data: [] })),
    apiFetch(`/api/faults/soiling-plant-pr?plant_id=${pid}${dq}`).catch(() => ({})),
    apiFetch(`/api/faults/soiling-rankings?plant_id=${pid}${dq}&group_by=scb`).catch(() => ({ rows: [] })),
    apiFetch(`/api/faults/inverter-efficiency-analysis?plant_id=${pid}${from && to ? `&date_from=${encodeURIComponent(from)}&date_to=${encodeURIComponent(to)}` : ''}`).catch(() => ({})),
  ]);

  const series = dsSummary.daily_energy_series || [];
  const dsEnergyOk = !!dsSummary.energy_available;
  const dsLossMwh = dsEnergyOk
    ? Math.round(series.reduce((s, d) => s + (Number(d.energy_loss_kwh) || 0), 0) / 1000 * 10000) / 10000
    : 0;
  const dsCount = Number(dsSummary.active_ds_faults || 0);

  const plSum = plPage.summary || {};
  const plLossMwh = Math.round(Number(plSum.total_energy_loss_kwh || 0) / 1000 * 10000) / 10000;
  const plCount = Number(plSum.active_pl_inverters || 0);

  const isCount = Number((isSummary && isSummary.active_shutdown_inverters) || 0);
  const isHours = Number((isSummary && isSummary.total_shutdown_hours) || 0);

  const gbCount = Number((gbSummary && gbSummary.active_grid_events) || 0);
  const gbHours = Number((gbSummary && gbSummary.total_grid_breakdown_hours) || 0);

  const commCount = Number((commSummary && commSummary.total_communication_issues) || 0);
  const commLossMwh = Math.round(Number((commSummary && commSummary.total_loss_kwh) || 0) / 1000 * 10000) / 10000;

  const solLoss = soilingPlant.soiling_loss_mwh;
  const solLossMwh = solLoss != null ? Math.round(Number(solLoss) * 10000) / 10000 : 0;
  const solRowsRaw = soilingRank.rows || [];
  const solCount = solRowsRaw.filter((r) => Number(r.loss_mwh || 0) > 1e-6).length;

  const invEffMetrics = (invEff && invEff.metrics) || {};
  const invEffLossMwh = Math.round(Number(invEffMetrics.total_loss_mwh || 0) * 10000) / 10000;
  const invEffRows = Array.isArray(invEff && invEff.inverters) ? invEff.inverters : [];
  const invEffCount = invEffRows.filter((r) => Number(r.loss_energy_mwh || 0) > 1e-3).length;

  const categories = [
    { id: 'ds', label: 'Disconnected Strings', loss_mwh: dsLossMwh, fault_count: dsCount, metric_note: dsEnergyOk ? 'Energy loss summed over range' : (dsSummary.energy_note || 'Energy N/A') },
    { id: 'pl', label: 'Power Limitation', loss_mwh: plLossMwh, fault_count: plCount, metric_note: '10:00–15:00 window; energy loss (kWh) / 1000' },
    { id: 'is', label: 'Inverter Shutdown', loss_mwh: 0, fault_count: isCount, metric_note: `Shutdown hours (plant total): ${isHours.toFixed(2)} h; MWh not modeled in feed` },
    { id: 'gb', label: 'Grid Breakdown', loss_mwh: 0, fault_count: gbCount, metric_note: `Breakdown hours (plant total): ${gbHours.toFixed(2)} h; MWh not modeled in feed` },
    { id: 'comm', label: 'Communication Issue', loss_mwh: commLossMwh, fault_count: commCount, metric_note: 'Hierarchical communication ownership with expected-power loss and no SCB duplicate loss' },
    { id: 'scb_perf', label: 'Soiling', loss_mwh: solLossMwh, fault_count: solCount, metric_note: 'Plant PR-regression loss + top SCB peer losses (estimated)' },
    { id: 'inv_eff', label: 'Inverter Efficiency', loss_mwh: invEffLossMwh, fault_count: invEffCount, metric_note: 'DC→AC conversion loss Σ (Pdc−Pac)·dt across inverters (same basis as the Inverter Efficiency tab)' },
    { id: 'damage', label: 'ByPass Diode/Module Damage', loss_mwh: 0, fault_count: 0, metric_note: 'No unified rows yet — use category tab' },
  ];

  const totalLossMwh = Math.round(categories.reduce((s, c) => s + (Number(c.loss_mwh) || 0), 0) * 10000) / 10000;
  const totalFaultCount = categories.reduce((s, c) => s + (Number(c.fault_count) || 0), 0);

  const rows = [];
  const toDay = to || '';

  (dsPack.data || []).forEach((drow) => {
    const ms = Number(drow.missing_strings || drow.range_min_missing_strings || 0);
    if (ms <= 0) return;
    const scb = drow.scb_id;
    if (!scb) return;
    const ekwh = Number(drow.energy_loss_kwh || 0);
    rows.push({
      id: `ds:${scb}`,
      category: 'ds',
      category_label: 'Disconnected Strings',
      occurred_at: String(drow.timestamp || `${toDay} 00:00:00`),
      equipment_id: scb,
      equipment_level: 'scb',
      severity_energy_kwh: Math.round(ekwh * 10000) / 10000,
      severity_hours: null,
      duration_note: drow.recurring_days ? `${drow.recurring_days} recurring days` : null,
      status: String(drow.fault_status || 'DS'),
      investigate: { kind: 'ds', scb_id: scb },
      _sort_loss_kwh: ekwh,
    });
  });

  ((plPage.inverter_status && plPage.inverter_status.data) || []).forEach((prow) => {
    const inv = prow.inverter_id;
    if (!inv) return;
    const ekwh = Number(prow.total_energy_loss_kwh || 0);
    if (ekwh <= 0) return;
    rows.push({
      id: `pl:${inv}`,
      category: 'pl',
      category_label: 'Power Limitation',
      occurred_at: String(prow.last_seen_fault || prow.investigation_window_end || `${toDay} 23:59:59`),
      equipment_id: inv,
      equipment_level: 'inverter',
      severity_energy_kwh: Math.round(ekwh * 10000) / 10000,
      severity_hours: null,
      duration_note: null,
      status: 'Power limitation',
      investigate: { kind: 'pl', inverter_id: inv },
      _sort_loss_kwh: ekwh,
    });
  });

  (isInv.data || []).forEach((irow) => {
    const inv = irow.inverter_id;
    if (!inv) return;
    const hrs = Number(irow.shutdown_hours || 0);
    const pts = Number(irow.shutdown_points || 0);
    if (pts <= 0 && hrs <= 0) return;
    rows.push({
      id: `is:${inv}`,
      category: 'is',
      category_label: 'Inverter Shutdown',
      occurred_at: String(irow.last_seen_shutdown || irow.investigation_window_end || `${toDay} 23:59:59`),
      equipment_id: inv,
      equipment_level: 'inverter',
      severity_energy_kwh: 0,
      severity_hours: Math.round(hrs * 10000) / 10000,
      duration_note: `${pts} points`,
      status: 'Inverter shutdown',
      investigate: { kind: 'is', inverter_id: inv },
      _sort_loss_kwh: hrs * 50,
    });
  });

  (gbEv.data || []).forEach((erow) => {
    const eid = erow.event_id;
    if (!eid) return;
    const hrs = Number(erow.breakdown_hours || 0);
    const pts = Number(erow.breakdown_points || 0);
    if (pts <= 0 && hrs <= 0) return;
    rows.push({
      id: `gb:${eid}`,
      category: 'gb',
      category_label: 'Grid Breakdown',
      occurred_at: String(erow.last_seen_breakdown || erow.investigation_window_end || `${toDay} 23:59:59`),
      equipment_id: String(eid),
      equipment_level: 'plant_event',
      severity_energy_kwh: 0,
      severity_hours: Math.round(hrs * 10000) / 10000,
      duration_note: `${pts} points`,
      status: 'Grid breakdown',
      investigate: { kind: 'gb', event_id: String(eid) },
      _sort_loss_kwh: hrs * 100,
    });
  });

  (commEv.data || []).forEach((crow) => {
    const eqId = crow.equipment_id;
    const eqLevel = crow.equipment_level;
    if (!eqId || !eqLevel) return;
    const hrs = Number(crow.communication_hours || 0);
    const pts = Number(crow.communication_points || 0);
    const ekwh = Number(crow.estimated_loss_kwh || 0);
    const issueKind = String(crow.issue_kind || '');
    rows.push({
      id: `comm:${eqLevel}:${eqId}:${issueKind || 'event'}`,
      category: 'comm',
      category_label: 'Communication Issue',
      occurred_at: String(crow.last_seen_communication || crow.investigation_window_end || `${toDay} 23:59:59`),
      equipment_id: String(eqId),
      equipment_level: String(eqLevel),
      severity_energy_kwh: Math.round(ekwh * 10000) / 10000,
      severity_hours: Math.round(hrs * 10000) / 10000,
      duration_note: `${Number(crow.communication_windows || 0)} windows / ${pts} points`,
      status: String(crow.status || 'Communication issue'),
      investigate: { kind: 'comm', equipment_level: String(eqLevel), equipment_id: String(eqId), issue_kind: issueKind || undefined, inverter_id: crow.inverter_id ? String(crow.inverter_id) : undefined },
      _sort_loss_kwh: ekwh > 0 ? ekwh : (hrs * 25),
    });
  });

  solRowsRaw.forEach((srow) => {
    const sid = srow.id;
    const lm = Number(srow.loss_mwh || 0);
    if (!sid || lm <= 1e-6) return;
    rows.push({
      id: `scb_perf:${sid}`,
      category: 'scb_perf',
      category_label: 'Soiling',
      occurred_at: `${toDay} 12:00:00`,
      equipment_id: sid,
      equipment_level: 'scb',
      severity_energy_kwh: Math.round(lm * 1000 * 10000) / 10000,
      severity_hours: null,
      duration_note: 'Peer-based estimate (range)',
      status: 'Soiling (est.)',
      investigate: { kind: 'scb_perf', scb_id: sid },
      _sort_loss_kwh: lm * 1000,
    });
  });

  rows.sort((a, b) => (Number(b._sort_loss_kwh) || 0) - (Number(a._sort_loss_kwh) || 0));
  rows.forEach((r) => { delete r._sort_loss_kwh; });

  return {
    date_from: from,
    date_to: to,
    plant_id: plantId,
    categories,
    totals: { loss_mwh: totalLossMwh, fault_count: totalFaultCount },
    rows: rows.slice(0, MAX_ROWS),
    row_limit: MAX_ROWS,
    ds_energy_note: dsSummary.energy_note,
    _merged_on_client: true,
    _merge_reason: 'GET /api/faults/unified-feed was not found (404). Merged ds-summary, pl-page, is/gb, soiling-* in the browser. Restart the API from the current backend code to use one round-trip.',
  };
}

// ── Faults ────────────────────────────────────────────────────────────────────
const Faults = {
  dsSummary:         (plantId, from, to) => apiFetch(`/api/faults/ds-summary?plant_id=${plantId}${(from && to) ? `&date_from=${from}&date_to=${to}` : ''}`),
  dsScbStatus:       (plantId, from, to) => apiFetch(`/api/faults/ds-scb-status?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  dsTimeline:        (plantId, scbId, from, to) => apiFetch(`/api/faults/ds-timeline?plant_id=${plantId}${scbId ? `&scb_id=${scbId}` : ''}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  dsFilterSummary:   (plantId, from, to) => apiFetch(`/api/faults/ds-filter-summary?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  plSummary:         (plantId, from, to) => apiFetch(`/api/faults/pl-summary?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  plInverterStatus:  (plantId, from, to) => apiFetch(`/api/faults/pl-inverter-status?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  /** Single request: summary + table (one backend run_power_limitation — use instead of parallel plSummary + plInverterStatus). */
  plPage:            (plantId, from, to) => apiFetch(`/api/faults/pl-page?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  /** Warms server cache for PL, IS, GB tabs in one parallel backend pass (optional prefetch). */
  runtimeTabsBundle: (plantId, from, to) => apiFetch(`/api/faults/runtime-tabs-bundle?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  /**
   * Overview tiles + unified fault rows. Tries GET /api/faults/unified-feed first;
   * on 404 Not Found (stale API), merges the same data from legacy endpoints in parallel.
   */
  unifiedFeed: async (plantId, from, to) => {
    const qs = `plant_id=${encodeURIComponent(plantId || '')}${from ? `&date_from=${encodeURIComponent(from)}` : ''}${to ? `&date_to=${encodeURIComponent(to)}` : ''}`;
    try {
      return await apiFetch(`/api/faults/unified-feed?${qs}`);
    } catch (e) {
      const msg = String((e && e.message) || e);
      const st = e && e.status;
      const allowFallback = allowLegacyUnifiedFeedFallback();
      // Stale API (404) or server overload / DB timeout (5xx) — merge legacy endpoints so Overview still loads.
      if (allowFallback && st >= 500 && st < 600) {
        console.warn('unified-feed returned', st, msg.slice(0, 200), '— using client-side merge (slower).');
        return buildUnifiedFeedClientSide(plantId, from, to);
      }
      if (!allowFallback || !/not found|404/i.test(msg)) throw e;
      return buildUnifiedFeedClientSide(plantId, from, to);
    }
  },
  plTimeline:        (plantId, inverterId, from, to) => apiFetch(`/api/faults/pl-timeline?plant_id=${plantId}${inverterId ? `&inverter_id=${encodeURIComponent(inverterId)}` : ''}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  isSummary:         (plantId, from, to) => apiFetch(`/api/faults/is-summary?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  isInverterStatus:  (plantId, from, to) => apiFetch(`/api/faults/is-inverter-status?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  isTimeline:        (plantId, inverterId, from, to) => apiFetch(`/api/faults/is-timeline?plant_id=${plantId}${inverterId ? `&inverter_id=${encodeURIComponent(inverterId)}` : ''}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  gbSummary:         (plantId, from, to) => apiFetch(`/api/faults/gb-summary?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  gbEvents:          (plantId, from, to) => apiFetch(`/api/faults/gb-events?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  gbTimeline:        (plantId, from, to) => apiFetch(`/api/faults/gb-timeline?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  commSummary:       (plantId, from, to) => apiFetch(`/api/faults/comm-summary?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  commEvents:        (plantId, from, to) => apiFetch(`/api/faults/comm-events?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  commInverterLoss:  (plantId, from, to) => apiFetch(`/api/faults/comm-inverter-loss?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  commTimeline:      (plantId, equipmentLevel, equipmentId, issueKind, from, to) => apiFetch(`/api/faults/comm-timeline?plant_id=${plantId}&equipment_level=${encodeURIComponent(equipmentLevel)}&equipment_id=${encodeURIComponent(equipmentId)}${issueKind ? `&issue_kind=${encodeURIComponent(issueKind)}` : ''}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  // Clipping & Derating (GTI virtual-power model)
  cdSummary:         (plantId, from, to) => apiFetch(`/api/faults/cd-summary?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  cdInverterStatus:  (plantId, from, to) => apiFetch(`/api/faults/cd-inverter-status?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  cdPage:            (plantId, from, to) => apiFetch(`/api/faults/cd-page?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  cdTimeline:        (plantId, inverterId, from, to) => apiFetch(`/api/faults/cd-timeline?plant_id=${plantId}${inverterId ? `&inverter_id=${encodeURIComponent(inverterId)}` : ''}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  inverterEfficiency:         (plantId, from, to) => apiFetch(`/api/faults/inverter-efficiency?plant_id=${plantId}&date_from=${from}&date_to=${to}`),
  inverterEfficiencyAnalysis: (plantId, from, to) => apiFetch(`/api/faults/inverter-efficiency-analysis?plant_id=${plantId}&date_from=${from}&date_to=${to}`),
  scbPerformanceHeatmap:     (plantId, from, to) => apiFetch(`/api/faults/scb-performance-heatmap?plant_id=${plantId}&date_from=${from || ''}&date_to=${to || ''}`),
  scbTrend:                  (plantId, scbId, from, to) => apiFetch(`/api/faults/scb-trend?plant_id=${plantId}&scb_id=${encodeURIComponent(scbId)}&date_from=${from || ''}&date_to=${to || ''}`),
  soilingPlantPr:            (plantId, from, to) => apiFetch(`/api/faults/soiling-plant-pr?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  soilingRankings:           (plantId, from, to, groupBy) => apiFetch(`/api/faults/soiling-rankings?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}&group_by=${encodeURIComponent(groupBy || 'inverter')}`),
  scbSoilingTrend:           (plantId, scbId, from, to) => apiFetch(`/api/faults/scb-soiling-trend?plant_id=${plantId}&scb_id=${encodeURIComponent(scbId)}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  // Review / sign-off
  getReviews:  (plantId, from, to) => apiFetch(`/api/faults/ds-review?plant_id=${plantId}${from ? `&date_from=${from}` : ''}${to ? `&date_to=${to}` : ''}`),
  saveReview:  (body) => apiFetch('/api/faults/ds-review', { method: 'POST', body: JSON.stringify(body) }),
};

const SiteAppearance = {
  get: () => apiFetch('/api/site/appearance'),
};

const Admin = {
  listUsers: () => apiFetch('/api/admin/users'),
  createUser: (data) => apiFetch('/api/admin/users', { method: 'POST', body: JSON.stringify(data) }),
  updateUser: (userId, data) => apiFetch(`/api/admin/users/${userId}`, { method: 'PUT', body: JSON.stringify(data) }),
  deleteUser: (userId) => apiFetch(`/api/admin/users/${userId}`, { method: 'DELETE' }),
  deletePlant: (plantId) => apiFetch(`/api/admin/plants/${encodeURIComponent(plantId)}`, { method: 'DELETE' }),
  updatePlant: (plantId, data) => apiFetch(`/api/admin/plants/${encodeURIComponent(plantId)}`, { method: 'PUT', body: JSON.stringify(data || {}) }),
  updateSiteAppearance: (data) => apiFetch('/api/admin/site-appearance', { method: 'PUT', body: JSON.stringify(data) }),
  // ── Performance monitoring ──────────────────────────────────────────────
  perfOverview: () => apiFetch('/api/admin/perf/overview'),
  perfSlowQueries: (limit) => apiFetch(`/api/admin/perf/slow-queries?limit=${limit || 50}`),
  perfEndpointStats: (minutes) => apiFetch(`/api/admin/perf/endpoint-stats?minutes=${minutes || 60}`),
  perfDbHealth: () => apiFetch('/api/admin/perf/db-health'),
  perfRequestLog: (limit, pathFilter) => {
    let url = `/api/admin/perf/request-log?limit=${limit || 100}`;
    if (pathFilter) url += `&path_filter=${encodeURIComponent(pathFilter)}`;
    return apiFetch(url);
  },
  runPrecompute: () => apiFetch('/api/admin/perf/run-precompute', { method: 'POST' }),
  precomputeStatus: () => apiFetch('/api/admin/perf/precompute-status'),
  /** Durable queue: fills DS / unified / loss + fault tab snapshots. Worker: `python -m jobs.precompute_runner --once` */
  precomputeQueue: () => apiFetch('/api/admin/precompute/queue?limit=40'),
  precomputeEnqueue: (body) => apiFetch('/api/admin/precompute/enqueue', { method: 'POST', body: JSON.stringify(body || {}) }),
};

// ── Reports ───────────────────────────────────────────────────────────────────
// Binary endpoints — we cannot use apiFetch (JSON-only). Use raw fetch with the
// stored bearer token, then return a Blob for download.
const Reports = {
  options: () => apiFetch('/api/reports/options'),
  generate: async (body) => {
    const token = getToken();
    const headers = { 'Content-Type': 'application/json' };
    if (token) headers.Authorization = `Bearer ${token}`;
    const res = await fetch(`${API_BASE}/api/reports/generate`, {
      method: 'POST',
      headers,
      body: JSON.stringify(body || {}),
    });
    if (!res.ok) {
      let msg = `Report generation failed (${res.status})`;
      try { const j = await res.json(); if (j && j.detail) msg = j.detail; } catch (_) {}
      throw new Error(msg);
    }
    const blob = await res.blob();
    // Extract filename from Content-Disposition if present
    const cd = res.headers.get('Content-Disposition') || '';
    const match = /filename="?([^";]+)"?/i.exec(cd);
    const filename = match ? match[1] : `report.${body.format || 'pdf'}`;
    return { blob, filename };
  },
};

window.SolarAPI = { Auth, Plants, Dashboard, Analytics, Metadata, Tickets, Faults, LossAnalysis, Admin, Reports, SiteAppearance, getToken, setToken, clearToken, getUser, setUser, apiBase: API_BASE };
