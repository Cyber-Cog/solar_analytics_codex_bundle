// Loss Analysis — energy bridge + compact stats + entity table (minimal chrome).
const { useState, useEffect, useCallback, useMemo } = React;
const h = React.createElement;
const { Spinner, DataTable, LucideIcon } = window;

/**
 * Loss bridge colours — human-readable semantics:
 * - Expected: neutral slate (reference, not a "fault").
 * - Actual: green (delivered energy).
 * - Degradation / temperature / diagnostics: red family (all are real losses / faults).
 * - Unknown: grey (gap we have not attributed — not the same as a diagnosed loss).
 */
const LA_SEGMENT = {
  expected: { fill: '#475569' },
  actual: { fill: '#047857' },
  degradation: { fill: '#991b1b' },
  temperature: { fill: '#dc2626' },
  unknown: { fill: '#64748b' },
  loss: { fill: '#ef4444' },
};

const LA_DIAGNOSTICS = LA_SEGMENT.loss;

function fmtMwh(v, digits = 3) {
  if (v == null || Number.isNaN(v)) return '—';
  return `${Number(v).toFixed(digits)} MWh`;
}

function fmtKpiMwh(v) {
  if (v == null || Number.isNaN(v)) return '—';
  const n = Number(v);
  if (Math.abs(n) >= 100) return n.toFixed(1);
  if (Math.abs(n) >= 10) return n.toFixed(2);
  return n.toFixed(3);
}

function bridgeSegmentFill(entry) {
  const key = String(entry.key || '');
  if (key === 'expected') return LA_SEGMENT.expected.fill;
  if (key === 'actual') return LA_SEGMENT.actual.fill;
  if (key === 'degradation') return LA_SEGMENT.degradation.fill;
  if (key === 'temperature') return LA_SEGMENT.temperature.fill;
  if (key === 'unknown') return LA_SEGMENT.unknown.fill;
  if (key === 'diagnostics' || key.startsWith('diag_')) return LA_DIAGNOSTICS.fill;
  if (entry.kind === 'total') return LA_SEGMENT.expected.fill;
  return LA_DIAGNOSTICS.fill;
}

/** Corporate dual-tone iceberg: navy (fault frequency) · bronze (energy loss). */
const ICEBERG_NAVY = '#0A2540';
const ICEBERG_BRONZE = '#6B4423';
const ICEBERG_GRID = '#E8EBEF';
const ICEBERG_ALPHA_MIN = 0.22;
const ICEBERG_ALPHA_MAX = 0.94;

function hexToRgb(hex) {
  const h = String(hex || '#000').replace('#', '');
  const n = h.length === 3
    ? h.split('').map((c) => parseInt(c + c, 16))
    : [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)];
  return { r: n[0] || 0, g: n[1] || 0, b: n[2] || 0 };
}

function rgbaHex(hex, a) {
  const { r, g, b } = hexToRgb(hex);
  return `rgba(${r},${g},${b},${a})`;
}

/** Value-linked saturation: higher count/MWh → deeper shade. */
function icebergHeatAlpha(value, max) {
  if (max <= 0 || value <= 0) return ICEBERG_ALPHA_MIN;
  const t = Math.max(0, Math.min(1, value / max));
  return ICEBERG_ALPHA_MIN + t * (ICEBERG_ALPHA_MAX - ICEBERG_ALPHA_MIN);
}

/** Sort X-axis: highest MWh loss on the left. */
function icebergDisplayOrder(rows) {
  return [...rows].sort(
    (a, b) => (Number(b.loss_mwh) || 0) - (Number(a.loss_mwh) || 0),
  );
}

function icebergCanvasLabel(name) {
  const s = String(name || '').trim();
  if (!s) return '';
  if (s.length <= 14) return s;
  const mid = Math.ceil(s.length / 2);
  const sp = s.lastIndexOf(' ', mid);
  if (sp > 4) return `${s.slice(0, sp)}\n${s.slice(sp + 1)}`;
  return `${s.slice(0, mid)}\n${s.slice(mid)}`;
}

/** Minimal dual-axis iceberg — navy / bronze heatmap, MWh sort left→right. */
function paintIcebergCanvas(canvas, wrap, rows) {
  if (!canvas || !wrap || !rows.length) return { hits: [], ordered: [] };
  const ordered = icebergDisplayOrder(rows);
  const cats = ordered.map((r) => ({
    id: r.id,
    label: r.label || r.id,
    faults: Number(r.fault_count) || 0,
    mwh: Math.max(0, Number(r.loss_mwh) || 0),
  }));
  const N = cats.length;
  const maxF = Math.max(1, ...cats.map((d) => d.faults));
  const maxM = Math.max(0.001, ...cats.map((d) => d.mwh));

  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const CW = Math.max(320, wrap.clientWidth || 640);
  const CH = 480;
  canvas.width = CW * dpr;
  canvas.height = CH * dpr;
  canvas.style.width = '100%';
  canvas.style.height = `${CH}px`;
  const c = canvas.getContext('2d');
  c.setTransform(dpr, 0, 0, dpr, 0, 0);

  const PAD = 20;
  const ML = PAD;
  const MR = PAD;
  const cw = CW - ML - MR;
  const col = cw / N;
  const hw = col * 0.41;
  const MID = 252;
  const AT = 56;
  const BB = CH - 52;
  const aH = MID - AT;
  const bH = BB - MID;
  const hits = [];
  const GRID_LINES = 4;

  c.clearRect(0, 0, CW, CH);
  c.fillStyle = '#FFFFFF';
  c.fillRect(0, 0, CW, CH);

  c.strokeStyle = ICEBERG_GRID;
  c.lineWidth = 1;
  for (let i = 1; i <= GRID_LINES; i += 1) {
    const yUp = MID - (i / GRID_LINES) * aH * 0.92;
    c.beginPath();
    c.moveTo(ML, yUp);
    c.lineTo(CW - MR, yUp);
    c.stroke();
    const yDn = MID + (i / GRID_LINES) * bH * 0.92;
    c.beginPath();
    c.moveTo(ML, yDn);
    c.lineTo(CW - MR, yDn);
    c.stroke();
  }

  c.beginPath();
  c.moveTo(ML, MID);
  c.lineTo(CW - MR, MID);
  c.strokeStyle = '#94A3B8';
  c.lineWidth = 1.25;
  c.stroke();

  c.font = '600 10px Inter, system-ui, sans-serif';
  c.textAlign = 'center';
  c.fillStyle = ICEBERG_NAVY;
  c.textBaseline = 'top';
  c.fillText('FAULT FREQUENCY (EVENT COUNT) ↑', CW / 2, 18);
  c.fillStyle = ICEBERG_BRONZE;
  c.textBaseline = 'bottom';
  c.fillText('FINANCIAL / ENERGY LOSS (MWh) ↓', CW / 2, CH - 14);

  cats.forEach((d, i) => {
    const cx = ML + col * i + col / 2;
    hits.push({ index: i, x0: cx - hw, x1: cx + hw, row: ordered[i] });

    if (d.faults > 0) {
      const fAlpha = icebergHeatAlpha(d.faults, maxF);
      const tipY = MID - (d.faults / maxF) * aH * 0.92;
      const gu = c.createLinearGradient(cx, MID, cx, tipY);
      gu.addColorStop(0, rgbaHex(ICEBERG_NAVY, fAlpha));
      gu.addColorStop(1, rgbaHex(ICEBERG_NAVY, fAlpha * 0.42));
      c.fillStyle = gu;
      c.beginPath();
      c.moveTo(cx, tipY);
      c.lineTo(cx - hw, MID);
      c.lineTo(cx + hw, MID);
      c.closePath();
      c.fill();
      c.font = '600 11px Inter, system-ui, sans-serif';
      c.fillStyle = ICEBERG_NAVY;
      c.textAlign = 'center';
      c.textBaseline = 'bottom';
      c.fillText(String(d.faults), cx, tipY - 5);
    }

    if (d.mwh > 1e-6) {
      const mAlpha = icebergHeatAlpha(d.mwh, maxM);
      const dtipY = MID + (d.mwh / maxM) * bH * 0.92;
      const gd = c.createLinearGradient(cx, MID, cx, dtipY);
      gd.addColorStop(0, rgbaHex(ICEBERG_BRONZE, mAlpha));
      gd.addColorStop(1, rgbaHex(ICEBERG_BRONZE, mAlpha * 0.42));
      c.fillStyle = gd;
      c.beginPath();
      c.moveTo(cx - hw, MID);
      c.lineTo(cx + hw, MID);
      c.lineTo(cx, dtipY);
      c.closePath();
      c.fill();
      c.font = '600 11px Inter, system-ui, sans-serif';
      c.fillStyle = ICEBERG_BRONZE;
      c.textAlign = 'center';
      c.textBaseline = 'top';
      c.fillText(d.mwh.toFixed(2), cx, dtipY + 5);
    } else {
      c.font = '500 10px Inter, system-ui, sans-serif';
      c.fillStyle = rgbaHex(ICEBERG_BRONZE, ICEBERG_ALPHA_MIN);
      c.textAlign = 'center';
      c.textBaseline = 'top';
      c.fillText('0.00', cx, MID + 6);
    }
  });

  wrap.style.setProperty('--ic-ml', `${ML}px`);
  wrap.style.setProperty('--ic-mr', `${MR}px`);
  wrap.style.setProperty('--ic-cols', String(N));

  return { hits, ordered };
}

function LossAnalysisIcebergChart({ rows }) {
  const wrapRef = React.useRef(null);
  const canvasRef = React.useRef(null);
  const layoutRef = React.useRef({ hits: [], ordered: [] });
  const [tip, setTip] = useState(null);
  const ordered = useMemo(() => icebergDisplayOrder(rows || []), [rows]);

  const redraw = useCallback(() => {
    layoutRef.current = paintIcebergCanvas(canvasRef.current, wrapRef.current, ordered) || { hits: [], ordered: [] };
    setTip(null);
  }, [ordered]);

  useEffect(() => {
    redraw();
    const wrap = wrapRef.current;
    if (!wrap || typeof ResizeObserver === 'undefined') return undefined;
    const ro = new ResizeObserver(() => redraw());
    ro.observe(wrap);
    return () => ro.disconnect();
  }, [redraw]);

  const onMouseMove = useCallback((e) => {
    const canvas = canvasRef.current;
    const layout = layoutRef.current;
    if (!canvas || !layout.hits.length) return;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const hit = layout.hits.find((h) => x >= h.x0 && x <= h.x1);
    if (!hit) {
      setTip(null);
      return;
    }
    setTip({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
      label: hit.row.label,
      faults: hit.row.fault_count,
      mwh: hit.row.loss_mwh,
    });
  }, []);

  if (!ordered.length) return null;

  return h('div', { className: 'la-iceberg-canvas-wrap', ref: wrapRef },
    h('div', { className: 'la-iceberg-canvas-inner' },
      h('canvas', {
        ref: canvasRef,
        className: 'la-iceberg-canvas',
        role: 'img',
        'aria-label': 'Dual-axis iceberg: fault frequency above baseline, energy loss below, sorted by MWh',
        onMouseMove: onMouseMove,
        onMouseLeave: () => setTip(null),
      }),
      tip && h('div', {
        className: 'la-iceberg-tooltip',
        style: { left: `${Math.min(tip.x + 12, (wrapRef.current?.clientWidth || 400) - 180)}px`, top: `${Math.max(8, tip.y - 48)}px` },
        role: 'tooltip',
      },
        h('strong', null, tip.label),
        h('span', null, `Faults: ${tip.faults}`),
        h('span', null, `MWh: ${fmtKpiMwh(tip.mwh)}`),
      ),
    ),
    h('div', { className: 'la-iceberg-canvas-labels' },
      ...ordered.map((row) => h('div', {
        key: row.id,
        className: 'la-iceberg-canvas-label',
        title: row.label || row.id,
        dangerouslySetInnerHTML: {
          __html: icebergCanvasLabel(row.label || row.id).replace(/\n/g, '<br>'),
        },
      })),
    ),
  );
}

/** Iceberg rows from bridge API only (same date range as waterfall — no separate feed merge). */
function icebergRowsFromPayload(data, primary) {
  const fromApi = data && data.iceberg_faults;
  if (Array.isArray(fromApi) && fromApi.length) {
    return fromApi.map((c) => ({
      id: c.id,
      label: c.label || c.id,
      fault_count: Number(c.fault_count) || 0,
      loss_mwh: Number(c.loss_mwh) || 0,
      metric_note: c.metric_note,
    }));
  }
  const fcats = data && data.fault_categories;
  if (Array.isArray(fcats) && fcats.length) {
    return fcats.map((c) => ({
      id: c.id,
      label: c.label || c.id,
      fault_count: Number(c.fault_count) || 0,
      loss_mwh: Number(c.loss_mwh) || 0,
      metric_note: c.metric_note,
    }));
  }
  const cats = (primary && primary.category_losses_mwh) || [];
  return cats.map((c) => ({
    id: c.id,
    label: c.label || c.id,
    fault_count: 0,
    loss_mwh: Number(c.mwh) || 0,
  }));
}

function icebergCountsNeedRefresh(rows) {
  if (!Array.isArray(rows) || !rows.length) return true;
  const hasLoss = rows.some((r) => Number(r.loss_mwh) > 1e-6);
  const sumCnt = rows.reduce((s, r) => s + (Number(r.fault_count) || 0), 0);
  return hasLoss && sumCnt <= 0;
}

function scopeLabel(scope) {
  const m = { plant: 'Whole plant', inverter: 'Inverter', scb: 'SCB', string: 'String' };
  return m[scope] || scope;
}

/** True when bridge numbers are meaningful enough to plot (avoids all-zero glitches). */
function bridgeHasEnergy(primary) {
  if (!primary || typeof primary !== 'object') return false;
  const e = Number(primary.expected_mwh) || 0;
  const a = Number(primary.actual_mwh) || 0;
  return e > 1e-6 || a > 1e-6;
}

function stringKeyFromOpt(s) {
  if (!s || !s.inverter_id) return '';
  return `${s.inverter_id}::${s.scb_id}::${s.string_id}`;
}

/** Stable empty refs so downstream useMemo deps do not churn every render when data is null. */
const LA_EMPTY_BRIDGE = [];
const LA_EMPTY_WORST = [];
const LA_EMPTY_TABLE = [];

const LOSS_ANALYSIS_TABLE_COLUMNS = [
  { key: 'label', label: 'Entity', render: (r) => h('span', { className: 'la-entity-name' }, r.label) },
  { key: 'expected_mwh', label: 'Expected', sortValue: (r) => r.expected_mwh ?? -1, render: (r) => fmtMwh(r.expected_mwh) },
  { key: 'actual_mwh', label: 'Actual', sortValue: (r) => r.actual_mwh ?? -1, render: (r) => fmtMwh(r.actual_mwh) },
  { key: 'degradation_mwh', label: 'Degrad.', sortValue: (r) => r.degradation_mwh ?? -1, render: (r) => fmtMwh(r.degradation_mwh) },
  { key: 'temperature_loss_mwh', label: 'Temp.', sortValue: (r) => r.temperature_loss_mwh ?? -1, render: (r) => fmtMwh(r.temperature_loss_mwh) },
  { key: 'diagnostics_loss_mwh', label: 'Diag.', sortValue: (r) => r.diagnostics_loss_mwh ?? -1, render: (r) => fmtMwh(r.diagnostics_loss_mwh) },
  { key: 'unknown_mwh', label: 'Unknown', sortValue: (r) => r.unknown_mwh ?? -1, render: (r) => fmtMwh(r.unknown_mwh) },
];

/** Catches render errors so the route is not a silent blank. */
class LossAnalysisBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  render() {
    if (this.state.hasError) {
      const err = this.state.error;
      const msg = err && (err.message || String(err));
      return h('div', { className: 'card', style: { padding: 24, maxWidth: 720, margin: '24px auto', borderColor: 'rgba(220,38,38,0.35)' } },
        h('h2', { style: { fontSize: 18, marginBottom: 10 } }, 'Loss Analysis cannot render'),
        h('p', { style: { fontSize: 13, color: 'var(--text-soft)', marginBottom: 8 } },
          'A runtime error stopped this page. Hard-refresh (Ctrl+Shift+R) after deploy. If it persists, send the text below to support.',
        ),
        h('pre', { style: { margin: 0, padding: 12, background: 'rgba(15,23,42,0.06)', borderRadius: 8, fontSize: 12, whiteSpace: 'pre-wrap', wordBreak: 'break-word' } }, msg || 'Unknown error'),
      );
    }
    return this.props.children;
  }
}
window.LossAnalysisBoundary = LossAnalysisBoundary;

window.LossAnalysisPage = ({ plantId, dateFrom, dateTo }) => {
  const [scope, setScope] = useState('plant');
  const [equipmentId, setEquipmentId] = useState('');
  const [opts, setOpts] = useState({ inverters: [], scbs: [], strings: [] });
  const [optsErr, setOptsErr] = useState('');
  const [data, setData] = useState(null);
  const [err, setErr] = useState('');
  const [loadingBridge, setLoadingBridge] = useState(false);
  const [optsLoading, setOptsLoading] = useState(false);
  const [wfUnit, setWfUnit] = useState('mwh');
  const bridgeRetryRef = React.useRef(false);
  const datesKeyRef = React.useRef('');

  const invCount = (opts.inverters || []).length;
  const scbCount = (opts.scbs || []).length;
  const strCount = (opts.strings || []).length;

  const scopeHasNoPickList = scope === 'inverter' && invCount === 0
    || scope === 'scb' && scbCount === 0
    || scope === 'string' && strCount === 0;

  const stringSelectionStale = scope === 'string' && equipmentId && strCount > 0
    && !(opts.strings || []).some((s) => stringKeyFromOpt(s) === equipmentId);

  function friendlyApiMessage(e, context) {
    let m = (e && e.message) ? String(e.message) : String(e || 'Error');
    const st = e && e.status;
    if (st === 404 || /not found|^404$/i.test(m)) {
      return `${context}: API route not found. Deploy latest backend and restart.`;
    }
    return `${context}: ${m}`;
  }

  useEffect(() => {
    setEquipmentId('');
    setData(null);
    setErr('');
    datesKeyRef.current = '';
  }, [plantId]);

  useEffect(() => {
    if (!plantId) return;
    setOptsErr('');
    setOptsLoading(true);
    window.SolarAPI.LossAnalysis.options(plantId)
      .then((o) => {
        setOpts(o && typeof o === 'object' ? o : { inverters: [], scbs: [], strings: [] });
        setOptsErr('');
      })
      .catch((e) => {
        setOpts({ inverters: [], scbs: [], strings: [] });
        setOptsErr(friendlyApiMessage(e, 'Could not load equipment list'));
      })
      .finally(() => setOptsLoading(false));
  }, [plantId]);

  useEffect(() => {
    if (stringSelectionStale) {
      setEquipmentId('');
      setData(null);
      setErr('');
    }
  }, [stringSelectionStale, scope]);

  const loadBridge = useCallback((forceRefresh = false) => {
    if (!plantId) return;
    if (scope !== 'plant' && scopeHasNoPickList) return;
    setLoadingBridge(true);
    setErr('');
    const eq = scope === 'plant' ? '' : equipmentId;
    window.SolarAPI.LossAnalysis.bridge(plantId, dateFrom, dateTo, scope, eq || undefined, forceRefresh)
      .then((d) => {
        if (d.error) {
          setErr(d.message || d.error);
          setData(null);
          bridgeRetryRef.current = false;
          return;
        }
        const ice = d.iceberg_faults || d.fault_categories;
        const rows = Array.isArray(ice) ? ice : [];
        const staleCounts = rows.length > 0 && icebergCountsNeedRefresh(rows);
        if (!forceRefresh && staleCounts && !bridgeRetryRef.current) {
          bridgeRetryRef.current = true;
          return window.SolarAPI.LossAnalysis.bridge(
            plantId, dateFrom, dateTo, scope, eq || undefined, true,
          ).then((d2) => {
            bridgeRetryRef.current = false;
            if (d2 && d2.error) {
              setErr(d2.message || d2.error);
              setData(null);
            } else {
              setData(d2);
            }
          });
        }
        bridgeRetryRef.current = false;
        setData(d);
      })
      .catch((e) => {
        setErr(e.message || String(e));
        setData(null);
        bridgeRetryRef.current = false;
      })
      .finally(() => setLoadingBridge(false));
  }, [plantId, dateFrom, dateTo, scope, equipmentId, scopeHasNoPickList]);

  useEffect(() => {
    bridgeRetryRef.current = false;
  }, [plantId, dateFrom, dateTo, scope, equipmentId]);

  useEffect(() => {
    if (!plantId) return;
    if (scope !== 'plant' && scopeHasNoPickList) {
      setData(null);
      setErr('');
      setLoadingBridge(false);
      return;
    }
    if (scope !== 'plant' && !equipmentId) {
      setData(null);
      setErr('');
      setLoadingBridge(false);
      return;
    }
    const datesKey = `${dateFrom || ''}|${dateTo || ''}`;
    const forceByDates = datesKeyRef.current !== '' && datesKeyRef.current !== datesKey;
    datesKeyRef.current = datesKey;
    loadBridge(forceByDates);
  }, [plantId, dateFrom, dateTo, scope, equipmentId, loadBridge, scopeHasNoPickList]);

  const bridgeRaw = (data && data.waterfall_bridge) || LA_EMPTY_BRIDGE;
  const worst = (data && data.worst_unknown) || LA_EMPTY_WORST;
  const tableRows = (data && data.table) || LA_EMPTY_TABLE;
  const primary = data && data.primary;
  const icebergRows = useMemo(
    () => icebergRowsFromPayload(data, primary),
    [data, primary],
  );
  const loadedRangeLabel = data && data.date_from && data.date_to
    ? `${data.date_from} → ${data.date_to}`
    : null;
  const expBase = primary && Number(primary.expected_mwh) > 0 ? Number(primary.expected_mwh) : 0;
  const actVal = Number(primary?.actual_mwh) || 0;
  const yieldPct = expBase > 0 ? (actVal / expBase) * 100 : null;
  const bridgeOk = !!(data && !data.error && bridgeHasEnergy(primary) && bridgeRaw.length > 0);

  const bridgeChartData = useMemo(() => {
    if (!bridgeRaw.length) return [];
    const scale = wfUnit === 'pct' && expBase > 0 ? 100 / expBase : 1;
    return bridgeRaw.map((row) => ({
      ...row,
      _inv: Number(row.invisible_mwh || 0) * scale,
      _vis: Number(row.visible_mwh || 0) * scale,
    }));
  }, [bridgeRaw, wfUnit, expBase]);

  const yAxisLabel = wfUnit === 'pct' && expBase > 0 ? '% of expected' : 'MWh';

  const legendItems = useMemo(() => {
    const hasDiagCats = bridgeRaw.some((row) => String(row.key || '').startsWith('diag_'));
    return [
      { key: 'expected', name: 'Expected', fill: LA_SEGMENT.expected.fill },
      { key: 'degradation', name: 'Degradation', fill: LA_SEGMENT.degradation.fill },
      { key: 'temperature', name: 'Temperature', fill: LA_SEGMENT.temperature.fill },
      {
        key: 'diagnostics',
        name: hasDiagCats ? 'Fault diagnostics (by category)' : 'Fault diagnostics',
        fill: LA_DIAGNOSTICS.fill,
      },
      { key: 'unknown', name: 'Unknown', fill: LA_SEGMENT.unknown.fill },
      { key: 'actual', name: 'Actual', fill: LA_SEGMENT.actual.fill },
    ];
  }, [bridgeRaw]);

  const waterfallHeight = useMemo(() => {
    const n = bridgeRaw.length;
    return Math.min(520, Math.max(400, n > 10 ? 440 : 400));
  }, [bridgeRaw.length]);

  const chartOption = useMemo(() => {
    if (!bridgeChartData.length) return null;
    const n = bridgeChartData.length;
    const bottomPad = n > 6 ? 72 : 52;
    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow', shadowStyle: { color: 'rgba(15,23,42,0.06)' } },
        backgroundColor: '#ffffff',
        borderColor: '#e2e8f0',
        borderWidth: 1,
        padding: [10, 14],
        textStyle: { color: '#0f172a', fontSize: 12 },
        extraCssText: 'box-shadow: 0 8px 24px rgba(15,23,42,0.12); border-radius: 10px;',
        formatter(params) {
          if (!params || !params.length) return '';
          const raw = (params.find((p) => p.seriesIndex === 1) || params[0]).data?.raw || params[0].data?.raw;
          if (!raw) return '';
          const vis = Number(raw.visible_mwh || 0);
          const disp = params.find((p) => p.seriesIndex === 1)?.value;
          const dispStr = wfUnit === 'pct' && expBase > 0 && disp != null
            ? `<div style="font-size:12px;margin-top:2px;color:#64748b">Bar height: <b>${Number(disp).toFixed(2)}%</b> of expected</div>`
            : '';
          const pct = expBase > 0 ? `<div style="margin-top:4px;color:#64748b;font-size:11px">${((vis / expBase) * 100).toFixed(1)}% of expected energy</div>` : '';
          return `<div style="font-weight:700;font-size:13px;margin-bottom:4px">${raw.label}</div>`
            + `<div style="font-size:12px"><b>${vis.toFixed(3)} MWh</b></div>${dispStr}${pct}`;
        },
      },
      grid: { top: 32, right: 16, left: 8, bottom: bottomPad, containLabel: true },
      xAxis: {
        type: 'category',
        data: bridgeChartData.map((d) => d.label),
        axisLabel: {
          color: '#64748b',
          fontSize: 11,
          interval: 0,
          rotate: n > 5 ? 26 : 12,
          hideOverlap: true,
          formatter(value) {
            const s = String(value || '');
            return s.length > 20 ? `${s.slice(0, 18)}…` : s;
          },
        },
        axisLine: { lineStyle: { color: '#e2e8f0' } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value',
        name: yAxisLabel,
        nameTextStyle: { color: '#94a3b8', fontSize: 11 },
        axisLabel: {
          color: '#64748b',
          fontSize: 10,
          formatter: (v) => (wfUnit === 'pct' ? `${Number(v).toFixed(0)}%` : Number(v).toFixed(2)),
        },
        splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } },
        min: 0,
      },
      series: [
        {
          name: '_spacer',
          type: 'bar',
          stack: 'wf',
          silent: true,
          itemStyle: { color: 'transparent', borderColor: 'transparent' },
          emphasis: { disabled: true },
          data: bridgeChartData.map((d) => ({ value: d._inv, raw: d })),
          animationDuration: 350,
        },
        {
          name: 'Segment',
          type: 'bar',
          stack: 'wf',
          barMaxWidth: 48,
          data: bridgeChartData.map((d) => {
            const key = String(d.key || '');
            const isUnknown = key === 'unknown';
            const isLoss = key === 'degradation' || key === 'temperature' || key === 'diagnostics' || key.startsWith('diag_');
            return {
              value: d._vis,
              itemStyle: {
                color: bridgeSegmentFill(d),
                borderRadius: [6, 6, 0, 0],
                borderColor: isUnknown ? '#94a3b8' : (isLoss ? 'rgba(153, 27, 27, 0.35)' : 'rgba(15,23,42,0.08)'),
                borderWidth: d._vis > 0 ? 1 : 0,
              },
              raw: d,
            };
          }),
          animationDuration: 450,
          animationEasing: 'cubicOut',
        },
      ],
    };
  }, [bridgeChartData, yAxisLabel, wfUnit, expBase]);

  const icebergOk = icebergRows.length > 0;
  const icebergLegendRows = useMemo(
    () => icebergDisplayOrder(icebergRows),
    [icebergRows],
  );
  const worstOption = useMemo(() => {
    if (!worst.length) return null;
    const sorted = [...worst].sort(
      (a, b) => (Number(b.unknown_mwh) || 0) - (Number(a.unknown_mwh) || 0),
    );
    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        backgroundColor: '#fff',
        borderColor: '#e2e8f0',
        textStyle: { color: '#0f172a', fontSize: 12 },
        formatter: (params) => {
          if (!params || !params.length) return '';
          const r = params[0];
          return `<b>${r.name}</b><br/>Unknown: ${fmtMwh(r.value)}`;
        },
      },
      grid: { top: 12, right: 28, left: 4, bottom: 12, containLabel: true },
      xAxis: {        type: 'value',
        name: 'MWh',
        nameTextStyle: { color: '#94a3b8', fontSize: 10 },
        axisLabel: { color: '#64748b', fontSize: 10 },
        splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } },
        min: 0,
      },
      yAxis: {
        type: 'category',
        inverse: true,
        data: sorted.map((d) => d.label),
        axisLabel: {
          color: '#334155',
          fontSize: 11,
          width: 140,
          overflow: 'truncate',
          formatter: (v) => (String(v).length > 22 ? `${String(v).slice(0, 20)}…` : v),
        },
        axisLine: { show: false },
        axisTick: { show: false },
      },
      series: [{
        type: 'bar',
        data: sorted.map((d) => ({
          value: Math.max(0, Number(d.unknown_mwh) || 0),
          itemStyle: {
            color: LA_SEGMENT.unknown.fill,
            borderRadius: [0, 6, 6, 0],
            borderColor: '#94a3b8',
            borderWidth: 1,
          },
        })),
        barMaxWidth: 22,
      }],
    };
  }, [worst]);

  const enrichedTableRows = useMemo(() => tableRows.map((r) => ({
    ...r,
    diagnostics_loss_mwh: Number(r.diagnostics_loss_mwh ?? (
      (Number(r.all_losses_mwh) || 0) - (Number(r.degradation_mwh) || 0) - (Number(r.temperature_loss_mwh) || 0)
    )),
  })), [tableRows]);

  const scopeOptions = [
    { id: 'plant', label: 'Plant', disabled: false },
    { id: 'inverter', label: 'Inverter', disabled: invCount === 0 },
    { id: 'scb', label: 'SCB', disabled: scbCount === 0 },
    { id: 'string', label: 'String', disabled: strCount === 0 },
  ];

  const renderEquipmentSelect = () => {
    if (scope === 'plant') return null;
    const common = {
      className: 'la-select',
      value: equipmentId,
      onChange: (e) => setEquipmentId(e.target.value),
      disabled: scopeHasNoPickList,
    };
    if (scope === 'inverter') {
      return h('select', common,
        h('option', { value: '' }, '— Inverter —'),
        ...(opts.inverters || []).map((id) => h('option', { key: id, value: id }, id)),
      );
    }
    if (scope === 'scb') {
      return h('select', common,
        h('option', { value: '' }, '— SCB —'),
        ...(opts.scbs || []).map((id) => h('option', { key: id, value: id }, id)),
      );
    }
    return h('select', common,
      h('option', { value: '' }, '— String —'),
      ...(opts.strings || []).map((s) =>
        h('option', { key: stringKeyFromOpt(s), value: stringKeyFromOpt(s) }, s.label),
      ),
    );
  };

  const showEquipmentPicker = scope !== 'plant' && !scopeHasNoPickList;
  const showContent = plantId && (scope === 'plant' || (equipmentId && !scopeHasNoPickList));
  const primaryLabel = primary?.label || scopeLabel(scope);
  const showWorstUnknown = scope === 'plant' && worst.length > 1;

  const hierarchyHint = scope === 'string' && strCount === 0
    ? 'This plant has no string-level rows in metadata (inverter / SCB / string with DC). Use Plant, Inverter, or SCB, or add strings under Metadata.'
    : scope === 'scb' && scbCount === 0
      ? 'No SCBs are listed for this plant. Use Plant or Inverter, or complete plant architecture.'
      : scope === 'inverter' && invCount === 0
        ? 'No inverters are listed for this plant. Use Plant or check equipment metadata.'
        : null;

  const noBridgeChart = showContent && !loadingBridge && (!bridgeOk || !chartOption);

  return h('div', { className: 'loss-analysis-page' },
    h('div', { className: 'la-compact-head' },
      h('h1', { className: 'la-compact-title' }, 'Loss Analysis'),
      h('div', { className: 'la-compact-meta' },
        dateFrom && dateTo && h('span', { className: 'la-compact-meta-item' }, `${dateFrom} → ${dateTo}`),
        plantId && h('span', { className: 'la-compact-meta-item' }, plantId),
      ),
    ),

    h('section', { className: 'la-toolbar card' },
      h('div', { className: 'la-toolbar-row' },
        h('div', { className: 'la-scope-group', role: 'group', 'aria-label': 'Level' },
          scopeOptions.map((opt) => h('button', {
            key: opt.id,
            type: 'button',
            disabled: opt.disabled,
            title: opt.disabled ? 'No equipment list at this level for this plant' : undefined,
            className: `la-scope-btn${scope === opt.id ? ' la-scope-btn--active' : ''}${opt.disabled ? ' la-scope-btn--disabled' : ''}`,
            onClick: () => {
              if (opt.disabled || opt.id === scope) return;
              setEquipmentId('');
              setData(null);
              setErr('');
              setScope(opt.id);
            },
          }, opt.label)),
        ),
        showEquipmentPicker && h('div', { className: 'la-equip-picker' },
          h('label', { className: 'la-equip-label' }, scope === 'inverter' ? 'Inverter' : scope === 'scb' ? 'SCB' : 'String'),
          renderEquipmentSelect(),
        ),
        h('div', { className: 'la-toolbar-actions' },
          h('button', {
            type: 'button',
            className: 'btn btn-primary la-refresh-btn',
            onClick: () => loadBridge(true),
            disabled: loadingBridge || optsLoading || scopeHasNoPickList || (scope !== 'plant' && !equipmentId),
          },
            loadingBridge ? (Spinner ? h(Spinner, { size: 16 }) : null) : (LucideIcon ? h(LucideIcon, { name: 'RefreshCw', size: 16 }) : null),
            loadingBridge ? ' …' : ' Refresh',
          ),
        ),
      ),
      optsErr && h('div', { className: 'la-alert la-alert--error', role: 'alert' }, optsErr),
      hierarchyHint && h('div', { className: 'la-alert la-alert--info', role: 'status' }, hierarchyHint),
      err && h('div', { className: 'la-alert la-alert--error', role: 'alert' }, err),
      data && bridgeOk && h('p', { className: 'la-meta-line' },
        `Insolation ${Number(data.insolation_kwh_m2 || 0).toFixed(2)} kWh/m² · `
        + `module ${Number(data.module_temp_c || 0).toFixed(1)}°C · `
        + `temp coeff ${Number(data.temp_coefficient_used || 0).toFixed(4)} /°C · `
        + primaryLabel,
      ),
    ),

    showContent && primary && bridgeOk && h('div', { className: 'la-stats-row card' },
      h('div', { className: 'la-stat la-stat--expected' },
        h('span', { className: 'la-stat-label' }, 'Expected'),
        h('strong', { className: 'la-stat-val la-stat-val--expected' }, `${fmtKpiMwh(primary.expected_mwh)} MWh`),
      ),
      h('div', { className: 'la-stat la-stat--actual' },
        h('span', { className: 'la-stat-label' }, 'Actual'),
        h('strong', { className: 'la-stat-val la-stat-val--actual' }, `${fmtKpiMwh(primary.actual_mwh)} MWh`),
      ),
      h('div', { className: 'la-stat' },
        h('span', { className: 'la-stat-label', title: 'Performance ratio (actual ÷ expected)' }, 'PR'),
        h('strong', { className: 'la-stat-val' }, yieldPct != null ? `${yieldPct.toFixed(1)}%` : '—'),
      ),
      h('div', { className: 'la-stat la-stat--unknown' },
        h('span', { className: 'la-stat-label' }, 'Unknown'),
        h('strong', { className: 'la-stat-val la-stat-val--unknown' }, `${fmtKpiMwh(primary.unknown_mwh)} MWh`),
      ),
    ),

    showContent && h('section', { className: 'la-chart-section card' },
      h('div', { className: 'la-panel-head la-panel-head--tight' },
        h('h2', { className: 'la-panel-title' }, 'Bridge'),
        h('div', { className: 'la-unit-toggle', role: 'group', 'aria-label': 'Units' },
        h('button', {
          type: 'button',
            className: `la-unit-btn${wfUnit === 'mwh' ? ' la-unit-btn--active' : ''}`,
          onClick: () => setWfUnit('mwh'),
            disabled: !bridgeOk,
        }, 'MWh'),
        h('button', {
          type: 'button',
            className: `la-unit-btn${wfUnit === 'pct' ? ' la-unit-btn--active' : ''}`,
          onClick: () => setWfUnit('pct'),
            disabled: !bridgeOk || expBase <= 0,
            title: expBase <= 0 ? 'No expected energy' : '% of expected',
          }, '%'),
        ),
      ),
      bridgeOk && h('div', { className: 'la-legend', role: 'list', 'aria-label': 'Colours' },
        ...legendItems.map((leg) => h('span', { key: leg.key, className: 'la-legend-item', role: 'listitem' },
          h('span', {
            className: 'la-legend-swatch',
            style: { background: leg.fill },
            'aria-hidden': 'true',
          }),
          leg.name,
        )),
      ),
      loadingBridge && h('div', { className: 'la-chart-loading' },
        Spinner && h(Spinner, { size: 24 }),
        h('span', null, 'Loading…'),
      ),
      !loadingBridge && bridgeOk && chartOption && window.EChart && h('div', { className: 'la-chart-body' },
        h(window.EChart, {
          className: 'la-waterfall-chart',
          style: { width: '100%', height: waterfallHeight },
          option: chartOption,
        }),
      ),
      noBridgeChart && !err && h('div', { className: 'la-empty la-empty--compact' },
        scopeHasNoPickList
          ? h('p', { className: 'la-empty-text' }, hierarchyHint || 'Nothing to load at this level for this plant.')
          : h('p', { className: 'la-empty-text' }, 'No bridge to show (zero expected and actual at this level). Pick Plant, Inverter, or SCB, another string, another date range, or check metadata and meter data.'),
      ),
    ),

    showContent && !loadingBridge && icebergOk && h('section', { className: 'la-chart-section card la-iceberg-section' },
      h('div', { className: 'la-panel-head la-panel-head--tight' },
        h('h2', { className: 'la-panel-title' }, 'Fault impact by category'),
        h('span', { className: 'la-iceberg-sub' },
          'Sorted by energy loss (high → low)',
          loadedRangeLabel && h('span', { className: 'la-iceberg-range' }, ` · ${loadedRangeLabel}`),
        ),
      ),
      data.iceberg_scope_note && h('p', { className: 'la-iceberg-note' }, data.iceberg_scope_note),
      h('div', { className: 'la-iceberg-tone-key', 'aria-hidden': 'true' },
        h('span', { className: 'la-iceberg-tone-key-item' },
          h('span', { className: 'la-iceberg-tone-swatch la-iceberg-tone-swatch--navy' }),
          'Fault frequency',
        ),
        h('span', { className: 'la-iceberg-tone-key-item' },
          h('span', { className: 'la-iceberg-tone-swatch la-iceberg-tone-swatch--bronze' }),
          'Energy loss (MWh)',
        ),
      ),
      h('div', { className: 'la-iceberg-legend', role: 'list', 'aria-label': 'Categories by energy loss' },
        icebergLegendRows.map((row) => h('span', { key: row.id, className: 'la-iceberg-legend-item', role: 'listitem' },
          h('span', { className: 'la-iceberg-legend-label' }, row.label),
          h('span', { className: 'la-iceberg-legend-meta' },
            `${Number(row.fault_count) || 0} events · ${fmtKpiMwh(row.loss_mwh)} MWh`,
          ),
        )),
      ),
      h('div', { className: 'la-chart-body la-chart-body--iceberg' },
        h(LossAnalysisIcebergChart, { rows: icebergRows }),
      ),
    ),

    showContent && !loadingBridge && showWorstUnknown && worstOption && window.EChart && h('section', { className: 'la-chart-section card' },
      h('h2', { className: 'la-panel-title la-panel-title--solo' }, 'Unknown by entity (plant)'),
      h('div', { className: 'la-chart-body la-chart-body--compact' },
        h(window.EChart, {
          className: 'la-worst-chart',
          style: { width: '100%', height: Math.min(480, Math.max(180, worst.length * 36 + 40)) },
          option: worstOption,
        }),
      ),
    ),

    showContent && !loadingBridge && bridgeOk && enrichedTableRows.length > 0 && DataTable && h('section', { className: 'la-table-section card' },
      h('h2', { className: 'la-panel-title la-panel-title--solo' }, 'Entities'),
      h(DataTable, {
        columns: LOSS_ANALYSIS_TABLE_COLUMNS,
        rows: enrichedTableRows,
        emptyMessage: 'No rows',
        maxHeight: 420,
        filename: `loss_analysis_${plantId || 'plant'}_${dateFrom}_${dateTo}.csv`,
        initialSortKey: 'unknown_mwh',
        initialSortDir: 'desc',
      }),
    ),

    !plantId && h('div', { className: 'la-empty card' },
      LucideIcon && h(LucideIcon, { name: 'Building2', size: 36 }),
      h('p', null, 'Pick a plant and dates from the header.'),
    ),

    plantId && scope !== 'plant' && !scopeHasNoPickList && !equipmentId && h('div', { className: 'la-empty card' },
      LucideIcon && h(LucideIcon, { name: 'Layers', size: 36 }),
      h('p', null, 'Select equipment above.'),
    ),
  );
};

console.info('[solar-trace] loss_analysis.js loaded (LossAnalysisPage + LossAnalysisBoundary)');
