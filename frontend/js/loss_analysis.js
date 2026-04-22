// Loss Analysis — hierarchy selector, stacked waterfall bridge, worst unknown chart, detail table.
const { useState, useEffect, useCallback, useMemo } = React;
const h = React.createElement;
const { Card } = window;
const { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Cell, ReferenceLine } = window.Recharts || {};

function fmtMwh(v) {
  if (v == null || Number.isNaN(v)) return '—';
  return `${Number(v).toFixed(3)} MWh`;
}

function fmtPctOfExpected(v, expectedMwh) {
  if (v == null || Number.isNaN(v) || !expectedMwh || expectedMwh <= 0) return '—';
  return `${((Number(v) / expectedMwh) * 100).toFixed(2)} %`;
}

function bridgeSegmentFill(entry) {
  if (entry.kind === 'total') return '#22c55e';
  if (entry.kind === 'unknown') return '#a855f7';
  return '#2563eb';
}

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

  function friendlyApiMessage(e, context) {
    let m = (e && e.message) ? String(e.message) : String(e || 'Error');
    const st = e && e.status;
    if (st === 404 || /not found|^404$/i.test(m)) {
      return `${context}: server returned 404. Deploy the latest API (routes: /api/dashboard/loss-analysis/* and /api/loss-analysis/*) and restart.`;
    }
    return `${context}: ${m}`;
  }

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
        setOptsErr(friendlyApiMessage(e, 'Could not load hierarchy options'));
      })
      .finally(() => setOptsLoading(false));
  }, [plantId]);

  const loadBridge = useCallback(() => {
    if (!plantId) return;
    setLoadingBridge(true);
    setErr('');
    const eq =
      scope === 'plant'
        ? ''
        : scope === 'string'
          ? equipmentId
          : equipmentId;
    window.SolarAPI.LossAnalysis.bridge(plantId, dateFrom, dateTo, scope, eq || undefined)
      .then((d) => {
        if (d.error) {
          setErr(d.message || d.error);
          setData(null);
        } else {
          setData(d);
        }
      })
      .catch((e) => {
        setErr(e.message || String(e));
        setData(null);
      })
      .finally(() => setLoadingBridge(false));
  }, [plantId, dateFrom, dateTo, scope, equipmentId]);

  useEffect(() => {
    if (!plantId) return;
    if (scope !== 'plant' && !equipmentId) {
      setData(null);
      setErr('');
      setLoadingBridge(false);
      return;
    }
    loadBridge();
  }, [plantId, dateFrom, dateTo, scope, equipmentId, loadBridge]);

  useEffect(() => {
    setEquipmentId('');
  }, [scope]);

  const bridgeRaw = (data && data.waterfall_bridge) || [];
  const worst = data && data.worst_unknown ? data.worst_unknown : [];
  const tableRows = data && data.table ? data.table : [];
  const primary = data && data.primary;
  const expBase = primary && Number(primary.expected_mwh) > 0 ? Number(primary.expected_mwh) : 0;

  const bridgeChartData = useMemo(() => {
    if (!bridgeRaw.length) return [];
    const scale = wfUnit === 'pct' && expBase > 0 ? 100 / expBase : 1;
    return bridgeRaw.map((row) => ({
      ...row,
      _inv: Number(row.invisible_mwh || 0) * scale,
      _vis: Number(row.visible_mwh || 0) * scale,
    }));
  }, [bridgeRaw, wfUnit, expBase]);

  const yAxisLabel = wfUnit === 'pct' && expBase > 0 ? '% of expected (100% = expected)' : 'MWh';

  const tableColumns = [
    { key: 'label', label: 'Entity', render: (r) => h('strong', null, r.label) },
    { key: 'expected_mwh', label: 'Expected', render: (r) => fmtMwh(r.expected_mwh) },
    { key: 'actual_mwh', label: 'Actual', render: (r) => fmtMwh(r.actual_mwh) },
    { key: 'all_losses_mwh', label: 'All losses', render: (r) => fmtMwh(r.all_losses_mwh) },
    { key: 'unknown_mwh', label: 'Unknown', render: (r) => fmtMwh(r.unknown_mwh) },
  ];

  const bridgeTooltip = useCallback(
    ({ active, payload }) => {
      if (!active || !payload || !payload.length) return null;
      const row = payload[0].payload;
      const visRaw = Number(row.visible_mwh || 0);
      return h(
        'div',
        {
          className: 'recharts-default-tooltip',
          style: { background: 'var(--panel)', border: '1px solid var(--line)', padding: 8, borderRadius: 6 },
        },
        h('div', { style: { fontWeight: 600, marginBottom: 4 } }, row.label),
        h('div', { style: { fontSize: 12 } }, `Step: ${fmtMwh(visRaw)}`),
        expBase > 0 &&
          h('div', { style: { fontSize: 11, color: 'var(--text-muted)' } }, `${fmtPctOfExpected(visRaw, expBase)} of expected`),
      );
    },
    [expBase],
  );

  return h('div', { style: { display: 'flex', flexDirection: 'column', gap: 16 } },
    h(Card, { title: 'Loss Analysis — energy bridge' },
      h('p', { style: { fontSize: 12, color: 'var(--text-muted)', marginBottom: 12, maxWidth: 900 } },
        'Waterfall steps down from Expected through degradation, temperature, and each Fault Diagnostics category, then Unknown, to Actual. ',
        'Table stays aggregated (Expected, Actual, All losses, Unknown). Set degradation % under Metadata → Equipment.',
      ),
      h('div', { style: { display: 'flex', flexWrap: 'wrap', gap: 12, alignItems: 'flex-end', marginBottom: 12 } },
        h('div', { className: 'form-group', style: { minWidth: 160 } },
          h('label', { className: 'form-label' }, 'Hierarchy'),
          h('select', {
            className: 'form-input',
            style: { height: 36, minWidth: 160 },
            value: scope,
            onChange: (e) => setScope(e.target.value),
          },
            h('option', { value: 'plant' }, 'Plant (whole site)'),
            h('option', { value: 'inverter' }, 'Equipment (inverter)'),
            h('option', { value: 'scb' }, 'SCB'),
            h('option', { value: 'string' }, 'String'),
          ),
        ),
        scope !== 'plant' && h('div', { className: 'form-group', style: { flex: '1 1 220px', minWidth: 200 } },
          h('label', { className: 'form-label' }, scope === 'inverter' ? 'Inverter' : scope === 'scb' ? 'SCB' : 'String'),
          scope === 'inverter' && h('select', {
            className: 'form-input',
            style: { height: 36, width: '100%' },
            value: equipmentId,
            onChange: (e) => setEquipmentId(e.target.value),
          },
            h('option', { value: '' }, '— Select —'),
            ...(opts.inverters || []).map((id) => h('option', { key: id, value: id }, id)),
          ),
          scope === 'scb' && h('select', {
            className: 'form-input',
            style: { height: 36, width: '100%' },
            value: equipmentId,
            onChange: (e) => setEquipmentId(e.target.value),
          },
            h('option', { value: '' }, '— Select —'),
            ...(opts.scbs || []).map((id) => h('option', { key: id, value: id }, id)),
          ),
          scope === 'string' && h('select', {
            className: 'form-input',
            style: { height: 36, width: '100%' },
            value: equipmentId,
            onChange: (e) => setEquipmentId(e.target.value),
          },
            h('option', { value: '' }, '— Select —'),
            ...(opts.strings || []).map((s) =>
              h('option', { key: s.label, value: `${s.inverter_id}::${s.scb_id}::${s.string_id}` }, s.label),
            ),
          ),
        ),
        h('button', {
          type: 'button',
          className: 'btn btn-primary',
          onClick: loadBridge,
          disabled: loadingBridge || optsLoading || (scope !== 'plant' && !equipmentId),
        }, loadingBridge ? 'Loading…' : 'Refresh'),
        optsLoading && h('span', { style: { fontSize: 11, color: 'var(--text-muted)', alignSelf: 'center' } }, 'Updating equipment list…'),
      ),
      optsErr && h('div', { className: 'empty-state', style: { color: 'var(--bad)', marginBottom: 12 } }, optsErr),
      err && h('div', { className: 'empty-state', style: { color: 'var(--bad)', marginBottom: 12 } }, err),
      data && h('div', { style: { fontSize: 11, color: 'var(--text-muted)', marginBottom: 8 } },
        `Insolation ${data.insolation_kwh_m2} kWh/m² · Module temp ~${data.module_temp_c}°C · Temp coeff ${data.temp_coefficient_used}`,
      ),
    ),

    plantId && (scope === 'plant' || equipmentId) && h(Card, { title: 'Energy bridge (primary selection)' },
      !loadingBridge && primary && bridgeChartData.length > 0 && h('div', {
        style: {
          display: 'flex',
          flexWrap: 'wrap',
          gap: 8,
          alignItems: 'center',
          marginBottom: 10,
        },
      },
        h('span', { style: { fontSize: 12, color: 'var(--text-muted)' } }, 'Waterfall scale:'),
        h('button', {
          type: 'button',
          className: wfUnit === 'mwh' ? 'btn btn-primary' : 'btn btn-outline',
          style: { minHeight: 32, padding: '0 12px', fontSize: 12 },
          onClick: () => setWfUnit('mwh'),
        }, 'MWh'),
        h('button', {
          type: 'button',
          className: wfUnit === 'pct' ? 'btn btn-primary' : 'btn btn-outline',
          style: { minHeight: 32, padding: '0 12px', fontSize: 12 },
          disabled: expBase <= 0,
          title: expBase <= 0 ? 'Expected energy is zero — use MWh' : 'Expected energy = 100%',
          onClick: () => setWfUnit('pct'),
        }, '% (expected = 100%)'),
        h('span', { style: { fontSize: 11, color: 'var(--text-muted)' } },
          'Green: totals · Blue: losses · Purple: unknown',
        ),
      ),
      loadingBridge && h('div', { style: { padding: 20, textAlign: 'center', color: 'var(--text-muted)', fontSize: 13 } }, 'Loading energy bridge…'),
      !loadingBridge && primary && bridgeChartData.length > 0 && ResponsiveContainer && h(ResponsiveContainer, { width: '100%', height: 400 },
        h(BarChart, { data: bridgeChartData, margin: { top: 16, right: 16, left: 8, bottom: 72 } },
          h(CartesianGrid, { strokeDasharray: '3 3', stroke: 'var(--line)' }),
          h(XAxis, {
            dataKey: 'label',
            tick: { fontSize: 8, fill: 'var(--text-muted)' },
            interval: 0,
            angle: -40,
            textAnchor: 'end',
            height: 78,
          }),
          h(YAxis, {
            tick: { fontSize: 10, fill: 'var(--text-soft)' },
            label: { value: yAxisLabel, angle: -90, position: 'insideLeft', fill: 'var(--text-muted)', fontSize: 10 },
          }),
          h(Tooltip, { content: bridgeTooltip }),
          h(ReferenceLine, { y: 0, stroke: 'var(--line)' }),
          h(Bar, {
            dataKey: '_inv',
            stackId: 'wf',
            fill: 'rgba(0,0,0,0)',
            stroke: 'none',
            isAnimationActive: false,
          }),
          h(Bar, { dataKey: '_vis', stackId: 'wf', name: 'Step', isAnimationActive: false },
            bridgeChartData.map((entry, i) => h(Cell, { key: i, fill: bridgeSegmentFill(entry) })),
          ),
        ),
      ),
      !loadingBridge && primary && bridgeChartData.length > 0 && !ResponsiveContainer && h('div', { className: 'empty-state' }, 'Charts require Recharts.'),
      !loadingBridge && primary && !bridgeChartData.length && !err && h('div', { className: 'empty-state' }, 'No bridge data for this range.'),
    ),

    !loadingBridge && primary && worst.length > 0 && h(Card, { title: 'Worst unknown loss (top 10)' },
      ResponsiveContainer && h(ResponsiveContainer, { width: '100%', height: 260 },
        h(BarChart, { layout: 'vertical', data: worst, margin: { left: 100, right: 16 } },
          h(CartesianGrid, { strokeDasharray: '3 3' }),
          h(XAxis, { type: 'number', tick: { fontSize: 10 } }),
          h(YAxis, { type: 'category', dataKey: 'label', width: 96, tick: { fontSize: 10 } }),
          h(Tooltip, { formatter: (v) => fmtMwh(v) }),
          h(Bar, { dataKey: 'unknown_mwh', fill: '#a855f7', radius: [0, 4, 4, 0] }),
        ),
      ),
    ),

    !loadingBridge && tableRows.length > 0 && window.DataTable && h(Card, { title: 'Detail table' },
      h(window.DataTable, {
        columns: tableColumns,
        rows: tableRows,
        emptyMessage: 'No rows',
        maxHeight: 420,
        filename: `loss_analysis_${plantId || 'plant'}.csv`,
      }),
    ),
    !loadingBridge && tableRows.length > 0 && !window.DataTable && h(Card, { title: 'Detail table' },
      h('table', { className: 'data-table', style: { width: '100%', fontSize: 12 } },
        h('thead', null,
          h('tr', null, tableColumns.map((c) => h('th', { key: c.key, style: { textAlign: 'left', padding: 8 } }, c.label))),
        ),
        h('tbody', null,
          tableRows.map((r, i) =>
            h('tr', { key: i },
              tableColumns.map((c) => h('td', { key: c.key, style: { padding: 8, borderTop: '1px solid var(--line)' } },
                c.render ? c.render(r) : r[c.key],
              )),
            ),
          ),
        ),
      ),
    ),
  );
};
