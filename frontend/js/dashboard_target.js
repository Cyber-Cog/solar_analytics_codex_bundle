// Dashboard: Expected vs Actual generation (MWh) — from dashboard bundle + fault_cache snapshot; Loss Analysis bridge as fallback.
const { useState, useEffect, useMemo } = React;
const h = React.createElement;
const {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  Cell,
  LabelList,
} = window.Recharts || {};

function _fmtMwh(v) {
  if (v == null || Number.isNaN(Number(v))) return '—';
  const n = Number(v);
  if (Math.abs(n) >= 1000) return `${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

window.DashboardTargetGeneration = ({ plantId, dateFrom, dateTo, targetGeneration }) => {
  const { Card, Spinner } = window;
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState('');
  const [expectedMwh, setExpectedMwh] = useState(null);
  const [actualMwh, setActualMwh] = useState(null);

  const bundleExp = targetGeneration != null ? targetGeneration.expected_mwh : undefined;
  const bundleAct = targetGeneration != null ? targetGeneration.actual_mwh : undefined;

  useEffect(() => {
    if (!plantId || !dateFrom || !dateTo) {
      setExpectedMwh(null);
      setActualMwh(null);
      return;
    }
    if (bundleExp != null && bundleAct != null) {
      setExpectedMwh(Number(bundleExp));
      setActualMwh(Number(bundleAct));
      setErr('');
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setErr('');
    window.SolarAPI.LossAnalysis.bridge(plantId, dateFrom, dateTo, 'plant')
      .then((d) => {
        if (cancelled) return;
        if (d && d.error) {
          setErr(d.message || d.error || 'Could not load generation comparison');
          setExpectedMwh(null);
          setActualMwh(null);
          return;
        }
        const p = d && d.primary;
        if (p) {
          setExpectedMwh(Number(p.expected_mwh));
          setActualMwh(Number(p.actual_mwh));
        } else {
          setExpectedMwh(null);
          setActualMwh(null);
        }
      })
      .catch((e) => {
        if (cancelled) return;
        setErr(e.message || String(e));
        setExpectedMwh(null);
        setActualMwh(null);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [plantId, dateFrom, dateTo, bundleExp, bundleAct]);

  const chartData = useMemo(() => {
    const e = expectedMwh != null && !Number.isNaN(expectedMwh) ? expectedMwh : null;
    const a = actualMwh != null && !Number.isNaN(actualMwh) ? actualMwh : null;
    if (e == null && a == null) return [];
    return [
      { label: 'Expected', mwh: e != null ? e : 0, fillKey: 'exp' },
      { label: 'Actual', mwh: a != null ? a : 0, fillKey: 'act' },
    ];
  }, [expectedMwh, actualMwh]);

  const yMax = useMemo(() => {
    if (!chartData.length) return 1;
    const m = Math.max(...chartData.map((r) => Number(r.mwh) || 0), 0);
    return m <= 0 ? 1 : m * 1.12;
  }, [chartData]);

  const ratioPct = useMemo(() => {
    if (expectedMwh == null || actualMwh == null || Number.isNaN(expectedMwh) || Number.isNaN(actualMwh)) return null;
    if (expectedMwh <= 0) return null;
    return (actualMwh / expectedMwh) * 100;
  }, [expectedMwh, actualMwh]);

  const hasChart = chartData.length > 0 && ResponsiveContainer && BarChart;

  return h(Card, {
    title: 'Expected vs actual generation (MWh)',
    style: { minHeight: 480, display: 'flex', flexDirection: 'column' },
  },
    h('p', {
      style: {
        fontSize: 12,
        color: 'var(--text-muted)',
        marginBottom: 10,
        lineHeight: 1.45,
        flexShrink: 0,
      },
    },
      'Expected uses the same loss-analysis model as the Loss Analysis page (DC kWp × tilt insolation for the selected range). ',
      'Actual is metered plant AC energy for the same range.',
    ),
    ratioPct != null && h('div', {
      style: {
        fontSize: 13,
        fontWeight: 700,
        color: ratioPct >= 95 ? '#15803d' : ratioPct >= 80 ? '#a16207' : '#b91c1c',
        marginBottom: 10,
        fontVariantNumeric: 'tabular-nums',
        flexShrink: 0,
      },
    }, `Actual / expected: ${ratioPct.toFixed(1)}%`),
    loading && h('div', { style: { padding: 36, textAlign: 'center', flex: 1 } }, h(Spinner)),
    err && !loading && h('div', { className: 'empty-state', style: { color: 'var(--bad)' } }, err),
    !loading && !err && !hasChart && h('div', { className: 'empty-state' }, 'No data for this range.'),
    // Explicit pixel height: ResponsiveContainer height="100%" collapses to 0 when the parent
    // only has minHeight (no resolved height), which yields a blank chart while KPI text still shows.
    !loading && !err && hasChart && h('div', {
      style: {
        width: '100%',
        height: 400,
        borderRadius: 10,
        background: 'linear-gradient(180deg, rgba(14,165,233,0.06) 0%, rgba(15,23,42,0.02) 45%, transparent 100%)',
        border: '1px solid var(--line)',
        padding: '8px 4px 4px',
        boxSizing: 'border-box',
      },
    },
      h(ResponsiveContainer, { width: '100%', height: 400 },
        h(BarChart, {
          data: chartData,
          margin: { top: 28, right: 16, left: 4, bottom: 4 },
          barCategoryGap: '28%',
        },
          h('defs', null,
            h('linearGradient', { id: 'dashBarExpected', x1: '0', y1: '0', x2: '0', y2: '1' },
              h('stop', { offset: '0%', stopColor: '#38bdf8', stopOpacity: 1 }),
              h('stop', { offset: '100%', stopColor: '#0369a1', stopOpacity: 0.92 }),
            ),
            h('linearGradient', { id: 'dashBarActual', x1: '0', y1: '0', x2: '0', y2: '1' },
              h('stop', { offset: '0%', stopColor: '#4ade80', stopOpacity: 1 }),
              h('stop', { offset: '100%', stopColor: '#15803d', stopOpacity: 0.9 }),
            ),
          ),
          h(CartesianGrid, { strokeDasharray: '3 3', stroke: 'var(--line)', vertical: false }),
          h(XAxis, {
            dataKey: 'label',
            tick: { fill: 'var(--text-muted)', fontSize: 12, fontWeight: 600 },
            axisLine: { stroke: 'var(--line)' },
            tickLine: false,
          }),
          h(YAxis, {
            domain: [0, yMax],
            tick: { fill: 'var(--text-soft)', fontSize: 11 },
            axisLine: false,
            tickLine: false,
            width: 48,
            label: {
              value: 'MWh',
              angle: -90,
              position: 'insideLeft',
              fill: 'var(--text-muted)',
              fontSize: 11,
            },
          }),
          h(Tooltip, {
            formatter: (v) => [`${Number(v).toLocaleString(undefined, { maximumFractionDigits: 3 })} MWh`, ''],
            contentStyle: { background: 'var(--panel)', border: '1px solid var(--line)', borderRadius: 8 },
          }),
          h(Legend, { wrapperStyle: { fontSize: 11, paddingTop: 4 } }),
          h(Bar, {
            dataKey: 'mwh',
            name: 'Energy (MWh)',
            radius: [10, 10, 4, 4],
            maxBarSize: 140,
          },
            chartData.map((row, i) =>
              h(Cell, {
                key: i,
                fill: row.fillKey === 'exp' ? 'url(#dashBarExpected)' : 'url(#dashBarActual)',
              }),
            ),
            LabelList && h(LabelList, {
              dataKey: 'mwh',
              position: 'top',
              fill: 'var(--text)',
              fontSize: 13,
              fontWeight: 700,
              formatter: (v) => _fmtMwh(v),
            }),
          ),
        ),
      ),
    ),
  );
};
