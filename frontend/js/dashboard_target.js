// Dashboard: Expected vs Actual generation (MWh) — from dashboard bundle + fault_cache snapshot; Loss Analysis bridge as fallback.
const { useState, useEffect, useMemo } = React;
const h = React.createElement;
// Recharts removed during ECharts migration

function _fmtMwh(v) {
  if (v == null || Number.isNaN(Number(v))) return '—';
  const n = Number(v);
  if (Math.abs(n) >= 1000) return `${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

window.DashboardTargetGeneration = ({ plantId, dateFrom, dateTo, targetGeneration, apiTargetGenPending }) => {
  const { Card, Spinner } = window;
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState('');
  const [expectedMwh, setExpectedMwh] = useState(null);
  const [actualMwh, setActualMwh] = useState(null);

  const bundleExp = targetGeneration != null ? targetGeneration.expected_mwh : undefined;
  const bundleAct = targetGeneration != null ? targetGeneration.actual_mwh : undefined;
  const tgApiErr = targetGeneration && (targetGeneration.compute_error || targetGeneration.error);
  const tgPending = Boolean(apiTargetGenPending);

  useEffect(() => {
    if (!plantId || !dateFrom || !dateTo) {
      setExpectedMwh(null);
      setActualMwh(null);
      return;
    }
    if (tgApiErr) {
      setErr(String(tgApiErr));
      setExpectedMwh(null);
      setActualMwh(null);
      setLoading(false);
      return;
    }
    if (bundleExp != null && bundleAct != null) {
      setExpectedMwh(Number(bundleExp));
      setActualMwh(Number(bundleAct));
      setErr('');
      setLoading(false);
      return;
    }
    // Wait for /target-generation to finish; do not fire LossAnalysis.bridge in parallel (duplicate load).
    if (tgPending) {
      setLoading(true);
      setErr('');
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
  }, [plantId, dateFrom, dateTo, bundleExp, bundleAct, tgPending, tgApiErr]);

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

  const chartOption = useMemo(() => {
    if (chartData.length === 0) return null;
    return {
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        formatter: (params) => {
          const val = params[0].value;
          return `${params[0].name}<br/><span style="font-weight:bold">${Number(val).toLocaleString(undefined, { maximumFractionDigits: 3 })} MWh</span>`;
        },
        backgroundColor: 'var(--panel)',
        borderColor: 'var(--line)',
        textStyle: { color: 'var(--text)', fontSize: 13 },
      },
      grid: { top: 40, right: 16, bottom: 24, left: 56 },
      xAxis: {
        type: 'category',
        data: chartData.map((r) => r.label),
        axisLine: { lineStyle: { color: 'var(--line)' } },
        axisTick: { show: false },
        axisLabel: { color: 'var(--text-muted)', fontSize: 13, fontWeight: 600, margin: 12 },
      },
      yAxis: {
        type: 'value',
        name: 'MWh',
        nameLocation: 'middle',
        nameGap: 40,
        nameTextStyle: { color: 'var(--text-muted)', fontSize: 12 },
        max: yMax,
        splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)', opacity: 0.5 } },
        axisLabel: { color: 'var(--text-soft)', fontSize: 12 },
      },
      series: [
        {
          type: 'bar',
          data: chartData.map((r) => ({
            value: r.mwh,
            itemStyle: {
              color: new window.echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: r.fillKey === 'exp' ? '#38bdf8' : '#4ade80' },
                { offset: 1, color: r.fillKey === 'exp' ? '#0369a1' : '#15803d' },
              ]),
              borderRadius: [6, 6, 0, 0],
            },
          })),
          barMaxWidth: 140,
          label: {
            show: true,
            position: 'top',
            formatter: (p) => _fmtMwh(p.value),
            color: 'var(--text)',
            fontSize: 13,
            fontWeight: 700,
            distance: 8,
          },
        },
      ],
    };
  }, [chartData, yMax]);

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
    !loading && !err && !chartOption && h('div', { className: 'empty-state' }, 'No data for this range.'),
    !loading && !err && chartOption && h('div', {
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
      h(window.EChart, { option: chartOption, style: { width: '100%', height: 380 } })
    ),
  );
};
