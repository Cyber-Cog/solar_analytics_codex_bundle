// frontend/js/inv_eff_analysis.js
// Inverter Efficiency Loss Analysis Module
const { useState, useEffect, useMemo } = React;
const { 
  ResponsiveContainer, ComposedChart, LineChart, Line, BarChart, Bar, XAxis, YAxis, 
  CartesianGrid, Tooltip, Legend, Cell, ErrorBar, Scatter, ReferenceLine, ReferenceArea, Brush
} = Recharts;
const { Card, Spinner, KpiCard } = window;

window.InverterEfficiencyAnalysis = ({ plantId, dateFrom, dateTo }) => {
  const h = React.createElement;
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [data, setData] = useState({ metrics: {}, inverters: [], trend: [], inverter_box_stats: [] });

  const fetchData = async () => {
    if (!plantId) return;
    setLoading(true);
    setError('');
    try {
      const res = await window.SolarAPI.Faults.inverterEfficiencyAnalysis(plantId, dateFrom, dateTo);
      setData(res);
    } catch (e) {
      console.error("Failed to fetch efficiency analysis:", e);
      setError(e.message || 'Failed to fetch inverter efficiency analysis.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [plantId, dateFrom, dateTo]);

  // Box plot: one vertical box per inverter from inverter_box_stats (per-timestamp Efficiency = AC/DC * 100)
  const boxPlotData = useMemo(() => {
    const stats = data.inverter_box_stats || [];
    if (stats.length === 0) return [];
    return stats.map((row) => {
      const errMin = row.errorMin != null ? row.errorMin : (row.median != null && row.min != null ? row.median - row.min : 0);
      const errMax = row.errorMax != null ? row.errorMax : (row.max != null && row.median != null ? row.max - row.median : 0);
      return {
        inverter_id: row.inverter_id,
        name: row.inverter_id,
        min: row.min,
        q1: row.q1,
        median: row.median,
        q3: row.q3,
        max: row.max,
        iqrLength: row.iqrLength != null ? row.iqrLength : (row.q3 != null && row.q1 != null ? Math.round((row.q3 - row.q1) * 100) / 100 : 0),
        errorMin: errMin,
        errorMax: errMax,
        errorRange: [errMin, errMax],
      };
    });
  }, [data.inverter_box_stats]);

  if (loading) return h('div', { className: 'empty-state' }, h(Spinner));
  if (error) return h('div', { className: 'empty-state' }, error);
  if (!data.metrics.total_dc_mwh) return h('div', { className: 'empty-state' }, 'No sufficient DC/AC power data for analysis in this period.');

  const { metrics, inverters, trend } = data;

  return h('div', { style: { display: 'flex', flexDirection: 'column', gap: 20 } },
    
    // ── KPI Section ──────────────────────────────────────────────────────────
    h('div', { className: 'kpi-grid', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' } },
      h(KpiCard, {
        label: 'Total DC Energy',
        value: metrics.total_dc_mwh, unit: 'MWh', color: 'var(--text-main)'
      }),
      h(KpiCard, {
        label: 'Total AC Energy',
        value: metrics.total_ac_mwh, unit: 'MWh', color: 'var(--accent)'
      }),
      h(KpiCard, {
        label: 'Conv. Loss Energy',
        value: metrics.total_loss_mwh, unit: 'MWh', color: '#EF4444'
      }),
      h(KpiCard, {
        label: 'Avg Efficiency (%)',
        value: metrics.avg_efficiency_pct, unit: `% (Target: ${metrics.target_efficiency_pct}%)`,
        color: metrics.avg_efficiency_pct < metrics.target_efficiency_pct ? '#F59E0B' : '#10B981'
      })
    ),

    // ── Box Plot: one vertical box per inverter; Efficiency (%) = (AC Power / DC Power) × 100 across time; X = Inverter ID, Y = Efficiency (94–100%) ──
    boxPlotData.length > 0 && h(Card, { title: 'Inverter Efficiency Distribution (Box Plot)' },
      h('div', { style: { height: Math.max(320, Math.min(520, 120 + boxPlotData.length * 28)), position: 'relative' } },
        h(ResponsiveContainer, { width: '100%', height: '100%' },
          h(ComposedChart, {
            data: boxPlotData,
            margin: { top: 20, right: 24, left: 12, bottom: 80 },
          },
            h(CartesianGrid, { strokeDasharray: '3 3', stroke: 'rgba(255,255,255,0.06)', vertical: true, horizontal: true }),
            h(XAxis, {
              type: 'category',
              dataKey: 'inverter_id',
              tick: { fontSize: 10, fill: 'var(--text-soft)' },
              interval: 0,
              angle: -45,
              textAnchor: 'end',
              label: { value: 'Inverter ID', position: 'insideBottom', offset: -8, fill: 'var(--text-soft)', fontSize: 12 },
            }),
            h(YAxis, {
              type: 'number',
              domain: [94, 100],
              tick: { fontSize: 11, fill: 'var(--text-soft)' },
              tickFormatter: v => v + '%',
              label: { value: 'Efficiency (%)', angle: -90, position: 'insideLeft', fill: 'var(--text-soft)', fontSize: 12 },
              axisLine: { stroke: 'var(--line-soft)' },
              tickLine: { stroke: 'var(--line-soft)' },
            }),
            h(Tooltip, {
              cursor: { fill: 'rgba(255,255,255,0.04)', stroke: 'var(--accent)', strokeWidth: 1 },
              content: ({ active, payload }) => {
                if (!active || !payload || !payload.length) return null;
                const d = payload[0].payload;
                return h('div', { className: 'chart-tooltip', style: { background: 'var(--panel)', border: '1px solid var(--line)', borderRadius: 8, padding: '10px 12px', fontSize: 12 } },
                  h('div', { style: { fontWeight: 700, marginBottom: 6, color: 'var(--text)' } }, d.inverter_id || d.name),
                  h('div', { style: { marginBottom: 2 } }, `Min: ${d.min != null ? d.min : '—'}%`),
                  h('div', { style: { marginBottom: 2 } }, `Q1: ${d.q1 != null ? d.q1 : '—'}%`),
                  h('div', { style: { color: '#EF4444', fontWeight: 700, marginBottom: 2 } }, `Median: ${d.median != null ? d.median : '—'}%`),
                  h('div', { style: { marginBottom: 2 } }, `Q3: ${d.q3 != null ? d.q3 : '—'}%`),
                  h('div', null, `Max: ${d.max != null ? d.max : '—'}%`),
                );
              },
            }),
            h(Bar, { dataKey: 'q1', stackId: 'box', fill: 'transparent', barSize: 22, radius: 0 }),
            h(Bar, {
              dataKey: 'iqrLength',
              stackId: 'box',
              fill: 'rgba(14, 165, 233, 0.35)',
              stroke: 'var(--accent)',
              strokeWidth: 1,
              barSize: 22,
              radius: 0,
            }),
            h(Bar, {
              dataKey: 'median',
              fill: '#EF4444',
              barSize: 4,
              radius: 0,
              minPointSize: 2,
              children: [
                h(ErrorBar, {
                  dataKey: 'errorRange',
                  direction: 'y',
                  stroke: 'var(--accent)',
                  strokeWidth: 1.5,
                  strokeDasharray: '3 2',
                  width: 6,
                }),
              ],
            }),
          )
        ),
        h('div', { style: { position: 'absolute', bottom: 8, left: '50%', transform: 'translateX(-50%)', fontSize: 11, color: 'var(--text-muted)' } },
          'Whiskers: Min–Max | Box: Q1–Q3 | Red line: Median (per-inverter efficiency across selected day)'
        )
      )
    ),

    // ── Bar Chart: Loss per Inverter ─────────────────────────────────────────
    h(Card, { title: 'Inverter Conversion Loss Energy (MWh) - Descending' },
      h(ResponsiveContainer, { width: '100%', height: 300 },
        h(BarChart, { data: inverters, margin: { top: 5, right: 10, left: 0, bottom: 20 } },
          h(CartesianGrid, { strokeDasharray: '3 3', vertical: false }),
          h(XAxis, { dataKey: 'inverter_id', tick: { fontSize: 10 }, interval: 0, angle: -45, textAnchor: 'end' }),
          h(YAxis, { tick: { fontSize: 10 }, label: { value: 'MWh', angle: -90, position: 'insideLeft', fontSize: 11 } }),
          h(Tooltip, {
            content: ({ active, payload }) => {
              if (active && payload && payload.length) {
                const d = payload[0].payload;
                return h('div', { className: 'chart-tooltip' },
                  h('div', { style: { fontWeight: 700, marginBottom: 4 } }, d.inverter_id),
                  h('div', null, `Loss Energy: ${d.loss_energy_mwh} MWh`),
                  h('div', null, `Efficiency: ${d.efficiency_pct}%`),
                  h('div', null, `DC Energy: ${d.dc_energy_mwh} MWh`),
                  h('div', null, `AC Energy: ${d.ac_energy_mwh} MWh`)
                );
              }
              return null;
            }
          }),
          h(Bar, { dataKey: 'loss_energy_mwh', fill: '#EF4444', radius: [2, 2, 0, 0] },
            inverters.map((entry, index) => (
              h(Cell, { key: `cell-${index}`, fill: index < 3 ? '#B91C1C' : '#EF4444' })
            ))
          )
        )
      )
    ),

    // ── Trend Line Graph ─────────────────────────────────────────────────────
    h(Card, { title: 'Fleet-wide Efficiency Trend (%)' },
      h(ResponsiveContainer, { width: '100%', height: 300 },
        h(LineChart, { data: trend, margin: { top: 5, right: 20, left: 0, bottom: 5 } },
          h(CartesianGrid, { strokeDasharray: '3 3', stroke: '#F1F5F9' }),
          h(XAxis, { dataKey: 'timestamp', tick: { fontSize: 10 }, tickFormatter: v => v.slice(11, 16) }),
          h(YAxis, { domain: [d => Math.floor(d - 2), 100], tick: { fontSize: 10 } }),
          h(Tooltip),
          h(Legend, { verticalAlign: 'top', height: 36 }),
          h(Line, { 
            type: 'monotone', dataKey: 'efficiency_pct', name: 'Actual Efficiency (%)', 
            stroke: 'var(--accent)', strokeWidth: 2, dot: false, connectNulls: true 
          }),
          h(Line, { 
            type: 'stepAfter', dataKey: 'target_efficiency', name: 'Target Benchmark (%)', 
            stroke: '#10B981', strokeWidth: 1.5, strokeDasharray: '5 5', dot: false 
          }),
          h(Brush, { dataKey: 'timestamp', height: 30, stroke: '#8884d8' })
        )
      )
    )
  );
};
