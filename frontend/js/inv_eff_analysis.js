// frontend/js/inv_eff_analysis.js
// Inverter Efficiency Loss Analysis Module
const { useState, useEffect, useMemo } = React;
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

  // Box plot data extraction for ECharts
  const boxPlotData = useMemo(() => {
    const stats = data.inverter_box_stats || [];
    if (stats.length === 0) return [];
    return stats.map((row) => ({
      inverter_id: row.inverter_id,
      min: row.min,
      q1: row.q1,
      median: row.median,
      q3: row.q3,
      max: row.max,
    }));
  }, [data.inverter_box_stats]);

  const boxPlotOption = useMemo(() => {
    if (!boxPlotData || boxPlotData.length === 0) return null;
    return {
      tooltip: {
        trigger: 'item',
        backgroundColor: 'var(--panel)',
        borderColor: 'var(--line)',
        textStyle: { color: 'var(--text)' },
        formatter: (param) => {
          if (param.seriesType === 'boxplot') {
            const name = param.name;
            const [min, q1, median, q3, max] = param.data.slice(1);
            return `
              <div style="font-weight: bold; margin-bottom: 4px; color: var(--text)">${name}</div>
              <div>Max: ${max}%</div>
              <div>Q3: ${q3}%</div>
              <div style="color: #EF4444; font-weight: bold;">Median: ${median}%</div>
              <div>Q1: ${q1}%</div>
              <div>Min: ${min}%</div>
            `;
          }
          return '';
        }
      },
      grid: { top: 20, right: 24, left: 40, bottom: 80 },
      xAxis: {
        type: 'category',
        data: boxPlotData.map(d => d.inverter_id),
        axisLabel: { color: 'var(--text-soft)', fontSize: 10, interval: 0, rotate: 45 },
        nameLocation: 'middle',
        nameGap: 50,
        axisLine: { lineStyle: { color: 'var(--line)' } }
      },
      yAxis: {
        type: 'value',
        min: 94,
        max: 100,
        axisLabel: { color: 'var(--text-soft)', fontSize: 11, formatter: '{value}%' },
        name: 'Efficiency (%)',
        nameLocation: 'middle',
        nameGap: 30,
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)', type: 'dashed' } }
      },
      series: [
        {
          name: 'Efficiency',
          type: 'boxplot',
          data: boxPlotData.map(d => [d.min, d.q1, d.median, d.q3, d.max]),
          itemStyle: {
            color: 'rgba(14, 165, 233, 0.35)',
            borderColor: 'var(--accent)',
            borderWidth: 1
          },
          boxWidth: [10, 22]
        }
      ]
    };
  }, [boxPlotData]);

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

    // ── Box Plot: ECharts ──
    boxPlotData.length > 0 && h(Card, { title: 'Inverter Efficiency Distribution (Box Plot)' },
      h('div', { style: { height: Math.max(320, Math.min(520, 120 + boxPlotData.length * 28)), position: 'relative' } },
        h(window.EChart, { style: { width: '100%', height: '100%' }, option: boxPlotOption }),
        h('div', { style: { position: 'absolute', bottom: 8, left: '50%', transform: 'translateX(-50%)', fontSize: 11, color: 'var(--text-muted)' } },
          'Whiskers: Min–Max | Box: Q1–Q3 | Red line: Median (per-inverter efficiency across selected day)'
        )
      )
    ),

    // ── Bar Chart: Loss per Inverter ─────────────────────────────────────────
    h(Card, { title: 'Inverter Conversion Loss Energy (MWh) - Descending' },
      (() => {
        const option = {
          tooltip: { trigger: 'axis', backgroundColor: 'var(--panel)', borderColor: 'var(--line)', textStyle: { color: 'var(--text)' } },
          legend: { top: 0, textStyle: { color: 'var(--text-soft)' } },
          grid: { top: 35, right: 20, left: 40, bottom: 40 },
          xAxis: {
            type: 'category',
            data: trend.map(d => d.timestamp),
            axisLabel: { fontSize: 10, color: 'var(--text-soft)', formatter: v => v ? v.slice(11, 16) : '' },
            axisLine: { lineStyle: { color: 'var(--line)' } }
          },
          yAxis: {
            type: 'value',
            scale: true,
            axisLabel: { fontSize: 10, color: 'var(--text-soft)' },
            splitLine: { lineStyle: { type: 'dashed', color: '#1e293b' } }
          },
          dataZoom: [{ type: 'slider', height: 24, bottom: 5, borderColor: '#1e293b' }],
          series: [
            {
              name: 'Actual Efficiency (%)',
              type: 'line',
              data: trend.map(d => d.efficiency_pct),
              itemStyle: { color: '#0ea5e9' },
              symbol: 'none',
              smooth: true,
              lineStyle: { width: 2 }
            },
            {
              name: 'Target Benchmark (%)',
              type: 'line',
              step: 'end',
              data: trend.map(d => d.target_efficiency),
              itemStyle: { color: '#10B981' },
              symbol: 'none',
              lineStyle: { width: 1.5, type: 'dashed' }
            }
          ]
        };
        return h(window.EChart, { style: { width: '100%', height: 300 }, option: option });
      })()
    )
  );
};
