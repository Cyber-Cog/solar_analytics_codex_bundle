import re
import sys
import os

def modify_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Remove Recharts Destructuring
    content = re.sub(
        r'const \{\s*ResponsiveContainer.*?window\.Recharts \|\| \{\};\n?',
        '// Recharts explicitly removed during ECharts migration\n',
        content,
        flags=re.DOTALL
    )

    # 2. plSummary.inverters BarChart
    content = content.replace('''h(ResponsiveContainer, { width: '100%', height: 300 },
          h(BarChart, { data: plSummary.inverters || [], margin: { top: 10, right: 10, left: 0, bottom: 0 } },
            h(CartesianGrid, { strokeDasharray: '3 3', stroke: '#F1F5F9' }),
            h(XAxis, { dataKey: 'inverter_id', tick: { fontSize: 10 } }),
            h(YAxis, { tick: { fontSize: 10 } }),
            h(Tooltip),
            h(Bar, { dataKey: 'energy_loss_kwh', fill: '#f59e0b', name: 'Energy Loss (kWh)' })
          )
        )''',
    '''(()=>{
          const data = plSummary.inverters || [];
          return h(window.EChart, {
            style: { width: '100%', height: 300 },
            option: {
              tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
              grid: { top: 10, right: 10, left: 40, bottom: 24 },
              xAxis: { type: 'category', data: data.map(d=>d.inverter_id), axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
              yAxis: { type: 'value', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } } },
              series: [{ type: 'bar', name: 'Energy Loss (kWh)', data: data.map(d=>d.energy_loss_kwh), itemStyle: { color: '#f59e0b', borderRadius: [4,4,0,0] } }]
            }
          })
        })()''')

    # 3. commLossByInverter BarChart
    content = content.replace('''h(ResponsiveContainer, { width: '100%', height: 300 },
          h(BarChart, { data: commLossByInverter, margin: { top: 10, right: 10, left: 0, bottom: 0 } },
            h(CartesianGrid, { strokeDasharray: '3 3', stroke: '#F1F5F9' }),
            h(XAxis, { dataKey: 'inverter_id', tick: { fontSize: 10 } }),
            h(YAxis, { tick: { fontSize: 10 } }),
            h(Tooltip),
            h(Bar, { dataKey: 'estimated_loss_kwh', fill: '#ef4444', name: 'Loss (kWh)' })
          )
        )''',
    '''(()=>{
          return h(window.EChart, {
            style: { width: '100%', height: 300 },
            option: {
              tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
              grid: { top: 10, right: 10, left: 40, bottom: 24 },
              xAxis: { type: 'category', data: commLossByInverter.map(d=>d.inverter_id), axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
              yAxis: { type: 'value', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } } },
              series: [{ type: 'bar', name: 'Loss (kWh)', data: commLossByInverter.map(d=>d.estimated_loss_kwh), itemStyle: { color: '#ef4444', borderRadius: [4,4,0,0] } }]
            }
          })
        })()''')

    # 4. cdTimeline ComposedChart
    content = content.replace('''h(ResponsiveContainer, { width: '100%', height: 460 },
                h(ComposedChart, { data: cdTimeline, margin: { top: 8, right: 24, left: 8, bottom: 12 } },
                  h(CartesianGrid, { strokeDasharray: '3 3', stroke: '#1e293b' }),
                  h(XAxis, { dataKey: 'timestamp', tick: { fontSize: 10, fill: '#94a3b8' }, tickFormatter: (t) => String(t).slice(5, 16) }),
                  h(YAxis, { yAxisId: 'kw', tick: { fontSize: 10, fill: '#94a3b8' }, label: { value: 'Power (kW)', angle: -90, position: 'insideLeft', style: { fontSize: 11, fill: '#94a3b8' } } }),
                  h(YAxis, { yAxisId: 'gti', orientation: 'right', tick: { fontSize: 10, fill: '#f59e0b' }, label: { value: 'GTI (W/m²)', angle: 90, position: 'insideRight', style: { fontSize: 11, fill: '#f59e0b' } } }),
                  h(Tooltip, { contentStyle: { background: '#0f172a', border: '1px solid #1e293b', borderRadius: 8, fontSize: 12 }, labelFormatter: (l) => String(l).slice(0, 19) }),
                  h(Legend, { wrapperStyle: { fontSize: 12 } }),
                  ...runs.map((run, i) => h(ReferenceArea, {
                    key: `${run.state}-${i}`,
                    yAxisId: 'kw',
                    x1: run.start,
                    x2: run.end,
                    fill: CD_KIND_COLOR[run.state] || '#64748b',
                    fillOpacity: 0.15,
                    stroke: 'none',
                  })),
                  h(Line, { yAxisId: 'kw', type: 'monotone', dataKey: 'virtual_ac_kw', name: 'Virtual (expected)', stroke: '#10b981', dot: false, strokeWidth: 2, strokeDasharray: '5 3' }),
                  h(Line, { yAxisId: 'kw', type: 'monotone', dataKey: 'actual_ac_kw',  name: 'Actual',            stroke: '#3b82f6', dot: false, strokeWidth: 2 }),
                  h(Line, { yAxisId: 'gti', type: 'monotone', dataKey: 'gti',          name: 'GTI (W/m²)',        stroke: '#f59e0b', dot: false, strokeWidth: 1.3, strokeOpacity: 0.8 }),
                  h(ReferenceLine, { yAxisId: 'kw', y: Number(invMeta?.dc_capacity_kw || 0), stroke: '#ef4444', strokeDasharray: '3 3' })
                )
              )''',
    '''(()=>{
                const markAreaData = runs.map(run => [{
                   xAxis: run.start,
                   itemStyle: { color: CD_KIND_COLOR[run.state] || '#64748b', opacity: 0.15 }
                }, {
                   xAxis: run.end
                }]);
                
                return h(window.EChart, {
                  style: { width: '100%', height: 460 },
                  option: {
                    tooltip: { trigger: 'axis', backgroundColor: '#0f172a', borderColor: '#1e293b', textStyle: { color: '#f8fafc', fontSize: 12 } },
                    legend: { bottom: 0, textStyle: { color: '#94a3b8' } },
                    grid: { top: 20, right: 40, bottom: 40, left: 50 },
                    xAxis: {
                      type: 'category',
                      data: cdTimeline.map(d=>d.timestamp),
                      axisLabel: { color: '#94a3b8', fontSize: 10, formatter: (val) => val.slice(5, 16) }
                    },
                    yAxis: [
                      { type: 'value', name: 'Power (kW)', nameLocation: 'middle', nameGap: 35, nameTextStyle: {color: '#94a3b8'}, axisLabel: { color: '#94a3b8' }, splitLine: { lineStyle: { type: 'dashed', color: '#1e293b' } } },
                      { type: 'value', name: 'GTI (W/m²)', nameLocation: 'middle', nameGap: 35, nameTextStyle: {color: '#f59e0b'}, axisLabel: { color: '#f59e0b' }, splitLine: { show: false } }
                    ],
                    series: [
                      {
                        type: 'line', name: 'Virtual (expected)', data: cdTimeline.map(d=>d.virtual_ac_kw), smooth: true, showSymbol: false,
                        lineStyle: { color: '#10b981', width: 2, type: 'dashed' }, yAxisIndex: 0
                      },
                      {
                        type: 'line', name: 'Actual', data: cdTimeline.map(d=>d.actual_ac_kw), smooth: true, showSymbol: false,
                        lineStyle: { color: '#3b82f6', width: 2 }, yAxisIndex: 0,
                        markArea: { data: markAreaData },
                        markLine: invMeta?.dc_capacity_kw ? { data: [{ yAxis: Number(invMeta.dc_capacity_kw) }], lineStyle: { color: '#ef4444', type: 'dashed' }, symbol: ['none','none'] } : undefined
                      },
                      {
                        type: 'line', name: 'GTI (W/m²)', data: cdTimeline.map(d=>d.gti), smooth: true, showSymbol: false,
                        lineStyle: { color: '#f59e0b', width: 1.3, opacity: 0.8 }, yAxisIndex: 1
                      }
                    ]
                  }
                })
              })()''')

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == '__main__':
    modify_file(sys.argv[1])
