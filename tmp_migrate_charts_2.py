import re
import sys
import os

def modify_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. invLossForTab BarChart
    content = content.replace('''h(ResponsiveContainer, { width: '100%', height: 320 },
            h(BarChart, { data: invLossForTab, margin: { top: 10, right: 10, left: 0, bottom: 24 } },
              h(CartesianGrid, { strokeDasharray: '3 3', stroke: '#1e293b' }),
              h(XAxis, { dataKey: 'inverter_id', tick: { fontSize: 10, fill: '#94a3b8' }, angle: -35, textAnchor: 'end', height: 60 }),
              h(YAxis, { tick: { fontSize: 10, fill: '#94a3b8' }, label: { value: 'kWh', angle: -90, position: 'insideLeft', style: { fontSize: 10, fill: '#94a3b8' } } }),
              h(Tooltip, { formatter: (v) => Number(v).toFixed(2) + ' kWh', contentStyle: { background: '#0f172a', border: '1px solid #1e293b' } }),
              h(Bar, { dataKey: 'loss_kwh', fill: accent, name: isClip ? 'Clipping Loss (kWh)' : 'Derating Loss (kWh)' })
            )
          )''',
    '''h(window.EChart, {
            style: { width: '100%', height: 320 },
            option: {
              tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, formatter: (params) => `${params[0].name}<br/>${params[0].marker} ${params[0].seriesName}: <b>${Number(params[0].value).toFixed(2)} kWh</b>`, backgroundColor: '#0f172a', borderColor: '#1e293b', textStyle: { color: '#f8fafc', fontSize: 12 } },
              grid: { top: 10, right: 10, left: 45, bottom: 60 },
              xAxis: { type: 'category', data: invLossForTab.map(d=>d.inverter_id), axisLabel: { fontSize: 10, color: '#94a3b8', rotate: 35 }, axisLine: { lineStyle: { color: '#1e293b' } }, axisTick: {show:false} },
              yAxis: { type: 'value', name: 'kWh', nameLocation: 'middle', nameGap: 30, nameTextStyle: { color: '#94a3b8', fontSize: 10 }, axisLabel: { fontSize: 10, color: '#94a3b8' }, splitLine: { lineStyle: { type: 'dashed', color: '#1e293b' } } },
              series: [{ type: 'bar', name: isClip ? 'Clipping Loss (kWh)' : 'Derating Loss (kWh)', data: invLossForTab.map(d=>d.loss_kwh), itemStyle: { color: accent, borderRadius: [4,4,0,0] } }]
            }
          })''')

    # 2. prData LineChart
    content = content.replace('''h(ResponsiveContainer, { width: '100%', height: '100%' },
              h(LineChart, { data: prData, margin: { top: 28, right: 16, left: 4, bottom: 8 } },
                h(CartesianGrid, { strokeDasharray: '3 3', stroke: 'rgba(255,255,255,0.08)' }),
                h(XAxis, { dataKey: 'date', tick: { fontSize: 10 }, padding: { left: 4, right: 4 } }),
                h(YAxis, { tick: { fontSize: 10 }, domain: ([dataMin, dataMax]) => {
                  const loY = dataMin != null ? Number(dataMin) : 0;
                  const hiY = dataMax != null ? Number(dataMax) : 0;
                  const span = Math.max(hiY - loY, 1e-6);
                  const pad = Math.max(span * 0.18, 3);
                  return [
                    Math.max(0, loY - pad),
                    hiY + pad
                  ];
                } }),
                h(Tooltip, { contentStyle: { background: 'var(--panel)', border: '1px solid var(--line)', borderRadius: 8 } }),
                h(Line, { type: 'monotone', dataKey: 'pr_pct', stroke: '#14b8a6', strokeWidth: 2, dot: true })
              )
            )''',
    '''h(window.EChart, {
              style: { width: '100%', height: '100%' },
              option: {
                tooltip: { trigger: 'axis', backgroundColor: 'var(--panel)', borderColor: 'var(--line)', textStyle: { color: 'var(--text)' } },
                grid: { top: 28, right: 16, left: 40, bottom: 24 },
                xAxis: { type: 'category', boundaryGap: false, data: prData.map(d=>d.date), axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, axisLine: { lineStyle: { color: 'var(--line)' } } },
                yAxis: { type: 'value', min: 'dataMin', max: 'dataMax', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'rgba(255,255,255,0.08)' } } },
                series: [{ type: 'line', name: 'PR %', data: prData.map(d=>d.pr_pct), itemStyle: { color: '#14b8a6' }, symbol: 'circle', symbolSize: 6, lineStyle: { width: 2 }, smooth: true }]
              }
            })''')

    # Wait, prData has slightly different exact string. Let's do a regex for prData.
    content = re.sub(
        r"h\(ResponsiveContainer,\s*\{\s*width:\s*'100%',\s*height:\s*'100%'\s*\},[\s\n]*h\(LineChart,\s*\{\s*data:\s*prData.*?\)\n\s*\)",
        '''h(window.EChart, {
              style: { width: '100%', height: '100%' },
              option: {
                tooltip: { trigger: 'axis', backgroundColor: 'var(--panel)', borderColor: 'var(--line)', textStyle: { color: 'var(--text)' } },
                grid: { top: 28, right: 16, left: 40, bottom: 24 },
                xAxis: { type: 'category', boundaryGap: false, data: prData.map(d=>d.date), axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, axisLine: { lineStyle: { color: 'var(--line)' } } },
                yAxis: { type: 'value', scale: true, axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'rgba(255,255,255,0.08)' } } },
                series: [{ type: 'line', name: 'PR %', data: prData.map(d=>d.pr_pct), itemStyle: { color: '#14b8a6' }, symbol: 'circle', symbolSize: 6, lineStyle: { width: 2 }, smooth: true }]
              }
            })''',
        content,
        flags=re.DOTALL
    )

    # 3. soilingRankings.rows BarChart
    content = re.sub(
        r"h\(ResponsiveContainer,\s*\{\s*width:\s*'100%',\s*height:\s*360\s*\},[\s\n]*h\(BarChart,\s*\{\s*data:\s*soilingRankings\.rows\.map.*?radius:\s*\[2,\s*2,\s*0,\s*0\]\s*\}\)\n\s*\)\n\s*\)",
        '''(()=>{
          const rankingData = soilingRankings.rows.map(r => ({ name: r.label || r.id, loss: Number(r.loss_mwh) || 0 }));
          return h(window.EChart, {
            style: { width: '100%', height: 360 },
            option: {
              tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: 'var(--panel)', borderColor: 'var(--line)', textStyle: { color: 'var(--text)' } },
              grid: { top: 20, right: 20, left: 50, bottom: 80 },
              xAxis: { type: 'category', data: rankingData.map(d=>d.name), axisLabel: { fontSize: 9, color: 'var(--text-soft)', rotate: 35 }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
              yAxis: { type: 'value', name: 'MWh', nameLocation: 'middle', nameGap: 35, nameTextStyle: { color: 'var(--text-soft)', fontSize: 10 }, axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'rgba(255,255,255,0.08)' } } },
              series: [{ type: 'bar', name: 'Loss (MWh)', data: rankingData.map(d=>d.loss), itemStyle: { color: '#f97316', borderRadius: [2,2,0,0] } }]
            }
          })
        })()''',
        content,
        flags=re.DOTALL
    )

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == '__main__':
    modify_file(sys.argv[1])
