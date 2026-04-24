import re
import sys
import os

def modify_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. invChartData BarChart
    content = content.replace('''h(ResponsiveContainer, { width: '100%', height: 300 },
              h(BarChart, { data: invChartData, margin: { top: 10, right: 10, left: 0, bottom: 0 } },
                h(CartesianGrid, { strokeDasharray: '3 3', stroke: '#F1F5F9' }),
                h(XAxis, { dataKey: 'inverter_id', tick: { fontSize: 10 } }),
                h(YAxis, { tick: { fontSize: 10 } }),
                h(Tooltip),
                h(Bar, { dataKey: 'missing_strings', fill: '#f59e0b', name: 'Disconnected Strings (Mode)' })
              )
            )''',
    '''h(window.EChart, {
              style: { width: '100%', height: 300 },
              option: {
                tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
                grid: { top: 10, right: 10, left: 40, bottom: 24 },
                xAxis: { type: 'category', data: invChartData.map(d=>d.inverter_id), axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
                yAxis: { type: 'value', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } } },
                series: [{ type: 'bar', name: 'Disconnected Strings (Mode)', data: invChartData.map(d=>d.missing_strings), itemStyle: { color: '#f59e0b', borderRadius: [4,4,0,0] } }]
              }
            })''')

    # 2. energyChartData BarChart
    content = content.replace('''h(ResponsiveContainer, { width: '100%', height: 300 },
              h(BarChart, { data: energyChartData, margin: { top: 10, right: 10, left: 0, bottom: 0 } },
                h(CartesianGrid, { strokeDasharray: '3 3', stroke: '#F1F5F9' }),
                h(XAxis, { dataKey: 'timestamp', tick: { fontSize: 10 }, tickFormatter: v => v.slice(5) }),
                h(YAxis, { tick: { fontSize: 10 } }),
                h(Tooltip),
                h(Legend),
                h(Bar, { dataKey: 'total_energy_loss', fill: '#EF4444', name: 'Energy Loss (kWh)', radius: [2, 2, 0, 0] })
              )
            )''',
    '''h(window.EChart, {
              style: { width: '100%', height: 300 },
              option: {
                tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
                legend: { bottom: 0, textStyle: { color: 'var(--text-soft)' } },
                grid: { top: 10, right: 10, left: 40, bottom: 30 },
                xAxis: { type: 'category', data: energyChartData.map(d=>d.timestamp), axisLabel: { fontSize: 10, color: 'var(--text-soft)', formatter: v => v.slice(5) }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
                yAxis: { type: 'value', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } } },
                series: [{ type: 'bar', name: 'Energy Loss (kWh)', data: energyChartData.map(d=>d.total_energy_loss), itemStyle: { color: '#ef4444', borderRadius: [2,2,0,0] } }]
              }
            })''')

    # 3. Investigation Modal Recharts Fallback
    content = re.sub(
        r'window\.echarts\s*\?\s*h\(\'div\',\s*\{\s*ref:\s*chartRef,\s*style:\s*\{\s*height:\s*320,\s*width:\s*\'100%\'\s*\}\s*\}\)\s*:\s*h\(\'div\',\s*\{\s*style:\s*\{\s*height:\s*320,\s*width:\s*\'100%\'\s*\}\s*\},[\s\n]*h\(ResponsiveContainer.*?\)\n\s*\)',
        '''h('div', { ref: chartRef, style: { height: 320, width: '100%' } })''',
        content,
        flags=re.DOTALL
    )

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == '__main__':
    modify_file(sys.argv[1])
