import re
import sys

def modify_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Remove Recharts Destructuring
    content = re.sub(
        r'const \{\n\s*ResponsiveContainer.*?window\.Recharts \|\| \{\};\n?',
        '// Recharts explicitly removed during ECharts migration\n',
        content,
        flags=re.DOTALL
    )

    # 2. Box Plot Migration
    boxplot_regex = r"""h\(ResponsiveContainer.*?\radius:\s*0,\s*\n\s*minPointSize:\s*2,\s*\n\s*children:\s*\[[\s\S]*?\]\s*\}\),\s*\n\s*\)\n\s*\)"""
    boxplot_replacement = """(() => {
          const option = {
            tooltip: {
              trigger: 'item',
              axisPointer: { type: 'shadow' },
              backgroundColor: 'var(--panel)',
              borderColor: 'var(--line)',
              textStyle: { color: 'var(--text)' },
              formatter: function (params) {
                const d = params.data;
                const name = params.name;
                return `<div class="chart-tooltip" style="font-size:12px;">
                  <div style="font-weight:700; margin-bottom:6px; color:var(--text.main)">${name}</div>
                  <div style="margin-bottom:2px">Min: ${d[1]}%</div>
                  <div style="margin-bottom:2px">Q1: ${d[2]}%</div>
                  <div style="color:#EF4444; font-weight:700; margin-bottom:2px">Median: ${d[3]}%</div>
                  <div style="margin-bottom:2px">Q3: ${d[4]}%</div>
                  <div>Max: ${d[5]}%</div>
                </div>`;
              }
            },
            grid: { top: 20, right: 24, left: 40, bottom: 80 },
            xAxis: {
              type: 'category',
              data: boxPlotData.map(d => d.inverter_id),
              axisLabel: { fontSize: 10, color: 'var(--text-soft)', interval: 0, rotate: 45 },
              name: 'Inverter ID',
              nameLocation: 'middle',
              nameGap: 60,
              nameTextStyle: { color: 'var(--text-soft)', fontSize: 12 },
              axisLine: { lineStyle: { color: 'var(--line)' } },
              axisTick: { show: false }
            },
            yAxis: {
              type: 'value',
              name: 'Efficiency (%)',
              nameLocation: 'middle',
              nameGap: 30,
              nameTextStyle: { color: 'var(--text-soft)', fontSize: 12 },
              min: 94,
              max: 100,
              axisLabel: { fontSize: 11, color: 'var(--text-soft)', formatter: '{value}%' },
              splitLine: { lineStyle: { type: 'dashed', color: 'rgba(255,255,255,0.06)' } }
            },
            series: [{
              type: 'boxplot',
              data: boxPlotData.map(d => [d.min, d.q1, d.median, d.q3, d.max]),
              itemStyle: { borderColor: 'var(--accent)', borderWidth: 1, color: 'rgba(14, 165, 233, 0.35)' },
              boxWidth: [10, 22]
            }]
          };
          return h(window.EChart, { style: { width: '100%', height: '100%' }, option: option });
        })()"""
    
    content = re.sub(boxplot_regex, boxplot_replacement, content, flags=re.DOTALL)

    # 3. Bar Chart: Loss per Inverter
    bar_regex = r"""h\(ResponsiveContainer,\s*\{\s*width:\s*'100%',\s*height:\s*300\s*\},.*?#EF4444'\s*\)\s*\)\n\s*\)\n\s*\)"""
    bar_replacement = """(() => {
        const option = {
          tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            backgroundColor: 'var(--panel)',
            borderColor: 'var(--line)',
            textStyle: { color: 'var(--text)' },
            formatter: (params) => {
              if(!params[0]) return '';
              const d = params[0].data.raw;
              return `<div class="chart-tooltip">
                  <div style="font-weight: 700; margin-bottom: 4px">${d.inverter_id}</div>
                  <div>Loss Energy: ${d.loss_energy_mwh} MWh</div>
                  <div>Efficiency: ${d.efficiency_pct}%</div>
                  <div>DC Energy: ${d.dc_energy_mwh} MWh</div>
                  <div>AC Energy: ${d.ac_energy_mwh} MWh</div>
                </div>`;
            }
          },
          grid: { top: 20, right: 20, left: 40, bottom: 60 },
          xAxis: {
            type: 'category',
            data: inverters.map(d => d.inverter_id),
            axisLabel: { fontSize: 10, color: 'var(--text-soft)', interval: 0, rotate: 45 },
            axisLine: { lineStyle: { color: 'var(--line)' } },
            axisTick: { show: false }
          },
          yAxis: {
            type: 'value',
            name: 'MWh',
            nameLocation: 'middle',
            nameGap: 30,
            nameTextStyle: { color: 'var(--text-soft)', fontSize: 11 },
            axisLabel: { fontSize: 10, color: 'var(--text-soft)' },
            splitLine: { lineStyle: { type: 'dashed', color: 'rgba(255,255,255,0.06)' } }
          },
          series: [{
            type: 'bar',
            data: inverters.map((d, i) => ({ value: d.loss_energy_mwh, itemStyle: { color: i < 3 ? '#B91C1C' : '#EF4444', borderRadius: [2,2,0,0] }, raw: d }))
          }]
        };
        return h(window.EChart, { style: { width: '100%', height: 300 }, option: option });
      })()"""
    
    content = re.sub(bar_regex, bar_replacement, content, flags=re.DOTALL)

    # 4. Trend Line Graph
    trend_regex = r"""h\(ResponsiveContainer,\s*\{\s*width:\s*'100%',\s*height:\s*300\s*\},.*?#8884d8'\s*\}\)\n\s*\)\n\s*\)"""
    trend_replacement = """(() => {
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
      })()"""
      
    content = re.sub(trend_regex, trend_replacement, content, flags=re.DOTALL)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == '__main__':
    modify_file(sys.argv[1])
