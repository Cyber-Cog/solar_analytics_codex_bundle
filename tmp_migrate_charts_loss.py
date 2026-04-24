import re
import sys

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

    # 2. Bridge Chart
    bridge_chart_recharts = r"""!loadingBridge && primary && bridgeChartData\.length > 0 && ResponsiveContainer && h\(ResponsiveContainer,\s*\{\s*width:\s*'100%',\s*height:\s*400\s*\},.*?isAnimationActive:\s*false\s*\}\),\s*\n\s*\)\s*,\s*\n\s*\)\s*,"""
    
    # We will regex replace the two adjacent lines to replace the fallback cleanly as well:
    bridge_chart_regex = r"""!loadingBridge && primary && bridgeChartData\.length > 0 && ResponsiveContainer.*?Recharts\.'\),\n"""

    bridge_echart_replacement = """!loadingBridge && primary && bridgeChartData.length > 0 && (() => {
        const option = {
          tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            backgroundColor: 'var(--panel)',
            borderColor: 'var(--line)',
            textStyle: { color: 'var(--text)' },
            formatter: function (params) {
               if (!params || !params.length) return '';
               const row = params[1] ? params[1].data.raw : params[0].data.raw;
               const visRaw = Number(row.visible_mwh || 0);
               let pct = '';
               if (expBase > 0) {
                 pct = `<div style="font-size: 11px; color: var(--text-muted); margin-top:4px">${((visRaw / expBase) * 100).toFixed(2)}% of expected</div>`;
               }
               return `<div style="font-weight: 600; margin-bottom: 6px">${row.label}</div>
                       <div style="font-size: 12px">Step: <b>${visRaw.toFixed(3)} MWh</b></div>${pct}`;
            }
          },
          grid: { top: 20, right: 20, left: 60, bottom: 85 },
          xAxis: {
            type: 'category',
            data: bridgeChartData.map(d => d.label),
            axisLabel: { fontSize: 9, color: 'var(--text-muted)', interval: 0, rotate: 40 },
            axisLine: { lineStyle: { color: 'var(--line)' } },
            axisTick: { show: false }
          },
          yAxis: {
            type: 'value',
            name: yAxisLabel,
            nameLocation: 'middle',
            nameGap: 45,
            nameTextStyle: { color: 'var(--text-muted)', fontSize: 11 },
            axisLabel: { fontSize: 10, color: 'var(--text-soft)' },
            splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } }
          },
          series: [
            {
              type: 'bar',
              stack: 'wf',
              itemStyle: { color: 'rgba(0,0,0,0)', borderColor: 'rgba(0,0,0,0)' },
              data: bridgeChartData.map(d => ({ value: d._inv, raw: d })),
              animation: false
            },
            {
              type: 'bar',
              stack: 'wf',
              data: bridgeChartData.map(d => ({
                value: d._vis,
                itemStyle: { color: bridgeSegmentFill(d) },
                raw: d
              })),
              animation: false
            }
          ]
        };
        return h(window.EChart, { style: { width: '100%', height: 400 }, option: option });
      })(),\n"""

    content = re.sub(bridge_chart_regex, bridge_echart_replacement, content, flags=re.DOTALL)

    # 3. Worst Unknown Chart
    worst_regex = r"""ResponsiveContainer && h\(ResponsiveContainer.*?\radius:\s*\[0,\s*4,\s*4,\s*0\]\s*\}\),\s*\n\s*\),\s*\n\s*\)"""
    worst_replacement = """(() => {
        const option = {
          tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: 'var(--panel)', borderColor: 'var(--line)', textStyle: { color: 'var(--text)' } },
          grid: { top: 10, right: 20, left: 110, bottom: 20 },
          xAxis: { type: 'value', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } } },
          yAxis: { type: 'category', data: worst.map(d=>d.label), axisLabel: { fontSize: 10, color: 'var(--text)' }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
          series: [{ type: 'bar', name: 'Unknown Loss', data: worst.map(d=>d.unknown_mwh), itemStyle: { color: '#a855f7', borderRadius: [0,4,4,0] } }]
        };
        return h(window.EChart, { style: { width: '100%', height: 260 }, option: option });
      })()"""
      
    content = re.sub(worst_regex, worst_replacement, content, flags=re.DOTALL)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == '__main__':
    modify_file(sys.argv[1])
