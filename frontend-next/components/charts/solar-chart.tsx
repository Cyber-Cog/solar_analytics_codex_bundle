"use client";

import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

interface SolarChartProps {
  option: EChartsOption;
  height?: number | string;
  loading?: boolean;
  className?: string;
}

export function SolarChart({ option, height = 320, loading = false, className }: SolarChartProps) {
  const mergedOption: EChartsOption = {
    backgroundColor: "transparent",
    textStyle: { fontFamily: "inherit", color: "var(--foreground)" },
    grid: { top: 32, right: 16, bottom: 40, left: 48, containLabel: true },
    tooltip: {
      trigger: "axis",
      backgroundColor: "var(--card)",
      borderColor: "var(--border)",
      textStyle: { color: "var(--foreground)", fontSize: 12 },
      axisPointer: { type: "cross", lineStyle: { color: "var(--border)" } },
    },
    legend: {
      textStyle: { color: "var(--muted-foreground)", fontSize: 11 },
      itemHeight: 8,
    },
    xAxis: {
      axisLine: { lineStyle: { color: "var(--border)" } },
      axisTick: { lineStyle: { color: "var(--border)" } },
      axisLabel: { color: "var(--muted-foreground)", fontSize: 11 },
      splitLine: { lineStyle: { color: "var(--border)", type: "dashed" } },
    },
    yAxis: {
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: "var(--muted-foreground)", fontSize: 11 },
      splitLine: { lineStyle: { color: "var(--border)", type: "dashed" } },
    },
    color: ["#0ea5e9", "#f0a500", "#22c55e", "#a855f7", "#ef4444", "#06b6d4", "#fb923c"],
    ...option,
  };

  return (
    <div className={className}>
      <ReactECharts
        option={mergedOption}
        style={{ height: typeof height === "number" ? `${height}px` : height, width: "100%" }}
        showLoading={loading}
        loadingOption={{
          text: "",
          color: "#0ea5e9",
          maskColor: "rgba(0,0,0,0.05)",
        }}
        opts={{ renderer: "canvas" }}
      />
    </div>
  );
}
