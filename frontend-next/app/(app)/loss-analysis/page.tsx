"use client";

import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { LossAnalysis } from "@/lib/api";
import { usePlantContext } from "@/components/plant-context";
import { Card, CardHeader, CardTitle, CardBody } from "@/components/ui/card";
import { ErrorState, EmptyState } from "@/components/ui/states";
import { SolarChart } from "@/components/charts/solar-chart";
import { formatNumber, toDateStr } from "@/lib/utils";
import { subDays } from "date-fns";
import { TrendingDown } from "lucide-react";
import type { EChartsOption } from "echarts";

export default function LossAnalysisPage() {
  const { selectedPlant } = usePlantContext();
  const [dateFrom, setDateFrom] = useState(toDateStr(subDays(new Date(), 30)));
  const [dateTo, setDateTo] = useState(toDateStr(new Date()));

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["loss-analysis", "waterfall", selectedPlant, dateFrom, dateTo],
    queryFn: () => LossAnalysis.waterfall({ plant_id: selectedPlant, date_from: dateFrom, date_to: dateTo }),
    enabled: !!selectedPlant,
  });

  const { data: summary } = useQuery({
    queryKey: ["loss-analysis", "summary", selectedPlant, dateFrom, dateTo],
    queryFn: () => LossAnalysis.summary({ plant_id: selectedPlant, date_from: dateFrom, date_to: dateTo }),
    enabled: !!selectedPlant,
  });

  const waterfallOption = useMemo(() => {
    const items = (data as Record<string, unknown>)?.waterfall as Record<string, unknown>[] ?? [];
    if (!items.length) return null;

    // ECharts waterfall is implemented via stacked bar: transparent base + visible bar.
    const values = items.map((d) => Number(d.value ?? 0));
    let cumulative = 0;
    const baseData: number[] = [];
    const barData: number[] = [];
    values.forEach((v) => {
      baseData.push(cumulative);
      barData.push(v);
      cumulative += v;
    });

    return {
      xAxis: {
        type: "category" as const,
        data: items.map((d) => String(d.label ?? "")),
        axisLabel: { rotate: 30, fontSize: 10 },
      },
      yAxis: { type: "value" as const, name: "kWh" },
      series: [
        {
          type: "bar" as const,
          stack: "total",
          itemStyle: { borderColor: "transparent", color: "transparent" },
          emphasis: { itemStyle: { borderColor: "transparent", color: "transparent" } },
          data: baseData,
        },
        {
          type: "bar" as const,
          stack: "total",
          label: { show: true },
          data: barData.map((v) => ({
            value: v,
            itemStyle: { color: v >= 0 ? "#0ea5e9" : "#ef4444", borderRadius: [3, 3, 0, 0] },
          })),
        },
      ],
    } as EChartsOption;
  }, [data]);

  if (!selectedPlant)
    return (
      <div className="flex items-center justify-center h-64 text-[var(--muted-foreground)]">
        Select a plant to view loss analysis
      </div>
    );

  const summaryData = summary as Record<string, unknown> ?? {};

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-bold flex items-center gap-2">
          <TrendingDown className="w-5 h-5 text-red-500" />
          Loss Analysis
        </h1>
        <p className="text-sm text-[var(--muted-foreground)] mt-0.5">
          Energy loss waterfall decomposition
        </p>
      </div>

      {/* Date controls */}
      <div className="flex items-center gap-3 flex-wrap">
        <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)}
          className="border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none" />
        <span className="text-[var(--muted-foreground)]">to</span>
        <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)}
          className="border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none" />
      </div>

      {/* Summary stats */}
      {summaryData && Object.keys(summaryData).length > 0 && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          {[
            { label: "Reference Yield", key: "reference_yield_kwh", unit: "kWh" },
            { label: "Actual Yield", key: "actual_yield_kwh", unit: "kWh" },
            { label: "Total Losses", key: "total_losses_kwh", unit: "kWh" },
            { label: "Performance Ratio", key: "performance_ratio", unit: "%" },
          ].map(({ label, key, unit }) => (
            <Card key={key}>
              <CardBody className="py-3">
                <p className="text-xs text-[var(--muted-foreground)]">{label}</p>
                <p className="text-xl font-bold mt-0.5">
                  {formatNumber(summaryData[key] as number)}
                  <span className="text-sm font-normal text-[var(--muted-foreground)] ml-1">{unit}</span>
                </p>
              </CardBody>
            </Card>
          ))}
        </div>
      )}

      {/* Waterfall chart */}
      <Card>
        <CardHeader>
          <CardTitle>Loss Waterfall Breakdown</CardTitle>
        </CardHeader>
        <CardBody>
          {isLoading ? (
            <div className="h-80 flex items-center justify-center text-[var(--muted-foreground)] animate-pulse">Loading…</div>
          ) : error ? (
            <ErrorState message={String(error)} onRetry={() => refetch()} />
          ) : waterfallOption ? (
            <SolarChart option={waterfallOption} height={380} />
          ) : (
            <EmptyState title="No loss data" description="No waterfall data available for the selected period." />
          )}
        </CardBody>
      </Card>
    </div>
  );
}
