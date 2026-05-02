"use client";

import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Analytics } from "@/lib/api";
import { usePlantContext } from "@/components/plant-context";
import { Card, CardHeader, CardTitle, CardBody } from "@/components/ui/card";
import { PageSkeleton } from "@/components/ui/skeleton";
import { ErrorState, EmptyState } from "@/components/ui/states";
import { SolarChart } from "@/components/charts/solar-chart";
import { toDateStr, formatNumber } from "@/lib/utils";
import type { TimeseriesPoint } from "@/types";
import { subDays } from "date-fns";
import { FlaskConical } from "lucide-react";

const EQUIPMENT_LEVELS = ["inverter", "scb", "plant", "wms", "string"];
const COMMON_SIGNALS = [
  "dc_current", "dc_voltage", "ac_power", "active_power", "energy_kwh",
  "irradiance", "gti", "ghi", "temperature", "wind_speed", "string_current",
  "scb_current", "efficiency",
];

function DateRangePicker({
  from,
  to,
  onFromChange,
  onToChange,
}: {
  from: string;
  to: string;
  onFromChange: (v: string) => void;
  onToChange: (v: string) => void;
}) {
  return (
    <div className="flex items-center gap-2 flex-wrap">
      <input
        type="date"
        value={from}
        onChange={(e) => onFromChange(e.target.value)}
        className="border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none focus:ring-2 focus:ring-[var(--ring)]"
      />
      <span className="text-[var(--muted-foreground)] text-sm">to</span>
      <input
        type="date"
        value={to}
        onChange={(e) => onToChange(e.target.value)}
        className="border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none focus:ring-2 focus:ring-[var(--ring)]"
      />
    </div>
  );
}

export default function AnalyticsPage() {
  const { selectedPlant } = usePlantContext();
  const today = toDateStr(new Date());
  const sevenDaysAgo = toDateStr(subDays(new Date(), 7));

  const [dateFrom, setDateFrom] = useState(sevenDaysAgo);
  const [dateTo, setDateTo] = useState(today);
  const [level, setLevel] = useState("inverter");
  const [signal, setSignal] = useState("dc_current");
  const [selectedEquipment, setSelectedEquipment] = useState<string[]>([]);

  // Fetch equipment list
  const { data: eqList } = useQuery({
    queryKey: ["analytics", "equipment", selectedPlant, level],
    queryFn: () => Analytics.equipmentList(selectedPlant, level),
    enabled: !!selectedPlant,
  });

  const allEquipment = eqList?.equipment_ids ?? [];

  // Auto-select all if nothing selected
  const effectiveEquipment = selectedEquipment.length > 0 ? selectedEquipment : allEquipment.slice(0, 8);

  // Fetch timeseries for each selected equipment
  const { data: tsData, isLoading, error, refetch } = useQuery({
    queryKey: ["analytics", "timeseries", selectedPlant, level, signal, dateFrom, dateTo, effectiveEquipment.join(",")],
    queryFn: async () => {
      const results = await Promise.all(
        effectiveEquipment.map((eqId) =>
          Analytics.timeseries({
            plant_id: selectedPlant,
            equipment_id: eqId,
            equipment_level: level,
            signal,
            date_from: dateFrom,
            date_to: dateTo,
          }).then((r) => ({ eqId, data: r.data ?? [] }))
        )
      );
      return results;
    },
    enabled: !!selectedPlant && effectiveEquipment.length > 0,
    staleTime: 30_000,
  });

  // Build chart option
  const chartOption = useMemo(() => {
    if (!tsData?.length) return null;

    // Collect all unique timestamps
    const timestampSet = new Set<string>();
    tsData.forEach(({ data }) => data.forEach((p: TimeseriesPoint) => timestampSet.add(p.timestamp)));
    const timestamps = Array.from(timestampSet).sort();

    const seriesMap: Record<string, Record<string, number | null>> = {};
    tsData.forEach(({ eqId, data }) => {
      seriesMap[eqId] = {};
      data.forEach((p: TimeseriesPoint) => {
        seriesMap[eqId][p.timestamp] = p.value;
      });
    });

    return {
      xAxis: {
        type: "category" as const,
        data: timestamps.map((ts) => ts.slice(0, 16).replace("T", " ")),
        axisLabel: { rotate: 30, fontSize: 10 },
      },
      yAxis: { type: "value" as const, name: signal },
      legend: { show: true },
      dataZoom: [
        { type: "inside" as const, start: 0, end: 100 },
        { type: "slider" as const, height: 24 },
      ],
      series: Object.entries(seriesMap).map(([eqId, valueMap]) => ({
        name: eqId,
        type: "line" as const,
        data: timestamps.map((ts) => valueMap[ts] ?? null),
        symbol: "none",
        lineStyle: { width: 1.5 },
        connectNulls: false,
      })),
    };
  }, [tsData, signal]);

  // Stats
  const stats = useMemo(() => {
    if (!tsData) return null;
    const all = tsData.flatMap(({ data }) => data.map((p: TimeseriesPoint) => p.value));
    const valid = all.filter((v) => v != null && isFinite(v));
    if (!valid.length) return null;
    return {
      count: valid.length,
      min: Math.min(...valid),
      max: Math.max(...valid),
      avg: valid.reduce((a, b) => a + b, 0) / valid.length,
    };
  }, [tsData]);

  if (!selectedPlant)
    return (
      <div className="flex items-center justify-center h-64 text-[var(--muted-foreground)]">
        Select a plant to view analytics
      </div>
    );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-xl font-bold">Analytics Lab</h1>
        <p className="text-sm text-[var(--muted-foreground)] mt-0.5">
          Time-series exploration for any signal, equipment, and date range
        </p>
      </div>

      {/* Controls */}
      <Card>
        <CardBody className="space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Equipment level */}
            <div>
              <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1.5">Equipment Level</label>
              <select
                value={level}
                onChange={(e) => { setLevel(e.target.value); setSelectedEquipment([]); }}
                className="w-full border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none focus:ring-2 focus:ring-[var(--ring)]"
              >
                {EQUIPMENT_LEVELS.map((l) => (
                  <option key={l} value={l}>{l.charAt(0).toUpperCase() + l.slice(1)}</option>
                ))}
              </select>
            </div>

            {/* Signal */}
            <div>
              <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1.5">Signal</label>
              <select
                value={signal}
                onChange={(e) => setSignal(e.target.value)}
                className="w-full border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none focus:ring-2 focus:ring-[var(--ring)]"
              >
                {COMMON_SIGNALS.map((s) => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
            </div>

            {/* Date range */}
            <div className="sm:col-span-2">
              <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1.5">Date Range</label>
              <DateRangePicker
                from={dateFrom}
                to={dateTo}
                onFromChange={setDateFrom}
                onToChange={setDateTo}
              />
            </div>
          </div>

          {/* Equipment multi-select */}
          {allEquipment.length > 0 && (
            <div>
              <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1.5">
                Equipment ({allEquipment.length} available · {effectiveEquipment.length} selected)
              </label>
              <div className="flex flex-wrap gap-1.5 max-h-28 overflow-y-auto">
                <button
                  onClick={() => setSelectedEquipment([])}
                  className={`px-2.5 py-1 rounded-full text-xs font-medium border transition ${
                    selectedEquipment.length === 0
                      ? "bg-[#0ea5e9]/20 text-[#0ea5e9] border-[#0ea5e9]/40"
                      : "border-[var(--border)] text-[var(--muted-foreground)] hover:border-[#0ea5e9]/40"
                  }`}
                >
                  All (first 8)
                </button>
                {allEquipment.map((eq) => {
                  const sel = selectedEquipment.includes(eq);
                  return (
                    <button
                      key={eq}
                      onClick={() =>
                        setSelectedEquipment((prev) =>
                          sel ? prev.filter((e) => e !== eq) : [...prev, eq]
                        )
                      }
                      className={`px-2.5 py-1 rounded-full text-xs font-medium border transition ${
                        sel
                          ? "bg-[#0ea5e9]/20 text-[#0ea5e9] border-[#0ea5e9]/40"
                          : "border-[var(--border)] text-[var(--muted-foreground)] hover:border-[#0ea5e9]/40"
                      }`}
                    >
                      {eq}
                    </button>
                  );
                })}
              </div>
            </div>
          )}
        </CardBody>
      </Card>

      {/* Stats row */}
      {stats && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {[
            { label: "Data Points", value: stats.count.toLocaleString() },
            { label: "Min", value: formatNumber(stats.min) },
            { label: "Max", value: formatNumber(stats.max) },
            { label: "Average", value: formatNumber(stats.avg) },
          ].map(({ label, value }) => (
            <Card key={label}>
              <CardBody className="py-3">
                <p className="text-xs text-[var(--muted-foreground)]">{label}</p>
                <p className="text-lg font-bold mt-0.5">{value}</p>
              </CardBody>
            </Card>
          ))}
        </div>
      )}

      {/* Chart */}
      <Card>
        <CardHeader>
          <CardTitle>
            {signal} · {level} · {dateFrom} → {dateTo}
          </CardTitle>
        </CardHeader>
        <CardBody>
          {isLoading ? (
            <div className="h-80 flex items-center justify-center text-[var(--muted-foreground)] text-sm animate-pulse">
              Loading timeseries data…
            </div>
          ) : error ? (
            <ErrorState message={String(error)} onRetry={() => refetch()} />
          ) : chartOption ? (
            <SolarChart option={chartOption} height={380} />
          ) : (
            <EmptyState
              title="No data"
              description="No data found for the selected parameters."
              icon={<FlaskConical className="w-5 h-5" />}
            />
          )}
        </CardBody>
      </Card>
    </div>
  );
}
