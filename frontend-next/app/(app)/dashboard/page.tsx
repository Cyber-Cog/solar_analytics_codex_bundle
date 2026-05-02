"use client";

import { useQuery } from "@tanstack/react-query";
import { Dashboard as DashboardAPI } from "@/lib/api";
import { usePlantContext } from "@/components/plant-context";
import { Card, CardHeader, CardTitle, CardBody } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { PageSkeleton } from "@/components/ui/skeleton";
import { ErrorState } from "@/components/ui/states";
import { SolarChart } from "@/components/charts/solar-chart";
import { formatNumber, formatPercent } from "@/lib/utils";
import {
  Zap, Sun, TrendingUp, Battery, Activity,
  Thermometer, Wind, Droplets,
} from "lucide-react";

interface KPICardProps {
  label: string;
  value: string;
  unit?: string;
  icon: React.ReactNode;
  color?: string;
  trend?: number;
}

function KPICard({ label, value, unit, icon, color = "#0ea5e9", trend }: KPICardProps) {
  return (
    <Card>
      <CardBody className="flex items-start gap-3">
        <div
          className="w-10 h-10 rounded-xl flex items-center justify-center flex-shrink-0"
          style={{ background: `${color}20` }}
        >
          <span style={{ color }}>{icon}</span>
        </div>
        <div className="min-w-0">
          <p className="text-xs text-[var(--muted-foreground)] mb-0.5">{label}</p>
          <p className="text-xl font-bold text-[var(--foreground)] leading-tight">
            {value}
            {unit && <span className="text-sm font-normal text-[var(--muted-foreground)] ml-1">{unit}</span>}
          </p>
          {trend !== undefined && (
            <p className={`text-xs mt-0.5 ${trend >= 0 ? "text-green-500" : "text-red-500"}`}>
              {trend >= 0 ? "↑" : "↓"} {Math.abs(trend).toFixed(1)}% vs yesterday
            </p>
          )}
        </div>
      </CardBody>
    </Card>
  );
}

function InverterTable({ data }: { data: Record<string, unknown>[] }) {
  if (!data?.length) return null;
  return (
    <div className="overflow-x-auto -mx-1">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-[var(--border)]">
            {["Inverter", "Power (kW)", "Energy (kWh)", "DC Voltage (V)", "Efficiency (%)", "Status"].map((h) => (
              <th key={h} className="px-3 py-2 text-left text-xs font-medium text-[var(--muted-foreground)] whitespace-nowrap">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((inv: Record<string, unknown>, i) => (
            <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--background)] transition">
              <td className="px-3 py-2 font-medium">{String(inv.inverter_id ?? "—")}</td>
              <td className="px-3 py-2">{formatNumber(inv.power_kw as number)}</td>
              <td className="px-3 py-2">{formatNumber(inv.energy_kwh as number)}</td>
              <td className="px-3 py-2">{formatNumber(inv.dc_voltage as number, 1)}</td>
              <td className="px-3 py-2">{formatPercent(inv.efficiency as number)}</td>
              <td className="px-3 py-2">
                <Badge variant={inv.status === "ON" ? "success" : "muted"}>
                  {String(inv.status ?? "—")}
                </Badge>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function DashboardPage() {
  const { selectedPlant } = usePlantContext();

  const { data: summary, isLoading, error, refetch } = useQuery({
    queryKey: ["dashboard", "summary", selectedPlant],
    queryFn: () => DashboardAPI.summary(selectedPlant),
    enabled: !!selectedPlant,
    refetchInterval: 5 * 60_000,
  });

  const { data: inverters } = useQuery({
    queryKey: ["dashboard", "inverters", selectedPlant],
    queryFn: () => DashboardAPI.inverters(selectedPlant),
    enabled: !!selectedPlant,
    refetchInterval: 5 * 60_000,
  });

  if (!selectedPlant) {
    return (
      <div className="flex items-center justify-center h-64 text-[var(--muted-foreground)]">
        Select a plant to view the dashboard
      </div>
    );
  }

  if (isLoading) return <PageSkeleton />;
  if (error) return <ErrorState message={String(error)} onRetry={() => refetch()} />;

  const kpi = (summary as Record<string, unknown>)?.kpi as Record<string, number> | undefined;
  const weather = (summary as Record<string, unknown>)?.weather as Record<string, unknown> | undefined;
  const energyData = (summary as Record<string, unknown>)?.energy as Record<string, unknown>[] | undefined;
  const invData = (inverters as Record<string, unknown>)?.inverters as Record<string, unknown>[] || 
                  (Array.isArray(inverters) ? inverters as Record<string, unknown>[] : []);

  // Build energy chart
  const energyOption = energyData?.length
    ? {
        xAxis: {
          type: "category" as const,
          data: energyData.map((d) => String(d.date ?? d.timestamp ?? "")),
        },
        yAxis: [
          { type: "value" as const, name: "Energy (kWh)" },
          { type: "value" as const, name: "PR (%)", min: 0, max: 100 },
        ],
        series: [
          {
            name: "Energy",
            type: "bar" as const,
            data: energyData.map((d) => d.energy_kwh ?? d.value ?? 0),
            itemStyle: { color: "#0ea5e9", borderRadius: [3, 3, 0, 0] },
          },
          {
            name: "PR",
            type: "line" as const,
            yAxisIndex: 1,
            data: energyData.map((d) => d.performance_ratio ?? null),
            lineStyle: { color: "#f0a500", width: 2 },
            symbol: "none",
          },
        ],
      }
    : null;

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-[var(--foreground)]">Dashboard</h1>
          <p className="text-sm text-[var(--muted-foreground)] mt-0.5">
            Real-time plant overview · {new Date().toLocaleDateString("en-IN", { weekday: "long", year: "numeric", month: "long", day: "numeric" })}
          </p>
        </div>
        <Badge variant={kpi?.availability != null && kpi.availability > 90 ? "success" : "warning"}>
          {kpi?.plant_status ?? "Live"}
        </Badge>
      </div>

      {/* KPI grid */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-4">
        <KPICard
          label="Current Power"
          value={formatNumber(kpi?.current_power_kw, 1)}
          unit="kW"
          icon={<Zap className="w-5 h-5" />}
          color="#0ea5e9"
        />
        <KPICard
          label="Today's Energy"
          value={formatNumber(kpi?.today_energy_kwh, 0)}
          unit="kWh"
          icon={<Battery className="w-5 h-5" />}
          color="#22c55e"
        />
        <KPICard
          label="Irradiation"
          value={formatNumber(kpi?.today_irradiation, 1)}
          unit="kWh/m²"
          icon={<Sun className="w-5 h-5" />}
          color="#f0a500"
        />
        <KPICard
          label="Performance Ratio"
          value={formatPercent(kpi?.performance_ratio)}
          icon={<TrendingUp className="w-5 h-5" />}
          color="#a855f7"
        />
        <KPICard
          label="CUF"
          value={formatPercent(kpi?.cuf)}
          icon={<Activity className="w-5 h-5" />}
          color="#06b6d4"
        />
        <KPICard
          label="Availability"
          value={formatPercent(kpi?.availability)}
          icon={<Activity className="w-5 h-5" />}
          color="#22c55e"
        />
      </div>

      {/* Weather + Energy charts */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        {/* Energy & PR chart */}
        <Card className="xl:col-span-2">
          <CardHeader>
            <CardTitle>Energy Generation & Performance Ratio</CardTitle>
          </CardHeader>
          <CardBody>
            {energyOption ? (
              <SolarChart option={energyOption} height={280} />
            ) : (
              <div className="h-[280px] flex items-center justify-center text-[var(--muted-foreground)] text-sm">
                No energy data available
              </div>
            )}
          </CardBody>
        </Card>

        {/* Weather panel */}
        <Card>
          <CardHeader>
            <CardTitle>Weather Conditions</CardTitle>
          </CardHeader>
          <CardBody className="space-y-4">
            {[
              { label: "Temperature", value: `${formatNumber(weather?.temperature as number, 1)} °C`, icon: <Thermometer className="w-4 h-4 text-orange-400" /> },
              { label: "Wind Speed", value: `${formatNumber(weather?.wind_speed as number, 1)} m/s`, icon: <Wind className="w-4 h-4 text-blue-400" /> },
              { label: "Humidity", value: `${formatPercent(weather?.humidity as number)}`, icon: <Droplets className="w-4 h-4 text-cyan-400" /> },
              { label: "Irradiance", value: `${formatNumber(weather?.irradiance as number, 0)} W/m²`, icon: <Sun className="w-4 h-4 text-yellow-400" /> },
            ].map(({ label, value, icon }) => (
              <div key={label} className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-sm text-[var(--muted-foreground)]">
                  {icon}
                  {label}
                </div>
                <span className="text-sm font-medium text-[var(--foreground)]">{value}</span>
              </div>
            ))}
          </CardBody>
        </Card>
      </div>

      {/* Inverter table */}
      {invData.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Inverter Status</CardTitle>
          </CardHeader>
          <CardBody className="p-0">
            <InverterTable data={invData} />
          </CardBody>
        </Card>
      )}
    </div>
  );
}
