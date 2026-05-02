"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Faults } from "@/lib/api";
import { usePlantContext } from "@/components/plant-context";
import { Card, CardHeader, CardTitle, CardBody } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { PageSkeleton } from "@/components/ui/skeleton";
import { ErrorState, EmptyState } from "@/components/ui/states";
import { SolarChart } from "@/components/charts/solar-chart";
import { formatNumber, toDateStr } from "@/lib/utils";
import type { FaultDiagnostic, FaultEvent } from "@/types";
import { subDays, format } from "date-fns";
import { AlertTriangle, History, Filter } from "lucide-react";
import { cn } from "@/lib/utils";

type Tab = "ds-status" | "diagnostics" | "history";

const TABS: { id: Tab; label: string }[] = [
  { id: "ds-status",    label: "DS Status"    },
  { id: "diagnostics",  label: "Diagnostics"  },
  { id: "history",      label: "Fault History"},
];

function DSStatusTab({ plantId }: { plantId: string }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["faults", "ds-status", plantId],
    queryFn: () => Faults.dsStatus(plantId),
    enabled: !!plantId,
    refetchInterval: 3 * 60_000,
  });

  if (isLoading) return <PageSkeleton />;
  if (error) return <ErrorState message={String(error)} />;
  if (!data) return <EmptyState title="No DS data" />;

  const summary = (data as Record<string, unknown>)?.summary as Record<string, unknown> ?? {};
  const scbRows = ((data as Record<string, unknown>)?.scb_statuses as Record<string, unknown>[]) ?? [];

  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {[
          { label: "Total SCBs", value: summary.total_scbs ?? scbRows.length, color: "text-[var(--foreground)]" },
          { label: "Affected SCBs", value: summary.affected_scbs ?? scbRows.filter((r) => r.fault_status === "CONFIRMED_DS").length, color: "text-red-500" },
          { label: "Missing Strings", value: summary.total_missing_strings ?? "—", color: "text-orange-500" },
          { label: "Energy Loss Today", value: `${formatNumber(summary.total_energy_loss_kwh as number)} kWh`, color: "text-yellow-600" },
        ].map(({ label, value, color }) => (
          <Card key={label}>
            <CardBody className="py-3">
              <p className="text-xs text-[var(--muted-foreground)]">{label}</p>
              <p className={`text-xl font-bold mt-0.5 ${color}`}>{String(value)}</p>
            </CardBody>
          </Card>
        ))}
      </div>

      {/* SCB table */}
      <Card>
        <CardHeader>
          <CardTitle>SCB Fault Status</CardTitle>
        </CardHeader>
        <CardBody className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[var(--border)]">
                  {["Inverter", "SCB", "Status", "Missing Strings", "Power Loss (kW)", "Energy Loss (kWh)", "Active Since"].map((h) => (
                    <th key={h} className="px-3 py-2 text-left text-xs font-medium text-[var(--muted-foreground)] whitespace-nowrap">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {scbRows.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-3 py-8 text-center text-[var(--muted-foreground)]">
                      No DS faults detected
                    </td>
                  </tr>
                ) : scbRows.map((row: Record<string, unknown>, i) => (
                  <tr key={i} className={cn("border-b border-[var(--border)]/50 transition", row.fault_status === "CONFIRMED_DS" ? "bg-red-500/5" : "hover:bg-[var(--background)]")}>
                    <td className="px-3 py-2 text-xs">{String(row.inverter_id ?? "—")}</td>
                    <td className="px-3 py-2 font-medium">{String(row.scb_id ?? "—")}</td>
                    <td className="px-3 py-2">
                      <Badge variant={row.fault_status === "CONFIRMED_DS" ? "destructive" : "success"}>
                        {String(row.fault_status ?? "NORMAL")}
                      </Badge>
                    </td>
                    <td className="px-3 py-2">{String(row.missing_strings ?? 0)}</td>
                    <td className="px-3 py-2">{formatNumber(row.power_loss_kw as number)}</td>
                    <td className="px-3 py-2">{formatNumber(row.energy_loss_kwh as number)}</td>
                    <td className="px-3 py-2 text-xs text-[var(--muted-foreground)]">
                      {row.active_since ? format(new Date(String(row.active_since)), "dd MMM HH:mm") : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardBody>
      </Card>
    </div>
  );
}

function DiagnosticsTab({ plantId }: { plantId: string }) {
  const today = toDateStr(new Date());
  const [dateFrom, setDateFrom] = useState(toDateStr(subDays(new Date(), 3)));
  const [dateTo, setDateTo] = useState(today);
  const [filter, setFilter] = useState<"all" | "CONFIRMED_DS" | "NORMAL">("CONFIRMED_DS");

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["faults", "diagnostics", plantId, dateFrom, dateTo, filter],
    queryFn: () => Faults.diagnostics({
      plant_id: plantId,
      date_from: dateFrom,
      date_to: dateTo,
      fault_status: filter === "all" ? undefined : filter,
    }),
    enabled: !!plantId,
  });

  const rows = (Array.isArray(data) ? data : []) as FaultDiagnostic[];

  // Build chart: energy loss per hour
  const chartOption = rows.filter((r) => r.fault_status === "CONFIRMED_DS" && r.energy_loss_kwh).length > 0
    ? {
        xAxis: {
          type: "category" as const,
          data: rows.filter((r) => r.fault_status === "CONFIRMED_DS").slice(-50).map((r) => r.timestamp?.slice(0, 16).replace("T", " ")),
          axisLabel: { rotate: 30, fontSize: 9 },
        },
        yAxis: { type: "value" as const, name: "kWh Lost" },
        series: [{
          name: "Energy Loss",
          type: "bar" as const,
          data: rows.filter((r) => r.fault_status === "CONFIRMED_DS").slice(-50).map((r) => r.energy_loss_kwh ?? 0),
          itemStyle: { color: "#ef4444", borderRadius: [2, 2, 0, 0] },
        }],
      }
    : null;

  return (
    <div className="space-y-6">
      {/* Controls */}
      <div className="flex flex-wrap items-center gap-3">
        <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)}
          className="border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none" />
        <span className="text-[var(--muted-foreground)] text-sm">to</span>
        <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)}
          className="border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none" />
        <div className="flex gap-1">
          {(["all", "CONFIRMED_DS", "NORMAL"] as const).map((f) => (
            <button key={f} onClick={() => setFilter(f)}
              className={cn("px-3 py-1.5 rounded-lg text-xs font-medium border transition",
                filter === f ? "bg-[#1e3a5f] text-white border-[#1e3a5f]" : "border-[var(--border)] text-[var(--muted-foreground)] hover:text-[var(--foreground)]"
              )}>
              {f === "all" ? "All" : f === "CONFIRMED_DS" ? "DS Only" : "Normal"}
            </button>
          ))}
        </div>
        <span className="text-xs text-[var(--muted-foreground)] ml-auto">{rows.length} rows</span>
      </div>

      {/* Chart */}
      {chartOption && (
        <Card>
          <CardHeader><CardTitle>Energy Loss Timeline (DS events)</CardTitle></CardHeader>
          <CardBody><SolarChart option={chartOption} height={220} /></CardBody>
        </Card>
      )}

      {/* Table */}
      <Card>
        <CardBody className="p-0">
          <div className="overflow-x-auto">
            {isLoading ? (
              <div className="py-12 text-center text-[var(--muted-foreground)] text-sm animate-pulse">Loading…</div>
            ) : error ? (
              <ErrorState message={String(error)} onRetry={() => refetch()} />
            ) : rows.length === 0 ? (
              <EmptyState title="No diagnostics" description="Try adjusting the date range or filter." icon={<Filter className="w-5 h-5" />} />
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[var(--border)]">
                    {["Timestamp", "Inverter", "SCB", "Status", "Missing Strings", "Missing A", "Power Loss kW", "Energy kWh"].map((h) => (
                      <th key={h} className="px-3 py-2 text-left text-xs font-medium text-[var(--muted-foreground)] whitespace-nowrap">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.slice(0, 500).map((row, i) => (
                    <tr key={i} className={cn("border-b border-[var(--border)]/40 text-xs", row.fault_status === "CONFIRMED_DS" ? "bg-red-500/5" : "")}>
                      <td className="px-3 py-1.5 whitespace-nowrap">{row.timestamp?.slice(0, 16)}</td>
                      <td className="px-3 py-1.5">{row.inverter_id}</td>
                      <td className="px-3 py-1.5 font-medium">{row.scb_id}</td>
                      <td className="px-3 py-1.5">
                        <Badge variant={row.fault_status === "CONFIRMED_DS" ? "destructive" : "success"}>
                          {row.fault_status === "CONFIRMED_DS" ? "DS" : "OK"}
                        </Badge>
                      </td>
                      <td className="px-3 py-1.5 text-center">{row.missing_strings ?? 0}</td>
                      <td className="px-3 py-1.5">{formatNumber(row.missing_current, 2)}</td>
                      <td className="px-3 py-1.5">{formatNumber(row.power_loss_kw, 3)}</td>
                      <td className="px-3 py-1.5">{formatNumber(row.energy_loss_kwh, 4)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </CardBody>
      </Card>
    </div>
  );
}

function FaultHistoryTab({ plantId }: { plantId: string }) {
  const [dateFrom, setDateFrom] = useState(toDateStr(subDays(new Date(), 30)));
  const [dateTo, setDateTo] = useState(toDateStr(new Date()));

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["faults", "events", plantId, dateFrom, dateTo],
    queryFn: () => Faults.faultEvents({ plant_id: plantId, date_from: dateFrom, date_to: dateTo }),
    enabled: !!plantId,
  });

  const events = (Array.isArray(data) ? data : []) as FaultEvent[];

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 flex-wrap">
        <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)}
          className="border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none" />
        <span className="text-[var(--muted-foreground)] text-sm">to</span>
        <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)}
          className="border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none" />
        <span className="text-xs text-[var(--muted-foreground)] ml-auto">{events.length} events</span>
      </div>

      <Card>
        <CardBody className="p-0">
          <div className="overflow-x-auto">
            {isLoading ? (
              <div className="py-12 text-center text-[var(--muted-foreground)] animate-pulse">Loading…</div>
            ) : error ? (
              <ErrorState message={String(error)} onRetry={() => refetch()} />
            ) : events.length === 0 ? (
              <EmptyState title="No fault events" description="No DS fault intervals recorded in this period." icon={<History className="w-5 h-5" />} />
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[var(--border)]">
                    {["Equipment", "Inverter", "Start", "End", "Duration", "Severity", "Missing Strings", "Status"].map((h) => (
                      <th key={h} className="px-3 py-2 text-left text-xs font-medium text-[var(--muted-foreground)] whitespace-nowrap">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {events.map((ev) => (
                    <tr key={ev.id} className="border-b border-[var(--border)]/40 hover:bg-[var(--background)] transition text-xs">
                      <td className="px-3 py-2 font-medium">{ev.equipment_id}</td>
                      <td className="px-3 py-2">{ev.inverter_id ?? "—"}</td>
                      <td className="px-3 py-2 whitespace-nowrap">{format(new Date(ev.start_time), "dd MMM HH:mm")}</td>
                      <td className="px-3 py-2 whitespace-nowrap">{ev.end_time ? format(new Date(ev.end_time), "dd MMM HH:mm") : <span className="text-red-500">Ongoing</span>}</td>
                      <td className="px-3 py-2 whitespace-nowrap">
                        {ev.duration_minutes != null ? `${Math.round(ev.duration_minutes)} min` : "—"}
                      </td>
                      <td className="px-3 py-2">
                        <Badge variant={ev.severity === "high" ? "destructive" : ev.severity === "medium" ? "warning" : "muted"}>
                          {ev.severity ?? "—"}
                        </Badge>
                      </td>
                      <td className="px-3 py-2 text-center">{ev.missing_strings ?? "—"}</td>
                      <td className="px-3 py-2">
                        <Badge variant={ev.status === "open" ? "destructive" : "muted"}>{ev.status}</Badge>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </CardBody>
      </Card>
    </div>
  );
}

export default function FaultsPage() {
  const { selectedPlant } = usePlantContext();
  const [activeTab, setActiveTab] = useState<Tab>("ds-status");

  if (!selectedPlant)
    return (
      <div className="flex items-center justify-center h-64 text-[var(--muted-foreground)]">
        Select a plant to view fault diagnostics
      </div>
    );

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-bold flex items-center gap-2">
          <AlertTriangle className="w-5 h-5 text-orange-500" />
          Fault Diagnostics
        </h1>
        <p className="text-sm text-[var(--muted-foreground)] mt-0.5">
          Disconnected string detection, diagnostic history, and fault events
        </p>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 p-1 bg-[var(--border)]/30 rounded-xl w-fit">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={cn(
              "px-4 py-2 rounded-lg text-sm font-medium transition",
              activeTab === tab.id
                ? "bg-[var(--card)] text-[var(--foreground)] shadow-sm"
                : "text-[var(--muted-foreground)] hover:text-[var(--foreground)]"
            )}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === "ds-status"   && <DSStatusTab    plantId={selectedPlant} />}
      {activeTab === "diagnostics" && <DiagnosticsTab plantId={selectedPlant} />}
      {activeTab === "history"     && <FaultHistoryTab plantId={selectedPlant} />}
    </div>
  );
}
