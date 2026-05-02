"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Reports as ReportsAPI } from "@/lib/api";
import { usePlantContext } from "@/components/plant-context";
import { Card, CardHeader, CardTitle, CardBody } from "@/components/ui/card";
import { ErrorState, EmptyState } from "@/components/ui/states";
import { toDateStr } from "@/lib/utils";
import { subDays, format } from "date-fns";
import { FileText, Download, Plus } from "lucide-react";

const REPORT_TYPES = ["daily", "weekly", "monthly", "custom"];

export default function ReportsPage() {
  const { selectedPlant } = usePlantContext();
  const [reportType, setReportType] = useState("monthly");
  const [dateFrom, setDateFrom] = useState(toDateStr(subDays(new Date(), 30)));
  const [dateTo, setDateTo] = useState(toDateStr(new Date()));
  const [generating, setGenerating] = useState(false);
  const [generatedReport, setGeneratedReport] = useState<Record<string, unknown> | null>(null);

  const { data: reportsList = [], isLoading, error } = useQuery({
    queryKey: ["reports", selectedPlant],
    queryFn: () => ReportsAPI.list(selectedPlant),
    enabled: !!selectedPlant,
  });

  async function handleGenerate() {
    if (!selectedPlant) return;
    setGenerating(true);
    try {
      const result = await ReportsAPI.generate({
        plant_id: selectedPlant,
        report_type: reportType,
        date_from: dateFrom,
        date_to: dateTo,
      });
      setGeneratedReport(result as Record<string, unknown>);
    } catch (e) {
      alert("Failed to generate report: " + String(e));
    } finally {
      setGenerating(false);
    }
  }

  if (!selectedPlant)
    return (
      <div className="flex items-center justify-center h-64 text-[var(--muted-foreground)]">
        Select a plant to view reports
      </div>
    );

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-bold flex items-center gap-2">
          <FileText className="w-5 h-5 text-blue-500" />
          Reports
        </h1>
        <p className="text-sm text-[var(--muted-foreground)] mt-0.5">Generate and download plant performance reports</p>
      </div>

      {/* Generator */}
      <Card>
        <CardHeader><CardTitle>Generate Report</CardTitle></CardHeader>
        <CardBody>
          <div className="grid grid-cols-1 sm:grid-cols-4 gap-4 items-end">
            <div>
              <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1.5">Report Type</label>
              <select value={reportType} onChange={(e) => setReportType(e.target.value)}
                className="w-full border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none">
                {REPORT_TYPES.map((t) => <option key={t} value={t}>{t.charAt(0).toUpperCase() + t.slice(1)}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1.5">From</label>
              <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)}
                className="w-full border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none" />
            </div>
            <div>
              <label className="block text-xs font-medium text-[var(--muted-foreground)] mb-1.5">To</label>
              <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)}
                className="w-full border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm bg-[var(--card)] text-[var(--foreground)] focus:outline-none" />
            </div>
            <button onClick={handleGenerate} disabled={generating}
              className="flex items-center gap-2 justify-center px-4 py-1.5 bg-[#1e3a5f] text-white rounded-lg text-sm font-medium hover:bg-[#2a5080] disabled:opacity-60 transition">
              <Plus className="w-4 h-4" />
              {generating ? "Generating…" : "Generate"}
            </button>
          </div>

          {generatedReport && (
            <div className="mt-4 p-3 bg-green-500/10 border border-green-500/30 rounded-lg text-sm text-green-700 dark:text-green-400">
              Report generated successfully.
              {Boolean((generatedReport as Record<string, unknown>).download_url) && (
                <a href={String((generatedReport as Record<string, unknown>).download_url)} download
                  className="ml-2 inline-flex items-center gap-1 underline">
                  <Download className="w-3.5 h-3.5" /> Download
                </a>
              )}
            </div>
          )}
        </CardBody>
      </Card>

      {/* Reports list */}
      <Card>
        <CardHeader><CardTitle>Recent Reports</CardTitle></CardHeader>
        <CardBody className="p-0">
          {isLoading ? (
            <div className="py-8 text-center text-[var(--muted-foreground)] text-sm animate-pulse">Loading…</div>
          ) : error ? (
            <ErrorState message={String(error)} />
          ) : (reportsList as unknown[]).length === 0 ? (
            <EmptyState title="No reports" description="Generate your first report above." icon={<FileText className="w-5 h-5" />} />
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[var(--border)]">
                  {["Report Name", "Type", "Period", "Generated", ""].map((h) => (
                    <th key={h} className="px-3 py-2 text-left text-xs font-medium text-[var(--muted-foreground)]">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(reportsList as Record<string, unknown>[]).map((r, i) => (
                  <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--background)] transition">
                    <td className="px-3 py-2 font-medium">{String(r.name ?? `Report ${i + 1}`)}</td>
                    <td className="px-3 py-2 capitalize">{String(r.report_type ?? "—")}</td>
                    <td className="px-3 py-2 text-xs text-[var(--muted-foreground)]">
                      {r.date_from && r.date_to ? `${r.date_from} – ${r.date_to}` : "—"}
                    </td>
                    <td className="px-3 py-2 text-xs text-[var(--muted-foreground)]">
                      {r.created_at ? format(new Date(String(r.created_at)), "dd MMM yyyy HH:mm") : "—"}
                    </td>
                    <td className="px-3 py-2">
                      {Boolean(r.download_url) && (
                        <a href={String(r.download_url)} download
                          className="flex items-center gap-1 text-[#0ea5e9] text-xs hover:underline">
                          <Download className="w-3.5 h-3.5" /> Download
                        </a>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </CardBody>
      </Card>
    </div>
  );
}
