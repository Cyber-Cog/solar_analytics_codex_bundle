"use client";

import { useQuery } from "@tanstack/react-query";
import { Metadata } from "@/lib/api";
import { usePlantContext } from "@/components/plant-context";
import { Card, CardHeader, CardTitle, CardBody } from "@/components/ui/card";
import { ErrorState, EmptyState } from "@/components/ui/states";
import { PageSkeleton } from "@/components/ui/skeleton";
import type { ArchitectureRow } from "@/types";
import { Database } from "lucide-react";

export default function MetadataPage() {
  const { selectedPlant } = usePlantContext();

  const { data: arch, isLoading, error } = useQuery({
    queryKey: ["metadata", "architecture", selectedPlant],
    queryFn: () => Metadata.architecture(selectedPlant),
    enabled: !!selectedPlant,
  });

  const { data: specs } = useQuery({
    queryKey: ["metadata", "specs", selectedPlant],
    queryFn: () => Metadata.specs(selectedPlant),
    enabled: !!selectedPlant,
  });

  if (!selectedPlant)
    return (
      <div className="flex items-center justify-center h-64 text-[var(--muted-foreground)]">
        Select a plant to view metadata
      </div>
    );

  if (isLoading) return <PageSkeleton />;
  if (error) return <ErrorState message={String(error)} />;

  const archRows = (Array.isArray(arch) ? arch : []) as ArchitectureRow[];
  const specsData = specs as Record<string, unknown> ?? {};

  // Group by inverter
  const byInverter: Record<string, ArchitectureRow[]> = {};
  archRows.forEach((row) => {
    const inv = row.inverter_id ?? "Unknown";
    if (!byInverter[inv]) byInverter[inv] = [];
    byInverter[inv].push(row);
  });

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-bold flex items-center gap-2">
          <Database className="w-5 h-5 text-purple-500" />
          Plant Metadata
        </h1>
        <p className="text-sm text-[var(--muted-foreground)] mt-0.5">
          Architecture, equipment specs, and configuration
        </p>
      </div>

      {/* Specs summary */}
      {Object.keys(specsData).length > 0 && (
        <Card>
          <CardHeader><CardTitle>Equipment Specifications</CardTitle></CardHeader>
          <CardBody>
            <dl className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-3 text-sm">
              {Object.entries(specsData).filter(([, v]) => v !== null && v !== undefined && typeof v !== "object").map(([k, v]) => (
                <div key={k}>
                  <dt className="text-xs text-[var(--muted-foreground)] capitalize">{k.replace(/_/g, " ")}</dt>
                  <dd className="font-medium mt-0.5">{String(v)}</dd>
                </div>
              ))}
            </dl>
          </CardBody>
        </Card>
      )}

      {/* Architecture tree */}
      <Card>
        <CardHeader>
          <CardTitle>Plant Architecture ({archRows.length} SCBs across {Object.keys(byInverter).length} inverters)</CardTitle>
        </CardHeader>
        <CardBody className="p-0">
          {archRows.length === 0 ? (
            <EmptyState title="No architecture data" />
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[var(--border)]">
                    {["Inverter", "SCB", "Strings/SCB", "Spare", "String ID"].map((h) => (
                      <th key={h} className="px-3 py-2 text-left text-xs font-medium text-[var(--muted-foreground)]">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {archRows.map((row, i) => (
                    <tr key={i} className="border-b border-[var(--border)]/40 hover:bg-[var(--background)] transition text-xs">
                      <td className="px-3 py-1.5 font-medium">{row.inverter_id}</td>
                      <td className="px-3 py-1.5">{row.scb_id}</td>
                      <td className="px-3 py-1.5 text-center">{row.strings_per_scb ?? "—"}</td>
                      <td className="px-3 py-1.5 text-center">{row.spare_flag ? "✓" : ""}</td>
                      <td className="px-3 py-1.5 text-[var(--muted-foreground)]">{row.string_id ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardBody>
      </Card>
    </div>
  );
}
