"use client";

import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Admin as AdminAPI } from "@/lib/api";
import { usePlantContext } from "@/components/plant-context";
import { Card, CardHeader, CardTitle, CardBody } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ErrorState } from "@/components/ui/states";
import { PageSkeleton } from "@/components/ui/skeleton";
import type { PlantResponse } from "@/types";
import { ShieldCheck, Users, Zap, RefreshCw } from "lucide-react";
import { useSession } from "next-auth/react";
import { redirect } from "next/navigation";

function UsersTab() {
  const { data: users = [], isLoading, error } = useQuery({
    queryKey: ["admin", "users"],
    queryFn: () => AdminAPI.users(),
  });

  if (isLoading) return <PageSkeleton />;
  if (error) return <ErrorState message={String(error)} />;

  return (
    <Card>
      <CardHeader><CardTitle>System Users ({(users as unknown[]).length})</CardTitle></CardHeader>
      <CardBody className="p-0">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[var(--border)]">
              {["Name", "Email", "Role", "Plants", "Status"].map((h) => (
                <th key={h} className="px-3 py-2 text-left text-xs font-medium text-[var(--muted-foreground)]">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(users as Record<string, unknown>[]).map((user, i) => (
              <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--background)] transition text-sm">
                <td className="px-3 py-2 font-medium">{String(user.full_name ?? "—")}</td>
                <td className="px-3 py-2 text-[var(--muted-foreground)]">{String(user.email ?? "—")}</td>
                <td className="px-3 py-2">
                  <Badge variant={user.is_admin ? "info" : "muted"}>{user.is_admin ? "Admin" : "Viewer"}</Badge>
                </td>
                <td className="px-3 py-2 text-xs">
                  {Array.isArray(user.allowed_plants)
                    ? user.allowed_plants.join(", ") || "All"
                    : "All"}
                </td>
                <td className="px-3 py-2">
                  <Badge variant={user.is_active ? "success" : "muted"}>{user.is_active ? "Active" : "Inactive"}</Badge>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </CardBody>
    </Card>
  );
}

function PlantsTab() {
  const queryClient = useQueryClient();
  const { data: plants = [], isLoading, error } = useQuery({
    queryKey: ["admin", "plants"],
    queryFn: () => AdminAPI.plants(),
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<PlantResponse> }) =>
      AdminAPI.updatePlant(id, data),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["admin", "plants"] }),
  });

  if (isLoading) return <PageSkeleton />;
  if (error) return <ErrorState message={String(error)} />;

  return (
    <Card>
      <CardHeader><CardTitle>Plants ({(plants as unknown[]).length})</CardTitle></CardHeader>
      <CardBody className="p-0">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[var(--border)]">
              {["Plant ID", "Name", "Location", "Capacity", "Plant Type", "Status"].map((h) => (
                <th key={h} className="px-3 py-2 text-left text-xs font-medium text-[var(--muted-foreground)]">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(plants as PlantResponse[]).map((p) => (
              <tr key={p.plant_id} className="border-b border-[var(--border)]/50 hover:bg-[var(--background)] transition text-sm">
                <td className="px-3 py-2 font-mono text-xs">{p.plant_id}</td>
                <td className="px-3 py-2 font-medium">{p.name}</td>
                <td className="px-3 py-2 text-[var(--muted-foreground)]">{p.location ?? "—"}</td>
                <td className="px-3 py-2">{p.capacity_mwp ? `${p.capacity_mwp} MWp` : "—"}</td>
                <td className="px-3 py-2">
                  <select
                    value={p.plant_type ?? "SCB"}
                    onChange={(e) =>
                      updateMutation.mutate({ id: p.plant_id, data: { plant_type: e.target.value } })
                    }
                    className="border border-[var(--border)] rounded px-2 py-0.5 text-xs bg-[var(--card)] focus:outline-none"
                  >
                    <option value="SCB">SCB</option>
                    <option value="MPPT">MPPT</option>
                  </select>
                </td>
                <td className="px-3 py-2">
                  <Badge variant={p.status === "active" ? "success" : "muted"}>{p.status ?? "—"}</Badge>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </CardBody>
    </Card>
  );
}

function PerfTab() {
  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["admin", "perf"],
    queryFn: () => AdminAPI.perfStatus(),
  });

  if (isLoading) return <PageSkeleton />;
  if (error) return <ErrorState message={String(error)} onRetry={() => refetch()} />;

  const info = data as Record<string, unknown> ?? {};
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold">TimescaleDB Status</h3>
        <button onClick={() => refetch()} className="flex items-center gap-1 text-xs text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition">
          <RefreshCw className="w-3.5 h-3.5" /> Refresh
        </button>
      </div>
      <Card>
        <CardBody>
          <dl className="grid grid-cols-2 sm:grid-cols-3 gap-4 text-sm">
            {Object.entries(info).map(([k, v]) => (
              <div key={k}>
                <dt className="text-xs text-[var(--muted-foreground)] capitalize">{k.replace(/_/g, " ")}</dt>
                <dd className="font-medium mt-0.5">{String(v)}</dd>
              </div>
            ))}
          </dl>
        </CardBody>
      </Card>
    </div>
  );
}

type AdminTab = "users" | "plants" | "perf";

export default function AdminPage() {
  const { data: session } = useSession();
  const [activeTab, setActiveTab] = useState<AdminTab>("users");

  const isAdmin = !!(session?.user as Record<string, unknown>)?.isAdmin;
  if (session && !isAdmin) redirect("/dashboard");

  const tabs: { id: AdminTab; label: string; icon: React.ReactNode }[] = [
    { id: "users",  label: "Users",       icon: <Users className="w-4 h-4" /> },
    { id: "plants", label: "Plants",      icon: <Zap className="w-4 h-4" /> },
    { id: "perf",   label: "Performance", icon: <RefreshCw className="w-4 h-4" /> },
  ];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-bold flex items-center gap-2">
          <ShieldCheck className="w-5 h-5 text-indigo-500" />
          Admin Portal
        </h1>
        <p className="text-sm text-[var(--muted-foreground)] mt-0.5">Manage users, plants, and system configuration</p>
      </div>

      <div className="flex gap-1 p-1 bg-[var(--border)]/30 rounded-xl w-fit">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium transition ${
              activeTab === tab.id
                ? "bg-[var(--card)] text-[var(--foreground)] shadow-sm"
                : "text-[var(--muted-foreground)] hover:text-[var(--foreground)]"
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === "users"  && <UsersTab />}
      {activeTab === "plants" && <PlantsTab />}
      {activeTab === "perf"   && <PerfTab />}
    </div>
  );
}
