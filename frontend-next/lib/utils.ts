import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatNumber(n: number | null | undefined, decimals = 2): string {
  if (n == null || isNaN(n)) return "—";
  return n.toLocaleString("en-IN", { maximumFractionDigits: decimals });
}

export function formatMW(kw: number | null | undefined): string {
  if (kw == null) return "—";
  const mw = kw / 1000;
  return mw >= 1 ? `${mw.toFixed(2)} MW` : `${kw.toFixed(1)} kW`;
}

export function formatPercent(val: number | null | undefined): string {
  if (val == null || isNaN(val)) return "—";
  return `${val.toFixed(1)}%`;
}

export function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleDateString("en-IN", {
      day: "2-digit", month: "short", year: "numeric",
    });
  } catch {
    return iso;
  }
}

export function toDateStr(d: Date): string {
  return d.toISOString().slice(0, 10);
}

export function statusColor(status: string): string {
  const s = status?.toLowerCase();
  if (s === "normal" || s === "online" || s === "active") return "text-green-500";
  if (s === "confirmed_ds" || s === "fault" || s === "offline") return "text-red-500";
  if (s === "warning" || s === "degraded") return "text-yellow-500";
  return "text-muted";
}
