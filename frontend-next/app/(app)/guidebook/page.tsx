import { BookOpen } from "lucide-react";
import { Card, CardHeader, CardTitle, CardBody } from "@/components/ui/card";

const SECTIONS = [
  {
    title: "Getting Started",
    content: [
      "Log in with your corporate email and password.",
      "Select your plant from the plant selector in the top navigation bar.",
      "The Dashboard shows real-time KPIs, inverter status, and energy generation.",
    ],
  },
  {
    title: "Analytics Lab",
    content: [
      "Choose an equipment level (inverter, SCB, plant, WMS) and signal.",
      "Select a date range and the equipment IDs you want to compare.",
      "Charts are zoomable — use the range slider below the chart or scroll to zoom.",
      "Up to 8 equipment items are plotted simultaneously for clarity.",
    ],
  },
  {
    title: "Fault Diagnostics — Disconnected Strings (DS)",
    content: [
      "The DS Status tab shows the latest snapshot of all SCBs with a confirmed fault.",
      "The Diagnostics tab lets you browse raw fault_diagnostics rows for any date range.",
      "The Fault History tab shows interval-based fault events (start/end/duration) for audit purposes.",
      "Detection algorithm: plant-type-aware (MPPT uses 75th-percentile reference, SCB uses top-25% virtual reference).",
      "Filters applied before detection: irradiance ≥ 150 W/m², inverter status ON, non-zero export.",
    ],
  },
  {
    title: "Loss Analysis",
    content: [
      "Shows an energy-loss waterfall breakdown for any date range.",
      "Categories typically include: optical losses, temperature losses, mismatch, wiring, DS faults, inverter inefficiency.",
      "Performance Ratio (PR) and CUF are computed from actual vs. reference yield.",
    ],
  },
  {
    title: "Reports",
    content: [
      "Generate daily, weekly, or monthly PDF/Excel reports.",
      "Reports include energy yield, PR, CUF, specific yield, and DS fault summary.",
      "Generated reports are stored and can be re-downloaded from the history list.",
    ],
  },
  {
    title: "Admin Portal",
    content: [
      "Manage users: grant/revoke admin, set allowed plants.",
      "Manage plants: update plant type (SCB/MPPT) — critical for correct DS detection.",
      "Performance tab: shows TimescaleDB CAGG status and last refresh timestamp.",
    ],
  },
  {
    title: "Performance Tips",
    content: [
      "Analytics Lab: narrow date range and select fewer equipment IDs for faster loads.",
      "Fault Diagnostics: use the filter (DS Only) to skip the large NORMAL rows.",
      "TimescaleDB CAGG: when enabled (SOLAR_ANALYTICS_USE_TIMESCALE_CAGG=1), Analytics Lab loads in < 2 seconds for 7-day queries.",
      "Snapshots (SOLAR_SNAPSHOT_READ_ONLY=1): fast dashboard loads from pre-computed snapshots.",
    ],
  },
];

export default function GuidebookPage() {
  return (
    <div className="space-y-6 max-w-4xl">
      <div>
        <h1 className="text-xl font-bold flex items-center gap-2">
          <BookOpen className="w-5 h-5 text-emerald-500" />
          Guidebook
        </h1>
        <p className="text-sm text-[var(--muted-foreground)] mt-0.5">
          User guide for the Solar Analytics Platform
        </p>
      </div>

      {SECTIONS.map((section) => (
        <Card key={section.title}>
          <CardHeader>
            <CardTitle>{section.title}</CardTitle>
          </CardHeader>
          <CardBody>
            <ul className="space-y-2">
              {section.content.map((item, i) => (
                <li key={i} className="flex items-start gap-2 text-sm text-[var(--foreground)]">
                  <span className="w-1.5 h-1.5 rounded-full bg-[#0ea5e9] mt-2 flex-shrink-0" />
                  {item}
                </li>
              ))}
            </ul>
          </CardBody>
        </Card>
      ))}
    </div>
  );
}
