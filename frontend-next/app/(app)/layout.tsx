"use client";

import { useState } from "react";
import { Sidebar, MobileSidebar } from "@/components/layout/sidebar";
import { Navbar } from "@/components/layout/navbar";
import { PlantProvider, usePlantContext } from "@/components/plant-context";
import { usePlants } from "@/hooks/use-plants";
import { useSession } from "next-auth/react";

function AppShell({ children }: { children: React.ReactNode }) {
  const { data: session } = useSession();
  const { plants, selectedPlant, setSelectedPlant } = usePlantContext();
  const [collapsed, setCollapsed] = useState(false);
  const isAdmin = !!(session?.user as Record<string, unknown>)?.isAdmin;
  const plantName = plants.find((p) => p.plant_id === selectedPlant)?.name;

  return (
    <div className="h-screen flex overflow-hidden bg-[var(--background)]">
      {/* Desktop Sidebar */}
      <div className="hidden lg:flex flex-shrink-0">
        <Sidebar
          collapsed={collapsed}
          onToggle={() => setCollapsed((v) => !v)}
          isAdmin={isAdmin}
          plantName={plantName}
        />
      </div>

      {/* Main content area */}
      <div className="flex-1 flex flex-col overflow-hidden min-w-0">
        <Navbar
          plants={plants}
          selectedPlant={selectedPlant}
          onPlantChange={setSelectedPlant}
        />
        <main className="flex-1 overflow-y-auto p-4 sm:p-6">
          {children}
        </main>
      </div>

      {/* Mobile sidebar */}
      <MobileSidebar isAdmin={isAdmin} />
    </div>
  );
}

export default function AppLayout({ children }: { children: React.ReactNode }) {
  const { data: plants = [], isLoading } = usePlants();

  return (
    <PlantProvider plants={plants} isLoading={isLoading}>
      <AppShell>{children}</AppShell>
    </PlantProvider>
  );
}
