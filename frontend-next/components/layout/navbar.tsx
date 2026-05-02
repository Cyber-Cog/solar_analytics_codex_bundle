"use client";

import { signOut, useSession } from "next-auth/react";
import { Bell, LogOut, User, ChevronDown, RefreshCw } from "lucide-react";
import { useState } from "react";
import { cn } from "@/lib/utils";
import type { PlantResponse } from "@/types";

interface NavbarProps {
  plants: PlantResponse[];
  selectedPlant: string;
  onPlantChange: (id: string) => void;
}

export function Navbar({ plants, selectedPlant, onPlantChange }: NavbarProps) {
  const { data: session } = useSession();
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [plantMenuOpen, setPlantMenuOpen] = useState(false);

  const currentPlant = plants.find((p) => p.plant_id === selectedPlant);
  const user = session?.user;

  return (
    <header className="h-16 flex items-center justify-between px-4 sm:px-6 border-b border-[var(--border)] bg-[var(--card)] flex-shrink-0">
      {/* Plant selector */}
      <div className="relative">
        <button
          onClick={() => setPlantMenuOpen((v) => !v)}
          className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-[var(--border)] hover:bg-[var(--background)] transition text-sm"
        >
          <span className="w-2 h-2 rounded-full bg-green-500 flex-shrink-0" />
          <span className="font-medium truncate max-w-[160px]">
            {currentPlant?.name ?? selectedPlant ?? "Select Plant"}
          </span>
          <ChevronDown className={cn("w-3.5 h-3.5 text-[var(--muted-foreground)] transition-transform", plantMenuOpen && "rotate-180")} />
        </button>

        {plantMenuOpen && (
          <>
            <div className="fixed inset-0 z-40" onClick={() => setPlantMenuOpen(false)} />
            <div className="absolute left-0 top-full mt-1 z-50 w-64 bg-[var(--card)] border border-[var(--border)] rounded-xl shadow-xl overflow-hidden">
              <div className="p-2 space-y-0.5 max-h-72 overflow-y-auto">
                {plants.map((p) => (
                  <button
                    key={p.plant_id}
                    onClick={() => {
                      onPlantChange(p.plant_id);
                      setPlantMenuOpen(false);
                    }}
                    className={cn(
                      "w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition text-left",
                      selectedPlant === p.plant_id
                        ? "bg-[#0ea5e9]/10 text-[#0ea5e9]"
                        : "hover:bg-[var(--background)] text-[var(--foreground)]"
                    )}
                  >
                    <span className={cn("w-2 h-2 rounded-full flex-shrink-0", p.status === "active" ? "bg-green-500" : "bg-slate-400")} />
                    <div className="min-w-0">
                      <p className="font-medium truncate">{p.name}</p>
                      <p className="text-xs text-[var(--muted-foreground)] truncate">{p.location ?? p.plant_id}</p>
                    </div>
                    {p.capacity_mwp && (
                      <span className="ml-auto text-xs text-[var(--muted-foreground)] flex-shrink-0">
                        {p.capacity_mwp} MWp
                      </span>
                    )}
                  </button>
                ))}
              </div>
            </div>
          </>
        )}
      </div>

      {/* Right actions */}
      <div className="flex items-center gap-1">
        <button
          onClick={() => window.location.reload()}
          className="p-2 text-[var(--muted-foreground)] hover:text-[var(--foreground)] hover:bg-[var(--background)] rounded-lg transition"
          title="Refresh"
        >
          <RefreshCw className="w-4 h-4" />
        </button>

        <button className="p-2 text-[var(--muted-foreground)] hover:text-[var(--foreground)] hover:bg-[var(--background)] rounded-lg transition relative">
          <Bell className="w-4 h-4" />
        </button>

        {/* User menu */}
        <div className="relative ml-1">
          <button
            onClick={() => setUserMenuOpen((v) => !v)}
            className="flex items-center gap-2 pl-2 pr-3 py-1.5 rounded-lg hover:bg-[var(--background)] transition"
          >
            <div className="w-7 h-7 bg-[#1e3a5f] rounded-full flex items-center justify-center">
              <User className="w-3.5 h-3.5 text-white" />
            </div>
            <div className="hidden sm:block text-left">
              <p className="text-xs font-medium leading-tight text-[var(--foreground)]">
                {user?.name ?? user?.email ?? "User"}
              </p>
            </div>
            <ChevronDown className={cn("w-3 h-3 text-[var(--muted-foreground)] transition-transform", userMenuOpen && "rotate-180")} />
          </button>

          {userMenuOpen && (
            <>
              <div className="fixed inset-0 z-40" onClick={() => setUserMenuOpen(false)} />
              <div className="absolute right-0 top-full mt-1 z-50 w-48 bg-[var(--card)] border border-[var(--border)] rounded-xl shadow-xl overflow-hidden">
                <div className="p-3 border-b border-[var(--border)]">
                  <p className="text-xs font-medium text-[var(--foreground)]">{user?.name}</p>
                  <p className="text-xs text-[var(--muted-foreground)] truncate">{user?.email}</p>
                </div>
                <div className="p-1">
                  <button
                    onClick={() => signOut({ callbackUrl: "/login" })}
                    className="w-full flex items-center gap-2 px-3 py-2 text-sm text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-lg transition"
                  >
                    <LogOut className="w-3.5 h-3.5" />
                    Sign out
                  </button>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </header>
  );
}
