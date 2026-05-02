"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import {
  LayoutDashboard, FlaskConical, AlertTriangle, TrendingDown,
  FileText, Database, ShieldCheck, BookOpen, ChevronLeft,
  Sun, X, Menu,
} from "lucide-react";
import { useState } from "react";

export interface NavItem {
  label: string;
  href: string;
  icon: React.ReactNode;
  adminOnly?: boolean;
}

const navItems: NavItem[] = [
  { label: "Dashboard",         href: "/dashboard",      icon: <LayoutDashboard className="w-4 h-4" /> },
  { label: "Analytics Lab",     href: "/analytics",      icon: <FlaskConical    className="w-4 h-4" /> },
  { label: "Fault Diagnostics", href: "/faults",         icon: <AlertTriangle   className="w-4 h-4" /> },
  { label: "Loss Analysis",     href: "/loss-analysis",  icon: <TrendingDown    className="w-4 h-4" /> },
  { label: "Reports",           href: "/reports",        icon: <FileText        className="w-4 h-4" /> },
  { label: "Metadata",          href: "/metadata",       icon: <Database        className="w-4 h-4" /> },
  { label: "Admin",             href: "/admin",          icon: <ShieldCheck     className="w-4 h-4" />, adminOnly: true },
  { label: "Guidebook",         href: "/guidebook",      icon: <BookOpen        className="w-4 h-4" /> },
];

interface SidebarProps {
  collapsed: boolean;
  onToggle: () => void;
  isAdmin?: boolean;
  plantName?: string;
}

export function Sidebar({ collapsed, onToggle, isAdmin, plantName }: SidebarProps) {
  const pathname = usePathname();

  return (
    <aside
      className={cn(
        "flex flex-col h-full bg-[var(--sidebar-bg)] border-r border-white/5 transition-all duration-300 ease-in-out select-none",
        collapsed ? "w-[60px]" : "w-[220px]"
      )}
    >
      {/* Logo */}
      <div className="flex items-center px-4 h-16 border-b border-white/5 flex-shrink-0">
        <div className="flex items-center gap-2.5 min-w-0">
          <div className="w-8 h-8 bg-[#f0a500] rounded-lg flex items-center justify-center flex-shrink-0">
            <Sun className="w-5 h-5 text-white" strokeWidth={2} />
          </div>
          {!collapsed && (
            <span className="text-white font-semibold text-sm truncate leading-tight">
              Solar Analytics
            </span>
          )}
        </div>
      </div>

      {/* Plant badge */}
      {!collapsed && plantName && (
        <div className="mx-3 mt-3 px-2.5 py-1.5 bg-white/5 rounded-lg border border-white/10">
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-0.5">Active Plant</p>
          <p className="text-xs text-slate-200 font-medium truncate">{plantName}</p>
        </div>
      )}

      {/* Nav */}
      <nav className="flex-1 overflow-y-auto py-3 space-y-0.5 px-2">
        {navItems
          .filter((item) => !item.adminOnly || isAdmin)
          .map((item) => {
            const active = pathname.startsWith(item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                title={collapsed ? item.label : undefined}
                className={cn(
                  "flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-sm font-medium transition-colors",
                  active
                    ? "bg-[#0ea5e9]/20 text-[#38bdf8]"
                    : "text-slate-400 hover:text-slate-200 hover:bg-white/5"
                )}
              >
                <span className="flex-shrink-0">{item.icon}</span>
                {!collapsed && <span className="truncate">{item.label}</span>}
                {!collapsed && active && (
                  <span className="ml-auto w-1.5 h-1.5 bg-[#0ea5e9] rounded-full" />
                )}
              </Link>
            );
          })}
      </nav>

      {/* Collapse toggle */}
      <div className="flex-shrink-0 p-2 border-t border-white/5">
        <button
          onClick={onToggle}
          className="w-full flex items-center justify-center gap-2 px-2.5 py-2 rounded-lg text-slate-500 hover:text-slate-300 hover:bg-white/5 transition text-xs"
        >
          <ChevronLeft className={cn("w-4 h-4 transition-transform", collapsed && "rotate-180")} />
          {!collapsed && <span>Collapse</span>}
        </button>
      </div>
    </aside>
  );
}

/* Mobile drawer wrapper */
export function MobileSidebar({ isAdmin }: { isAdmin?: boolean }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button
        onClick={() => setOpen(true)}
        className="lg:hidden p-2 text-slate-400 hover:text-slate-200"
      >
        <Menu className="w-5 h-5" />
      </button>
      {open && (
        <div className="fixed inset-0 z-50 lg:hidden">
          <div className="absolute inset-0 bg-black/50" onClick={() => setOpen(false)} />
          <div className="relative h-full">
            <Sidebar collapsed={false} onToggle={() => setOpen(false)} isAdmin={isAdmin} />
            <button
              onClick={() => setOpen(false)}
              className="absolute top-4 right-4 text-slate-400 hover:text-white"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>
      )}
    </>
  );
}
