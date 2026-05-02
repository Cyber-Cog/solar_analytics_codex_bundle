import { cn } from "@/lib/utils";

type BadgeVariant = "default" | "success" | "warning" | "destructive" | "info" | "muted";

const variantClasses: Record<BadgeVariant, string> = {
  default:     "bg-[var(--border)] text-[var(--foreground)]",
  success:     "bg-green-500/15 text-green-600 dark:text-green-400",
  warning:     "bg-yellow-500/15 text-yellow-700 dark:text-yellow-400",
  destructive: "bg-red-500/15 text-red-600 dark:text-red-400",
  info:        "bg-[#0ea5e9]/15 text-[#0284c7]",
  muted:       "bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400",
};

export function Badge({
  variant = "default",
  className,
  children,
  ...props
}: React.HTMLAttributes<HTMLSpanElement> & { variant?: BadgeVariant }) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium",
        variantClasses[variant],
        className
      )}
      {...props}
    >
      {children}
    </span>
  );
}
