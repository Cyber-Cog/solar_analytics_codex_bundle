import { cn } from "@/lib/utils";
import { AlertCircle, RefreshCw } from "lucide-react";

interface ErrorStateProps {
  message?: string;
  onRetry?: () => void;
  className?: string;
}

export function ErrorState({ message, onRetry, className }: ErrorStateProps) {
  return (
    <div className={cn("flex flex-col items-center justify-center py-16 gap-4 text-center", className)}>
      <div className="w-12 h-12 rounded-full bg-red-500/10 flex items-center justify-center">
        <AlertCircle className="w-6 h-6 text-red-500" />
      </div>
      <div>
        <p className="font-medium text-[var(--foreground)]">Failed to load data</p>
        <p className="text-sm text-[var(--muted-foreground)] mt-1 max-w-sm">
          {message ?? "An unexpected error occurred. Check your connection and try again."}
        </p>
      </div>
      {onRetry && (
        <button
          onClick={onRetry}
          className="flex items-center gap-2 px-4 py-2 text-sm bg-[var(--border)] hover:bg-[var(--input)] text-[var(--foreground)] rounded-lg transition"
        >
          <RefreshCw className="w-4 h-4" />
          Retry
        </button>
      )}
    </div>
  );
}

export function EmptyState({
  title = "No data",
  description,
  icon,
}: {
  title?: string;
  description?: string;
  icon?: React.ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center py-16 gap-3 text-center">
      {icon && <div className="w-12 h-12 rounded-full bg-[var(--border)] flex items-center justify-center text-[var(--muted-foreground)]">{icon}</div>}
      <p className="font-medium text-[var(--foreground)]">{title}</p>
      {description && <p className="text-sm text-[var(--muted-foreground)] max-w-sm">{description}</p>}
    </div>
  );
}
