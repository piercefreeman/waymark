import { RefreshCw } from "lucide-react";
import { cn } from "@/lib/cn";
import { formatClock, formatRelative } from "@/lib/format";

/** Freshness and availability of the page's API data. */
export interface SourceStatus {
  fetchedAt: Date | null;
  error: Error | null;
  loading: boolean;
  /** False when a paged read stopped before the end. */
  complete: boolean;
  refresh?: () => void;
}

/**
 * A refresh failure never empties a view. It adds this line, with the last
 * successful time and a retry, above data that is still on screen.
 */
export function SourceNotice({
  source,
  now,
  className,
}: {
  source: SourceStatus;
  now: Date;
  className?: string;
}) {
  if (!source.error && source.complete) return null;
  return (
    <div
      role={source.error ? "alert" : "status"}
      className={cn(
        "flex flex-wrap items-center gap-x-3 gap-y-1 rounded-panel border px-3 py-2 text-label",
        source.error
          ? "border-danger/40 bg-danger/10 text-fg"
          : "border-waiting/40 bg-waiting/10 text-fg",
        className,
      )}
    >
      {source.error ? (
        <>
          <span className="font-medium text-danger">Couldn't refresh.</span>
          <span className="mono-data truncate text-fg-muted">
            {source.error.message}
          </span>
          {source.fetchedAt && (
            <span className="text-fg-muted">
              Showing data from {formatClock(source.fetchedAt)} (
              {formatRelative(source.fetchedAt, now)}).
            </span>
          )}
        </>
      ) : (
        <span className="text-fg-muted">
          Showing a partial read; more history exists than was loaded.
        </span>
      )}
      {source.refresh && (
        <button
          type="button"
          onClick={source.refresh}
          disabled={source.loading}
          className="ml-auto inline-flex h-6 items-center gap-1 rounded-control border border-line-strong px-2 text-micro text-fg transition-colors duration-fast hover:bg-surface-raised disabled:opacity-50"
        >
          <RefreshCw
            className={cn("size-3", source.loading && "animate-spin")}
            aria-hidden
          />
          Retry
        </button>
      )}
    </div>
  );
}
