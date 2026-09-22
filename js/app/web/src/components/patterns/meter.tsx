import { cn } from "@/lib/cn";
import { capacityPercent } from "@/lib/capacity";

/**
 * Used-of-capacity meter. Load is blue, never a status color: a busy node is
 * not a failed node. Unknown capacity renders as unavailable, never 0%.
 */
export function Meter({
  value,
  max,
  label,
  showValue = true,
  className,
}: {
  value: number | null;
  max: number | null;
  label: string;
  showValue?: boolean;
  className?: string;
}) {
  const percent = capacityPercent(value, max);
  return (
    <div className={cn("flex min-w-0 items-center gap-2", className)}>
      {percent === null ? (
        <div
          className="h-1.5 flex-1 rounded-full border border-dashed border-line-strong"
          role="img"
          aria-label={`${label}: unavailable`}
        />
      ) : (
        <div
          role="meter"
          aria-label={label}
          aria-valuenow={Math.min(percent, 100)}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuetext={`${value} of ${max}, ${Math.round(percent)}%`}
          className="relative h-1.5 flex-1 overflow-hidden rounded-full bg-surface-raised"
        >
          <div
            className={cn(
              "h-full rounded-full",
              percent > 100 ? "bg-danger" : "bg-capacity",
            )}
            style={{ width: `${Math.min(percent, 100)}%` }}
          />
          {percent >= 90 && percent <= 100 && (
            <div
              className="absolute inset-y-0 right-0 w-px bg-fg/40"
              aria-hidden
            />
          )}
        </div>
      )}
      {showValue && (
        <span className="mono-data w-14 shrink-0 text-right text-micro text-fg-muted">
          {percent === null ? "—" : `${value}/${max}`}
        </span>
      )}
    </div>
  );
}
