import { capacityPercent } from "../lib/capacity";

export function CapacityBar({
  used,
  capacity,
  label = "Action concurrency",
}: {
  used: number | null;
  capacity: number | null;
  label?: string;
}) {
  const percent = capacityPercent(used, capacity);
  return (
    <div className="min-w-28">
      <div className="mb-2 flex justify-between gap-3 text-[11px]">
        <span className="text-muted-foreground">{label}</span>
        <span className="font-mono">
          {percent === null ? "Unavailable" : `${used} / ${capacity}`}
        </span>
      </div>
      {percent === null ? (
        <div className="h-1.5 rounded-full border border-dashed" />
      ) : (
        <div
          role="meter"
          aria-label={label}
          aria-valuenow={Math.min(percent, 100)}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuetext={`${used} of ${capacity}, ${Math.round(percent)}%`}
          className="h-1.5 overflow-hidden rounded-full bg-muted"
        >
          <div
            className="h-full rounded-full bg-running"
            style={{ width: `${Math.min(percent, 100)}%` }}
          />
        </div>
      )}
    </div>
  );
}
