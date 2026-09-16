import { cn } from "cn";
import { StatusBadge, type Status } from "./status-badge";

export interface TimelineItem {
  id: string;
  name: string;
  status: Status;
  start: number;
  duration: number | null;
}

export function Timeline({
  items,
  total,
  selected,
  onSelect,
}: {
  items: TimelineItem[];
  total: number;
  selected?: string;
  onSelect: (id: string) => void;
}) {
  const scale = Math.max(1, total);
  return (
    <div>
      <div
        className="mb-2 flex justify-between pl-32 font-mono text-[10px] text-muted-foreground"
        aria-hidden="true"
      >
        <span>0 ms</span>
        <span>{Math.round(scale / 2)} ms</span>
        <span>{scale} ms</span>
      </div>
      <ol className="space-y-1">
        {items.map((item) => (
          <li key={item.id}>
            <button
              type="button"
              aria-pressed={selected === item.id}
              onClick={() => onSelect(item.id)}
              className={cn(
                "grid w-full grid-cols-[120px_1fr] items-center gap-2 rounded-md px-2 py-2.5 text-left transition-colors hover:bg-accent/50",
                selected === item.id &&
                  "bg-running/10 ring-1 ring-inset ring-running/20",
              )}
            >
              <span className="min-w-0">
                <span
                  className="block truncate font-mono text-[11px]"
                  title={item.name}
                >
                  {item.name}
                </span>
                <span className="block text-[10px] text-muted-foreground">
                  {item.status} ·{" "}
                  {item.duration === null ? "not timed" : `${item.duration} ms`}
                </span>
              </span>
              <span className="timeline-track relative block h-6">
                {item.duration !== null ? (
                  <span
                    aria-hidden="true"
                    className={`status-${item.status} absolute top-1 h-4 min-w-1 rounded-sm bg-current/25 border border-current/50`}
                    style={{
                      left: `${Math.min(100, Math.max(0, (item.start / scale) * 100))}%`,
                      width: `${Math.max(0, (Math.min(item.duration, scale - item.start) / scale) * 100)}%`,
                    }}
                  />
                ) : (
                  <span
                    aria-hidden="true"
                    className="absolute inset-x-0 top-1 h-4 rounded-sm border border-dashed opacity-60"
                  />
                )}
                <span className="sr-only">
                  {item.duration === null
                    ? "timing unavailable"
                    : `${item.duration} milliseconds, starts at ${item.start} milliseconds`}
                </span>
              </span>
            </button>
          </li>
        ))}
      </ol>
      <div className="mt-4 flex flex-wrap gap-2">
        {[...new Set(items.map((item) => item.status))].map((status) => (
          <StatusBadge key={status} status={status} />
        ))}
      </div>
    </div>
  );
}
