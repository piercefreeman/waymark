import type { ReactNode } from "react";
import { cn } from "@/lib/cn";

export interface KeyValueItem {
  label: string;
  value: ReactNode;
  /** Render the value in the data font. */
  mono?: boolean;
  /** Secondary explanation under the value. */
  note?: ReactNode;
}

/** Definition list on the 4px grid, one or two columns, values right-aligned in row mode. */
export function KeyValueList({
  items,
  layout = "rows",
  className,
}: {
  items: KeyValueItem[];
  layout?: "rows" | "grid";
  className?: string;
}) {
  if (layout === "grid") {
    return (
      <dl
        className={cn(
          "grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3",
          className,
        )}
      >
        {items.map((item) => (
          <div key={item.label} className="min-w-0">
            <dt className="text-micro text-fg-subtle">{item.label}</dt>
            <dd
              className={cn(
                "mt-0.5 truncate text-label text-fg",
                item.mono && "mono-data",
              )}
            >
              {item.value}
            </dd>
            {item.note && (
              <dd className="mt-0.5 text-micro text-fg-muted">{item.note}</dd>
            )}
          </div>
        ))}
      </dl>
    );
  }
  return (
    <dl className={cn("divide-y divide-line", className)}>
      {items.map((item) => (
        <div
          key={item.label}
          className="flex items-baseline justify-between gap-4 py-1.5"
        >
          <dt className="shrink-0 text-label text-fg-muted">{item.label}</dt>
          <dd className="min-w-0 text-right">
            <span
              className={cn(
                "block truncate text-label text-fg",
                item.mono && "mono-data",
              )}
            >
              {item.value}
            </span>
            {item.note && (
              <span className="block text-micro text-fg-subtle">
                {item.note}
              </span>
            )}
          </dd>
        </div>
      ))}
    </dl>
  );
}
