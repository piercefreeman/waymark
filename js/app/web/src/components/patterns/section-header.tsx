import type { ReactNode } from "react";
import { cn } from "@/lib/cn";

/** One heading style for every section: sentence case, quiet count, actions right. */
export function SectionHeader({
  title,
  count,
  description,
  actions,
  as: Heading = "h2",
  className,
}: {
  title: string;
  count?: number | string;
  description?: ReactNode;
  actions?: ReactNode;
  as?: "h1" | "h2" | "h3";
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex min-h-8 flex-wrap items-baseline justify-between gap-x-4 gap-y-1",
        className,
      )}
    >
      <div className="flex min-w-0 items-baseline gap-2">
        <Heading className="text-section font-semibold text-fg">
          {title}
        </Heading>
        {count !== undefined && (
          <span className="mono-data text-label text-fg-subtle">{count}</span>
        )}
        {description && (
          <span className="text-label text-fg-muted">{description}</span>
        )}
      </div>
      {actions && <div className="flex items-center gap-2">{actions}</div>}
    </div>
  );
}
