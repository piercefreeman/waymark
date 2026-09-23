import type { ReactNode } from "react";
import { CircleAlert, Filter, Inbox, Unplug } from "lucide-react";
import { cn } from "@/lib/cn";

/**
 * Four distinct situations that must never look alike: nothing exists,
 * filters hid everything, the source is unavailable, or a request failed.
 */
export function EmptyState({
  variant,
  title,
  description,
  action,
  className,
}: {
  variant: "empty" | "filtered" | "unavailable" | "error";
  title: string;
  description?: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  const Icon = {
    empty: Inbox,
    filtered: Filter,
    unavailable: Unplug,
    error: CircleAlert,
  }[variant];
  return (
    <div
      role={variant === "error" ? "alert" : undefined}
      className={cn(
        "flex flex-col items-center justify-center gap-2 px-6 py-12 text-center",
        variant === "unavailable" && "hatched",
        className,
      )}
    >
      <Icon
        className={cn(
          "size-4",
          variant === "error" ? "text-danger" : "text-fg-subtle",
        )}
        aria-hidden
      />
      <p className="text-label font-medium text-fg">{title}</p>
      {description && (
        <p className="max-w-sm text-micro text-fg-muted">{description}</p>
      )}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}
