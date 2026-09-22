import { cn } from "@/lib/cn";
import {
  formatClock,
  formatDuration,
  formatRelative,
  formatTimestamp,
} from "@/lib/format";
import { Tooltip, TooltipContent, TooltipTrigger } from "../ui/tooltip";

/** Relative time with the absolute UTC timestamp on hover. */
export function TimeAgo({
  at,
  now,
  className,
}: {
  at: Date;
  now: Date;
  className?: string;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <time
          dateTime={at.toISOString()}
          className={cn("mono-data whitespace-nowrap", className)}
          tabIndex={0}
        >
          {formatRelative(at, now)}
        </time>
      </TooltipTrigger>
      <TooltipContent className="mono-data">
        {formatTimestamp(at)}
      </TooltipContent>
    </Tooltip>
  );
}

/** Wall clock in UTC with the full timestamp on hover. */
export function Clock({
  at,
  withSeconds = true,
  className,
}: {
  at: Date;
  withSeconds?: boolean;
  className?: string;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <time
          dateTime={at.toISOString()}
          className={cn("mono-data whitespace-nowrap", className)}
          tabIndex={0}
        >
          {formatClock(at, withSeconds)}
        </time>
      </TooltipTrigger>
      <TooltipContent className="mono-data">
        {formatTimestamp(at)}
      </TooltipContent>
    </Tooltip>
  );
}

/**
 * A duration that says what it measures. Call-to-settlement includes queue
 * time; nothing here is labeled "execution time" because workers don't
 * report it.
 */
export function Duration({
  ms,
  kind = "elapsed",
  className,
}: {
  ms: number | null;
  kind?: "elapsed" | "call-to-settlement" | "open";
  className?: string;
}) {
  const label =
    kind === "call-to-settlement"
      ? "Call → settlement, including queueing"
      : kind === "open"
        ? "Open since the call; still unsettled"
        : "Elapsed";
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          className={cn("mono-data whitespace-nowrap", className)}
          tabIndex={0}
        >
          {formatDuration(ms)}
        </span>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  );
}
