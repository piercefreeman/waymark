import type { ReactNode } from "react";
import { cn } from "@/lib/cn";
import { toneText, type Tone } from "@/domain/status";

/** A flat strip of tiles separated by rules. No cards. */
export function MetricStrip({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <dl
      className={cn(
        "grid grid-cols-2 divide-x divide-line border-y border-line bg-surface sm:grid-cols-3 lg:grid-cols-6",
        className,
      )}
    >
      {children}
    </dl>
  );
}

/**
 * Label, value with unit, and a scope caption. Every number says what it
 * covers ("3 nodes · 1 stale excluded"). Numbers never exceed 20px.
 */
export function MetricTile({
  label,
  value,
  unit,
  scope,
  tone,
  href,
  className,
}: {
  label: string;
  value: ReactNode;
  unit?: string;
  scope: ReactNode;
  tone?: Tone;
  href?: string;
  className?: string;
}) {
  const body = (
    <>
      <dt className="text-micro text-fg-muted">{label}</dt>
      <dd className="mt-0.5 flex items-baseline gap-1">
        <span
          className={cn(
            "mono-data text-metric font-medium text-fg",
            tone && toneText[tone],
          )}
        >
          {value}
        </span>
        {unit && (
          <span className="mono-data text-micro text-fg-subtle">{unit}</span>
        )}
      </dd>
      <dd className="mt-0.5 truncate text-micro text-fg-subtle">{scope}</dd>
    </>
  );
  const classes = cn("min-w-0 px-gutter py-2.5", className);
  if (href) {
    return (
      <div
        className={cn(
          classes,
          "transition-colors duration-fast hover:bg-surface-raised",
        )}
      >
        <a href={href} className="block outline-offset-4">
          {body}
        </a>
      </div>
    );
  }
  return <div className={classes}>{body}</div>;
}
