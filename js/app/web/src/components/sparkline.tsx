import { useId } from "react";

export function Sparkline({
  values,
  label,
  tone = "running",
  className,
}: {
  values: number[];
  label: string;
  tone?: "running" | "success" | "waiting";
  className?: string;
}) {
  const gradient = useId();
  const peak = Math.max(1, ...values);
  const points = values
    .map(
      (value, index) =>
        `${(index / Math.max(1, values.length - 1)) * 120},${38 - (value / peak) * 32}`,
    )
    .join(" ");
  return (
    <svg
      role="img"
      aria-label={label}
      viewBox="0 0 120 42"
      preserveAspectRatio="none"
      className={className ?? "h-9 w-24 shrink-0"}
      style={{ color: `var(--${tone})` }}
    >
      <defs>
        <linearGradient id={gradient} x1="0" y1="0" x2="0" y2="1">
          <stop stopColor="currentColor" stopOpacity="0.18" />
          <stop offset="1" stopColor="currentColor" stopOpacity="0" />
        </linearGradient>
      </defs>
      <polygon points={`0,42 ${points} 120,42`} fill={`url(#${gradient})`} />
      <polyline
        points={points}
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        vectorEffect="non-scaling-stroke"
        strokeLinejoin="round"
      />
    </svg>
  );
}
