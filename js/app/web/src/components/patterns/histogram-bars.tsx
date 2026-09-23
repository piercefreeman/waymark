import type { Histogram } from "@/domain/api";
import { histogramBuckets, histogramPercentile } from "@/domain/metrics";
import { formatSeconds } from "@/lib/format";
import { toneVariable, type Tone } from "@/domain/status";
import { cn } from "@/lib/cn";

/**
 * Bucket counts as bars, log-spaced as the bounds are. Keeps bimodal
 * handling times visible where a single median would hide them.
 */
export function HistogramBars({
  histogram,
  tone = "running",
  height = 40,
  label,
  markers = [0.5, 0.95],
  caption = true,
  className,
}: {
  histogram: Histogram;
  tone?: Tone;
  height?: number;
  label: string;
  markers?: number[];
  caption?: boolean;
  className?: string;
}) {
  const buckets = histogramBuckets(histogram);
  const peak = Math.max(1, ...buckets);
  const width = 100;
  const barWidth = width / buckets.length;
  const percentiles = markers.map((fraction) => ({
    fraction,
    seconds: histogramPercentile(histogram, fraction),
  }));
  const position = (seconds: number | null) => {
    if (seconds === null) return null;
    const index = histogram.bounds.findIndex((bound) => seconds <= bound);
    if (index < 0) return null;
    const lower =
      index === 0 ? histogram.bounds[0] / 10 : histogram.bounds[index - 1];
    const upper = histogram.bounds[index];
    const within =
      (Math.log(seconds) - Math.log(lower)) /
      (Math.log(upper) - Math.log(lower));
    return (index + Math.max(0, Math.min(1, within))) * barWidth;
  };
  return (
    <figure className={cn("min-w-0", className)}>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={label}
        className="block w-full"
        style={{ height }}
      >
        {buckets.map((count, index) => (
          <rect
            key={index}
            x={index * barWidth + 0.3}
            width={barWidth - 0.6}
            y={height - (count / peak) * (height - 4)}
            height={(count / peak) * (height - 4)}
            fill={toneVariable[tone]}
            opacity={0.55}
          />
        ))}
        {percentiles.map(({ fraction, seconds }) => {
          const x = position(seconds);
          if (x === null) return null;
          return (
            <line
              key={fraction}
              x1={x}
              x2={x}
              y1={0}
              y2={height}
              stroke="var(--fg)"
              strokeWidth={0.4}
              strokeDasharray={fraction === 0.5 ? undefined : "1 1"}
              vectorEffect="non-scaling-stroke"
            />
          );
        })}
      </svg>
      <figcaption
        className={cn(
          "mt-1 flex justify-between text-[10px] text-fg-subtle",
          !caption && "sr-only",
        )}
      >
        <span className="mono-data">{formatSeconds(histogram.bounds[0])}</span>
        <span className="mono-data">
          {percentiles
            .map(
              ({ fraction, seconds }) =>
                `p${Math.round(fraction * 100)} ${seconds === null ? `>${formatSeconds(histogram.bounds[histogram.bounds.length - 1])}` : formatSeconds(seconds)}`,
            )
            .join(" · ")}
        </span>
        <span className="mono-data">
          {formatSeconds(histogram.bounds[histogram.bounds.length - 1])}
        </span>
      </figcaption>
    </figure>
  );
}
