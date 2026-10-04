import { useId } from "react";
import { cn } from "@/lib/cn";
import { formatClock } from "@/lib/format";
import { seriesMax, type SeriesPoint } from "@/domain/metrics";
import { toneVariable, type Tone } from "@/domain/status";

export interface ChartSeries {
  id: string;
  label: string;
  tone: Tone;
  points: SeriesPoint[];
  /** Optional band drawn behind the line (e.g. p95 around p50). */
  band?: { upper: SeriesPoint[]; lower: SeriesPoint[] };
  dashed?: boolean;
}

/**
 * Small-multiple line chart. All charts on a page share the same window and
 * x-axis. A null point breaks the line: gaps are drawn as gaps, never as
 * zero, and never interpolated across a missing sample.
 */
export function TimeSeriesChart({
  title,
  unit,
  series,
  window,
  yMax,
  capacity,
  height = 96,
  formatValue = (value: number) => value.toFixed(0),
  scale = "linear",
  className,
}: {
  title: string;
  unit?: string;
  series: ChartSeries[];
  window: { from: Date; to: Date };
  yMax?: number;
  /** Horizontal reference line, e.g. max in-flight slots. */
  capacity?: number | null;
  height?: number;
  formatValue?: (value: number) => string;
  /** Log scale for latencies whose tail would flatten the median on a linear axis. */
  scale?: "linear" | "log";
  className?: string;
}) {
  const clip = useId();
  const width = 600;
  const padTop = 6;
  const padBottom = 4;
  const plotHeight = height - padTop - padBottom;
  const dataMax = Math.max(
    seriesMax(series.map((item) => item.points)),
    ...series.flatMap((item) =>
      item.band ? [seriesMax([item.band.upper])] : [],
    ),
    capacity ?? 0,
  );
  const top = yMax ?? niceCeiling(dataMax);
  const logMin = scale === "log" ? Math.max(1, positiveMin(series)) / 2 : 0;
  const logFloor = scale === "log" ? Math.log10(logMin) : 0;
  const logSpan =
    scale === "log"
      ? Math.max(1e-6, Math.log10(Math.max(top, logMin * 10)) - logFloor)
      : 1;
  const span = window.to.getTime() - window.from.getTime();
  const x = (at: Date) =>
    ((at.getTime() - window.from.getTime()) / span) * width;
  const y = (value: number) =>
    scale === "log"
      ? padTop +
        plotHeight -
        ((Math.log10(Math.max(value, logMin)) - logFloor) / logSpan) *
          plotHeight
      : padTop + plotHeight - (value / Math.max(1, top)) * plotHeight;
  const path = (points: SeriesPoint[]) => {
    let d = "";
    let open = false;
    for (const point of points) {
      if (point.value === null) {
        open = false;
        continue;
      }
      d += `${open ? "L" : "M"}${x(point.at).toFixed(1)},${y(point.value).toFixed(1)} `;
      open = true;
    }
    return d;
  };
  const bandPath = (upper: SeriesPoint[], lower: SeriesPoint[]) => {
    const segments: string[] = [];
    let current: { up: string[]; down: string[] } | null = null;
    for (let index = 0; index < upper.length; index += 1) {
      const u = upper[index];
      const l = lower[index];
      if (u.value === null || l?.value === null || l === undefined) {
        if (current)
          segments.push(
            `M${current.up.join(" L")} L${current.down.reverse().join(" L")} Z`,
          );
        current = null;
        continue;
      }
      current ??= { up: [], down: [] };
      current.up.push(`${x(u.at).toFixed(1)},${y(u.value).toFixed(1)}`);
      current.down.push(`${x(l.at).toFixed(1)},${y(l.value).toFixed(1)}`);
    }
    if (current)
      segments.push(
        `M${current.up.join(" L")} L${current.down.reverse().join(" L")} Z`,
      );
    return segments.join(" ");
  };
  const ticks = timeTicks(window);
  const gridValues =
    scale === "log"
      ? decades(logMin, Math.max(top, logMin * 10))
      : top <= 2
        ? [0, top]
        : [0, top / 2, top];
  const latest = series.map((item) => {
    const point = [...item.points]
      .reverse()
      .find((entry) => entry.value !== null);
    return { item, value: point?.value ?? null };
  });
  return (
    <figure className={cn("min-w-0", className)}>
      <figcaption className="mb-1 flex items-baseline justify-between gap-3">
        <span className="text-label font-medium text-fg">
          {title}
          {unit && (
            <span className="ml-1 text-micro font-normal text-fg-subtle">
              {unit}
            </span>
          )}
        </span>
        <span className="flex flex-wrap items-center justify-end gap-x-3 gap-y-0.5">
          {latest.map(({ item, value }) => (
            <span
              key={item.id}
              className="inline-flex items-center gap-1 text-micro text-fg-muted"
            >
              <span
                aria-hidden
                className="inline-block h-0.5 w-3 rounded-full"
                style={{
                  background: toneVariable[item.tone],
                  opacity: item.dashed ? 0.6 : 1,
                }}
              />
              {item.label}
              <span className="mono-data text-fg">
                {value === null ? "—" : formatValue(value)}
              </span>
            </span>
          ))}
        </span>
      </figcaption>
      <div className="grid grid-cols-[minmax(0,1fr)_2.5rem] gap-1">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          preserveAspectRatio="none"
          role="img"
          aria-label={`${title}${unit ? ` in ${unit}` : ""}`}
          className="block w-full"
          style={{ height }}
        >
          <defs>
            <clipPath id={clip}>
              <rect x="0" y="0" width={width} height={height} />
            </clipPath>
          </defs>
          {gridValues.map((value) => (
            <line
              key={value}
              x1={0}
              x2={width}
              y1={y(value)}
              y2={y(value)}
              stroke="var(--chart-grid)"
              strokeWidth={1}
              vectorEffect="non-scaling-stroke"
            />
          ))}
          {ticks.map((tick) => (
            <line
              key={tick.getTime()}
              x1={x(tick)}
              x2={x(tick)}
              y1={padTop}
              y2={height - padBottom}
              stroke="var(--chart-grid)"
              strokeWidth={1}
              vectorEffect="non-scaling-stroke"
            />
          ))}
          {capacity !== undefined && capacity !== null && (
            <line
              x1={0}
              x2={width}
              y1={y(capacity)}
              y2={y(capacity)}
              stroke="var(--chart-axis)"
              strokeWidth={1}
              strokeDasharray="3 3"
              vectorEffect="non-scaling-stroke"
            />
          )}
          <g clipPath={`url(#${clip})`}>
            {series.map((item) =>
              item.band ? (
                <path
                  key={`${item.id}-band`}
                  d={bandPath(item.band.upper, item.band.lower)}
                  fill={toneVariable[item.tone]}
                  opacity={0.14}
                />
              ) : null,
            )}
            {series.map((item) => (
              <path
                key={item.id}
                d={path(item.points)}
                fill="none"
                stroke={toneVariable[item.tone]}
                strokeWidth={1.5}
                strokeDasharray={item.dashed ? "4 3" : undefined}
                strokeLinejoin="round"
                strokeLinecap="round"
                vectorEffect="non-scaling-stroke"
              />
            ))}
          </g>
        </svg>
        <div
          aria-hidden
          className="relative text-[10px] text-fg-subtle"
          style={{ height }}
        >
          {gridValues.map((value) => (
            <span
              key={value}
              className="mono-data absolute right-0 -translate-y-1/2 leading-none"
              style={{ top: y(value) }}
            >
              {formatValue(value)}
            </span>
          ))}
        </div>
      </div>
      <div
        aria-hidden
        className="relative mt-1 h-3.5 text-[10px] text-fg-subtle"
      >
        {ticks.map((tick, index) => (
          <span
            key={tick.getTime()}
            className={cn(
              "mono-data absolute top-0 leading-none",
              index === ticks.length - 1
                ? "-translate-x-full"
                : "-translate-x-1/2",
            )}
            style={{ left: `${(x(tick) / width) * 100}%` }}
          >
            {formatClock(tick, false)}
          </span>
        ))}
      </div>
    </figure>
  );
}

function positiveMin(series: ChartSeries[]): number {
  let min = Number.POSITIVE_INFINITY;
  for (const item of series)
    for (const point of [...item.points, ...(item.band?.lower ?? [])])
      if (point.value !== null && point.value > 0 && point.value < min)
        min = point.value;
  return Number.isFinite(min) ? min : 1;
}

function decades(min: number, max: number): number[] {
  const values: number[] = [];
  for (let power = Math.ceil(Math.log10(min)); 10 ** power <= max; power += 1)
    values.push(10 ** power);
  return values.length ? values : [min, max];
}

function niceCeiling(value: number): number {
  if (value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const normalized = value / magnitude;
  const nice =
    normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
  return nice * magnitude;
}

export function timeTicks(window: { from: Date; to: Date }, count = 4): Date[] {
  const span = window.to.getTime() - window.from.getTime();
  const candidates = [
    60_000,
    5 * 60_000,
    15 * 60_000,
    60 * 60_000,
    6 * 60 * 60_000,
  ];
  const step =
    candidates.find((candidate) => span / candidate <= count + 1) ??
    candidates[candidates.length - 1];
  const ticks: Date[] = [];
  const first = Math.ceil(window.from.getTime() / step) * step;
  for (let at = first; at <= window.to.getTime(); at += step)
    ticks.push(new Date(at));
  return ticks;
}
