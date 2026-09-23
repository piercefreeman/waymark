import type { Histogram, NodeSample } from "./api.ts";

/**
 * Derivations over node samples. Rates come from counter differences per
 * node boot; percentiles come from bucket counts and are aggregated by
 * summing buckets, never by averaging medians.
 */

export const STALE_FACTOR = 3;

export function isStale(
  sampledAt: Date,
  now: Date,
  sampleIntervalMs: number,
  factor = STALE_FACTOR,
): boolean {
  return now.getTime() - sampledAt.getTime() > sampleIntervalMs * factor;
}

/** Completions per second between the last two samples of one node boot. */
export function completionRate(samples: NodeSample[]): number | null {
  if (samples.length < 2) return null;
  const last = samples[samples.length - 1];
  const previous = samples[samples.length - 2];
  const seconds =
    (new Date(last.sampled_at).getTime() -
      new Date(previous.sampled_at).getTime()) /
    1000;
  if (seconds <= 0) return null;
  const delta = last.actions_completed_total - previous.actions_completed_total;
  return delta < 0 ? null : delta / seconds;
}

/** Per-sample completion rate series for charting. */
export function completionRateSeries(
  samples: NodeSample[],
): { at: Date; value: number | null }[] {
  return samples.map((sample, index) => {
    if (index === 0) return { at: new Date(sample.sampled_at), value: null };
    const previous = samples[index - 1];
    const seconds =
      (new Date(sample.sampled_at).getTime() -
        new Date(previous.sampled_at).getTime()) /
      1000;
    const delta =
      sample.actions_completed_total - previous.actions_completed_total;
    return {
      at: new Date(sample.sampled_at),
      value: seconds > 0 && delta >= 0 ? delta / seconds : null,
    };
  });
}

/** Interpolated percentile in seconds; null when it lies above the last bound. */
export function histogramPercentile(
  histogram: Histogram,
  fraction: number,
): number | null {
  const total = histogram.counts[histogram.counts.length - 1] ?? 0;
  if (total === 0) return null;
  const target = total * fraction;
  let lowerBound = 0;
  let lowerCount = 0;
  for (let index = 0; index < histogram.bounds.length; index += 1) {
    const upperCount = histogram.counts[index];
    const upperBound = histogram.bounds[index];
    if (upperCount >= target) {
      const span = upperCount - lowerCount;
      if (span <= 0) return upperBound;
      return (
        lowerBound + ((target - lowerCount) / span) * (upperBound - lowerBound)
      );
    }
    lowerBound = upperBound;
    lowerCount = upperCount;
  }
  return null;
}

export function histogramTotal(histogram: Histogram): number {
  return histogram.counts[histogram.counts.length - 1] ?? 0;
}

/** Sum bucket counts across nodes that share bounds. */
export function aggregateHistograms(histograms: Histogram[]): Histogram | null {
  const [first] = histograms;
  if (!first) return null;
  const counts = first.bounds.map((_, index) =>
    histograms.reduce(
      (sum, histogram) => sum + (histogram.counts[index] ?? 0),
      0,
    ),
  );
  const sum = histograms.reduce((total, histogram) => total + histogram.sum, 0);
  const aggregate: Histogram = { bounds: first.bounds, counts, sum, p50: null };
  aggregate.p50 = histogramPercentile(aggregate, 0.5);
  return aggregate;
}

/** Non-cumulative counts per bucket, for drawing bars. */
export function histogramBuckets(histogram: Histogram): number[] {
  return histogram.counts.map((count, index) =>
    index === 0 ? count : count - histogram.counts[index - 1],
  );
}

export interface SeriesPoint {
  at: Date;
  value: number | null;
}

/**
 * Align samples to a fixed bucket grid over a window so gaps appear as gaps.
 * Missing buckets become null; never zero.
 */
export function alignSeries(
  samples: NodeSample[],
  pick: (sample: NodeSample) => number | null,
  window: { from: Date; to: Date },
  bucketMs: number,
): SeriesPoint[] {
  const points: SeriesPoint[] = [];
  const byBucket = new Map<number, number | null>();
  for (const sample of samples) {
    const at = new Date(sample.sampled_at).getTime();
    const bucket = Math.floor((at - window.from.getTime()) / bucketMs);
    byBucket.set(bucket, pick(sample));
  }
  const bucketCount = Math.ceil(
    (window.to.getTime() - window.from.getTime()) / bucketMs,
  );
  for (let index = 0; index < bucketCount; index += 1) {
    points.push({
      at: new Date(window.from.getTime() + index * bucketMs),
      value: byBucket.get(index) ?? null,
    });
  }
  return points;
}

export function seriesMax(series: SeriesPoint[][]): number {
  let max = 0;
  for (const points of series)
    for (const point of points)
      if (point.value !== null && point.value > max) max = point.value;
  return max;
}
