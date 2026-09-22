const MILLISECOND = 1;
const SECOND = 1000 * MILLISECOND;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/** Human duration with a unit, sized to the magnitude: "180 ms", "1.42 s", "1m 12s", "2h 05m". */
export function formatDuration(ms: number | null): string {
  if (ms === null || !Number.isFinite(ms)) return "—";
  const abs = Math.max(0, ms);
  if (abs < SECOND) return `${Math.round(abs)} ms`;
  if (abs < MINUTE) return `${(abs / SECOND).toFixed(2)} s`;
  if (abs < HOUR) {
    const minutes = Math.floor(abs / MINUTE);
    const seconds = Math.round((abs % MINUTE) / SECOND);
    return `${minutes}m ${seconds.toString().padStart(2, "0")}s`;
  }
  if (abs < DAY) {
    const hours = Math.floor(abs / HOUR);
    const minutes = Math.round((abs % HOUR) / MINUTE);
    return `${hours}h ${minutes.toString().padStart(2, "0")}m`;
  }
  const days = Math.floor(abs / DAY);
  const hours = Math.round((abs % DAY) / HOUR);
  return `${days}d ${hours}h`;
}

/** Seconds from a histogram or latency value: "84 µs", "12 ms", "1.3 s". */
export function formatSeconds(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return "—";
  if (seconds < 0.001) return `${Math.round(seconds * 1_000_000)} µs`;
  if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 2 : 1)} s`;
  return formatDuration(seconds * 1000);
}

/** Relative time, coarse enough to stay stable while polling: "12s ago", "4m ago". */
export function formatRelative(at: Date, now: Date): string {
  const delta = now.getTime() - at.getTime();
  // Clocks on a page tick less often than fetches land; a few seconds of
  // skew is "just now", not the future.
  if (delta < -10 * SECOND) return `in ${formatDuration(-delta)}`;
  if (delta < 10 * SECOND) return "just now";
  if (delta < MINUTE) return `${Math.floor(delta / SECOND)}s ago`;
  if (delta < HOUR) return `${Math.floor(delta / MINUTE)}m ago`;
  if (delta < DAY) return `${Math.floor(delta / HOUR)}h ago`;
  return `${Math.floor(delta / DAY)}d ago`;
}

/** Wall clock in UTC, "14:32:09". */
export function formatClock(at: Date, withSeconds = true): string {
  const parts = [at.getUTCHours(), at.getUTCMinutes()];
  if (withSeconds) parts.push(at.getUTCSeconds());
  return parts.map((part) => part.toString().padStart(2, "0")).join(":");
}

/** Full timestamp for titles and copy: "2026-09-22 14:32:09.412 UTC". */
export function formatTimestamp(at: Date): string {
  return `${at.toISOString().replace("T", " ").replace("Z", "")} UTC`;
}

/** Middle-truncated UUID: the first and last groups carry the most entropy visually. */
export function shortId(id: string): string {
  if (id.length <= 13) return id;
  return `${id.slice(0, 8)}…${id.slice(-4)}`;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

export function formatRate(perSecond: number | null): string {
  if (perSecond === null || !Number.isFinite(perSecond)) return "—";
  if (perSecond >= 100) return perSecond.toFixed(0);
  return perSecond.toFixed(1);
}

export function formatPercent(fraction: number | null): string {
  if (fraction === null || !Number.isFinite(fraction)) return "—";
  return `${Math.round(fraction * 100)}%`;
}

export function formatInteger(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US").format(value);
}
