export function parseTimeRange(from: string | null, to: string | null) {
  if (!from || !to) return null;
  const start = new Date(from);
  const end = new Date(to);
  if (
    !Number.isFinite(start.getTime()) ||
    !Number.isFinite(end.getTime()) ||
    start >= end
  )
    return null;
  return { from: start, to: end };
}
