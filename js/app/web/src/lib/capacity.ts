export function capacityPercent(
  used: number | null,
  capacity: number | null,
): number | null {
  if (
    used === null ||
    capacity === null ||
    !Number.isFinite(used) ||
    !Number.isFinite(capacity) ||
    used < 0 ||
    capacity <= 0
  )
    return null;
  const percent = (used / capacity) * 100;
  return Number.isFinite(percent) ? percent : null;
}
