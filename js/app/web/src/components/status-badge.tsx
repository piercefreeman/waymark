import {
  Check,
  Circle,
  CircleDashed,
  CirclePause,
  CircleX,
  TriangleAlert,
  X,
} from "lucide-react";
import { Badge } from "./ui/badge";
import { cn } from "cn";

export const statuses = {
  running: { label: "Running", icon: Circle },
  success: { label: "Success", icon: Check },
  waiting: { label: "Waiting", icon: CirclePause },
  failing: { label: "Failing", icon: TriangleAlert },
  failed: { label: "Failed", icon: CircleX },
  cancelled: { label: "Cancelled", icon: X },
  unknown: { label: "Unknown", icon: CircleDashed },
} as const;

export type Status = keyof typeof statuses;

export function StatusBadge({
  status,
  className,
}: {
  status: Status;
  className?: string;
}) {
  const { label, icon: Icon } = statuses[status];
  return (
    <Badge
      variant="outline"
      className={cn(
        "status-badge gap-1.5 py-0.5 text-[11px] font-normal",
        `status-${status}`,
        className,
      )}
    >
      <Icon aria-hidden="true" />
      {label}
    </Badge>
  );
}
