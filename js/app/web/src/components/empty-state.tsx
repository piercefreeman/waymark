import type { ReactNode } from "react";
import { CircleDashed } from "lucide-react";

export function EmptyState({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center px-6 py-10 text-center">
      <CircleDashed
        className="mb-3 size-6 text-muted-foreground"
        aria-hidden="true"
      />
      <p className="text-sm font-medium">{title}</p>
      <p className="mt-1 max-w-72 text-xs leading-5 text-muted-foreground">
        {description}
      </p>
      {children && <div className="mt-4">{children}</div>}
    </div>
  );
}
