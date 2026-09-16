import type { ReactNode } from "react";

export function Metric({
  label,
  value,
  unit,
  detail,
  children,
}: {
  label: string;
  value: string | number;
  unit?: string;
  detail: string;
  children?: ReactNode;
}) {
  return (
    <div className="min-w-0 px-5 py-5">
      <p className="mb-3 text-xs text-muted-foreground">{label}</p>
      <div className="flex items-end justify-between gap-4">
        <p className="whitespace-nowrap font-mono text-[28px] leading-8 tracking-tight">
          {value}
          <span className="ml-1.5 text-xs tracking-normal text-muted-foreground">
            {unit}
          </span>
        </p>
        {children && (
          <span className="hidden min-w-0 xl:block">{children}</span>
        )}
      </div>
      <p className="mt-2 text-[11px] text-muted-foreground">{detail}</p>
    </div>
  );
}
