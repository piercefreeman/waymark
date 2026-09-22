import { cn } from "@/lib/cn";

export function Kbd({
  children,
  className,
}: {
  children: string;
  className?: string;
}) {
  return (
    <kbd
      className={cn(
        "inline-flex h-4 min-w-4 items-center justify-center rounded-sm border border-line-strong bg-surface-raised px-1 text-[10px] leading-none text-fg-muted",
        className,
      )}
    >
      {children}
    </kbd>
  );
}
