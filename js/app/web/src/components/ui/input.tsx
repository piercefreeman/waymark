import * as React from "react";
import { cn } from "@/lib/cn";

function Input({ className, ...props }: React.ComponentProps<"input">) {
  return (
    <input
      data-slot="input"
      className={cn(
        "h-control w-full min-w-0 rounded-control border border-line-strong bg-surface px-2.5 text-label text-fg transition-colors duration-fast placeholder:text-fg-subtle focus-visible:border-focus disabled:cursor-not-allowed disabled:opacity-50",
        className,
      )}
      {...props}
    />
  );
}

export { Input };
