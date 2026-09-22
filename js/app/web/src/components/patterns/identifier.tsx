import { useState } from "react";
import { Check, Copy } from "lucide-react";
import { cn } from "@/lib/cn";
import { shortId } from "@/lib/format";
import { Tooltip, TooltipContent, TooltipTrigger } from "../ui/tooltip";

/**
 * A UUID or other identifier in monospace. Truncated in the middle by
 * default with the full value on hover and an explicit copy affordance.
 */
export function Identifier({
  value,
  full = false,
  copyable = false,
  className,
}: {
  value: string;
  full?: boolean;
  copyable?: boolean;
  className?: string;
}) {
  const text = full ? value : shortId(value);
  return (
    <span className={cn("inline-flex min-w-0 items-center gap-1", className)}>
      {full ? (
        <span className="mono-data truncate text-fg">{text}</span>
      ) : (
        <Tooltip>
          <TooltipTrigger asChild>
            <span className="mono-data truncate text-fg" tabIndex={0}>
              {text}
            </span>
          </TooltipTrigger>
          <TooltipContent className="mono-data">{value}</TooltipContent>
        </Tooltip>
      )}
      {copyable && <CopyButton value={value} />}
    </span>
  );
}

export function CopyButton({
  value,
  label = "Copy",
  className,
}: {
  value: string;
  label?: string;
  className?: string;
}) {
  const [copied, setCopied] = useState(false);
  async function copy() {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      // Clipboard access can be denied; the text remains selectable.
    }
  }
  return (
    <button
      type="button"
      onClick={copy}
      aria-label={copied ? "Copied" : label}
      className={cn(
        "inline-flex size-5 shrink-0 items-center justify-center rounded-sm text-fg-subtle transition-colors duration-fast hover:bg-surface-raised hover:text-fg",
        className,
      )}
    >
      {copied ? (
        <Check className="size-3 text-success" aria-hidden />
      ) : (
        <Copy className="size-3" aria-hidden />
      )}
    </button>
  );
}
