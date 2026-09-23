import { useState } from "react";
import { WrapText } from "lucide-react";
import { cn } from "@/lib/cn";
import { CopyButton } from "./identifier";

/**
 * Recorded, not recorded, and pending are different facts and must not look
 * alike. A recorded null is a value; "not recorded" explains what exists.
 */
export type Payload =
  | { kind: "recorded"; value: unknown }
  | { kind: "not-recorded"; reason: string }
  | { kind: "pending" };

export function PayloadViewer({
  label,
  payload,
  className,
}: {
  label: string;
  payload: Payload;
  className?: string;
}) {
  const [wrap, setWrap] = useState(true);
  const text =
    payload.kind === "recorded" ? JSON.stringify(payload.value, null, 2) : null;
  return (
    <section
      aria-label={label}
      className={cn(
        "overflow-hidden rounded-panel border border-line bg-surface",
        className,
      )}
    >
      <div className="flex h-7 items-center justify-between border-b border-line px-2.5">
        <span className="text-micro text-fg-muted">{label}</span>
        {text !== null && (
          <div className="flex items-center gap-0.5">
            <button
              type="button"
              aria-label="Toggle wrapping"
              aria-pressed={wrap}
              onClick={() => setWrap(!wrap)}
              className={cn(
                "inline-flex size-5 items-center justify-center rounded-sm text-fg-subtle hover:bg-surface-raised hover:text-fg",
                wrap && "text-fg",
              )}
            >
              <WrapText className="size-3" aria-hidden />
            </button>
            <CopyButton value={text} label={`Copy ${label.toLowerCase()}`} />
          </div>
        )}
      </div>
      {payload.kind === "recorded" && (
        <pre
          tabIndex={0}
          className={cn(
            "mono-data max-h-64 overflow-auto p-3 text-micro leading-[1.7] text-fg",
            wrap ? "whitespace-pre-wrap break-words" : "whitespace-pre",
          )}
        >
          {text}
        </pre>
      )}
      {payload.kind === "not-recorded" && (
        <div className="hatched p-3">
          <p className="text-label text-fg">Not recorded</p>
          <p className="mt-0.5 text-micro text-fg-muted">{payload.reason}</p>
        </div>
      )}
      {payload.kind === "pending" && (
        <p className="p-3 text-micro text-fg-muted">
          Nothing to show yet. The promise is still open.
        </p>
      )}
    </section>
  );
}
