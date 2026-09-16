import { useState } from "react";
import { Check, Copy, WrapText } from "lucide-react";
import { Button } from "./ui/button";

export function PayloadViewer({
  value,
  label = "Payload",
  unavailable = "This value was not recorded.",
  unavailableTitle = "Not recorded",
}: {
  value?: unknown;
  label?: string;
  unavailable?: string;
  unavailableTitle?: string;
}) {
  const [wrap, setWrap] = useState(true);
  const [feedback, setFeedback] = useState<{ text: string; message: string }>();
  const text = value === undefined ? undefined : JSON.stringify(value, null, 2);
  const message = feedback?.text === text ? feedback?.message : "";
  async function copy() {
    if (text === undefined) return;
    try {
      await navigator.clipboard.writeText(text);
      setFeedback({ text, message: "Copied" });
    } catch {
      setFeedback({
        text,
        message: "Copy unavailable. Select the text to copy it.",
      });
    }
  }
  return (
    <section
      className="overflow-hidden rounded-lg border bg-background"
      aria-label={label}
    >
      <div className="flex h-10 items-center justify-between gap-2 border-b px-3">
        <span className="text-xs text-muted-foreground">{label}</span>
        {text !== undefined && (
          <div className="flex gap-1">
            <Button
              variant="ghost"
              size="icon-xs"
              aria-label={`Wrap ${label.toLowerCase()}`}
              aria-pressed={wrap}
              onClick={() => setWrap(!wrap)}
            >
              <WrapText />
            </Button>
            <Button
              variant="ghost"
              size="icon-xs"
              aria-label={`Copy ${label.toLowerCase()}`}
              onClick={copy}
            >
              {message === "Copied" ? <Check /> : <Copy />}
            </Button>
          </div>
        )}
      </div>
      {text === undefined ? (
        <p className="p-4 text-xs leading-5 text-muted-foreground">
          <span className="mb-1 block text-foreground">{unavailableTitle}</span>
          {unavailable}
        </p>
      ) : (
        <pre
          tabIndex={0}
          className={`max-h-72 overflow-auto p-4 font-mono text-[11px] leading-[1.8] ${wrap ? "whitespace-pre-wrap break-words" : "whitespace-pre"}`}
        >
          {text}
        </pre>
      )}
      <p
        role="status"
        className={
          message
            ? "border-t px-3 py-1 text-[11px] text-muted-foreground"
            : "sr-only"
        }
      >
        {message}
      </p>
    </section>
  );
}
