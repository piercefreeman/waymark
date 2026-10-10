import { useEffect, useRef, type ReactNode } from "react";
import { X } from "lucide-react";
import { cn } from "@/lib/cn";

/**
 * A quick look at a selected item that overlays the workspace instead of
 * pushing its columns around. Non-modal: the list behind stays keyboard
 * navigable. Escape closes and focus returns to the row that opened it.
 */
export function PeekPanel({
  open,
  onClose,
  title,
  actions,
  children,
  side = "right",
  className,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  side?: "right" | "bottom";
  className?: string;
}) {
  const panel = useRef<HTMLElement>(null);
  const opener = useRef<Element | null>(null);
  const close = useRef(onClose);

  useEffect(() => {
    close.current = onClose;
  }, [onClose]);

  useEffect(() => {
    if (!open) return;
    opener.current = document.activeElement;
    panel.current?.focus({ preventScroll: true });
    function onKey(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        close.current();
      }
    }
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("keydown", onKey);
      if (opener.current instanceof HTMLElement)
        opener.current.focus({ preventScroll: true });
    };
  }, [open]);

  if (!open) return null;
  return (
    <aside
      ref={panel}
      tabIndex={-1}
      role="complementary"
      aria-label="Selected item"
      className={cn(
        "fixed bottom-0 right-0 z-20 flex flex-col border-line bg-surface outline-none",
        side === "bottom"
          ? "left-rail h-[min(45svh,24rem)] border-t"
          : "top-bar w-peek max-w-[calc(100vw-var(--spacing-rail))] border-l shadow-[-12px_0_24px_-16px_rgba(0,0,0,0.6)] animate-in slide-in-from-right-4 fade-in-0 duration-panel",
        className,
      )}
    >
      <header className="flex h-10 shrink-0 items-center gap-2 border-b border-line px-gutter">
        <div className="min-w-0 flex-1">{title}</div>
        {actions}
        <button
          type="button"
          onClick={onClose}
          aria-label="Close"
          className="inline-flex size-6 items-center justify-center rounded-control text-fg-muted transition-colors duration-fast hover:bg-surface-raised hover:text-fg"
        >
          <X className="size-3.5" aria-hidden />
        </button>
      </header>
      <div tabIndex={0} className="min-h-0 flex-1 overflow-y-auto">
        {children}
      </div>
    </aside>
  );
}
