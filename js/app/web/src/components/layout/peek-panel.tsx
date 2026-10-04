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
  className,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  const panel = useRef<HTMLElement>(null);
  const opener = useRef<Element | null>(null);

  useEffect(() => {
    if (!open) return;
    opener.current = document.activeElement;
    panel.current?.focus({ preventScroll: true });
    function onKey(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
      }
    }
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("keydown", onKey);
      if (opener.current instanceof HTMLElement)
        opener.current.focus({ preventScroll: true });
    };
  }, [open, onClose]);

  if (!open) return null;
  return (
    <aside
      ref={panel}
      tabIndex={-1}
      role="complementary"
      aria-label="Selected item"
      className={cn(
        "fixed bottom-0 right-0 top-bar z-20 flex w-peek max-w-[calc(100vw-var(--spacing-rail))] flex-col border-l border-line bg-surface shadow-[-12px_0_24px_-16px_rgba(0,0,0,0.6)] outline-none animate-in slide-in-from-right-4 fade-in-0 duration-panel",
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
      <div className="min-h-0 flex-1 overflow-y-auto">{children}</div>
    </aside>
  );
}
