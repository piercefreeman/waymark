import { useEffect, useId, useRef, useState } from "react";
import { Check, ChevronDown, Search } from "lucide-react";
import { Popover } from "radix-ui";
import { timeWindows, useTimeWindow } from "@/components/layout/app-shell";
import { Input } from "@/components/ui/input";
import { isExactId } from "@/data/instances";
import {
  instanceStateOrder,
  instanceStates,
  type InstanceState,
} from "@/domain/status";
import { cn } from "@/lib/cn";
import { parseTimeRange } from "@/lib/time-range";

export function WorkflowSearch({
  query,
  range,
  customRange,
  states,
  onQueryChange,
  onRangeChange,
  onStatesChange,
}: {
  query: string;
  range: { from: Date; to: Date };
  customRange: boolean;
  states: InstanceState[];
  onQueryChange: (query: string) => void;
  onRangeChange: (range: { from: Date; to: Date }) => void;
  onStatesChange: (states: InstanceState[]) => void;
}) {
  const [timeWindow, setTimeWindow] = useTimeWindow();
  const [draft, setDraft] = useState(query);
  const [open, setOpen] = useState(false);
  const [rangeDraft, setRangeDraft] = useState<{
    from: string;
    to: string;
  } | null>(null);
  const [rangeError, setRangeError] = useState<string | null>(null);
  const timeLabel = customRange ? "Custom" : timeWindow.label;
  const anchor = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const content = useRef<HTMLDivElement>(null);
  const contentId = useId();

  useEffect(() => {
    function onKey(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      const typing =
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement ||
        target?.isContentEditable;
      if (
        (event.key === "k" && (event.metaKey || event.ctrlKey)) ||
        (event.key === "/" && !typing)
      ) {
        event.preventDefault();
        input.current?.focus();
        input.current?.select();
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => setDraft(query), [query]);
  useEffect(() => {
    if (draft.trim() === query) return;
    const timer = window.setTimeout(
      () => onQueryChange(draft.trim()),
      isExactId(draft) ? 0 : 350,
    );
    return () => window.clearTimeout(timer);
  }, [draft, query, onQueryChange]);

  const choiceClass =
    "flex h-8 w-full items-center justify-between gap-2 rounded-control px-2 text-left text-label text-fg-muted hover:bg-surface-raised hover:text-fg focus-visible:bg-surface-raised";

  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Anchor asChild>
        <div ref={anchor} className="relative w-80 max-w-[calc(100vw-88px)]">
          <Search
            className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-fg-subtle"
            aria-hidden
          />
          <Input
            ref={input}
            type="search"
            aria-label="Search workflows"
            aria-haspopup="dialog"
            aria-expanded={open}
            aria-controls={open ? contentId : undefined}
            placeholder="Search or filter workflows…"
            value={draft}
            onChange={(event) => {
              const next = event.target.value;
              setDraft(next);
              // Cancel an in-flight ID lookup as soon as the user edits it.
              if (isExactId(query)) onQueryChange(next.trim());
            }}
            onFocus={() => setOpen(true)}
            onClick={() => setOpen(true)}
            onKeyDown={(event) => {
              if (event.key === "ArrowDown") {
                event.preventDefault();
                setOpen(true);
                content.current?.querySelector("button")?.focus();
              } else if (event.key === "Escape") {
                event.preventDefault();
                setOpen(false);
              } else if (event.key === "Enter") {
                onQueryChange(draft.trim());
                setOpen(false);
              }
            }}
            className="pl-8 pr-20"
          />
          <Popover.Trigger asChild>
            <button
              type="button"
              aria-label={
                customRange
                  ? "Filter workflows, custom time range"
                  : `Filter workflows, last ${timeWindow.label}`
              }
              aria-controls={contentId}
              className="absolute inset-y-1 right-1 flex items-center gap-1 rounded-sm border-l border-line px-1.5 text-micro text-fg-muted hover:bg-surface-raised hover:text-fg"
            >
              <span className="mono-data">{timeLabel}</span>
              <ChevronDown className="size-3" aria-hidden />
            </button>
          </Popover.Trigger>
        </div>
      </Popover.Anchor>
      <Popover.Portal>
        <Popover.Content
          ref={content}
          id={contentId}
          aria-label="Workflow filters"
          align="end"
          sideOffset={4}
          collisionPadding={12}
          onOpenAutoFocus={(event) => event.preventDefault()}
          onCloseAutoFocus={(event) => event.preventDefault()}
          onEscapeKeyDown={() => input.current?.focus()}
          onInteractOutside={(event) => {
            const target = event.detail.originalEvent.target;
            if (target instanceof Node && anchor.current?.contains(target))
              event.preventDefault();
          }}
          className="z-50 max-h-[var(--radix-popover-content-available-height)] w-80 max-w-[calc(100vw-24px)] overflow-y-auto rounded-panel border border-line-strong bg-surface-overlay p-2 shadow-lg"
        >
          <div role="group" aria-label="Time filter">
            <p className="px-2 pb-1 text-micro text-fg-subtle">Time</p>
            <div className="grid grid-cols-2 gap-0.5">
              {timeWindows.map((option) => (
                <button
                  key={option.id}
                  type="button"
                  aria-pressed={!customRange && option.id === timeWindow.id}
                  onClick={() => {
                    setTimeWindow(option.id);
                    setRangeDraft(null);
                    setRangeError(null);
                  }}
                  className={cn(
                    choiceClass,
                    !customRange && option.id === timeWindow.id && "text-fg",
                  )}
                >
                  Last {option.label}
                  {!customRange && option.id === timeWindow.id && (
                    <Check className="size-3" aria-hidden />
                  )}
                </button>
              ))}
            </div>
            <button
              type="button"
              aria-expanded={rangeDraft !== null}
              onClick={() => {
                setRangeDraft({
                  from: range.from.toISOString().slice(0, 19),
                  to: range.to.toISOString().slice(0, 19),
                });
                setRangeError(null);
              }}
              className={cn(choiceClass, customRange && "text-fg")}
            >
              Custom range…
              {customRange && <Check className="size-3" aria-hidden />}
            </button>
            {rangeDraft && (
              <form
                className="grid gap-2 px-2 pb-2 pt-1"
                onSubmit={(event) => {
                  event.preventDefault();
                  const next = parseTimeRange(
                    `${rangeDraft.from}Z`,
                    `${rangeDraft.to}Z`,
                  );
                  if (!next) {
                    setRangeError("End must be after start.");
                    return;
                  }
                  onRangeChange(next);
                  setRangeDraft(null);
                  input.current?.focus();
                  setOpen(false);
                }}
              >
                <label className="grid gap-1 text-micro text-fg-muted">
                  From (UTC)
                  <Input
                    type="datetime-local"
                    step="1"
                    required
                    value={rangeDraft.from}
                    onChange={(event) => {
                      setRangeDraft({
                        ...rangeDraft,
                        from: event.target.value,
                      });
                      setRangeError(null);
                    }}
                  />
                </label>
                <label className="grid gap-1 text-micro text-fg-muted">
                  To (UTC)
                  <Input
                    type="datetime-local"
                    step="1"
                    required
                    min={rangeDraft.from}
                    value={rangeDraft.to}
                    onChange={(event) => {
                      setRangeDraft({ ...rangeDraft, to: event.target.value });
                      setRangeError(null);
                    }}
                  />
                </label>
                {rangeError && (
                  <p role="alert" className="text-micro text-danger">
                    {rangeError}
                  </p>
                )}
                <button
                  type="submit"
                  className="h-control rounded-control border border-line-strong px-2 text-label text-fg hover:bg-surface-raised"
                >
                  Apply range
                </button>
              </form>
            )}
          </div>
          <div
            role="group"
            aria-label="State filter"
            className="mt-2 border-t border-line pt-2"
          >
            <div className="flex items-center justify-between px-2 pb-1 text-micro">
              <span className="text-fg-subtle">State</span>
              {states.length > 0 && (
                <button
                  type="button"
                  onClick={() => onStatesChange([])}
                  className="text-accent hover:underline"
                >
                  Clear states
                </button>
              )}
            </div>
            <div className="grid grid-cols-2 gap-0.5">
              {instanceStateOrder.map((state) => (
                <button
                  key={state}
                  type="button"
                  aria-pressed={states.includes(state)}
                  onClick={() =>
                    onStatesChange(
                      states.includes(state)
                        ? states.filter((selected) => selected !== state)
                        : [...states, state],
                    )
                  }
                  className={cn(
                    choiceClass,
                    states.includes(state) && "text-fg",
                  )}
                >
                  {instanceStates[state].label}
                  {states.includes(state) && (
                    <Check className="size-3 shrink-0" aria-hidden />
                  )}
                </button>
              ))}
            </div>
          </div>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
