import { useEffect, useRef, type ReactNode } from "react";
import { ListTree, Moon, Server, Sun } from "lucide-react";
import { cn } from "@/lib/cn";
import { formatClock, formatRelative } from "@/lib/format";
import {
  navigate,
  onLinkClick,
  useLocation,
  useSearchParam,
  withSearch,
} from "@/lib/router";
import { useTheme } from "@/providers/theme";
import { Kbd } from "../patterns/kbd";
import type { SourceStatus } from "../patterns/source-notice";
import { StatusDot } from "../patterns/status-ink";
import { Tooltip, TooltipContent, TooltipTrigger } from "../ui/tooltip";

export const timeWindows = [
  { id: "15m", label: "15m", ms: 15 * 60_000 },
  { id: "1h", label: "1h", ms: 60 * 60_000 },
  { id: "6h", label: "6h", ms: 6 * 60 * 60_000 },
  { id: "24h", label: "24h", ms: 24 * 60 * 60_000 },
] as const;
export type TimeWindowId = (typeof timeWindows)[number]["id"];

export function useTimeWindow() {
  const [raw, setRaw] = useSearchParam("w");
  const current =
    timeWindows.find((window) => window.id === raw) ?? timeWindows[0];
  return [
    current,
    (id: TimeWindowId) => setRaw(id === "15m" ? null : id, { replace: true }),
  ] as const;
}

const railItems = [
  {
    href: "/instances",
    label: "Instances",
    icon: ListTree,
    match: "/instances",
  },
  { href: "/fleet", label: "Fleet", icon: Server, match: "/fleet" },
] as const;

/**
 * One 40px global bar carries identity, environment, page title, the time
 * window, live state, a jump box, and theme. A 48px icon rail navigates.
 * The workspace gets everything else.
 */
export function AppShell({
  title,
  now,
  source,
  pinnedTo = null,
  children,
}: {
  title: ReactNode;
  now: Date;
  source: SourceStatus;
  /** When set, the page is frozen at this instant and does not poll. */
  pinnedTo?: Date | null;
  children: ReactNode;
}) {
  const { pathname, search } = useLocation();
  const [window, setWindow] = useTimeWindow();
  const [pausedParam, setPaused] = useSearchParam("paused");
  const live = pausedParam !== "1";
  const { theme, setTheme } = useTheme();
  const jump = useRef<HTMLInputElement>(null);

  useEffect(() => {
    function onKey(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      const typing =
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement;
      if (
        (event.key === "k" && (event.metaKey || event.ctrlKey)) ||
        (event.key === "/" && !typing)
      ) {
        event.preventDefault();
        jump.current?.focus();
        jump.current?.select();
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  return (
    <div className="grid min-h-svh grid-rows-[var(--spacing-bar)_minmax(0,1fr)] bg-canvas">
      <a
        href="#main"
        className="sr-only fixed left-2 top-2 z-50 rounded-control bg-accent px-3 py-1 text-fg-on-accent focus:not-sr-only"
      >
        Skip to content
      </a>
      <header className="sticky top-0 z-30 flex h-bar items-center gap-3 border-b border-line bg-surface px-3">
        <a
          href="/instances"
          onClick={onLinkClick}
          className="flex items-center gap-2 pr-1 text-label font-semibold text-fg"
          aria-label="Waymark home"
        >
          <span aria-hidden className="flex h-4 w-2.5 items-center">
            <span className="block h-4 w-2.5 rounded-[1px] bg-fg" />
          </span>
          Waymark
        </a>
        <span className="text-micro text-fg-subtle">local</span>
        <span className="h-4 w-px bg-line" aria-hidden />
        <h1 className="min-w-0 truncate text-label font-medium text-fg">
          {title}
        </h1>
        <div className="ml-auto flex items-center gap-2">
          <div
            role="group"
            aria-label="Time window"
            className="flex h-control items-center rounded-control border border-line-strong bg-surface p-0.5"
          >
            {timeWindows.map((option) => (
              <button
                key={option.id}
                type="button"
                aria-pressed={option.id === window.id}
                onClick={() => setWindow(option.id)}
                className={cn(
                  "mono-data h-full rounded-[3px] px-2 text-micro text-fg-muted transition-colors duration-fast hover:text-fg",
                  option.id === window.id && "bg-surface-raised text-fg",
                )}
              >
                {option.label}
              </button>
            ))}
          </div>
          {pinnedTo ? (
            <button
              type="button"
              onClick={() =>
                navigate(
                  withSearch(pathname, search, { to: null, after: null }),
                )
              }
              className="flex h-control items-center gap-2 rounded-control border border-waiting/50 bg-waiting/10 px-2 text-micro text-fg transition-colors duration-fast hover:bg-waiting/20"
              title="This page is frozen at the moment you paged. Click to return to live."
            >
              <StatusDot tone="waiting" />
              <span className="font-medium">Frozen</span>
              <span className="mono-data hidden sm:inline">
                {formatClock(pinnedTo)}
              </span>
              <span className="text-fg-muted">· resume live</span>
            </button>
          ) : (
            <button
              type="button"
              aria-pressed={live}
              onClick={() => setPaused(live ? "1" : null, { replace: true })}
              className="flex h-control items-center gap-2 rounded-control border border-line-strong bg-surface px-2 text-micro text-fg-muted transition-colors duration-fast hover:text-fg"
              title={
                live
                  ? "Polling every 5 s. Click to pause."
                  : "Paused. Click to resume."
              }
            >
              <StatusDot
                tone={source.error ? "danger" : live ? "running" : "neutral"}
                pulse={live && !source.error}
              />
              <span className="font-medium text-fg">
                {live ? "Live" : "Paused"}
              </span>
              <span className="mono-data hidden sm:inline">
                {source.fetchedAt
                  ? `${formatRelative(source.fetchedAt, now)} · ${formatClock(source.fetchedAt)}`
                  : source.error
                    ? "unreachable"
                    : "loading"}
              </span>
            </button>
          )}
          <form
            className="relative hidden md:block"
            onSubmit={(event) => {
              event.preventDefault();
              const value = jump.current?.value.trim() ?? "";
              if (!value) return;
              if (/^[0-9a-f-]{36}$/i.test(value))
                navigate(`/instances/${value}`);
              else navigate(`/instances?q=${encodeURIComponent(value)}`);
              jump.current?.blur();
            }}
          >
            <input
              ref={jump}
              type="search"
              aria-label="Jump to instance by id"
              placeholder="Jump to vm_id"
              className="mono-data h-control w-52 rounded-control border border-line-strong bg-surface pl-2.5 pr-12 text-micro text-fg placeholder:text-fg-subtle focus-visible:border-focus"
            />
            <span className="pointer-events-none absolute right-1.5 top-1/2 flex -translate-y-1/2 gap-0.5">
              <Kbd>⌘</Kbd>
              <Kbd>K</Kbd>
            </span>
          </form>
          <button
            type="button"
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            aria-label={
              theme === "dark"
                ? "Switch to light theme"
                : "Switch to dark theme"
            }
            className="inline-flex size-control items-center justify-center rounded-control border border-line-strong bg-surface text-fg-muted transition-colors duration-fast hover:text-fg"
          >
            {theme === "dark" ? (
              <Sun className="size-3.5" aria-hidden />
            ) : (
              <Moon className="size-3.5" aria-hidden />
            )}
          </button>
        </div>
      </header>
      <div className="grid grid-cols-[var(--spacing-rail)_minmax(0,1fr)]">
        <nav
          aria-label="Main"
          className="sticky top-bar flex h-[calc(100svh-var(--spacing-bar))] flex-col items-center gap-1 border-r border-line bg-surface py-2"
        >
          {railItems.map((item) => (
            <RailLink
              key={item.href}
              href={item.href}
              label={item.label}
              icon={item.icon}
              current={pathname.startsWith(item.match)}
            />
          ))}
        </nav>
        <main id="main" className="min-w-0">
          {children}
        </main>
      </div>
    </div>
  );
}

function RailLink({
  href,
  label,
  icon: Icon,
  current,
}: {
  href: string;
  label: string;
  icon: typeof ListTree;
  current: boolean;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <a
          href={href}
          onClick={onLinkClick}
          aria-current={current ? "page" : undefined}
          aria-label={label}
          className={cn(
            "relative inline-flex size-8 items-center justify-center rounded-control text-fg-muted transition-colors duration-fast hover:bg-surface-raised hover:text-fg",
            current &&
              "bg-surface-raised text-fg before:absolute before:-left-2 before:top-1.5 before:h-5 before:w-0.5 before:rounded-full before:bg-fg",
          )}
        >
          <Icon className="size-4" aria-hidden />
        </a>
      </TooltipTrigger>
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  );
}
