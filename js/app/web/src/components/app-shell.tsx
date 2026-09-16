import type { ReactNode } from "react";
import {
  Activity,
  ArrowUpRight,
  Boxes,
  ChevronRight,
  Component,
  Workflow,
} from "lucide-react";
import { cn } from "cn";
import { ThemeToggle } from "./theme-toggle";
import { Badge } from "./ui/badge";

export const sections = [
  { id: "workflows", label: "Workflows", icon: Workflow },
  { id: "workers", label: "Workers", icon: Activity },
  { id: "components", label: "Components", icon: Component },
] as const;
export type Section = (typeof sections)[number]["id"];

export function AppShell({
  section,
  onNavigate,
  children,
}: {
  section: Section;
  onNavigate: (section: Section) => void;
  children: ReactNode;
}) {
  return (
    <div className="min-h-svh lg:grid lg:grid-cols-[208px_minmax(0,1fr)]">
      <a
        href="#main"
        className="sr-only fixed left-4 top-4 z-50 rounded bg-primary px-4 py-2 text-primary-foreground focus:not-sr-only"
      >
        Skip to content
      </a>
      <aside className="flex flex-col border-b bg-card lg:sticky lg:top-0 lg:h-svh lg:border-b-0 lg:border-r">
        <div className="hidden items-center gap-2.5 px-5 py-6 lg:flex">
          <span className="flex size-7 items-center justify-center rounded-md border bg-muted text-muted-foreground">
            <Boxes className="size-4" />
          </span>
          <div>
            <p className="text-xs font-medium">Workspace</p>
            <p className="text-[10px] text-muted-foreground">
              Design exploration
            </p>
          </div>
        </div>
        <p className="eyebrow hidden px-5 pb-2 lg:block">Observability</p>
        <nav
          aria-label="Main navigation"
          className="flex gap-1 p-2 lg:flex-col lg:px-3"
        >
          {sections.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => onNavigate(id)}
              aria-current={section === id ? "page" : undefined}
              className={cn(
                "flex flex-1 items-center gap-2.5 rounded-md px-3 py-2 text-xs text-muted-foreground transition-colors hover:bg-accent/50 hover:text-foreground lg:flex-none",
                section === id &&
                  "bg-running/10 text-running hover:text-running",
              )}
            >
              <Icon className="size-4" />
              {label}
              {id === "components" && (
                <span className="ml-auto hidden font-mono text-[10px] opacity-60 lg:inline">
                  01
                </span>
              )}
            </button>
          ))}
        </nav>
        <div className="mt-auto hidden p-5 lg:block">
          <div className="mb-4 h-px bg-border" />
          <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
            <span className="size-1.5 rounded-full bg-waiting" />
            Sample data only
          </div>
          <p className="mt-2 text-[11px] leading-5 text-muted-foreground">
            A preview of the components
            <br />
            behind the next Waymark UI.
          </p>
        </div>
      </aside>
      <div className="min-w-0">
        <header className="flex h-14 items-center justify-between border-b px-5 sm:px-7">
          <div className="flex items-center gap-2 text-xs">
            <span className="text-muted-foreground">Workspace</span>
            <ChevronRight className="size-3 text-muted-foreground" />
            <span>{sections.find((item) => item.id === section)?.label}</span>
          </div>
          <div className="flex items-center gap-3">
            <Badge
              variant="outline"
              className="text-[10px] font-normal text-muted-foreground"
            >
              Design preview
            </Badge>
            <ThemeToggle />
          </div>
        </header>
        <main id="main" className="min-w-0">
          {children}
        </main>
        <footer className="flex flex-wrap justify-between gap-2 border-t px-5 py-3 text-[10px] text-muted-foreground sm:px-7">
          <span>
            Waymark design system <span className="mx-2 text-border">/</span>{" "}
            React + shadcn/ui
          </span>
          <button
            onClick={() => onNavigate("components")}
            className="inline-flex items-center gap-1 hover:text-foreground"
          >
            Explore the components
            <ArrowUpRight className="size-3" />
          </button>
        </footer>
      </div>
    </div>
  );
}
