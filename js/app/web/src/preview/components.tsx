import { useState } from "react";
import { ArrowRight, Check, Copy, Layers, Search } from "lucide-react";
import { PageHeader } from "../components/page-header";
import { StatusBadge, type Status } from "../components/status-badge";
import { CapacityBar } from "../components/capacity-bar";
import { PayloadViewer } from "../components/payload-viewer";
import { EmptyState } from "../components/empty-state";
import { Button } from "../components/ui/button";
import { Badge } from "../components/ui/badge";
import { Input } from "../components/ui/input";
import { Skeleton } from "../components/ui/skeleton";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "../components/ui/tabs";

export function ComponentPreview() {
  const [loading, setLoading] = useState(false);
  const [pressed, setPressed] = useState(false);
  const statuses: Status[] = [
    "running",
    "success",
    "waiting",
    "failing",
    "failed",
    "cancelled",
    "unknown",
  ];
  return (
    <>
      <PageHeader
        title="Built to be clear."
        description="A shared visual language for every workflow, action, and worker."
        actions={
          <Badge
            variant="outline"
            className="gap-2 font-normal text-muted-foreground"
          >
            <Layers className="size-3" />
            Component library · 01
          </Badge>
        }
      />
      <div className="border-t px-5 py-8 sm:px-7">
        <div className="mb-8 grid gap-5 sm:grid-cols-3">
          {[
            [
              "01",
              "Calm by default",
              "Near-black surfaces, precise type, and fine borders. Let the work take focus.",
            ],
            [
              "02",
              "Color with a purpose",
              "Blue is running. Green is success. Amber is waiting. Red asks for attention.",
            ],
            [
              "03",
              "Detail on demand",
              "Scan the whole system, then move into a run, an action, and its payload.",
            ],
          ].map(([number, title, description]) => (
            <div key={number} className="border-l pl-4">
              <span className="font-mono text-[10px] text-muted-foreground">
                {number}
              </span>
              <h2 className="mt-2 text-sm font-medium">{title}</h2>
              <p className="mt-2 max-w-72 text-xs leading-5 text-muted-foreground">
                {description}
              </p>
            </div>
          ))}
        </div>
        <section className="border-y py-7">
          <div className="mb-5">
            <p className="eyebrow">Foundations</p>
            <h2 className="panel-heading mt-1">Semantic color</h2>
          </div>
          <div className="grid grid-cols-3 gap-4 sm:grid-cols-6">
            {[
              ["running", "Running / focus"],
              ["success", "Success"],
              ["waiting", "Waiting"],
              ["failed", "Failure"],
              ["neutral", "Unknown"],
              ["card", "Surface"],
            ].map(([token, label]) => (
              <div key={token}>
                <div
                  className="mb-3 h-12 rounded-md border"
                  style={{ background: `var(--${token})` }}
                />
                <p className="text-[11px]">{label}</p>
                <p className="mt-1 font-mono text-[10px] text-muted-foreground">
                  --{token}
                </p>
              </div>
            ))}
          </div>
        </section>
        <div className="grid divide-y lg:grid-cols-2 lg:divide-x lg:divide-y-0">
          <section className="py-7 lg:pr-7">
            <p className="eyebrow">State</p>
            <h2 className="panel-heading mt-1">A consistent vocabulary</h2>
            <p className="mb-5 mt-2 text-xs text-muted-foreground">
              An icon and a label carry meaning alongside color.
            </p>
            <div className="flex flex-wrap gap-3">
              {statuses.map((status) => (
                <StatusBadge key={status} status={status} />
              ))}
            </div>
            <p className="mt-5 text-[11px] leading-5 text-muted-foreground">
              Failing describes observed trouble; failed means a terminal
              outcome. Missing evidence stays unknown.
            </p>
          </section>
          <section className="py-7 lg:pl-7">
            <p className="eyebrow">Controls</p>
            <h2 className="panel-heading mb-5 mt-1">
              Quiet until you need them
            </h2>
            <div className="mb-4 flex flex-wrap items-center gap-2">
              <Button
                size="sm"
                onClick={() => setPressed(!pressed)}
                aria-pressed={pressed}
              >
                {pressed ? <Check /> : <ArrowRight />}
                {pressed ? "Selected" : "Primary action"}
              </Button>
              <Button
                size="sm"
                variant="outline"
                onClick={() => setLoading(!loading)}
                aria-pressed={loading}
              >
                {loading ? "Show content" : "Show loading"}
              </Button>
              <Button size="sm" variant="secondary" disabled>
                Disabled
              </Button>
            </div>
            <div className="relative max-w-sm">
              <Search className="pointer-events-none absolute left-3 top-2.5 size-3.5 text-muted-foreground" />
              <Input
                aria-label="Example text input"
                placeholder="An accessible, reusable input"
                className="h-9 pl-9 text-xs"
              />
            </div>
          </section>
        </div>
        <div className="grid divide-y border-y lg:grid-cols-2 lg:divide-x lg:divide-y-0">
          <section className="py-7 lg:pr-7">
            <p className="eyebrow">Load</p>
            <h2 className="panel-heading mb-5 mt-1">
              Capacity, without guesswork
            </h2>
            <div className="max-w-lg space-y-5">
              <CapacityBar used={48} capacity={80} label="Active" />
              <CapacityBar used={0} capacity={80} label="Idle" />
              <CapacityBar
                used={100}
                capacity={80}
                label="Above reported capacity"
              />
              <CapacityBar used={null} capacity={null} label="No sample" />
            </div>
          </section>
          <section className="py-7 lg:pl-7">
            <p className="eyebrow">Feedback</p>
            <h2 className="panel-heading mb-5 mt-1">Every state has a place</h2>
            <div aria-busy={loading}>
              {loading ? (
                <div role="status" className="space-y-4 rounded-lg border p-6">
                  <span className="sr-only">Loading example content</span>
                  <Skeleton className="h-4 w-1/3" />
                  <Skeleton className="h-3 w-3/4" />
                  <Skeleton className="h-3 w-2/3" />
                  <Skeleton className="h-12 w-full" />
                </div>
              ) : (
                <EmptyState
                  title="No workflows in this window"
                  description="Workflows will appear here when observations are available."
                >
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setLoading(true)}
                  >
                    Preview loading
                  </Button>
                </EmptyState>
              )}
            </div>
          </section>
        </div>
        <section className="grid gap-6 py-7 lg:grid-cols-[1fr_1.4fr]">
          <div>
            <p className="eyebrow">Inspection</p>
            <h2 className="panel-heading mt-1">Payloads stay readable</h2>
            <p className="mt-2 max-w-sm text-xs leading-5 text-muted-foreground">
              Monospace, selectable text, wrapping, and explicit copy. A missing
              value looks different from a successful null result.
            </p>
            <p className="mt-4 flex items-center gap-2 text-[11px] text-muted-foreground">
              <Copy className="size-3" />
              Try copying a payload or turning off wrapping.
            </p>
          </div>
          <Tabs defaultValue="result">
            <TabsList variant="line">
              <TabsTrigger value="result">Result</TabsTrigger>
              <TabsTrigger value="null">Null</TabsTrigger>
              <TabsTrigger value="missing">Not recorded</TabsTrigger>
            </TabsList>
            <TabsContent value="result">
              <PayloadViewer
                label="Response · sample"
                value={{
                  order_id: "ord_10482",
                  status: "confirmed",
                  items: [{ sku: "WM-042", quantity: 2 }],
                  message: "Your order is ready for fulfillment.",
                }}
              />
            </TabsContent>
            <TabsContent value="null">
              <PayloadViewer label="Response · null sample" value={null} />
            </TabsContent>
            <TabsContent value="missing">
              <PayloadViewer label="Input · unavailable sample" />
            </TabsContent>
          </Tabs>
        </section>
      </div>
    </>
  );
}
