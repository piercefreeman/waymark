import { useRef, useState } from "react";
import {
  ArrowDown,
  ArrowUpRight,
  CircleDot,
  Search,
  Server,
  Workflow,
} from "lucide-react";
import { cn } from "cn";
import { PageHeader } from "../components/page-header";
import { Metric } from "../components/metric";
import { Sparkline } from "../components/sparkline";
import { CapacityBar } from "../components/capacity-bar";
import { StatusBadge } from "../components/status-badge";
import { EmptyState } from "../components/empty-state";
import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import { Badge } from "../components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetTitle,
  SheetDescription,
} from "../components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../components/ui/table";
import { Inspector } from "./inspector";
import { workers, workflows } from "./data";

const inFlight = workers.reduce((sum, worker) => sum + worker.used, 0);
const capacity = workers.reduce((sum, worker) => sum + worker.capacity, 0);
const queued = workers.reduce((sum, worker) => sum + worker.queued, 0);
const rate = workers
  .reduce((sum, worker) => sum + Number(worker.rate), 0)
  .toFixed(1);
const loadSamples = workers[0].samples.map((_, index) =>
  workers.reduce((sum, worker) => sum + worker.samples[index], 0),
);

export function WorkflowPreview({ onWorkers }: { onWorkers: () => void }) {
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [selectedId, setSelectedId] = useState(workflows[0].id);
  const [sheetOpen, setSheetOpen] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const selected = workflows.find((item) => item.id === selectedId)!;
  const visible = workflows.filter(
    (item) =>
      (status === "all" ||
        item.status === status ||
        (status === "attention" &&
          ["failed", "failing"].includes(item.status))) &&
      `${item.name} ${item.module} ${item.id} ${item.node}`
        .toLowerCase()
        .includes(query.toLowerCase()),
  );
  return (
    <>
      <PageHeader
        title="Workflows"
        description="Every run. Every action. One clear view."
        actions={
          <Badge
            variant="outline"
            className="gap-2 font-normal text-muted-foreground"
          >
            <CircleDot className="size-3 text-waiting" />
            Sample workspace
          </Badge>
        }
      />
      <div className="grid grid-cols-2 divide-x divide-y border-y bg-card sm:grid-cols-4 sm:divide-y-0">
        <Metric
          label="Running workflows"
          value={workflows.filter((item) => item.status === "running").length}
          detail="Of 8 sample workflows"
        >
          <Workflow className="size-5 text-running" />
        </Metric>
        <Metric
          label="Need attention"
          value={
            workflows.filter((item) =>
              ["failed", "failing"].includes(item.status),
            ).length
          }
          detail="1 failed · 1 failing"
        >
          <span className="size-2 rounded-full bg-failed" />
        </Metric>
        <Metric
          label="Action concurrency"
          value={inFlight}
          unit={`/ ${capacity}`}
          detail="Across 3 sample nodes"
        >
          <Sparkline values={loadSamples} label="Sample concurrency trend" />
        </Metric>
        <Metric
          label="Completion rate"
          value={rate}
          unit="/ s"
          detail="Actions · last sample interval"
        >
          <Sparkline
            values={[32, 48, 40, 56, 54, 70, 65, 72, 80, 74, 86.8]}
            label="Sample action completion rate"
            tone="success"
          />
        </Metric>
      </div>
      <div className="grid xl:grid-cols-[minmax(0,1fr)_360px]">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-3 border-b px-5 py-4">
            <div className="relative min-w-40 flex-1">
              <Search className="pointer-events-none absolute left-3 top-2.5 size-3.5 text-muted-foreground" />
              <Input
                aria-label="Search sample workflows"
                placeholder="Search workflows, IDs, nodes…"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                className="h-9 pl-9 text-xs shadow-none"
              />
            </div>
            <Select value={status} onValueChange={setStatus}>
              <SelectTrigger
                aria-label="Filter workflow status"
                size="sm"
                className="w-36 shadow-none"
              >
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All states</SelectItem>
                <SelectItem value="running">Running</SelectItem>
                <SelectItem value="attention">Need attention</SelectItem>
                <SelectItem value="success">Success</SelectItem>
                <SelectItem value="waiting">Waiting</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <Table aria-label="Sample workflows">
            <TableHeader>
              <TableRow className="bg-card hover:bg-card">
                <TableHead className="pl-5">Workflow</TableHead>
                <TableHead>State</TableHead>
                <TableHead>Node</TableHead>
                <TableHead className="pr-5 text-right">Elapsed</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {visible.map((item) => (
                <TableRow
                  key={item.id}
                  data-state={item.id === selectedId ? "selected" : undefined}
                  className="h-14 data-[state=selected]:bg-running/8"
                >
                  <TableCell className="pl-5">
                    <button
                      onClick={(event) => {
                        trigger.current = event.currentTarget;
                        setSelectedId(item.id);
                        if (!window.matchMedia("(min-width: 1280px)").matches)
                          setSheetOpen(true);
                      }}
                      aria-label={`Inspect ${item.name}`}
                      aria-pressed={item.id === selectedId}
                      className="group block max-w-64 text-left"
                    >
                      <span
                        className={cn(
                          "flex items-center gap-2 text-xs font-medium group-hover:text-running",
                          item.id === selectedId && "text-running",
                        )}
                      >
                        <span className="truncate">{item.name}</span>
                        <ArrowUpRight className="size-3 shrink-0 opacity-0 group-hover:opacity-100" />
                      </span>
                      <span className="mt-1 block truncate font-mono text-[10px] text-muted-foreground">
                        {item.module}
                      </span>
                    </button>
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={item.status} />
                  </TableCell>
                  <TableCell className="font-mono text-[10px] text-muted-foreground">
                    {item.node}
                  </TableCell>
                  <TableCell className="pr-5 text-right font-mono text-xs">
                    {item.duration}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          {visible.length === 0 && (
            <EmptyState
              title="No matching workflows"
              description="Try another name, instance ID, node, or state."
            >
              <Button
                variant="outline"
                size="sm"
                onClick={() => {
                  setQuery("");
                  setStatus("all");
                }}
              >
                Clear filters
              </Button>
            </EmptyState>
          )}
          <div
            role="status"
            className="flex justify-between border-b px-5 py-3 text-[10px] text-muted-foreground"
          >
            <span>
              {visible.length} of {workflows.length} sample workflows
            </span>
            <span>Static preview · 14:32:09 UTC</span>
          </div>
          <section className="px-5 py-6">
            <div className="mb-5 flex items-center justify-between">
              <div>
                <h2 className="panel-heading">Worker load</h2>
                <p className="mt-1 text-[11px] text-muted-foreground">
                  Action concurrency across the sample cluster
                </p>
              </div>
              <Button
                variant="ghost"
                size="sm"
                onClick={onWorkers}
                className="text-xs text-muted-foreground"
              >
                View workers
                <ArrowUpRight className="size-3" />
              </Button>
            </div>
            <div className="space-y-5">
              {workers.map((worker) => (
                <div
                  key={worker.name}
                  className="grid grid-cols-[100px_minmax(0,1fr)] items-center gap-5"
                >
                  <span className="flex items-center gap-2 font-mono text-[11px]">
                    <Server className="size-3.5 text-muted-foreground" />
                    {worker.name}
                  </span>
                  <CapacityBar
                    used={worker.used}
                    capacity={worker.capacity}
                    label={`${worker.queued} queued`}
                  />
                </div>
              ))}
            </div>
          </section>
        </div>
        <aside
          aria-label="Selected workflow"
          className="hidden border-l bg-card xl:block"
        >
          <Inspector key={selected.id} workflow={selected} />
        </aside>
      </div>
      <Sheet open={sheetOpen} onOpenChange={setSheetOpen}>
        <SheetContent
          className="w-full gap-0 overflow-y-auto sm:max-w-lg"
          onCloseAutoFocus={(event) => {
            event.preventDefault();
            trigger.current?.focus();
          }}
        >
          <SheetTitle className="sr-only">{selected.name} details</SheetTitle>
          <SheetDescription className="sr-only">
            Inspect sample actions and payloads.
          </SheetDescription>
          <Inspector key={selected.id} workflow={selected} />
        </SheetContent>
      </Sheet>
    </>
  );
}

export function WorkerPreview() {
  return (
    <>
      <PageHeader
        title="Workers"
        description="Understand capacity, queues, and the shape of your workload."
        actions={
          <Badge
            variant="outline"
            className="font-normal text-muted-foreground"
          >
            Sample window · 15 min
          </Badge>
        }
      />
      <div className="grid grid-cols-2 divide-x divide-y border-y bg-card sm:grid-cols-4 sm:divide-y-0">
        <Metric
          label="Worker processes"
          value={workers.reduce((sum, worker) => sum + worker.processes, 0)}
          detail="Across 3 sample nodes"
        />
        <Metric
          label="Action concurrency"
          value={`${Math.round((inFlight / capacity) * 100)}%`}
          detail={`${inFlight} / ${capacity} available slots`}
        />
        <Metric
          label="Queued dispatches"
          value={queued}
          detail="Waiting for an action slot"
        />
        <Metric
          label="Completion rate"
          value={rate}
          unit="/ s"
          detail="Actions · last sample interval"
        />
      </div>
      <section className="border-b p-5 sm:p-7">
        <div className="mb-6 flex items-start justify-between gap-4">
          <div>
            <h2 className="panel-heading">Concurrency over time</h2>
            <p className="mt-1 text-xs text-muted-foreground">
              In-flight actions · all sample nodes
            </p>
          </div>
          <span className="inline-flex items-center gap-2 text-[11px] text-muted-foreground">
            <span className="size-1.5 rounded-full bg-running" />
            Actions
          </span>
        </div>
        <div className="grid grid-cols-[28px_minmax(0,1fr)] gap-2">
          <div
            aria-hidden="true"
            className="flex h-[180px] flex-col justify-between py-2.5 font-mono text-[10px] text-muted-foreground"
          >
            {[200, 150, 100, 50, 0].map((value) => (
              <span key={value}>{value}</span>
            ))}
          </div>
          <svg
            viewBox="0 0 900 180"
            role="img"
            aria-label="Sample action concurrency rose from 42 to 126 over 15 minutes; capacity is 200"
            className="h-[180px] w-full overflow-visible"
            preserveAspectRatio="none"
          >
            {[0, 50, 100, 150, 200].map((value) => (
              <g key={value}>
                <line
                  x1="0"
                  y1={160 - value * 0.7}
                  x2="900"
                  y2={160 - value * 0.7}
                  stroke="var(--border)"
                  strokeDasharray="3 5"
                />
              </g>
            ))}
            <path
              d={`M0,160 ${loadSamples.map((value, index) => `L${(index / (loadSamples.length - 1)) * 900},${160 - value * 0.7}`).join(" ")} L900,160 Z`}
              fill="var(--running)"
              opacity=".06"
            />
            <polyline
              points={loadSamples
                .map(
                  (value, index) =>
                    `${(index / (loadSamples.length - 1)) * 900},${160 - value * 0.7}`,
                )
                .join(" ")}
              fill="none"
              stroke="var(--running)"
              strokeWidth="2"
              vectorEffect="non-scaling-stroke"
            />
          </svg>
        </div>
        <div className="flex justify-between pl-9 font-mono text-[10px] text-muted-foreground">
          <span>14:17</span>
          <span>14:22</span>
          <span>14:27</span>
          <span>14:32 UTC</span>
        </div>
      </section>
      <section className="py-6">
        <div className="mb-5 flex items-center justify-between px-5 sm:px-7">
          <h2 className="panel-heading">
            Nodes{" "}
            <span className="ml-2 text-muted-foreground">{workers.length}</span>
          </h2>
          <span className="text-[11px] text-muted-foreground">
            Illustrative metrics
          </span>
        </div>
        <Table aria-label="Sample worker load">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-7">Node</TableHead>
              <TableHead>Processes</TableHead>
              <TableHead className="min-w-52">Concurrency</TableHead>
              <TableHead>Queued</TableHead>
              <TableHead>Completed / s</TableHead>
              <TableHead>Handling p50</TableHead>
              <TableHead className="pr-7">15 min trend</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {workers.map((worker) => (
              <TableRow key={worker.name} className="h-24">
                <TableCell className="pl-7">
                  <span className="flex items-center gap-2 font-mono text-xs">
                    <Server className="size-4 text-muted-foreground" />
                    {worker.name}
                  </span>
                </TableCell>
                <TableCell className="font-mono text-xs">
                  {worker.processes}
                </TableCell>
                <TableCell>
                  <CapacityBar
                    used={worker.used}
                    capacity={worker.capacity}
                    label="Action slots"
                  />
                </TableCell>
                <TableCell className="font-mono text-xs">
                  {worker.queued}
                </TableCell>
                <TableCell className="font-mono text-xs">
                  {worker.rate}
                </TableCell>
                <TableCell className="font-mono text-xs">
                  {worker.latency}
                </TableCell>
                <TableCell className="pr-7">
                  <Sparkline
                    values={worker.samples}
                    label={`${worker.name} sample concurrency trend`}
                  />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <p className="mt-5 flex items-center gap-2 px-5 text-[11px] text-muted-foreground sm:px-7">
          <ArrowDown className="size-3" />
          Capacity measures action slots. Latency is per-node action handling
          time.
        </p>
      </section>
    </>
  );
}
