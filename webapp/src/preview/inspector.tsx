import { useState } from "react";
import { Clock3, Server } from "lucide-react";
import type { PreviewWorkflow } from "./data";
import { StatusBadge } from "../components/status-badge";
import { Timeline } from "../components/timeline";
import { PayloadViewer } from "../components/payload-viewer";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "../components/ui/tabs";

export function Inspector({ workflow }: { workflow: PreviewWorkflow }) {
  const [actionId, setActionId] = useState(workflow.actions[0].id);
  const action =
    workflow.actions.find((item) => item.id === actionId) ??
    workflow.actions[0];
  const total = Math.max(
    1,
    ...workflow.actions.map((item) => item.start + (item.duration ?? 0)),
  );
  return (
    <div className="min-w-0">
      <div className="border-b p-5">
        <div className="mb-5 flex items-center justify-between">
          <span className="eyebrow">Workflow inspector</span>
        </div>
        <h2 className="break-words text-base font-medium tracking-tight">
          {workflow.name}
        </h2>
        <p className="mt-1 break-all font-mono text-[10px] text-muted-foreground">
          {workflow.id}
        </p>
        <div className="mt-4">
          <StatusBadge status={workflow.status} />
        </div>
        <dl className="mt-5 grid grid-cols-2 gap-4 text-[11px]">
          <div>
            <dt className="mb-1 flex items-center gap-1.5 text-muted-foreground">
              <Clock3 className="size-3" />
              Elapsed
            </dt>
            <dd className="font-mono">{workflow.duration}</dd>
          </div>
          <div>
            <dt className="mb-1 flex items-center gap-1.5 text-muted-foreground">
              <Server className="size-3" />
              Node
            </dt>
            <dd className="font-mono">{workflow.node}</dd>
          </div>
        </dl>
      </div>
      <div className="border-b p-5">
        <div className="mb-5 flex items-center justify-between">
          <h3 className="panel-heading">Actions</h3>
          <span className="font-mono text-[10px] text-muted-foreground">
            {
              workflow.actions.filter((item) => item.status === "success")
                .length
            }{" "}
            / {workflow.actions.length} complete
          </span>
        </div>
        <Timeline
          items={workflow.actions}
          total={total}
          selected={action.id}
          onSelect={setActionId}
        />
        <p className="mt-3 text-[10px] text-muted-foreground">
          Call-to-settlement time · sample timings
        </p>
      </div>
      <div className="p-5">
        <div className="mb-4 flex items-center justify-between gap-2">
          <h3 className="break-all font-mono text-xs">{action.name}</h3>
        </div>
        <Tabs key={action.id} defaultValue="input" className="gap-3">
          <TabsList variant="line" className="h-8 border-b p-0">
            <TabsTrigger value="input" className="text-xs">
              Input
            </TabsTrigger>
            <TabsTrigger value="response" className="text-xs">
              {action.status === "failed" ? "Error" : "Response"}
            </TabsTrigger>
          </TabsList>
          <TabsContent value="input">
            <PayloadViewer
              value={action.input}
              label="Input · sample"
              unavailable="The current event API doesn't record action arguments."
            />
          </TabsContent>
          <TabsContent value="response">
            <PayloadViewer
              value={action.response}
              unavailableTitle={
                action.status === "running"
                  ? "Awaiting response"
                  : "Not recorded"
              }
              label={
                action.status === "failed"
                  ? "Error · sample"
                  : "Response · sample"
              }
              unavailable={
                action.status === "running"
                  ? "This sample action is still running. No response is available."
                  : "The current event API doesn't record returned values."
              }
            />
          </TabsContent>
        </Tabs>
        <p className="mt-3 text-[10px] leading-4 text-muted-foreground">
          Illustrative payloads. Live input and response capture will require an
          API extension.
        </p>
      </div>
    </div>
  );
}
