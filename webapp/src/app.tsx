import { useState } from "react";
import { AppShell, type Section } from "./components/app-shell";
import { TooltipProvider } from "./components/ui/tooltip";
import { WorkflowPreview, WorkerPreview } from "./preview/dashboard";
import { ComponentPreview } from "./preview/components";

export function App() {
  const [section, setSection] = useState<Section>("workflows");
  return (
    <TooltipProvider delayDuration={200}>
      <AppShell section={section} onNavigate={setSection}>
        {section === "workflows" && (
          <WorkflowPreview onWorkers={() => setSection("workers")} />
        )}
        {section === "workers" && <WorkerPreview />}
        {section === "components" && <ComponentPreview />}
      </AppShell>
    </TooltipProvider>
  );
}
