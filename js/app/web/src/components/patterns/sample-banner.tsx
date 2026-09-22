import { FlaskConical } from "lucide-react";

/** The one place the preview says it is a preview. */
export function SampleBanner() {
  return (
    <span
      className="inline-flex h-5 items-center gap-1.5 rounded-control border border-waiting/40 bg-waiting/10 px-1.5 text-micro text-waiting"
      title="Sample data authored as event streams. Live responses are never mixed in."
    >
      <FlaskConical className="size-3" aria-hidden />
      Sample data
    </span>
  );
}
