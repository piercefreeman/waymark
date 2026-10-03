import { Fragment } from "react";
import { cn } from "@/lib/cn";
import { formatTimestamp, shortId } from "@/lib/format";
import type { Event } from "@/domain/api";
import { observationDetail, observationShortKind } from "@/domain/derive";

/**
 * Raw observations in fixed-width columns. Kinds are colored only where
 * they carry outcome: rejections, error stops, completion, unhandled
 * exceptions. Gaps in a run's sequence are rendered as rows so dropped
 * events can't hide.
 */
export function EventLog({
  events,
  highlightPromiseId,
  className,
}: {
  events: Event[];
  highlightPromiseId?: number | null;
  className?: string;
}) {
  const rows: (
    | { kind: "event"; event: Event }
    | { kind: "gap"; missing: number; key: string }
  )[] = [];
  let previousSequence: number | null = null;
  for (const event of events) {
    const sequence = event.payload.run_sequence;
    if (
      previousSequence !== null &&
      sequence > previousSequence + 1 &&
      sequence !== 0
    )
      rows.push({
        kind: "gap",
        missing: sequence - previousSequence - 1,
        key: `gap-${event.node_id}-${event.node_sequence}`,
      });
    rows.push({ kind: "event", event });
    previousSequence = sequence === 0 ? 0 : sequence;
  }
  return (
    <div className={cn("overflow-x-auto", className)}>
      <table className="mono-data w-full border-collapse text-micro">
        <thead>
          <tr className="text-left text-[10px] text-fg-subtle">
            <th scope="col" className="px-gutter py-1 font-normal">
              at (UTC)
            </th>
            <th scope="col" className="py-1 pr-3 font-normal">
              node
            </th>
            <th scope="col" className="py-1 pr-3 text-right font-normal">
              seq
            </th>
            <th scope="col" className="py-1 pr-3 font-normal">
              kind
            </th>
            <th scope="col" className="py-1 pr-gutter font-normal">
              detail
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-line">
          {rows.map((row) => {
            if (row.kind === "gap")
              return (
                <tr key={row.key} className="hatched text-waiting">
                  <td colSpan={5} className="px-gutter py-1">
                    {row.missing} event{row.missing === 1 ? "" : "s"} missing
                    from this run's sequence
                  </td>
                </tr>
              );
            const { event } = row;
            const observation = event.payload.observation;
            const tone = kindTone(observation);
            const involves =
              highlightPromiseId !== null &&
              highlightPromiseId !== undefined &&
              ((observation.kind === "effect_emitted" &&
                "promise_state_id" in observation.effect &&
                observation.effect.promise_state_id === highlightPromiseId) ||
                (observation.kind === "promise_settled" &&
                  observation.promise_state_id === highlightPromiseId));
            return (
              <Fragment key={`${event.node_id}-${event.node_sequence}`}>
                <tr
                  className={cn(
                    "h-7 align-middle transition-colors duration-fast hover:bg-surface-raised",
                    involves && "bg-surface-selected",
                  )}
                >
                  <td className="whitespace-nowrap px-gutter text-fg-muted">
                    {formatTimestamp(new Date(event.at)).slice(11, 23)}
                  </td>
                  <td className="whitespace-nowrap pr-3 text-fg-subtle">
                    {shortId(event.node_id).slice(0, 8)}
                  </td>
                  <td className="whitespace-nowrap pr-3 text-right text-fg-subtle">
                    {event.payload.run_sequence}
                  </td>
                  <td
                    className={cn("whitespace-nowrap pr-3", tone ?? "text-fg")}
                  >
                    {observationShortKind(observation)}
                  </td>
                  <td className="max-w-md truncate pr-gutter text-fg-muted">
                    {observationDetail(observation)}
                  </td>
                </tr>
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function kindTone(observation: Event["payload"]["observation"]): string | null {
  switch (observation.kind) {
    case "effect_emitted":
      if (observation.effect.kind === "complete") return "text-success";
      if (observation.effect.kind === "unhandled_exception")
        return "text-danger";
      return null;
    case "promise_settled":
      return observation.settlement.kind === "rejected" ? "text-danger" : null;
    case "vm_stopped":
      return "error" in observation.reason ? "text-danger" : "text-fg-muted";
    case "vm_started":
    case "snapshot_persisted":
      return "text-fg-muted";
  }
}
