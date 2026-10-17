import { vmTimeline } from "../api/client.ts";
import type { Event, Instance } from "../domain/api.ts";

export interface CachedTimeline {
  key: string;
  events: Event[];
  complete: boolean;
}

const CONCURRENCY = 6;

/**
 * Run `work` over every item with a bounded number in flight. An abort
 * stops the walk before the next item and after the last.
 */
export async function forEachBounded<T>(
  items: readonly T[],
  signal: AbortSignal,
  work: (item: T, index: number) => Promise<void>,
): Promise<void> {
  const queue = items.entries();
  await Promise.all(
    Array.from({ length: Math.min(CONCURRENCY, items.length) }, async () => {
      for (const [index, item] of queue) {
        signal.throwIfAborted();
        await work(item, index);
      }
    }),
  );
  signal.throwIfAborted();
}

function keyOf(instance: Instance): string {
  return `${instance.last_event.node_id}:${instance.last_event.node_sequence}`;
}

/**
 * Prepare timelines before publishing the next ledger snapshot. Unchanged
 * rows reuse their last read; changed rows load with bounded concurrency.
 * The previous snapshot stays untouched while these requests are pending.
 */
export async function fetchTimelines(
  instances: Instance[],
  previous: ReadonlyMap<string, CachedTimeline>,
  signal: AbortSignal,
) {
  const timelines = new Map<string, CachedTimeline>();
  await forEachBounded(instances, signal, async (instance) => {
    const cached = previous.get(instance.vm_id);
    if (cached) timelines.set(instance.vm_id, cached);
    if (cached?.key === keyOf(instance)) return;
    try {
      const timeline = await vmTimeline(instance.vm_id, signal);
      signal.throwIfAborted();
      timelines.set(instance.vm_id, {
        key: keyOf(instance),
        ...timeline,
      });
    } catch {
      signal.throwIfAborted();
      // Keep the previous row on failure. Its mismatched key makes the
      // next poll retry, even when no new event has arrived.
    }
  });
  return {
    timelines,
    complete: instances.every((instance) => {
      const timeline = timelines.get(instance.vm_id);
      return timeline?.key === keyOf(instance) && timeline.complete;
    }),
  };
}
