import { useEffect, useRef, useState } from "react";
import { vmTimeline } from "@/api/client";
import type { Event, Instance } from "@/domain/api";

/**
 * Per-instance timelines for the rows on screen, cached by the instance's
 * last observed event. A poll only refetches rows whose last event moved,
 * so a page of 100 costs 100 small reads once and a handful per tick.
 */

export interface CachedTimeline {
  key: string;
  events: Event[];
  complete: boolean;
}

const CONCURRENCY = 6;

function keyOf(instance: Instance): string {
  return `${instance.last_event.node_id}:${instance.last_event.node_sequence}`;
}

export function useTimelines(instances: Instance[]) {
  const cache = useRef(new Map<string, CachedTimeline>());
  const inFlight = useRef(new Set<string>());
  const [version, setVersion] = useState(0);
  const [pending, setPending] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    const stale = instances.filter(
      (instance) =>
        cache.current.get(instance.vm_id)?.key !== keyOf(instance) &&
        !inFlight.current.has(instance.vm_id),
    );
    if (stale.length === 0) return;
    setPending((count) => count + stale.length);
    const queue = [...stale];
    let active = 0;
    let cancelled = false;

    function pump() {
      while (active < CONCURRENCY && queue.length > 0) {
        const instance = queue.shift()!;
        active += 1;
        inFlight.current.add(instance.vm_id);
        vmTimeline(instance.vm_id, controller.signal)
          .then((timeline) => {
            if (cancelled) return;
            cache.current.set(instance.vm_id, {
              key: keyOf(instance),
              events: timeline.events,
              complete: timeline.complete,
            });
            setVersion((value) => value + 1);
          })
          .catch(() => {
            // A failed row keeps its previous timeline, if any; the next
            // poll retries because its key still mismatches.
          })
          .finally(() => {
            inFlight.current.delete(instance.vm_id);
            active -= 1;
            if (!cancelled) {
              setPending((count) => Math.max(0, count - 1));
              pump();
            }
          });
      }
    }
    pump();
    return () => {
      cancelled = true;
      controller.abort();
      for (const instance of stale) inFlight.current.delete(instance.vm_id);
      setPending(0);
    };
    // Re-run when the set of (vm_id, last event) pairs changes.
  }, [instances.map(keyOf).join("|")]);

  return {
    version,
    pending,
    get: (vmId: string) => cache.current.get(vmId),
  };
}
