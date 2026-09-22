import { useCallback, useEffect, useRef, useState } from "react";

/**
 * Polling with the behaviour the design requires: keep the previous
 * response while refreshing, expose the last successful time and the
 * current error side by side, pause while the tab is hidden or the user
 * paused, and cancel a superseded request.
 */
export interface LiveState<T> {
  data: T | null;
  error: Error | null;
  fetchedAt: Date | null;
  loading: boolean;
  refresh: () => void;
}

export function useLive<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  options: { intervalMs: number; enabled: boolean; key: string },
): LiveState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [fetchedAt, setFetchedAt] = useState<Date | null>(null);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  const latest = useRef(fetcher);
  latest.current = fetcher;

  const refresh = useCallback(() => setTick((value) => value + 1), []);

  useEffect(() => {
    let controller: AbortController | null = null;
    let timer: number | null = null;
    let cancelled = false;

    async function run() {
      controller?.abort();
      controller = new AbortController();
      const { signal } = controller;
      setLoading(true);
      try {
        const result = await latest.current(signal);
        if (signal.aborted || cancelled) return;
        setData(result);
        setError(null);
        setFetchedAt(new Date());
      } catch (caught) {
        if (signal.aborted || cancelled) return;
        setError(caught instanceof Error ? caught : new Error(String(caught)));
      } finally {
        if (!signal.aborted && !cancelled) setLoading(false);
      }
    }

    function schedule() {
      if (timer !== null) window.clearTimeout(timer);
      if (!options.enabled) return;
      timer = window.setTimeout(() => {
        if (document.visibilityState === "visible") void run().then(schedule);
        else schedule();
      }, options.intervalMs);
    }

    function onVisible() {
      if (document.visibilityState === "visible" && options.enabled)
        void run().then(schedule);
    }

    void run().then(schedule);
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      cancelled = true;
      controller?.abort();
      if (timer !== null) window.clearTimeout(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
    // `key` names the inputs that should restart polling; the fetcher itself
    // is read through a ref so a new closure per render doesn't refetch.
  }, [options.key, options.enabled, options.intervalMs, tick]);

  return { data, error, fetchedAt, loading, refresh };
}
