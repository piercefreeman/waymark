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
  const [state, setState] = useState<
    Omit<LiveState<T>, "refresh"> & { key: string; tick: number }
  >({
    key: options.key,
    tick: 0,
    data: null,
    error: null,
    fetchedAt: null,
    loading: true,
  });
  const [tick, setTick] = useState(0);
  const latest = useRef(fetcher);
  latest.current = fetcher;

  const refresh = useCallback(() => setTick((value) => value + 1), []);

  useEffect(() => {
    let controller: AbortController | null = null;
    let timer: number | null = null;
    let cancelled = false;

    async function run() {
      if (cancelled || controller) return;
      if (timer !== null) window.clearTimeout(timer);
      controller = new AbortController();
      const { signal } = controller;
      setState((previous) => ({
        key: options.key,
        tick,
        data: previous.key === options.key ? previous.data : null,
        error: previous.key === options.key ? previous.error : null,
        fetchedAt: previous.key === options.key ? previous.fetchedAt : null,
        loading: true,
      }));
      try {
        const result = await latest.current(signal);
        if (signal.aborted || cancelled) return;
        setState({
          key: options.key,
          tick,
          data: result,
          error: null,
          fetchedAt: new Date(),
          loading: false,
        });
      } catch (caught) {
        if (signal.aborted || cancelled) return;
        setState((previous) => ({
          ...previous,
          error: caught instanceof Error ? caught : new Error(String(caught)),
          loading: false,
        }));
      } finally {
        controller = null;
      }
    }

    function schedule() {
      // An aborted fetch can settle after cleanup. It must not revive its
      // polling loop or overlap a fetch triggered by returning to the tab.
      if (cancelled || controller || !options.enabled) return;
      if (timer !== null) window.clearTimeout(timer);
      timer = window.setTimeout(() => {
        if (document.visibilityState === "visible") void run().then(schedule);
        else schedule();
      }, options.intervalMs);
    }

    function onVisible() {
      if (document.visibilityState === "visible" && options.enabled)
        void run().then(schedule);
    }

    // Pausing stops background work without fetching once more. A new query
    // or an explicit retry still loads once, even when polling is paused.
    if (
      options.enabled ||
      state.key !== options.key ||
      state.tick !== tick ||
      (!state.fetchedAt && !state.error)
    )
      void run().then(schedule);
    else setState((previous) => ({ ...previous, loading: false }));
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      cancelled = true;
      controller?.abort();
      if (timer !== null) window.clearTimeout(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
    // `key` names the inputs that should restart polling; the fetcher itself
    // is read through a ref so a new closure per render doesn't refetch.
    // State changes record the result; they must not restart the effect.
  }, [options.key, options.enabled, options.intervalMs, tick]);

  // Keep data during same-query refreshes, never across pages or identities.
  const current = state.key === options.key ? state : null;
  return {
    data: current?.data ?? null,
    error: current?.error ?? null,
    fetchedAt: current?.fetchedAt ?? null,
    loading: current?.loading ?? true,
    refresh,
  };
}
