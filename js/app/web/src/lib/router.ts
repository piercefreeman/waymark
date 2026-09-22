import { useCallback, useSyncExternalStore } from "react";

/**
 * A minimal history router. Filters, selection, and time windows live in the
 * URL so an inspection can be shared and Back restores context. Swap for a
 * typed router library when API wiring lands; the hooks below are the seam.
 */

const listeners = new Set<() => void>();
const NAVIGATE_EVENT = "waymark:navigate";

function snapshot() {
  return window.location.pathname + window.location.search;
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  window.addEventListener("popstate", listener);
  window.addEventListener(NAVIGATE_EVENT, listener);
  return () => {
    listeners.delete(listener);
    window.removeEventListener("popstate", listener);
    window.removeEventListener(NAVIGATE_EVENT, listener);
  };
}

export function navigate(to: string, options: { replace?: boolean } = {}) {
  if (to === snapshot()) return;
  if (options.replace) window.history.replaceState(null, "", to);
  else window.history.pushState(null, "", to);
  window.dispatchEvent(new Event(NAVIGATE_EVENT));
}

export function useLocation() {
  const href = useSyncExternalStore(subscribe, snapshot, snapshot);
  const [pathname, search = ""] = href.split("?");
  return { pathname, search: new URLSearchParams(search), href };
}

export function matchPath(
  pattern: string,
  pathname: string,
): Record<string, string> | null {
  const patternParts = pattern.split("/").filter(Boolean);
  const pathParts = pathname.split("/").filter(Boolean);
  if (patternParts.length !== pathParts.length) return null;
  const params: Record<string, string> = {};
  for (let index = 0; index < patternParts.length; index += 1) {
    const expected = patternParts[index];
    const actual = pathParts[index];
    if (expected.startsWith(":")) params[expected.slice(1)] = actual;
    else if (expected !== actual) return null;
  }
  return params;
}

export function withSearch(
  pathname: string,
  search: URLSearchParams,
  patch: Record<string, string | null>,
) {
  const next = new URLSearchParams(search);
  for (const [key, value] of Object.entries(patch)) {
    if (value === null || value === "") next.delete(key);
    else next.set(key, value);
  }
  const query = next.toString();
  return query ? `${pathname}?${query}` : pathname;
}

/** Read and update one search param without losing the rest. */
export function useSearchParam(key: string) {
  const { pathname, search } = useLocation();
  const value = search.get(key);
  const setValue = useCallback(
    (next: string | null, options?: { replace?: boolean }) =>
      navigate(withSearch(pathname, search, { [key]: next }), options),
    [key, pathname, search],
  );
  return [value, setValue] as const;
}

/** Intercept plain left clicks on internal links so navigation stays in-app. */
export function onLinkClick(event: React.MouseEvent<HTMLAnchorElement>) {
  if (
    event.defaultPrevented ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey ||
    event.currentTarget.target === "_blank"
  )
    return;
  event.preventDefault();
  navigate(event.currentTarget.getAttribute("href") ?? "/");
}
