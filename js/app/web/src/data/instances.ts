import { ApiError, getInstance, listInstances } from "@/api/client";
import type { Instance } from "@/domain/api";
import { deriveFromInstance } from "@/domain/derive";
import { instanceStates, type InstanceState } from "@/domain/status";

/**
 * One page of the instance ledger, read through the list endpoint's own
 * cursor. Search and state filters have no server-side parameters, so they
 * walk the cursor across the window page by page and keep the matches;
 * the walk is bounded and the page says how far it got. An exact `vm_id`
 * skips the walk and reads the instance directly.
 */

export const PAGE_SIZE = 100;
/** Pages a filtered read may consume before it stops and says so. */
export const SCAN_PAGE_CAP = 20;

export interface InstancePageQuery {
  from: Date;
  to: Date;
  after: string | null;
  query: string;
  states: InstanceState[];
  now: Date;
}

export interface InstancePage {
  items: Instance[];
  /** Cursor for the page after this one; null at the end of the window. */
  next: string | null;
  /** Instances the read looked at, including non-matches. */
  scanned: number;
  /** True when the read stopped at the page cap with more window left. */
  capped: boolean;
  /** True when the read was a direct lookup rather than a page. */
  direct: boolean;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isExactId(query: string): boolean {
  return UUID.test(query.trim());
}

/** Fields the list endpoint reports; timelines are not consulted here. */
export function matchesQuery(
  instance: Instance,
  query: string,
  states: InstanceState[],
  now: Date,
): boolean {
  const state = deriveFromInstance(instance, [], now).state;
  if (states.length > 0 && !states.includes(state)) return false;
  const needle = query.trim().toLowerCase();
  if (needle === "") return true;
  return (
    instance.vm_id.toLowerCase().includes(needle) ||
    instance.last_event.node_id.toLowerCase().includes(needle) ||
    (instance.latest_run?.node_id.toLowerCase().includes(needle) ?? false) ||
    instanceStates[state].label.toLowerCase().includes(needle) ||
    state.includes(needle)
  );
}

export async function fetchInstancePage(
  query: InstancePageQuery,
  signal?: AbortSignal,
): Promise<InstancePage> {
  const range = { from: query.from, to: query.to };
  const text = query.query.trim();

  if (isExactId(text)) {
    try {
      const instance = await getInstance(text, signal);
      const keep =
        query.states.length === 0 ||
        matchesQuery(instance, "", query.states, query.now);
      return {
        items: keep ? [instance] : [],
        next: null,
        scanned: 1,
        capped: false,
        direct: true,
      };
    } catch (error) {
      if (error instanceof ApiError && error.status === 404)
        return {
          items: [],
          next: null,
          scanned: 0,
          capped: false,
          direct: true,
        };
      throw error;
    }
  }

  if (text === "" && query.states.length === 0) {
    const page = await listInstances(range, query.after ?? undefined, signal);
    return {
      items: page.items,
      next: page.items.length < PAGE_SIZE ? null : page.next,
      scanned: page.items.length,
      capped: false,
      direct: false,
    };
  }

  const items: Instance[] = [];
  let cursor = query.after ?? undefined;
  let scanned = 0;
  let pages = 0;
  let next: string | null = null;
  let capped = false;
  while (items.length < PAGE_SIZE) {
    if (pages >= SCAN_PAGE_CAP) {
      capped = true;
      next = cursor ?? null;
      break;
    }
    const page = await listInstances(range, cursor, signal);
    pages += 1;
    scanned += page.items.length;
    for (const instance of page.items) {
      if (matchesQuery(instance, text, query.states, query.now)) {
        items.push(instance);
        if (items.length >= PAGE_SIZE) break;
      }
    }
    if (items.length >= PAGE_SIZE) {
      next = page.next;
      break;
    }
    if (page.items.length < PAGE_SIZE || !page.next) {
      next = null;
      break;
    }
    cursor = page.next;
  }
  return { items, next, scanned, capped, direct: false };
}
