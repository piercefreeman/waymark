import type { LucideIcon } from "lucide-react";
import {
  Ban,
  Check,
  CircleDashed,
  Hourglass,
  Play,
  TriangleAlert,
  X,
} from "lucide-react";

/** Semantic color roles. Every colored element resolves to one of these. */
export type Tone = "running" | "success" | "waiting" | "danger" | "neutral";

/**
 * Instance state as it can be derived from observations. There is no
 * "failing": a rejected action may be caught, so rejections are counted
 * separately and never promoted to a workflow state.
 */
export type InstanceState =
  | "active"
  | "suspended"
  | "completed"
  | "unhandled_exception"
  | "run_error"
  | "cancelled"
  | "unknown";

export interface StateMeta {
  label: string;
  tone: Tone;
  icon: LucideIcon;
  /** The rule that produced this state, shown on hover. */
  rule: string;
}

export const instanceStates: Record<InstanceState, StateMeta> = {
  active: {
    label: "Active",
    tone: "running",
    icon: Play,
    rule: "Latest driver run has not stopped and its last event is fresh.",
  },
  suspended: {
    label: "Suspended",
    tone: "waiting",
    icon: Hourglass,
    rule: "Driver stopped with no ready frames; the VM is waiting on open promises or a sleep.",
  },
  completed: {
    label: "Completed",
    tone: "success",
    icon: Check,
    rule: "Terminal outcome `complete` was observed.",
  },
  unhandled_exception: {
    label: "Unhandled exception",
    tone: "danger",
    icon: X,
    rule: "Terminal outcome `unhandled_exception` was observed.",
  },
  run_error: {
    label: "Run error",
    tone: "danger",
    icon: TriangleAlert,
    rule: "Latest driver run stopped with an error. The workflow itself has no terminal outcome and may be revived.",
  },
  cancelled: {
    label: "Cancelled",
    tone: "neutral",
    icon: Ban,
    rule: "Latest driver run was cancelled. This does not prove the workflow was cancelled.",
  },
  unknown: {
    label: "Unknown",
    tone: "neutral",
    icon: CircleDashed,
    rule: "No stop or outcome was observed and the last event is older than the freshness threshold.",
  },
};

/** Order used for grouping and default sort: trouble first, then live, then settled. */
export const instanceStateOrder: InstanceState[] = [
  "unhandled_exception",
  "run_error",
  "active",
  "suspended",
  "cancelled",
  "unknown",
  "completed",
];

export type PromiseState = "open" | "resolved" | "rejected";

export const promiseStates: Record<
  PromiseState,
  { label: string; tone: Tone }
> = {
  open: { label: "Open", tone: "running" },
  resolved: { label: "Resolved", tone: "success" },
  rejected: { label: "Rejected", tone: "danger" },
};

export const toneText: Record<Tone, string> = {
  running: "text-running",
  success: "text-success",
  waiting: "text-waiting",
  danger: "text-danger",
  neutral: "text-neutral",
};

export const toneBackground: Record<Tone, string> = {
  running: "bg-running",
  success: "bg-success",
  waiting: "bg-waiting",
  danger: "bg-danger",
  neutral: "bg-neutral",
};

export const toneBorder: Record<Tone, string> = {
  running: "border-running",
  success: "border-success",
  waiting: "border-waiting",
  danger: "border-danger",
  neutral: "border-neutral",
};

export const toneVariable: Record<Tone, string> = {
  running: "var(--running)",
  success: "var(--success)",
  waiting: "var(--waiting)",
  danger: "var(--danger)",
  neutral: "var(--neutral)",
};
