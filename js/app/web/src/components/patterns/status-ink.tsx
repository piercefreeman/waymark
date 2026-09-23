import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/cn";
import {
  instanceStates,
  promiseStates,
  toneBackground,
  toneText,
  type InstanceState,
  type PromiseState,
  type Tone,
} from "@/domain/status";
import { Tooltip, TooltipContent, TooltipTrigger } from "../ui/tooltip";

/**
 * Status as ink: a glyph and colored text, never a pill. Color is never the
 * only signal; the label is always present (visually or for screen readers).
 */

const inkVariants = cva("inline-flex items-center gap-1.5 whitespace-nowrap", {
  variants: {
    size: {
      sm: "text-micro",
      md: "text-label",
    },
  },
  defaultVariants: { size: "md" },
});

export function StatusInk({
  tone,
  label,
  icon: Icon,
  pulse = false,
  size,
  hideLabel = false,
  className,
}: {
  tone: Tone;
  label: string;
  icon?: React.ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
  pulse?: boolean;
  hideLabel?: boolean;
  className?: string;
} & VariantProps<typeof inkVariants>) {
  return (
    <span className={cn(inkVariants({ size }), toneText[tone], className)}>
      {Icon ? (
        <Icon className="size-3" aria-hidden />
      ) : (
        <StatusDot tone={tone} pulse={pulse} />
      )}
      <span className={cn("font-medium", hideLabel && "sr-only")}>{label}</span>
    </span>
  );
}

export function StatusDot({
  tone,
  pulse = false,
  className,
}: {
  tone: Tone;
  pulse?: boolean;
  className?: string;
}) {
  return (
    <span
      aria-hidden
      className={cn(
        "relative inline-flex size-1.5 shrink-0 rounded-full",
        toneBackground[tone],
        className,
      )}
    >
      {pulse && (
        <span
          className={cn(
            "absolute inset-0 animate-ping rounded-full opacity-60 motion-reduce:hidden",
            toneBackground[tone],
          )}
        />
      )}
    </span>
  );
}

/** Instance state with the derivation rule available on hover. */
export function InstanceStateInk({
  state,
  size,
  className,
}: {
  state: InstanceState;
  size?: "sm" | "md";
  className?: string;
}) {
  const meta = instanceStates[state];
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className={cn("inline-flex", className)} tabIndex={0}>
          <StatusInk
            tone={meta.tone}
            label={meta.label}
            icon={meta.icon}
            size={size}
          />
        </span>
      </TooltipTrigger>
      <TooltipContent>
        <span className="text-fg-muted">Rule: </span>
        {meta.rule}
      </TooltipContent>
    </Tooltip>
  );
}

export function PromiseStateInk({
  state,
  size,
  className,
}: {
  state: PromiseState;
  size?: "sm" | "md";
  className?: string;
}) {
  const meta = promiseStates[state];
  return (
    <StatusInk
      tone={meta.tone}
      label={meta.label}
      size={size}
      pulse={state === "open"}
      className={className}
    />
  );
}
