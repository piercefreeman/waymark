import { cn } from "@/lib/cn";
import { toneBackground, toneText, type Tone } from "@/domain/status";

export interface FilterChipOption<Value extends string> {
  value: Value;
  label: string;
  count?: number;
  tone?: Tone;
}

/**
 * Multi-select toggles with counts. Counts are page-local unless the caller
 * says otherwise, so the scope caption sits next to the group.
 */
export function FilterChips<Value extends string>({
  options,
  value,
  onChange,
  scope,
  label,
  className,
}: {
  options: FilterChipOption<Value>[];
  value: Value[];
  onChange: (next: Value[]) => void;
  scope?: string;
  label: string;
  className?: string;
}) {
  function toggle(option: Value) {
    onChange(
      value.includes(option)
        ? value.filter((item) => item !== option)
        : [...value, option],
    );
  }
  return (
    <div
      role="group"
      aria-label={label}
      className={cn("flex flex-wrap items-center gap-1", className)}
    >
      {options.map((option) => {
        const active = value.includes(option.value);
        const tone = option.tone ?? "neutral";
        return (
          <button
            key={option.value}
            type="button"
            aria-pressed={active}
            onClick={() => toggle(option.value)}
            className={cn(
              "inline-flex h-6 items-center gap-1.5 rounded-control border px-2 text-micro transition-colors duration-fast",
              active
                ? "border-line-strong bg-surface-raised text-fg"
                : "border-transparent text-fg-muted hover:bg-surface-raised hover:text-fg",
            )}
          >
            <span
              aria-hidden
              className={cn(
                "size-1.5 rounded-full",
                toneBackground[tone],
                !active && "opacity-50",
              )}
            />
            {option.label}
            {option.count !== undefined && (
              <span
                className={cn(
                  "mono-data",
                  active ? toneText[tone] : "text-fg-subtle",
                )}
              >
                {option.count}
              </span>
            )}
          </button>
        );
      })}
      {scope && <span className="ml-1 text-micro text-fg-subtle">{scope}</span>}
    </div>
  );
}
