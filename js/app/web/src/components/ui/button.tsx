import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { Slot } from "radix-ui";
import { cn } from "@/lib/cn";

const buttonVariants = cva(
  "inline-flex shrink-0 items-center justify-center gap-1.5 whitespace-nowrap rounded-control border font-medium transition-colors duration-fast disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-3.5",
  {
    variants: {
      variant: {
        default:
          "border-accent bg-accent text-fg-on-accent hover:border-accent-hover hover:bg-accent-hover",
        outline:
          "border-line-strong bg-surface text-fg hover:bg-surface-raised",
        ghost:
          "border-transparent text-fg-muted hover:bg-surface-raised hover:text-fg",
        danger: "border-danger/40 bg-danger/10 text-danger hover:bg-danger/20",
      },
      size: {
        default: "h-control px-2.5 text-label",
        sm: "h-6 px-2 text-micro",
        icon: "size-control",
        "icon-sm": "size-6 [&_svg:not([class*='size-'])]:size-3",
      },
    },
    defaultVariants: {
      variant: "outline",
      size: "default",
    },
  },
);

function Button({
  className,
  variant,
  size,
  asChild = false,
  ...props
}: React.ComponentProps<"button"> &
  VariantProps<typeof buttonVariants> & { asChild?: boolean }) {
  const Comp = asChild ? Slot.Root : "button";
  return (
    <Comp
      data-slot="button"
      className={cn(buttonVariants({ variant, size }), className)}
      {...props}
    />
  );
}

export { Button, buttonVariants };
