import { clsx, type ClassValue } from "clsx";
import { extendTailwindMerge } from "tailwind-merge";

// Custom font-size tokens must be registered or tailwind-merge treats an
// unknown `text-*` class as a color and drops one of the pair.
const twMerge = extendTailwindMerge({
  extend: {
    classGroups: {
      "font-size": [
        "text-title",
        "text-section",
        "text-body",
        "text-label",
        "text-micro",
        "text-metric",
      ],
    },
  },
});

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
