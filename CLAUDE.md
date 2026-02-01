# Rappel

## UI Design Conventions

Follow a modern, developer-focused design language. The design prioritizes clarity, information density, and professional polish.

### Theme & Color System

**Dual Theme Support**
- Every component must support both light and dark modes
- Dark mode: Near-black backgrounds (#0a0a0a to #1a1a1a), light text
- Light mode: White/off-white backgrounds, dark text
- Use CSS variables or Tailwind's dark: prefix for all color values

**Semantic Color Palette**
- **Green** (#22c55e / emerald): Success, completed, active states, running processes
- **Blue** (#3b82f6): Primary actions, parent workflows, links, interactive elements
- **Yellow/Amber** (#eab308): Waiting, pending, in-progress states
- **Red** (#ef4444): Errors, failures, destructive actions
- **Gray** (#6b7280): Secondary text, metadata, timestamps, disabled states

### Visual Elements

**Background Treatment**
- Use subtle vertical or grid lines on dark backgrounds for depth and structure
- Lines should be very low contrast (e.g., #1f1f1f on #0a0a0a)
- Diagonal hatching patterns for "idle" or inactive regions

**Cards & Containers**
- Minimize card usage - prefer flat layouts with subtle borders
- When cards are needed: thin 1px borders, no shadows in dark mode
- Light mode cards: subtle shadows allowed, clean white backgrounds
- Border radius: consistent rounded-lg (8px) or rounded-xl (12px)

**Glassmorphism (Floating Elements Only)**
- Apply to modals, dropdowns, popovers, and floating UI
- Use backdrop-blur with semi-transparent backgrounds
- Dark mode: rgba(0,0,0,0.8) with backdrop-blur-lg
- Light mode: rgba(255,255,255,0.9) with backdrop-blur-lg

### Component Patterns

**Status Badges/Pills**
- Rounded-full pill shape with semantic border colors
- Transparent or semi-transparent fill with colored border
- Include status dot indicator when appropriate
- Example: Green border + "Completed" text for success states

**Timeline/Waterfall Visualizations**
- Horizontal bars showing duration and timing
- Color-coded by status (green=success, blue=running, yellow=waiting)
- Show function names and durations inline
- Use grid lines to indicate time intervals

**Code & Technical Text**
- Monospace font (font-mono) for: IDs, function names, code snippets, durations
- Syntax highlighting in code blocks with muted, readable colors
- Inline code: subtle background tint with rounded corners

**Typography Hierarchy**
- Headlines: Bold, larger size, high contrast
- Body text: Regular weight, comfortable reading size
- Metadata: Smaller size, muted gray color
- Use font-medium sparingly for emphasis

### Layout Principles

**Spacing**
- Generous whitespace between sections
- Consistent padding within components (p-4, p-6)
- Use gap utilities for flex/grid layouts

**Information Density**
- Dense data displays (tables, timelines) are acceptable
- Balance density with clear visual hierarchy
- Group related information visually

**Responsive Behavior**
- Mobile-first approach
- Stack horizontal layouts vertically on small screens
- Maintain readability at all breakpoints

### Interaction States

**Hover**
- Subtle background color shift
- Don't rely on hover for essential information

**Focus**
- Clear focus rings for accessibility
- Use ring-2 with brand/accent color

**Active/Selected**
- Distinct visual treatment from hover
- Consider using filled backgrounds instead of just borders
