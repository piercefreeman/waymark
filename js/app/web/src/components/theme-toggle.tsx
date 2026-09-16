import { useState } from "react";
import { Moon, Sun } from "lucide-react";
import { applyTheme, readTheme } from "../lib/theme";
import { Button } from "./ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "./ui/tooltip";

export function ThemeToggle() {
  const [theme, setTheme] = useState(readTheme);
  const label = `Switch to ${theme === "dark" ? "light" : "dark"} theme`;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label={label}
          onClick={() => {
            const next = theme === "dark" ? "light" : "dark";
            applyTheme(next);
            setTheme(next);
          }}
        >
          {theme === "dark" ? <Sun /> : <Moon />}
        </Button>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  );
}
