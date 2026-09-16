export type Theme = "dark" | "light";

export function readTheme(): Theme {
  try {
    return localStorage.getItem("waymark-theme") === "light" ? "light" : "dark";
  } catch {
    return "dark";
  }
}

export function applyTheme(theme: Theme) {
  document.documentElement.classList.toggle("dark", theme === "dark");
  try {
    localStorage.setItem("waymark-theme", theme);
  } catch {
    // The selected theme still works when browser storage is unavailable.
  }
}
