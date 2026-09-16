import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./app";
import { readTheme } from "./lib/theme";
import "./style.css";

document.documentElement.classList.toggle("dark", readTheme() === "dark");

ReactDOM.createRoot(document.getElementById("app")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
