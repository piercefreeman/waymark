import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./app";
import { readStoredTheme } from "./providers/theme";
import "./style.css";

document.documentElement.dataset.theme = readStoredTheme();

ReactDOM.createRoot(document.getElementById("app")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
