import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  publicDir: false,
  server: {
    proxy: {
      "/api": "http://127.0.0.1:24119",
      "/healthz": "http://127.0.0.1:24119",
    },
  },
});
