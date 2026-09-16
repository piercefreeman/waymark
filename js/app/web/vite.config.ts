import { defineConfig } from "vite";

export default defineConfig({
  publicDir: false,
  server: {
    proxy: {
      "/api": "http://127.0.0.1:24119",
      "/healthz": "http://127.0.0.1:24119",
    },
  },
});
