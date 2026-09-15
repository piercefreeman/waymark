import { defineConfig } from "vite";
import { viteSingleFile } from "vite-plugin-singlefile";

export default defineConfig({
  plugins: [viteSingleFile()],
  publicDir: false,
  server: {
    proxy: {
      "/api": "http://127.0.0.1:24119",
      "/healthz": "http://127.0.0.1:24119",
    },
  },
});
