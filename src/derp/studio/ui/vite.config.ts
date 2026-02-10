import { resolve } from "node:path";

import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  // Use root path in development mode to allow HMR to work.
  base: process.env.NODE_ENV === "development" ? "/" : "/static/",
  build: {
    outDir: resolve(__dirname, "../static"),
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": process.env.PUBLIC_API_URL,
    },
  },
});
