import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dashboard talks to the API through a relative /api and /ws path, so the
// same build works in dev (proxied here), in docker-compose (Caddy proxies) and
// behind any reverse proxy without rebuilding. Override with VITE_API_URL /
// VITE_WS_URL only when the API lives on another host.
const apiTarget = process.env.VITE_PROXY_TARGET ?? "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // httpOnly: the websocket upgrade must not be re-written by the REST proxy.
      "/api": { target: apiTarget, changeOrigin: true },
      "/ws": { target: apiTarget, changeOrigin: true, ws: true },
    },
  },
  build: { outDir: "dist", sourcemap: true },
});
