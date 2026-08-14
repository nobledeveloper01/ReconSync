import { defineConfig } from "vite";

export default defineConfig({
  // Built into the Go binary and served from the same origin, so assets are
  // referenced relatively rather than from an absolute root.
  base: "./",
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // Content-hashed filenames, which is what makes the immutable caching the
    // server sends on /assets/ correct. Pinning these to app.js and app.css
    // was a real bug: with a fixed name and a year-long immutable cache, a
    // deploy leaves every browser running the old app forever. It cost an hour
    // of wondering why a CSS fix would not appear.
    rollupOptions: {
      output: {
        entryFileNames: "assets/[name]-[hash].js",
        chunkFileNames: "assets/[name]-[hash].js",
        assetFileNames: "assets/[name]-[hash].[ext]",
      },
    },
  },
  server: {
    // `npm run dev` proxies the API to a locally running ReconSync, so the
    // dashboard behaves in development exactly as it does when embedded.
    proxy: {
      "/v1": "http://127.0.0.1:8080",
      "/healthz": "http://127.0.0.1:8080",
      "/metrics": "http://127.0.0.1:8080",
    },
  },
});
