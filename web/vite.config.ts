import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const environment = loadEnv(mode, ".", "RUNLOOM_");
  return {
    plugins: [svelte()],
    server: {
      host: "127.0.0.1",
      port: 5173,
      proxy: {
        "/api": environment.RUNLOOM_DEV_PROXY || "http://127.0.0.1:8787",
      },
    },
  };
});
