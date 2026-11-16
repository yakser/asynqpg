import path from "path"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(import.meta.dirname, "./src"),
    },
  },
  server: {
    proxy: {
      "/api": "http://localhost:8080",
    },
  },
  build: {
    outDir: "dist",
    // Preserve the browser baseline used before the Vite 8 upgrade.
    target: ["chrome107", "edge107", "firefox104", "safari16"],
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            { name: "vendor-react", test: /node_modules\/(react|react-dom|scheduler)\// },
            { name: "vendor-router", test: /node_modules\/react-router\// },
            { name: "vendor-query", test: /node_modules\/@tanstack\// },
            { name: "vendor-icons", test: /node_modules\/lucide-react\// },
          ],
        },
      },
    },
  },
})
