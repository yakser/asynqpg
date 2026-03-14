import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { BrowserRouter } from "react-router"
import App from "./App"
import { ThemeProvider } from "./contexts/ThemeContext"
import "./index.css"

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 2_000,
      refetchIntervalInBackground: false,
    },
  },
})

// Cancel in-flight requests when navigating away so the page can enter
// the back/forward cache (bfcache). Without this, active fetch connections
// prevent bfcache restoration.
window.addEventListener("pagehide", () => {
  queryClient.cancelQueries()
})

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <ThemeProvider>
          <App />
        </ThemeProvider>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
)
