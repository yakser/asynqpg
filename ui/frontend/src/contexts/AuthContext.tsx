import { useCallback, type ReactNode } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { Navigate } from "react-router"
import { getConfig, getMe, logout as logoutAPI } from "@/api/client"
import { AuthContext, useAuth } from "./auth"

export function AuthProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient()

  const { data: config, isLoading: configLoading } = useQuery({
    queryKey: ["config"],
    queryFn: getConfig,
    staleTime: 60_000,
  })

  const authMode = config?.auth_mode ?? "none"

  const { data: user, isLoading: userLoading } = useQuery({
    queryKey: ["auth", "me"],
    queryFn: getMe,
    enabled: authMode === "oauth",
    staleTime: 30_000,
    retry: false,
  })

  const isLoading = configLoading || (authMode === "oauth" && userLoading)
  const isAuthenticated = authMode !== "oauth" || user != null

  const logout = useCallback(() => {
    logoutAPI()
      .catch(() => {})
      .finally(() => {
        queryClient.setQueryData(["auth", "me"], null)
        queryClient.invalidateQueries({ queryKey: ["auth"] })
        window.location.href = "/login"
      })
  }, [queryClient])

  return (
    <AuthContext.Provider value={{ user: user ?? null, isLoading, isAuthenticated, authMode, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function RequireAuth({ children }: { children: ReactNode }) {
  const { isAuthenticated, isLoading, authMode } = useAuth()

  if (authMode !== "oauth") {
    return <>{children}</>
  }

  if (isLoading) {
    return (
      <main
        style={{
          display: "grid",
          placeItems: "center",
          height: "100vh",
          background: "var(--bg)",
          color: "var(--fg-3)",
          fontFamily: "var(--font-mono)",
          fontSize: 13,
        }}
      >
        <div role="status" aria-live="polite">
          loading…
        </div>
      </main>
    )
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  return <>{children}</>
}
