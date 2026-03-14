import { createContext, useContext, useCallback, type ReactNode } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { Navigate } from "react-router"
import { getConfig, getMe, logout as logoutAPI } from "@/api/client"
import type { AuthUser } from "@/api/types"

interface AuthContextValue {
  user: AuthUser | null
  isLoading: boolean
  isAuthenticated: boolean
  authMode: "none" | "basic" | "oauth"
  logout: () => void
}

const AuthContext = createContext<AuthContextValue>({
  user: null,
  isLoading: true,
  isAuthenticated: false,
  authMode: "none",
  logout: () => {},
})

export function useAuth(): AuthContextValue {
  return useContext(AuthContext)
}

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
      <main className="flex items-center justify-center h-screen">
        <div role="status" aria-live="polite">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900 dark:border-gray-100" />
          <span className="sr-only">Loading</span>
        </div>
      </main>
    )
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  return <>{children}</>
}
