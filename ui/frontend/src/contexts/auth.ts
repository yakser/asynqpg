import { createContext, useContext } from "react"
import type { AuthUser } from "@/api/types"

export interface AuthContextValue {
  user: AuthUser | null
  isLoading: boolean
  isAuthenticated: boolean
  authMode: "none" | "basic" | "oauth"
  logout: () => void
}

export const AuthContext = createContext<AuthContextValue>({
  user: null,
  isLoading: true,
  isAuthenticated: false,
  authMode: "none",
  logout: () => {},
})

export function useAuth(): AuthContextValue {
  return useContext(AuthContext)
}
