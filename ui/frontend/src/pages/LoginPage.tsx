import { useQuery } from "@tanstack/react-query"
import { Navigate, useSearchParams } from "react-router"
import { getAuthProviders } from "@/api/client"
import { useAuth } from "@/contexts/AuthContext"
import { LogIn, AlertCircle } from "lucide-react"

export function LoginPage() {
  const { isAuthenticated, authMode, isLoading: authLoading } = useAuth()
  const [searchParams] = useSearchParams()

  const error = searchParams.get("error")
  const errorMessage = searchParams.get("message")

  const {
    data: providers,
    isLoading,
    error: fetchError,
    refetch,
  } = useQuery({
    queryKey: ["auth", "providers"],
    queryFn: getAuthProviders,
    enabled: authMode === "oauth",
  })

  if (authLoading) {
    return (
      <main className="flex items-center justify-center h-screen bg-gray-50 dark:bg-gray-950">
        <div role="status" aria-live="polite">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900 dark:border-gray-100" />
          <span className="sr-only">Loading</span>
        </div>
      </main>
    )
  }

  if (authMode !== "oauth" || isAuthenticated) {
    return <Navigate to="/dashboard" replace />
  }

  return (
    <main className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-gray-950">
      <div className="w-full max-w-sm">
        <div className="bg-white dark:bg-gray-900 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
          <div className="text-center mb-6">
            <h1 className="text-xl font-bold tracking-tight text-gray-900 dark:text-gray-100">asynqpg</h1>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">Sign in to continue</p>
          </div>

          {error && (
            <div role="alert" className="mb-4 rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-950 p-3 flex items-start gap-2">
              <AlertCircle className="h-4 w-4 text-red-500 mt-0.5 flex-shrink-0" />
              <div className="text-sm text-red-700 dark:text-red-400">
                {error === "invalid_state" && "Authentication failed. Please try again."}
                {error === "provider_error" && (errorMessage || "Provider authentication failed.")}
                {error !== "invalid_state" && error !== "provider_error" && `Error: ${error}`}
              </div>
            </div>
          )}

          {isLoading && (
            <div role="status" aria-live="polite" className="flex justify-center py-6">
              <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-gray-400" />
              <span className="sr-only">Loading</span>
            </div>
          )}

          {fetchError && (
            <div className="text-center py-4">
              <p className="text-sm text-red-600 dark:text-red-400 mb-2">Failed to load providers</p>
              <button
                onClick={() => refetch()}
                className="text-sm text-blue-600 dark:text-blue-400 hover:underline"
              >
                Try again
              </button>
            </div>
          )}

          {providers && providers.length === 0 && (
            <p className="text-sm text-gray-500 dark:text-gray-400 text-center py-4">
              No authentication providers configured.
            </p>
          )}

          {providers && providers.length > 0 && (
            <div className="space-y-3">
              {providers.map((p) => (
                <a
                  key={p.id}
                  href={p.login_url}
                  className="flex items-center justify-center gap-2 w-full px-4 py-2.5 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
                >
                  {p.icon_url ? (
                    <img src={p.icon_url} alt="" className="h-5 w-5" />
                  ) : (
                    <LogIn className="h-4 w-4" />
                  )}
                  Sign in with {p.name}
                </a>
              ))}
            </div>
          )}
        </div>

        <p className="text-xs text-gray-400 dark:text-gray-400 text-center mt-4">
          Powered by asynqpg
        </p>
      </div>
    </main>
  )
}
