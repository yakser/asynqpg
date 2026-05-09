import { useQuery } from "@tanstack/react-query"
import { Navigate, useSearchParams } from "react-router"
import { getAuthProviders } from "@/api/client"
import { useAuth } from "@/contexts/auth"
import { Icon } from "@/components/Icon"

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
        loading…
      </main>
    )
  }

  if (authMode !== "oauth" || isAuthenticated) {
    return <Navigate to="/overview" replace />
  }

  return (
    <main
      style={{
        display: "grid",
        placeItems: "center",
        minHeight: "100vh",
        background: "var(--bg)",
        padding: 24,
      }}
    >
      <div style={{ width: "100%", maxWidth: 360 }}>
        <div
          style={{
            background: "var(--surface-1)",
            border: "1px solid var(--border)",
            borderRadius: "var(--r-3)",
            padding: 24,
          }}
        >
          <div style={{ textAlign: "center", marginBottom: 20 }}>
            <img
              src="/assets/logo-mark.svg"
              alt=""
              style={{ width: 28, height: 28, color: "var(--brand-600)" }}
            />
            <h1
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: 18,
                fontWeight: 600,
                margin: "10px 0 4px",
                color: "var(--fg-1)",
              }}
            >
              asynq<span style={{ color: "var(--brand-600)" }}>pg</span>
            </h1>
            <p style={{ fontSize: 12.5, color: "var(--fg-3)", margin: 0, fontFamily: "var(--font-mono)" }}>
              sign in to continue
            </p>
          </div>

          {error && (
            <div
              role="alert"
              style={{
                marginBottom: 14,
                padding: "10px 12px",
                borderRadius: "var(--r-2)",
                border: "1px solid color-mix(in oklch, var(--status-failed) 30%, transparent)",
                background: "var(--status-failed-bg)",
                color: "var(--status-failed)",
                fontSize: 12.5,
                fontFamily: "var(--font-mono)",
                display: "flex",
                gap: 8,
                alignItems: "flex-start",
              }}
            >
              <Icon name="x-circle" size={14} />
              <span>
                {error === "invalid_state" && "authentication failed. please try again."}
                {error === "provider_error" && (errorMessage || "provider authentication failed.")}
                {error !== "invalid_state" &&
                  error !== "provider_error" &&
                  `error: ${error}`}
              </span>
            </div>
          )}

          {isLoading && (
            <div
              role="status"
              aria-live="polite"
              style={{ textAlign: "center", padding: "16px 0", color: "var(--fg-3)", fontSize: 13 }}
            >
              loading providers…
            </div>
          )}

          {fetchError && (
            <div style={{ textAlign: "center", padding: "12px 0" }}>
              <p style={{ fontSize: 13, color: "var(--status-failed)", margin: "0 0 6px" }}>
                failed to load providers
              </p>
              <button
                onClick={() => refetch()}
                className="link-btn"
                type="button"
              >
                try again
              </button>
            </div>
          )}

          {providers && providers.length === 0 && (
            <p
              style={{
                fontSize: 13,
                color: "var(--fg-3)",
                textAlign: "center",
                padding: "12px 0",
                fontFamily: "var(--font-mono)",
              }}
            >
              no authentication providers configured.
            </p>
          )}

          {providers && providers.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {providers.map((p) => (
                <a key={p.id} href={p.login_url} className="btn lg" style={{ justifyContent: "center" }}>
                  {p.icon_url ? (
                    <img src={p.icon_url} alt="" style={{ width: 18, height: 18 }} />
                  ) : (
                    <Icon name="user" size={14} />
                  )}
                  Sign in with {p.name}
                </a>
              ))}
            </div>
          )}
        </div>

        <p
          style={{
            fontSize: 11,
            color: "var(--fg-3)",
            textAlign: "center",
            marginTop: 12,
            fontFamily: "var(--font-mono)",
          }}
        >
          powered by asynqpg
        </p>
      </div>
    </main>
  )
}
