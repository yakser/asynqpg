import { Fragment, useMemo } from "react"
import { useQueryClient } from "@tanstack/react-query"
import { Link, useLocation, useSearchParams } from "react-router"
import { Icon } from "@/components/Icon"
import { IconButton } from "@/components/Button"
import { useAuth } from "@/contexts/auth"

interface TopbarProps {
  onCmdK: () => void
  onToggleNav?: () => void
}

type ScreenId = "overview" | "tasks" | "workers" | "maintenance"

function pathnameToScreen(pathname: string): ScreenId | null {
  if (pathname.startsWith("/overview")) return "overview"
  if (pathname.startsWith("/tasks")) return "tasks"
  if (pathname.startsWith("/workers")) return "workers"
  if (pathname.startsWith("/maintenance")) return "maintenance"
  return null
}

function buildCrumbs(screen: ScreenId | null, typeFromQuery: string | null): string[] {
  if (screen === "overview") return ["Overview"]
  if (screen === "tasks") return ["Tasks", typeFromQuery || "all queues"]
  if (screen === "workers") return ["Workers"]
  if (screen === "maintenance") return ["Maintenance"]
  return []
}

function initialsFromName(name: string): string {
  const parts = name.trim().split(/\s+/).filter(Boolean)
  if (parts.length === 0) return ""
  const letters = parts.slice(0, 2).map((p) => p.charAt(0).toUpperCase())
  return letters.join("")
}

interface EnvPillInfo {
  label: string
  className: string
  showDot: boolean
}

function envPillInfo(label: string | undefined): EnvPillInfo | null {
  if (!label) return null
  const trimmed = label.trim()
  if (!trimmed) return null
  const lower = trimmed.toLowerCase()
  if (lower.includes("prod")) {
    return { label: trimmed, className: "env-pill prod", showDot: true }
  }
  if (lower.includes("stag")) {
    return { label: trimmed, className: "env-pill staging", showDot: false }
  }
  return { label: trimmed, className: "env-pill", showDot: false }
}

export function Topbar({ onCmdK, onToggleNav }: TopbarProps): React.JSX.Element {
  const location = useLocation()
  const [searchParams] = useSearchParams()
  const { user } = useAuth()
  const queryClient = useQueryClient()

  const screen = pathnameToScreen(location.pathname)
  const typeFromQuery = searchParams.get("type")
  const crumbs = useMemo(() => buildCrumbs(screen, typeFromQuery), [screen, typeFromQuery])

  const envLabel = (import.meta.env.VITE_ENV_LABEL as string | undefined) ?? undefined
  const envInfo = envPillInfo(envLabel)

  const initials = user?.name ? initialsFromName(user.name) : ""

  return (
    <header className="topbar" role="banner">
      {onToggleNav && (
        <button
          type="button"
          className="menu-toggle"
          onClick={onToggleNav}
          aria-label="Open navigation menu"
        >
          <Icon name="list" size={18} />
        </button>
      )}

      <nav className="crumbs" aria-label="Breadcrumb">
        {crumbs.map((c, i) => (
          <Fragment key={i}>
            {i > 0 && (
              <span className="sep" aria-hidden="true">
                /
              </span>
            )}
            <span
              className={i === crumbs.length - 1 ? "seg" : "muted"}
              style={i > 0 ? { fontFamily: "var(--font-mono)", fontSize: 12.5 } : undefined}
              aria-current={i === crumbs.length - 1 ? "page" : undefined}
            >
              {c}
            </span>
          </Fragment>
        ))}
      </nav>

      <span style={{ width: 12 }} aria-hidden="true" />

      {envInfo && (
        <span className={envInfo.className} aria-label={`Environment: ${envInfo.label}`}>
          {envInfo.showDot && <span className="dot s-failed" aria-hidden="true" />}
          <span className="label">env</span>
          {envInfo.label}
        </span>
      )}

      <button
        type="button"
        className="search-bar"
        onClick={onCmdK}
        aria-label="Open command palette"
      >
        <Icon name="search" size={14} />
        <span className="ph">Search tasks, types, workers…</span>
        <kbd aria-hidden="true">⌘K</kbd>
      </button>

      <span className="spacer" aria-hidden="true" />

      <IconButton
        icon="refresh-cw"
        title="Refresh (R)"
        aria-label="Refresh"
        onClick={() => queryClient.invalidateQueries()}
      />

      <span style={{ width: 8 }} aria-hidden="true" />

      <Link
        to="/profile"
        title={user?.email ?? user?.name ?? "Profile"}
        aria-label={user?.name ? `Profile: ${user.name}` : "Profile"}
        style={{
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          width: 28,
          height: 28,
          borderRadius: 999,
          textDecoration: "none",
          overflow: "hidden",
          flex: "0 0 auto",
          ...(user?.avatar_url
            ? { padding: 0, border: "1px solid var(--border)" }
            : user && initials
              ? {
                  background: "color-mix(in oklch, var(--brand-600) 18%, var(--surface-1))",
                  color: "var(--brand-600)",
                  fontFamily: "var(--font-mono)",
                  fontSize: 12,
                  fontWeight: 600,
                  border: "1px solid color-mix(in oklch, var(--brand-600) 30%, transparent)",
                }
              : {
                  background: "var(--surface-1)",
                  color: "var(--fg-3)",
                  border: "1px solid var(--border)",
                }),
        }}
      >
        {user?.avatar_url ? (
          <img
            src={user.avatar_url}
            alt=""
            style={{ width: "100%", height: "100%", objectFit: "cover" }}
            referrerPolicy="no-referrer"
          />
        ) : user && initials ? (
          initials.toLowerCase()
        ) : (
          <Icon name="user" size={14} />
        )}
      </Link>
    </header>
  )
}
