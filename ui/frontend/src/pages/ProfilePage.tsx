import { useAuth } from "@/contexts/auth"
import { useTheme, type Theme } from "@/contexts/theme"
import { Button } from "@/components/Button"
import { Icon } from "@/components/Icon"

const themeOptions: Array<{ value: Theme; label: string; icon: string }> = [
  { value: "light", label: "Light", icon: "sun" },
  { value: "dark", label: "Dark", icon: "moon" },
  { value: "system", label: "System", icon: "monitor" },
]

export function ProfilePage() {
  const { user, logout, authMode } = useAuth()
  const { theme, setTheme } = useTheme()

  const initials = user?.name
    ? user.name
        .split(" ")
        .map((n) => n[0])
        .slice(0, 2)
        .join("")
        .toUpperCase()
    : "?"

  return (
    <div>
      <div className="page-head">
        <div>
          <h1>Profile</h1>
          <div className="sub">
            {user
              ? <>signed in as <code style={{ color: "var(--fg-2)" }}>{user.email || user.id}</code></>
              : "no signed-in user · running without authentication"}
          </div>
        </div>
      </div>

      {user && (
        <div className="section-block">
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 16,
              border: "1px solid var(--border)",
              borderRadius: "var(--r-3)",
              padding: 18,
              background: "var(--surface-1)",
            }}
          >
            {user.avatar_url ? (
              <img
                src={user.avatar_url}
                alt={user.name}
                style={{ width: 56, height: 56, borderRadius: "50%" }}
              />
            ) : (
              <div
                style={{
                  width: 56,
                  height: 56,
                  borderRadius: "50%",
                  display: "grid",
                  placeItems: "center",
                  background: "color-mix(in oklch, var(--brand-600) 18%, var(--surface-2))",
                  color: "var(--brand-600)",
                  fontFamily: "var(--font-mono)",
                  fontSize: 18,
                  fontWeight: 600,
                  border: "1px solid color-mix(in oklch, var(--brand-600) 30%, transparent)",
                }}
              >
                {initials}
              </div>
            )}
            <div>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: 15, color: "var(--fg-1)", fontWeight: 500 }}>
                {user.name || user.email || user.id}
              </div>
              {user.email && (
                <div style={{ fontFamily: "var(--font-mono)", fontSize: 12.5, color: "var(--fg-3)", marginTop: 2 }}>
                  {user.email}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {user && (
        <div className="section-block">
          <div className="h2">
            <h2>Identity</h2>
          </div>
          <div
            style={{
              border: "1px solid var(--border)",
              borderRadius: "var(--r-3)",
              overflow: "hidden",
              background: "var(--bg)",
            }}
          >
            <ProfileRow k="provider" v={user.provider} />
            <ProfileRow k="user id" v={user.id} last />
          </div>
        </div>
      )}

      <div className="section-block">
        <div className="h2">
          <h2>Appearance</h2>
          <span className="sub">theme follows system preference when set to system</span>
        </div>
        <div className="theme-segmented" role="group" aria-label="Theme selection">
          {themeOptions.map(({ value, label, icon }) => (
            <button
              key={value}
              type="button"
              className="seg"
              onClick={() => setTheme(value)}
              aria-pressed={theme === value}
            >
              <Icon name={icon} size={14} />
              {label}
            </button>
          ))}
        </div>
      </div>

      {authMode === "oauth" && user && (
        <div className="section-block" style={{ display: "flex", gap: 8 }}>
          <Button variant="danger" onClick={logout} icon="x-circle">
            Log out
          </Button>
        </div>
      )}
    </div>
  )
}

function ProfileRow({ k, v, last }: { k: string; v: string; last?: boolean }) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        padding: "10px 14px",
        borderBottom: last ? "none" : "1px solid var(--border)",
        fontSize: 12.5,
      }}
    >
      <span style={{ color: "var(--fg-3)", fontFamily: "var(--font-mono)" }}>{k}</span>
      <span style={{ color: "var(--fg-1)", fontFamily: "var(--font-mono)", fontWeight: 500 }}>{v}</span>
    </div>
  )
}
