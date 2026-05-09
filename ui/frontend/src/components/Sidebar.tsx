import { useMemo, useState } from "react"
import { useNavigate, useLocation, useSearchParams } from "react-router"
import { useQuery } from "@tanstack/react-query"
import { Icon } from "@/components/Icon"
import { fmtN } from "@/lib/format"
import { getConfig, getLeader, getStats } from "@/api/client"
import type { StatsResponse, TaskStatus } from "@/api/types"

type ScreenId = "overview" | "tasks" | "workers" | "maintenance"

interface NavItem {
  id: ScreenId
  icon: string
  label: string
}

interface QueueRow {
  type: string
  pending: number
  running: number
  failed: number
  total: number
}

interface SidebarProps {
  open?: boolean
  onNavigate?: () => void
  /** When true, the sidebar is collapsed off-canvas and must be a11y-inert. */
  collapsed?: boolean
}

const NAV_ITEMS: readonly NavItem[] = [
  { id: "overview", icon: "layout-dashboard", label: "Overview" },
  { id: "tasks", icon: "list", label: "Tasks" },
  { id: "workers", icon: "cpu", label: "Workers" },
  { id: "maintenance", icon: "wrench", label: "Maintenance" },
] as const

function pathnameToScreen(pathname: string): ScreenId | null {
  if (pathname.startsWith("/overview")) return "overview"
  if (pathname.startsWith("/tasks")) return "tasks"
  if (pathname.startsWith("/workers")) return "workers"
  if (pathname.startsWith("/maintenance")) return "maintenance"
  return null
}

function statusCount(byStatus: Record<TaskStatus, number>, status: TaskStatus): number {
  const v = byStatus[status]
  return typeof v === "number" ? v : 0
}

function summarizeStats(stats: StatsResponse | undefined): QueueRow[] {
  if (!stats) return []
  return stats.by_type.map((q) => ({
    type: q.type,
    pending: statusCount(q.by_status, "pending"),
    running: statusCount(q.by_status, "running"),
    failed: statusCount(q.by_status, "failed"),
    total: q.total,
  }))
}

export function Sidebar({ open = false, onNavigate, collapsed = false }: SidebarProps = {}): React.JSX.Element {
  const navigate = useNavigate()
  const location = useLocation()
  const [searchParams] = useSearchParams()
  const [filter, setFilter] = useState("")
  const [showAll, setShowAll] = useState(false)

  const currentScreen = pathnameToScreen(location.pathname)
  const activeType = searchParams.get("type")

  const { data: stats } = useQuery({
    queryKey: ["stats"],
    queryFn: getStats,
    refetchInterval: 5000,
  })

  const { data: leader, isSuccess: leaderOk } = useQuery({
    queryKey: ["leader"],
    queryFn: getLeader,
    refetchInterval: 5000,
  })

  const { data: config } = useQuery({
    queryKey: ["config"],
    queryFn: getConfig,
  })

  const summary = useMemo(() => summarizeStats(stats), [stats])
  const failedTotal = stats ? statusCount(stats.by_status, "failed") : 0

  const filtered = useMemo(() => {
    const f = filter.trim().toLowerCase()
    const list = !f ? summary : summary.filter((q) => q.type.toLowerCase().includes(f))
    return list.slice().sort((a, b) => b.failed - a.failed || b.pending - a.pending)
  }, [summary, filter])

  const visible = showAll || filter ? filtered : filtered.slice(0, 8)
  const hidden = filtered.length - visible.length

  const goTo = (path: string) => {
    navigate(path)
    onNavigate?.()
  }

  return (
    <aside
      className={"sidebar" + (open ? " is-open" : "")}
      aria-label="Primary"
      aria-hidden={collapsed ? "true" : undefined}
      inert={collapsed}
    >
      <div className="brand-row">
        <button
          type="button"
          className="brand-link"
          onClick={() => goTo("/overview")}
          aria-label="asynqpg, go to overview"
        >
          <svg
            className="brand-lockup"
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 360 64"
            aria-hidden="true"
            focusable="false"
          >
            <g fill="currentColor" opacity="0.30">
              <circle cx="16" cy="16" r="3" />
              <circle cx="32" cy="16" r="3" />
              <circle cx="48" cy="16" r="3" />
              <circle cx="16" cy="32" r="3" />
              <circle cx="48" cy="32" r="3" />
              <circle cx="16" cy="48" r="3" />
              <circle cx="32" cy="48" r="3" />
              <circle cx="48" cy="48" r="3" />
            </g>
            <circle cx="32" cy="32" r="6" fill="var(--brand-600)" />
            <text
              x="80"
              y="44"
              fontFamily="IBM Plex Mono, ui-monospace, monospace"
              fontWeight="600"
              fontSize="36"
              letterSpacing="-0.01em"
              fill="currentColor"
            >
              asynq
              <tspan fill="var(--brand-600)" fontWeight="700">
                pg
              </tspan>
            </text>
          </svg>
        </button>
        {config?.version && <span className="brand-ver">{config.version}</span>}
      </div>

      <nav className="side-section" aria-label="Sections">
        <div className="h" id="side-operate-heading">Operate</div>
        <ul role="list" style={{ listStyle: "none", padding: 0, margin: 0 }}>
          {NAV_ITEMS.map((it) => {
            const active = currentScreen === it.id
            return (
              <li key={it.id}>
                <button
                  type="button"
                  className={"side-link" + (active ? " active" : "")}
                  onClick={() => goTo("/" + it.id)}
                  aria-current={active ? "page" : undefined}
                >
                  <Icon name={it.icon} size={15} />
                  {it.label}
                  {it.id === "tasks" && failedTotal > 0 && (
                    <span className="badge alert" aria-label={`${failedTotal} failed`}>{fmtN(failedTotal)}</span>
                  )}
                </button>
              </li>
            )
          })}
        </ul>
      </nav>

      <nav className="side-section side-queues" aria-labelledby="side-queues-heading">
        <div className="h" id="side-queues-heading">
          Queues
          <span
            style={{
              fontFamily: "var(--font-mono)",
              color: "var(--fg-3)",
              fontSize: 11,
              fontWeight: 400,
              textTransform: "none",
              letterSpacing: 0,
            }}
            aria-label={filter ? `${filtered.length} of ${summary.length} matching` : `${summary.length} total`}
          >
            {filter ? `${filtered.length}/${summary.length}` : summary.length}
          </span>
        </div>

        <label className="side-filter">
          <span className="sr-only">Filter queues</span>
          <Icon name="search" size={13} />
          <input
            type="text"
            value={filter}
            onChange={(e) => {
              setFilter(e.target.value)
              setShowAll(false)
            }}
            placeholder="Filter queues…"
            spellCheck={false}
            aria-label="Filter queues"
          />
          {filter && (
            <button type="button" onClick={() => setFilter("")} aria-label="Clear filter">
              <Icon name="x" size={11} />
            </button>
          )}
        </label>

        <ul role="list" className="side-queue-list" style={{ listStyle: "none", padding: 0, margin: 0 }}>
          {visible.length === 0 && <li className="side-empty">No queues match.</li>}
          {visible.map((q) => {
            const isActive = currentScreen === "tasks" && activeType === q.type
            const dotBg =
              q.failed > 0
                ? "var(--status-failed)"
                : q.running > 0
                ? "var(--status-running)"
                : q.pending > 0
                ? "var(--status-pending)"
                : "var(--status-cancelled)"
            const inFlight = q.pending + q.running
            return (
              <li key={q.type}>
                <button
                  type="button"
                  className={"side-link mono" + (isActive ? " active" : "")}
                  onClick={() => goTo("/tasks?type=" + encodeURIComponent(q.type))}
                  aria-current={isActive ? "page" : undefined}
                  aria-label={`${q.type}, ${inFlight} in flight, ${q.failed} failed`}
                >
                  <span className="dot" style={{ background: dotBg }} aria-hidden="true" />
                  <span className="qname">{q.type}</span>
                  <span className="badge" aria-hidden="true">{fmtN(inFlight)}</span>
                </button>
              </li>
            )
          })}
        </ul>

        {!filter && hidden > 0 && (
          <button type="button" className="side-link more" onClick={() => setShowAll(true)}>
            <Icon name="chevron-down" size={14} />
            <span>Show {hidden} more…</span>
          </button>
        )}
        {!filter && showAll && summary.length > 8 && (
          <button type="button" className="side-link more" onClick={() => setShowAll(false)}>
            <Icon name="chevron-up" size={14} />
            <span>Collapse</span>
          </button>
        )}
      </nav>

      <div className="side-foot">
        <div className="row">
          <span className="k">database</span>
          <span className="v">postgres</span>
        </div>
        <div className="row">
          <span className="k">connection</span>
          <span className="v conn-ok">
            <span className="dot s-completed" style={{ marginRight: 6 }} aria-hidden="true" />
            {leaderOk ? "healthy" : "…"}
          </span>
        </div>
        <div className="row">
          <span className="k">leader</span>
          <span className="v">{leader?.leader_id || "no leader"}</span>
        </div>
      </div>
    </aside>
  )
}
