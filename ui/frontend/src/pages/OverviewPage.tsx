import { useMemo, useState } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { useNavigate } from "react-router"
import { getConfig, getLeader, getStats } from "@/api/client"
import type { StatsResponse } from "@/api/types"
import { Button } from "@/components/Button"
import { FilterField } from "@/components/FilterField"
import { Icon } from "@/components/Icon"
import { Kpi } from "@/components/Kpi"
import { QueueCard } from "@/components/QueueCard"
import { fmtN } from "@/lib/format"

export function OverviewPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState("")

  const { data: stats } = useQuery({
    queryKey: ["stats"],
    queryFn: getStats,
    refetchInterval: 5000,
  })

  const { data: leader } = useQuery({
    queryKey: ["leader"],
    queryFn: getLeader,
    refetchInterval: 5000,
  })

  const { data: config } = useQuery({
    queryKey: ["config"],
    queryFn: getConfig,
    staleTime: 60_000,
  })

  const filtered = useMemo(() => {
    if (!stats) return []
    const f = filter.trim().toLowerCase()
    const list = !f ? stats.by_type : stats.by_type.filter((q) => q.type.toLowerCase().includes(f))
    return [...list].sort(
      (a, b) =>
        (b.by_status.failed - a.by_status.failed) ||
        (b.by_status.pending - a.by_status.pending),
    )
  }, [stats, filter])

  const totals: StatsResponse["by_status"] =
    stats?.by_status ?? { pending: 0, running: 0, completed: 0, failed: 0, cancelled: 0 }

  return (
    <div>
      <div className="page-head">
        <div>
          <h1>Overview</h1>
          <div className="sub">
            {stats?.by_type.length ?? 0} task types · current snapshot · auto-refresh 5s
          </div>
        </div>
        <div className="actions">
          <Button
            icon="rotate-cw"
            variant="ghost"
            data-mobile-hide="true"
            onClick={() => {
              queryClient.invalidateQueries({ queryKey: ["stats"] })
              queryClient.invalidateQueries({ queryKey: ["leader"] })
              queryClient.invalidateQueries({ queryKey: ["config"] })
            }}
          >
            Refresh
          </Button>
        </div>
      </div>

      <div className="kpi-strip">
        <Kpi label="pending" value={fmtN(totals.pending)} color="var(--status-pending)" />
        <Kpi label="running" value={fmtN(totals.running)} color="var(--status-running)" live />
        <Kpi label="failed" value={fmtN(totals.failed)} color="var(--status-failed)" />
        <Kpi label="completed" value={fmtN(totals.completed)} color="var(--status-completed)" />
        <Kpi label="cancelled" value={fmtN(totals.cancelled)} color="var(--status-cancelled)" />
      </div>

      <div className="queues-band">
        <div className="section-block" style={{ borderTop: 0, paddingBottom: 0 }}>
          <div
            className="h2"
            style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16, flexWrap: "wrap" }}
          >
            <div>
              <h2>Task types</h2>
              <span className="sub">
                {filter ? (
                  <>
                    showing {filtered.length} of {stats?.by_type.length ?? 0} matching{" "}
                    <code style={{ color: "var(--fg-1)" }}>{filter}</code>
                  </>
                ) : (
                  <>click any card to filter the task list to that type · sorted by failed desc, then pending desc</>
                )}
              </span>
            </div>
            <FilterField
              value={filter}
              onChange={setFilter}
              placeholder="Filter task types…"
              count={stats?.by_type.length}
            />
          </div>
        </div>

        {filtered.length === 0 ? (
          <div className="queues-empty">
            <Icon name="search-x" size={20} />
            <div>
              {stats == null
                ? "loading task types…"
                : filter
                  ? <>No task types match <code>{filter}</code></>
                  : "No task types yet · enqueue something"}
            </div>
            {filter && (
              <button type="button" className="link-btn" onClick={() => setFilter("")}>
                Clear filter
              </button>
            )}
          </div>
        ) : (
          <div className="queues-grid">
            {filtered.map((q) => (
              <QueueCard
                key={q.type}
                q={q}
                onOpen={() => navigate(`/tasks?type=${encodeURIComponent(q.type)}`)}
              />
            ))}
          </div>
        )}
      </div>

      <div className="section-block">
        <div className="h2">
          <h2>System</h2>
          <span className="sub">live cluster state</span>
        </div>
        <div
          style={{
            border: "1px solid var(--border)",
            borderRadius: "var(--r-3)",
            overflow: "hidden",
            background: "var(--bg)",
          }}
        >
          <SysRow k="version" v={config?.version ?? "—"} />
          <SysRow
            k="leader"
            v={leader?.leader_id || "no leader"}
            tag={
              leader?.lease_ttl_seconds && leader.lease_ttl_seconds > 0
                ? `lease ${leader.lease_ttl_seconds}s`
                : undefined
            }
            last
          />
        </div>
      </div>
    </div>
  )
}

function SysRow({
  k,
  v,
  tag,
  last,
}: {
  k: string
  v: React.ReactNode
  tag?: string
  last?: boolean
}) {
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
      <span style={{ display: "flex", gap: 10, alignItems: "center" }}>
        {tag && (
          <span style={{ color: "var(--fg-3)", fontFamily: "var(--font-mono)", fontSize: 11.5 }}>{tag}</span>
        )}
        <span style={{ color: "var(--fg-1)", fontFamily: "var(--font-mono)", fontWeight: 500 }}>{v}</span>
      </span>
    </div>
  )
}
