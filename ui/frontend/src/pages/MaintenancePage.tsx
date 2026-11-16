import type { ReactNode } from "react"
import { useQuery } from "@tanstack/react-query"
import { useNavigate } from "react-router"
import { getLeader, getMaintenance, getStats, getTasks } from "@/api/client"
import { Icon } from "@/components/Icon"
import { QueryNotice } from "@/components/QueryNotice"
import { Kpi } from "@/components/Kpi"
import { StatusChip } from "@/components/StatusChip"
import { ago, fmtDur, fmtMs, fmtN, fmtTime } from "@/lib/format"
import type { TaskSummary } from "@/api/types"

export function MaintenancePage() {
  const navigate = useNavigate()

  const leaderQuery = useQuery({
    queryKey: ["leader"],
    queryFn: getLeader,
    refetchInterval: 5000,
  })

  const statsQuery = useQuery({
    queryKey: ["stats"],
    queryFn: getStats,
    refetchInterval: 5000,
  })

  const failedQuery = useQuery({
    queryKey: ["tasks", { status: "failed", order: "DESC", limit: 10 }],
    queryFn: () =>
      getTasks({ status: "failed", order_by: "id", order: "DESC", limit: 10 }),
    refetchInterval: 5000,
  })

  const maintenanceQuery = useQuery({
    queryKey: ["maintenance"],
    queryFn: getMaintenance,
    refetchInterval: 30_000,
  })

  const { data: leader } = leaderQuery
  const { data: stats } = statsQuery
  const { data: failed } = failedQuery
  const { data: maintenance } = maintenanceQuery

  const electedAt = leader?.elected_at ? new Date(leader.elected_at) : null
  const failedTasks = failed?.tasks ?? []
  const totals = stats?.by_status ?? {
    pending: 0,
    running: 0,
    completed: 0,
    failed: 0,
    cancelled: 0,
  }

  // A "dead-letter" task is failed with no retries left — operator action
  // required (retry manually or delete).
  const deadLetter = failedTasks.filter((t) => t.attempts_left === 0).length

  const cfg = maintenance?.config
  const rescuable = maintenance?.rescuable
  const cleanable = maintenance?.cleanable
  const health = maintenance?.queue_health

  return (
    <div>
      <div className="page-head">
        <div>
          <h1>Maintenance</h1>
          <div className="sub">
            leader-elected background tasks · preview refreshes every 30s
          </div>
        </div>
      </div>

      <QueryNotice label="leader" {...leaderQuery} hasData={leader !== undefined} onRetry={() => { void leaderQuery.refetch() }} />
      {leader && <div className="section-block">
        <div className="leader-card">
          <span className="crown">
            <Icon name="crown" size={18} />
          </span>
          {leader && leader.leader_id ? (
            <>
              <div>
                <div className="who">
                  {leader.leader_id}{" "}
                  <span style={{ color: "var(--fg-3)" }}>holds the lease</span>
                </div>
                <div className="meta">
                  elected {electedAt ? fmtTime(electedAt) : "—"}
                  {electedAt ? ` · ${ago(electedAt)} ago` : ""} · expires_at{" "}
                  {leader.expires_at ? fmtTime(leader.expires_at) : "—"}
                </div>
              </div>
              <div className="lease">
                <div className="l">lease ttl</div>
                <div className="v">
                  {leader.lease_ttl_seconds}s
                  <span style={{ color: "var(--fg-3)", fontSize: 11 }}>
                    {" "}
                    · auto-renew
                  </span>
                </div>
              </div>
            </>
          ) : (
            <div>
              <div className="who" style={{ color: "var(--fg-3)" }}>
                no leader elected
              </div>
              <div className="meta">
                no row in asynqpg_leader · waiting for first consumer
              </div>
            </div>
          )}
        </div>
      </div>

      }
      <QueryNotice label="queue stats" {...statsQuery} hasData={stats !== undefined} onRetry={() => { void statsQuery.refetch() }} />
      {stats && <div className="kpi-strip kpi-strip-4">
        <Kpi
          label="pending"
          value={fmtN(totals.pending)}
          color="var(--status-pending)"
        />
        <Kpi
          label="running"
          value={fmtN(totals.running)}
          color="var(--status-running)"
          live
        />
        <Kpi
          label="failed"
          value={fmtN(totals.failed)}
          color="var(--status-failed)"
        />
        <Kpi
          label="dead-letter · recent"
          value={failed ? fmtN(deadLetter) : "—"}
          sub={failedQuery.isError && failed ? "stale recent failures" : "among the latest 10 failures"}
          color="var(--status-failed)"
        />
      </div>

      }
      <QueryNotice label="maintenance preview" {...maintenanceQuery} hasData={maintenance !== undefined} onRetry={() => { void maintenanceQuery.refetch() }} />
      {maintenance && <div className="kpi-strip kpi-strip-3">
        <Kpi
          label="oldest pending"
          value={fmtDur(health?.oldest_pending_ms)}
          color="var(--status-pending)"
          sub="time the queue's oldest waiting task has been pending"
        />
        <Kpi
          label="completed · 15m"
          value={fmtN(health?.completed_last_15m)}
          color="var(--status-completed)"
          live
          sub="retained rows finalized in the last 15 minutes"
        />
        <Kpi
          label="failed · 15m"
          value={fmtN(health?.failed_last_15m)}
          color="var(--status-failed)"
          sub="retained rows finalized in the last 15 minutes"
        />
      </div>

      }
      <div className="section-block">
        <div className="h2">
          <h2>Recent failures</h2>
          <span className="sub">
            {failed ? `${fmtN(failedTasks.length)} recent failures` : ""}
            {stats ? ` of ${fmtN(totals.failed)}${statsQuery.isError ? " (stale total)" : ""}` : ""}
          </span>
        </div>

        <QueryNotice label="recent failures" {...failedQuery} hasData={failed !== undefined} onRetry={() => { void failedQuery.refetch() }} />
        {failed !== undefined && (failedTasks.length === 0 ? (
          <div className="empty">
            <div className="h">No failures recorded.</div>
            <div>
              The leader's <code>Rescuer</code> moves stuck running tasks back
              to <code>pending</code>; the <code>Cleaner</code> trims old{" "}
              <code>completed</code>/<code>cancelled</code> rows.
            </div>
          </div>
        ) : (
          <div className="table-wrap table-compact">
            <table className="dt">
              <thead>
                <tr>
                  <th style={{ width: 100 }} className="mono">
                    task id
                  </th>
                  <th>type</th>
                  <th style={{ width: 100 }}>status</th>
                  <th style={{ width: 96, textAlign: "right" }} className="mono">
                    attempts
                  </th>
                  <th style={{ width: 110, textAlign: "right" }} className="mono">
                    failed
                  </th>
                </tr>
              </thead>
              <tbody>
                {failedTasks.map((t) => (
                  <FailedRow
                    key={t.id}
                    t={t}
                    onOpen={() => navigate(`/tasks?id=${t.id}`)}
                  />
                ))}
              </tbody>
            </table>
          </div>
        ))}
      </div>

      <div className="section-block">
        <div className="h2">
          <h2>Background tasks</h2>
          <span className="sub">
            runs only on the elected leader · current eligibility estimate
          </span>
        </div>
        <QueryNotice label="maintenance preview" {...maintenanceQuery} hasData={maintenance !== undefined} onRetry={() => { void maintenanceQuery.refetch() }} />
        {cfg && <p className="preview-settings">
          Preview settings: {cfg.source === "configured" ? "application configuration" : "library defaults"} (defaults apply to omitted fields).
          {" "}These settings must match the leader's configuration.
        </p>}
        <div className="maint-tasks">
          {maintenance && <>
          <MaintTask
            icon="rotate-ccw"
            name="Rescuer"
            desc="Retries tasks running past the stuck threshold, or marks them failed when no attempts remain."
            meta={
              <>
                <div>
                  stuck after {fmtDur(cfg?.stuck_threshold_ms)} · runs every{" "}
                  {fmtDur(cfg?.interval_ms)}
                </div>
                {rescuable && rescuable.total > 0 && (
                  <div>
                    → would retry {fmtN(rescuable.retriable)}, discard{" "}
                    {fmtN(rescuable.discardable)}
                  </div>
                )}
              </>
            }
            badge={rescuable ? `${fmtN(rescuable.total)} stuck` : undefined}
            badgeActive={!!rescuable && rescuable.total > 0}
          />
          <MaintTask
            icon="trash-2"
            name="Cleaner"
            desc="Deletes finalized tasks past their retention window to keep the table small."
            meta={
              <>
                <div>
                  keeps completed {fmtDur(cfg?.completed_retention_ms)} · failed{" "}
                  {fmtDur(cfg?.failed_retention_ms)} · cancelled{" "}
                  {fmtDur(cfg?.cancelled_retention_ms)}
                </div>
                {cleanable && cleanable.total > 0 && (
                  <div>
                    → {fmtN(cleanable.completed)} completed,{" "}
                    {fmtN(cleanable.failed)} failed, {fmtN(cleanable.cancelled)}{" "}
                    cancelled past retention
                  </div>
                )}
              </>
            }
            badge={cleanable ? `${fmtN(cleanable.total)} to delete` : undefined}
            badgeActive={!!cleanable && cleanable.total > 0}
          />
          </>}
          {leader && <MaintTask
            icon="crown"
            name="Leader election"
            desc="A single consumer holds asynqpg_leader at a time, refreshing the lease before TTL. On crash, another consumer takes over within one TTL."
            meta={
              leader && leader.leader_id ? (
                <div>
                  lease ttl {leader.lease_ttl_seconds}s
                  {electedAt ? ` · elected ${ago(electedAt)} ago` : ""}
                </div>
              ) : (
                <div>no leader elected yet</div>
              )
            }
          />}
        </div>
      </div>
    </div>
  )
}

function FailedRow({
  t,
  onOpen,
}: {
  t: TaskSummary
  onOpen: () => void
}) {
  const finalDuration =
    t.attempted_at && t.finalized_at
      ? new Date(t.finalized_at).getTime() -
        new Date(t.attempted_at).getTime()
      : null
  const lastMessage =
    t.messages.length > 0 ? t.messages[t.messages.length - 1] : null

  return (
    <tr
      onClick={onOpen}
      onKeyDown={(e) => {
        if (e.key === "Enter") {
          e.preventDefault()
          onOpen()
        }
      }}
      tabIndex={0}
      aria-label={`Open failed task ${t.id} (${t.type})`}
      title={lastMessage ?? undefined}
    >
      <td className="id">#{t.id}</td>
      <td className="type">{t.type}</td>
      <td>
        <StatusChip status={t.status} />
      </td>
      <td className="num">
        <span
          style={{
            color:
              t.attempts_left === 0
                ? "var(--status-failed)"
                : "var(--fg-2)",
          }}
        >
          {t.attempts_elapsed}/{t.attempts_elapsed + t.attempts_left}
          {finalDuration != null && (
            <span style={{ color: "var(--fg-3)", marginLeft: 6 }}>
              · {fmtMs(finalDuration)}
            </span>
          )}
        </span>
      </td>
      <td className="num muted">
        {t.finalized_at ? `${ago(t.finalized_at)} ago` : "—"}
      </td>
    </tr>
  )
}

function MaintTask({
  icon,
  name,
  desc,
  meta,
  badge,
  badgeActive,
}: {
  icon: string
  name: string
  desc: string
  meta?: ReactNode
  badge?: ReactNode
  badgeActive?: boolean
}) {
  return (
    <div className="maint-task">
      <span className="maint-task-icon">
        <Icon name={icon} size={16} />
      </span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div className="maint-task-name">{name}</div>
        <div className="maint-task-desc">{desc}</div>
        {meta && <div className="maint-task-meta">{meta}</div>}
      </div>
      {badge != null && (
        <span className={`maint-task-badge${badgeActive ? " active" : ""}`}>
          {badge}
        </span>
      )}
    </div>
  )
}
