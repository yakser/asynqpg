import { useMemo } from "react"
import { useQuery } from "@tanstack/react-query"
import { useNavigate } from "react-router"
import { getLeader, getStats, getTasks } from "@/api/client"
import { Icon } from "@/components/Icon"
import { Kpi } from "@/components/Kpi"
import { StatusChip } from "@/components/StatusChip"
import { ago, fmtMs, fmtN, fmtTime } from "@/lib/format"
import type { TaskSummary } from "@/api/types"

export function MaintenancePage() {
  const navigate = useNavigate()

  const { data: leader } = useQuery({
    queryKey: ["leader"],
    queryFn: getLeader,
    refetchInterval: 5000,
  })

  const { data: stats } = useQuery({
    queryKey: ["stats"],
    queryFn: getStats,
    refetchInterval: 5000,
  })

  const { data: failed } = useQuery({
    queryKey: ["tasks", { status: "failed", order: "DESC", limit: 10 }],
    queryFn: () =>
      getTasks({ status: "failed", order_by: "id", order: "DESC", limit: 10 }),
    refetchInterval: 5000,
  })

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
  const deadLetter = useMemo(
    () => failedTasks.filter((t) => t.attempts_left === 0).length,
    [failedTasks],
  )

  return (
    <div>
      <div className="page-head">
        <div>
          <h1>Maintenance</h1>
          <div className="sub">
            leader-elected background tasks · table <code>asynqpg_leader</code>
          </div>
        </div>
      </div>

      <div className="section-block">
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

      <div className="kpi-strip kpi-strip-4">
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
          label="dead-letter"
          value={fmtN(deadLetter)}
          color="var(--status-failed)"
        />
      </div>

      <div className="section-block">
        <div className="h2">
          <h2>Recent failures</h2>
          <span className="sub">
            top {fmtN(failedTasks.length)} of {fmtN(totals.failed)} · ordered by
            id desc
          </span>
        </div>

        {failedTasks.length === 0 ? (
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
        )}
      </div>

      <div className="section-block">
        <div className="h2">
          <h2>Background tasks</h2>
          <span className="sub">runs only on the elected leader</span>
        </div>
        <div className="maint-tasks">
          <MaintTask
            icon="rotate-ccw"
            name="Rescuer"
            desc="Returns tasks whose lease expired (consumer crashed mid-run) from running back to pending so they can be retried."
          />
          <MaintTask
            icon="trash-2"
            name="Cleaner"
            desc="Deletes old completed and cancelled tasks past the configured retention window to keep the table small."
          />
          <MaintTask
            icon="crown"
            name="Leader election"
            desc="A single consumer holds asynqpg_leader at a time, refreshing the lease before TTL. On crash, another consumer takes over within one TTL."
          />
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
}: {
  icon: string
  name: string
  desc: string
}) {
  return (
    <div className="maint-task">
      <span className="maint-task-icon">
        <Icon name={icon} size={16} />
      </span>
      <div>
        <div className="maint-task-name">{name}</div>
        <div className="maint-task-desc">{desc}</div>
      </div>
    </div>
  )
}
