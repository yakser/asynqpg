import { useEffect, useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { getConsumers, getLeader } from "@/api/client"
import type { ConsumerInfo } from "@/api/types"
import { Icon } from "@/components/Icon"
import { QueryNotice } from "@/components/QueryNotice"
import { Kpi } from "@/components/Kpi"
import { ago, fmtMs, fmtN, fmtTime } from "@/lib/format"

function useTick(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])
  return now
}

export function WorkersPage() {
  const now = useTick(1000)

  const leaderQuery = useQuery({
    queryKey: ["leader"],
    queryFn: getLeader,
    refetchInterval: 5000,
  })

  const consumersQuery = useQuery({
    queryKey: ["consumers"],
    queryFn: getConsumers,
    refetchInterval: 5000,
  })

  const { data: leader } = leaderQuery
  const { data: consumers } = consumersQuery
  const list = consumers ?? []
  const electedAt = leader?.elected_at ? new Date(leader.elected_at) : null

  const active = list.filter((c) => c.running)
  const totalWorkers = active.reduce(
    (sum, c) => sum + c.task_types.reduce((s, t) => s + t.workers, 0),
    0,
  )
  const busyWorkers = active.reduce(
    (sum, c) => sum + c.task_types.reduce((s, t) => s + t.busy, 0),
    0,
  )
  const distinctTypes = new Set(list.flatMap((c) => c.task_types.map((t) => t.type))).size

  return (
    <div>
      <div className="page-head">
        <div>
          <h1>Workers</h1>
          <div className="sub">
            executors running in this dashboard's process · auto-refresh 5s
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
                <div className="who">{leader.leader_id}</div>
                <div className="meta">
                  elected {electedAt ? fmtTime(electedAt) : "—"}
                  {electedAt ? ` · ${ago(electedAt, now)} ago` : ""}
                </div>
              </div>
              <div className="lease">
                <div className="l">lease remaining</div>
                <div className="v">
                  {leader.lease_ttl_seconds}s
                  <span style={{ color: "var(--fg-3)", fontSize: 11 }}> · auto-renew</span>
                </div>
              </div>
            </>
          ) : (
            <div>
              <div className="who" style={{ color: "var(--fg-3)" }}>
                no leader elected
              </div>
              <div className="meta">no row in asynqpg_leader · waiting for first consumer</div>
            </div>
          )}
        </div>
      </div>

      }
      {list.length > 0 && (
        <div className="kpi-strip kpi-strip-4">
          <Kpi label="attached consumers" value={fmtN(list.length)} sub={`${active.length} running`} />
          <Kpi label="active workers" value={fmtN(totalWorkers)} />
          <Kpi label="busy" value={fmtN(busyWorkers)} color="var(--status-running)" live={busyWorkers > 0} />
          <Kpi label="task types" value={fmtN(distinctTypes)} />
        </div>
      )}

      <div className="section-block">
        <div className="h2">
          <h2>Consumers</h2>
          <span className="sub">live snapshot of registered executors and their worker pools</span>
        </div>

        <QueryNotice label="consumers" {...consumersQuery} hasData={consumers !== undefined} onRetry={() => { void consumersQuery.refetch() }} />
        {consumers !== undefined && (list.length === 0 ? (
          <div className="empty">
            <div className="h">No consumers attached.</div>
            <div className="p">The Workers view shows executors running in the same process as this dashboard. None are attached to this instance.</div>
          </div>
        ) : (
          <div className="consumer-grid">
            {list.map((c) => (
              <ConsumerCard key={c.id} consumer={c} now={now} />
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}

function ConsumerCard({ consumer: c, now }: { consumer: ConsumerInfo; now: number }) {
  const workers = c.task_types.reduce((s, t) => s + t.workers, 0)
  const busy = c.task_types.reduce((s, t) => s + t.busy, 0)

  return (
    <div className="consumer-card">
      <div className="consumer-head">
        <div className="consumer-id">
          {c.is_leader && (
            <span className="leader-pill" title="current leader">
              <Icon name="crown" size={12} />
            </span>
          )}
          <span className="who">{c.id}</span>
        </div>
        <div className="consumer-tags">
          <span className={`tag ${c.maintenance_enabled ? "" : "muted"}`}>
            {c.maintenance_enabled ? "maintenance" : "no maintenance"}
          </span>
        </div>
      </div>

      <div className="consumer-meta">
        {c.running ? `running · up ${c.started_at ? ago(c.started_at, now) : "—"}` : c.started_at ? `stopped · started ${fmtTime(c.started_at)}` : "not started"} · {workers} configured workers · {busy} busy
      </div>

      {c.task_types.length === 0 ? (
        <div className="consumer-empty">no task types registered</div>
      ) : (
        <div className="table-wrap table-compact">
          <table className="dt">
            <thead>
              <tr>
                <th>type</th>
                <th style={{ width: 150 }}>workers</th>
                <th style={{ width: 64, textAlign: "right" }} className="mono">
                  retries
                </th>
                <th style={{ width: 72, textAlign: "right" }} className="mono">
                  timeout
                </th>
              </tr>
            </thead>
            <tbody>
              {c.task_types.map((t) => {
                const pct = t.workers > 0 ? Math.round((t.busy / t.workers) * 100) : 0
                return (
                  <tr key={t.type}>
                    <td className="type">{t.type}</td>
                    <td>
                      <div className="util">
                        <span className="util-track">
                          <span
                            className={`util-fill${t.busy > 0 ? " live" : ""}`}
                            style={{ width: `${pct}%` }}
                          />
                        </span>
                        <span className="util-num mono">
                          {t.busy}/{t.workers}
                        </span>
                      </div>
                    </td>
                    <td className="num">{t.max_attempts}</td>
                    <td className="num">{fmtMs(t.timeout_ms)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className="consumer-foot">
        <span>fetch {fmtMs(c.fetch_interval_ms)}</span>
        <span>stuck {fmtMs(c.stuck_threshold_ms)}</span>
        <span>janitor {fmtMs(c.janitor_interval_ms)}</span>
        <span>shutdown {fmtMs(c.shutdown_timeout_ms)}</span>
      </div>
    </div>
  )
}
