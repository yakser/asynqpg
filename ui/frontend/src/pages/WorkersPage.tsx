import { useEffect, useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { useNavigate } from "react-router"
import { getLeader, getTasks } from "@/api/client"
import { Icon } from "@/components/Icon"
import { ago, fmtTime } from "@/lib/format"

function useTick(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])
  return now
}

export function WorkersPage() {
  const navigate = useNavigate()
  const now = useTick(1000)

  const { data: leader } = useQuery({
    queryKey: ["leader"],
    queryFn: getLeader,
    refetchInterval: 5000,
  })

  const { data: running } = useQuery({
    queryKey: ["tasks", { status: "running", limit: 20 }],
    queryFn: () => getTasks({ status: "running", limit: 20, order_by: "updated_at", order: "DESC" }),
    refetchInterval: 5000,
  })

  const runningTasks = running?.tasks ?? []
  const electedAt = leader?.elected_at ? new Date(leader.elected_at) : null

  return (
    <div>
      <div className="page-head">
        <div>
          <h1>Workers</h1>
          <div className="sub">
            leader-elected via <code style={{ fontFamily: "var(--font-mono)" }}>asynqpg_leader</code>
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
                  {leader.leader_id}
                </div>
                <div className="meta">
                  elected {electedAt ? fmtTime(electedAt) : "—"}
                  {electedAt ? ` · ${ago(electedAt)} ago` : ""}
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
            <>
              <div>
                <div className="who" style={{ color: "var(--fg-3)" }}>
                  no leader elected
                </div>
                <div className="meta">no row in asynqpg_leader · waiting for first consumer</div>
              </div>
            </>
          )}
        </div>
      </div>

      <div className="section-block">
        <div className="h2">
          <h2>Currently processing</h2>
          <span className="sub">
            live snapshot — tasks in <code style={{ fontFamily: "var(--font-mono)" }}>status = 'running'</code>
          </span>
        </div>

        {runningTasks.length === 0 ? (
          <div className="empty">
            <div className="h">No tasks running.</div>
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
                  <th style={{ width: 80, textAlign: "right" }} className="mono">
                    attempt
                  </th>
                  <th style={{ width: 96, textAlign: "right" }} className="mono">
                    elapsed
                  </th>
                </tr>
              </thead>
              <tbody>
                {runningTasks.map((t) => {
                  const elapsed = t.attempted_at
                    ? Math.max(0, Math.floor((now - new Date(t.attempted_at).getTime()) / 1000))
                    : null
                  return (
                    <tr
                      key={t.id}
                      onClick={() => navigate(`/tasks?id=${t.id}`)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") {
                          e.preventDefault()
                          navigate(`/tasks?id=${t.id}`)
                        }
                      }}
                      tabIndex={0}
                      aria-label={`Open task ${t.id} (${t.type})`}
                    >
                      <td className="id">#{t.id}</td>
                      <td className="type">{t.type}</td>
                      <td className="num">
                        {t.attempts_elapsed + 1}/{t.attempts_elapsed + t.attempts_left + 1}
                      </td>
                      <td className="num">
                        <span className="dot s-running live" style={{ marginRight: 6, verticalAlign: "middle" }} />
                        {elapsed != null ? `${elapsed}s` : "—"}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

    </div>
  )
}
