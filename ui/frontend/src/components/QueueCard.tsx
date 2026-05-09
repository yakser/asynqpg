import type { TaskStatus } from "@/api/types"
import { fmtN } from "@/lib/format"
import { Icon } from "./Icon"

export interface QueueTypeStats {
  type: string
  total: number
  by_status: Record<TaskStatus, number>
}

interface QueueCardProps {
  q: QueueTypeStats
  onOpen: () => void
}

export function QueueCard({ q, onOpen }: QueueCardProps) {
  const [namespace, ...rest] = q.type.split(":")
  const tail = rest.join(":")
  const pending = q.by_status.pending ?? 0
  const running = q.by_status.running ?? 0
  const completed = q.by_status.completed ?? 0
  const failed = q.by_status.failed ?? 0
  const cancelled = q.by_status.cancelled ?? 0
  const health = failed > 0 ? "bad" : pending > 50 ? "warn" : "ok"
  const healthLabel =
    health === "bad" ? "unhealthy" : health === "warn" ? "warning" : "healthy"
  return (
    <button
      type="button"
      className="q-card"
      onClick={onOpen}
      aria-label={`${q.type}: ${fmtN(q.total)} total, ${fmtN(failed)} failed, ${fmtN(pending)} pending. Open task list.`}
    >
      <div className="head">
        <div style={{ minWidth: 0, flex: 1 }}>
          <div className="name">
            {namespace}
            {tail && (
              <>
                <em>:</em>
                {tail}
              </>
            )}
          </div>
          <div className="meta">{fmtN(q.total)} total</div>
        </div>
        <div className={"q-health " + health} aria-label={healthLabel}>
          <span className="dot" aria-hidden="true" />
        </div>
      </div>
      <div className="q-stats" aria-hidden="true">
        <div className="q-stat">
          <div className="l">
            <span className="dot s-pending" />
            pending
          </div>
          <div className="v">{fmtN(pending)}</div>
        </div>
        <div className="q-stat">
          <div className="l">
            <span className="dot s-running" />
            running
          </div>
          <div className="v">{fmtN(running)}</div>
        </div>
        <div className="q-stat">
          <div className="l">
            <span className="dot s-completed" />
            done
          </div>
          <div className="v">{fmtN(completed)}</div>
        </div>
        <div className={"q-stat failed" + (failed === 0 ? " zero" : "")}>
          <div className="l">
            <span className="dot s-failed" />
            failed
          </div>
          <div className="v">{fmtN(failed)}</div>
        </div>
        <div className="q-stat">
          <div className="l">
            <span className="dot s-cancelled" />
            cncl
          </div>
          <div className="v">{fmtN(cancelled)}</div>
        </div>
      </div>
      <div className="q-foot">
        <span />
        <span className="open-cue">
          view tasks <Icon name="arrow-right" size={12} />
        </span>
      </div>
    </button>
  )
}
