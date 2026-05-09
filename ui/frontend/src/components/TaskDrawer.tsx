import { useEffect, useMemo, useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  cancelTask,
  deleteTask,
  getTasks,
  retryTask,
} from "@/api/client"
import type { TaskDetail, TaskStatus, TaskSummary } from "@/api/types"
import { Button, IconButton } from "@/components/Button"
import { StatusChip, StatusDot } from "@/components/StatusChip"
import { JsonTree } from "@/components/JsonTree"
import { ago, fmtBytes, fmtMs, fmtTime } from "@/lib/format"

type TabKey = "payload" | "attempts" | "timing" | "related" | "raw"

interface TaskDrawerProps {
  task: TaskDetail
  onClose: () => void
  onPrev?: () => void
  onNext?: () => void
}

export function TaskDrawer({
  task,
  onClose,
  onPrev,
  onNext,
}: TaskDrawerProps): React.JSX.Element {
  const [tab, setTab] = useState<TabKey>("payload")

  // Escape key to close. j/k handled by parent.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        e.preventDefault()
        onClose()
      }
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [onClose])

  const queryClient = useQueryClient()
  const invalidateTasks = () => {
    queryClient.invalidateQueries({ queryKey: ["tasks"] })
  }

  const retryMut = useMutation({
    mutationFn: () => retryTask(task.id),
    onSuccess: () => {
      invalidateTasks()
    },
  })

  const cancelMut = useMutation({
    mutationFn: () => cancelTask(task.id),
    onSuccess: () => {
      invalidateTasks()
    },
  })

  const deleteMut = useMutation({
    mutationFn: () => deleteTask(task.id),
    onSuccess: () => {
      invalidateTasks()
      onClose()
    },
  })

  const actionPending =
    retryMut.isPending || cancelMut.isPending || deleteMut.isPending

  const attemptsMax = task.attempts_elapsed + task.attempts_left
  const attemptsCount = task.messages?.length ?? 0

  const duration = computeDurationMs(task)

  function copyCurl() {
    const cmd = `curl -X POST '/api/tasks/${task.id}/retry'`
    void navigator.clipboard.writeText(cmd)
  }

  function confirmDelete() {
    if (window.confirm(`Delete task #${task.id}?`)) {
      deleteMut.mutate()
    }
  }

  return (
    <>
      <div
        className="drawer-scrim"
        onClick={onClose}
        role="presentation"
        aria-hidden="true"
      />
      <aside
        className="drawer"
        role="dialog"
        aria-modal="true"
        aria-label={`Task #${task.id} detail`}
      >
        <div className="drawer-head">
          <div style={{ minWidth: 0, flex: 1 }}>
            <div className="id">
              task #{task.id} · idempotency {task.idempotency_token || "–"}
            </div>
            <div className="ttl">{task.type}</div>
            <div
              style={{
                marginTop: 8,
                display: "flex",
                alignItems: "center",
                gap: 8,
              }}
            >
              <StatusChip status={task.status} />
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: 12,
                  color: "var(--fg-3)",
                }}
              >
                {task.attempts_elapsed}/{attemptsMax} attempts ·{" "}
                {fmtBytes(task.payload_size)}
              </span>
            </div>
          </div>
          <div className="nav">
            <IconButton
              icon="chevron-up"
              title="Previous (k)"
              onClick={onPrev}
              disabled={!onPrev}
            />
            <IconButton
              icon="chevron-down"
              title="Next (j)"
              onClick={onNext}
              disabled={!onNext}
            />
            <IconButton icon="x" title="Close (Esc)" onClick={onClose} />
          </div>
        </div>

        <div className="drawer-summary">
          <div className="it">
            <div className="l">created</div>
            <div className="v">{fmtTime(task.created_at)}</div>
          </div>
          <div className="it">
            <div className="l">
              {task.finalized_at ? "finalized" : "blocked till"}
            </div>
            <div className="v">
              {fmtTime(task.finalized_at || task.blocked_till)}
            </div>
          </div>
          <div className="it">
            <div className="l">duration</div>
            <div className="v">{duration != null ? fmtMs(duration) : "–"}</div>
          </div>
          <div className="it">
            <div className="l">attempts</div>
            <div className="v">
              {task.attempts_elapsed}/{attemptsMax}
            </div>
          </div>
        </div>

        <div className="tabs" role="tablist" aria-label="Task detail tabs">
          <Tab id="payload" label="Payload" tab={tab} setTab={setTab} />
          <Tab
            id="attempts"
            label="Attempts"
            tab={tab}
            setTab={setTab}
            count={attemptsCount}
          />
          <Tab id="timing" label="Timing" tab={tab} setTab={setTab} />
          <Tab id="related" label="Related" tab={tab} setTab={setTab} />
          <Tab id="raw" label="Raw row" tab={tab} setTab={setTab} />
        </div>

        <div
          className="drawer-body"
          role="tabpanel"
          id={`tabpanel-${tab}`}
          aria-labelledby={`tab-${tab}`}
        >
          {tab === "payload" && <PayloadTab task={task} />}
          {tab === "attempts" && <AttemptsTab task={task} />}
          {tab === "timing" && (
            <TimingTab
              task={task}
              attemptsMax={attemptsMax}
              duration={duration}
            />
          )}
          {tab === "related" && <RelatedTab task={task} />}
          {tab === "raw" && <RawTab task={task} />}
        </div>

        <div className="drawer-foot">
          {task.status === "failed" && task.attempts_left > 0 && (
            <Button
              icon="rotate-ccw"
              variant="primary"
              size="sm"
              disabled={actionPending}
              onClick={() => retryMut.mutate()}
            >
              Retry now
            </Button>
          )}
          {task.status === "failed" && task.attempts_left === 0 && (
            <Button
              icon="rotate-ccw"
              variant="default"
              size="sm"
              disabled={actionPending}
              onClick={() => retryMut.mutate()}
            >
              Force retry (reset attempts)
            </Button>
          )}
          {(task.status === "pending" || task.status === "running") && (
            <Button
              icon="x-circle"
              variant="default"
              size="sm"
              disabled={actionPending}
              onClick={() => cancelMut.mutate()}
            >
              Cancel
            </Button>
          )}
          <span style={{ flex: 1 }} />
          <Button
            icon="copy"
            variant="ghost"
            size="sm"
            onClick={copyCurl}
          >
            Copy curl
          </Button>
          <Button
            icon="trash-2"
            variant="danger"
            size="sm"
            disabled={actionPending}
            onClick={confirmDelete}
          >
            Delete
          </Button>
        </div>
      </aside>
    </>
  )
}

interface TabProps {
  id: TabKey
  label: string
  tab: TabKey
  setTab: (t: TabKey) => void
  count?: number
}

function Tab({ id, label, tab, setTab, count }: TabProps): React.JSX.Element {
  const active = tab === id
  return (
    <button
      type="button"
      role="tab"
      id={`tab-${id}`}
      aria-selected={active}
      aria-controls={`tabpanel-${id}`}
      tabIndex={active ? 0 : -1}
      className={"t" + (active ? " active" : "")}
      onClick={() => setTab(id)}
    >
      {label}
      {count != null && <span className="ct">{count}</span>}
    </button>
  )
}

function PayloadTab({ task }: { task: TaskDetail }): React.JSX.Element {
  const parsed = useMemo<{ ok: true; data: unknown } | { ok: false }>(() => {
    if (task.payload == null) return { ok: false }
    try {
      return { ok: true, data: JSON.parse(task.payload) as unknown }
    } catch {
      return { ok: false }
    }
  }, [task.payload])

  if (task.payload === null) {
    return (
      <section>
        <h4>Payload</h4>
        <div
          style={{
            fontSize: 12.5,
            color: "var(--fg-3)",
            fontFamily: "var(--font-mono)",
          }}
        >
          Payload hidden by server config.
        </div>
      </section>
    )
  }

  return (
    <section>
      <h4>
        Payload{" "}
        <span
          style={{
            color: "var(--fg-3)",
            fontFamily: "var(--font-mono)",
            fontSize: 11.5,
          }}
        >
          · {fmtBytes(task.payload_size)} · application/json
        </span>
      </h4>
      {parsed.ok ? (
        <JsonTree data={parsed.data} defaultOpen={3} />
      ) : (
        <pre
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: 12.5,
            background: "var(--code-bg)",
            border: "1px solid var(--border)",
            borderRadius: "var(--r-3)",
            padding: "12px 14px",
            margin: 0,
            overflowX: "auto",
            color: "var(--code-fg)",
            whiteSpace: "pre-wrap",
            wordBreak: "break-all",
          }}
        >
          {task.payload}
        </pre>
      )}
    </section>
  )
}

function AttemptsTab({ task }: { task: TaskDetail }): React.JSX.Element {
  const messages = task.messages ?? []

  if (messages.length === 0) {
    if (task.status === "completed") {
      return (
        <section>
          <h4>Attempts</h4>
          <div className="attempt completed">
            <div className="head">
              <span className="n">#1</span>
              <StatusDot status="completed" />
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: 12,
                  color: "var(--fg-2)",
                }}
              >
                handler returned nil
              </span>
              <span style={{ flex: 1 }} />
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: 11.5,
                  color: "var(--fg-3)",
                }}
              >
                {fmtTime(task.finalized_at || task.updated_at)}
              </span>
            </div>
          </div>
        </section>
      )
    }
    return (
      <div className="empty">
        <div className="h">No attempts yet.</div>
        <div>
          This task is{" "}
          <code style={{ fontFamily: "var(--font-mono)" }}>{task.status}</code>{" "}
          · blocked_till {fmtTime(task.blocked_till)}.
        </div>
      </div>
    )
  }

  return (
    <section>
      <h4>Attempts</h4>
      {messages.map((msg, i) => {
        const isLast = i === messages.length - 1
        const status: TaskStatus =
          isLast && task.status === "completed" ? "completed" : "failed"
        return (
          <div key={i} className={"attempt " + status}>
            <div className="head">
              <span className="n">#{i + 1}</span>
              <StatusDot status={status} />
              <span className="err">{msg}</span>
            </div>
          </div>
        )
      })}
    </section>
  )
}

interface TimingTabProps {
  task: TaskDetail
  attemptsMax: number
  duration: number | null
}

function TimingTab({
  task,
  attemptsMax,
  duration,
}: TimingTabProps): React.JSX.Element {
  const rows: Array<{ k: string; v: string }> = [
    { k: "created_at", v: fmtTime(task.created_at) },
    { k: "attempted_at", v: fmtTime(task.attempted_at) },
    { k: "finalized_at", v: fmtTime(task.finalized_at) },
    { k: "duration", v: duration != null ? fmtMs(duration) : "–" },
    {
      k: "attempts",
      v: `${task.attempts_elapsed}/${attemptsMax}`,
    },
    { k: "retry policy", v: "exponential backoff" },
  ]

  return (
    <section>
      <h4>Timing</h4>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: 12.5,
          lineHeight: 1.7,
          color: "var(--fg-2)",
        }}
      >
        {rows.map((r) => (
          <div
            key={r.k}
            style={{
              display: "grid",
              gridTemplateColumns: "140px 1fr",
              gap: 8,
            }}
          >
            <span style={{ color: "var(--fg-3)" }}>{r.k}</span>
            <span style={{ color: "var(--fg-1)" }}>{r.v}</span>
          </div>
        ))}
      </div>
    </section>
  )
}

function RelatedTab({ task }: { task: TaskDetail }): React.JSX.Element {
  const { data, isLoading, error } = useQuery({
    queryKey: ["tasks", "related", task.type, task.id],
    queryFn: () => getTasks({ type: task.type, limit: 4 }),
  })

  const related: TaskSummary[] = (data?.tasks ?? [])
    .filter((t) => t.id !== task.id)
    .slice(0, 3)

  return (
    <section>
      {task.idempotency_token && (
        <>
          <h4>Idempotency token</h4>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 12.5,
              padding: "6px 0 14px",
            }}
          >
            <span style={{ color: "var(--fg-3)" }}>tok</span>{" "}
            <span style={{ color: "var(--fg-1)" }}>
              {task.idempotency_token}
            </span>
          </div>
        </>
      )}

      <h4>Recent {task.type}</h4>
      {isLoading && (
        <div style={{ fontSize: 12.5, color: "var(--fg-3)" }}>Loading…</div>
      )}
      {error && (
        <div style={{ fontSize: 12.5, color: "var(--status-failed)" }}>
          Failed to load related tasks.
        </div>
      )}
      {!isLoading && !error && related.length === 0 && (
        <div style={{ fontSize: 12.5, color: "var(--fg-3)" }}>
          No other tasks of this type.
        </div>
      )}
      {related.length > 0 && (
        <table className="dt" style={{ borderTop: "1px solid var(--border)" }}>
          <tbody>
            {related.map((t) => {
              const dur = computeDurationMs(t)
              return (
                <tr key={t.id} style={{ height: 30 }}>
                  <td className="id" style={{ paddingLeft: 0 }}>
                    #{t.id}
                  </td>
                  <td>
                    <StatusChip status={t.status} />
                  </td>
                  <td className="num">{dur != null ? fmtMs(dur) : "–"}</td>
                  <td className="num muted">{ago(t.created_at)} ago</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </section>
  )
}

function RawTab({ task }: { task: TaskDetail }): React.JSX.Element {
  return (
    <section>
      <h4>asynqpg_tasks · row {task.id}</h4>
      <JsonTree data={task} defaultOpen={2} />
    </section>
  )
}

// Compute end-to-end run duration in ms from task fields, or null if unknown.
function computeDurationMs(t: {
  attempted_at: string | null
  finalized_at: string | null
  updated_at: string
}): number | null {
  if (!t.attempted_at) return null
  const start = new Date(t.attempted_at).getTime()
  const endIso = t.finalized_at || t.updated_at
  const end = new Date(endIso).getTime()
  if (isNaN(start) || isNaN(end) || end < start) return null
  return end - start
}
