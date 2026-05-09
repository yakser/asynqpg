import { useEffect, useMemo, useState } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { useSearchParams } from "react-router"
import {
  cancelTask,
  deleteTask,
  getTask,
  getTaskTypes,
  getTasks,
  retryTask,
} from "@/api/client"
import type { TaskListParams, TaskStatus, TaskSummary } from "@/api/types"
import { Button } from "@/components/Button"
import { BulkBar } from "@/components/BulkBar"
import { FilterPill } from "@/components/FilterPill"
import { Icon } from "@/components/Icon"
import { SavedViews, type SavedView } from "@/components/SavedViews"
import { StatusChip } from "@/components/StatusChip"
import { TaskDrawer } from "@/components/TaskDrawer"
import { useDebouncedValue } from "@/hooks/useDebouncedValue"
import { ago, fmtMs, fmtN } from "@/lib/format"

type ViewId = "all" | "pending" | "running" | "failed" | "needs-retry" | "deadletter"

interface ViewDef extends SavedView<ViewId> {
  filter: { status?: TaskStatus[]; idempotency_token?: "has" | "none" }
  postFilter?: (t: TaskSummary) => boolean
}

const VIEWS: ViewDef[] = [
  { id: "all", label: "All", filter: {} },
  { id: "pending", label: "Pending", filter: { status: ["pending"] } },
  { id: "running", label: "Running", filter: { status: ["running"] } },
  { id: "failed", label: "Failed", filter: { status: ["failed"] } },
  {
    id: "needs-retry",
    label: "Needs retry",
    filter: { status: ["failed"] },
    postFilter: (t) => t.attempts_left > 0,
  },
  {
    id: "deadletter",
    label: "Dead-letter",
    filter: { status: ["failed"] },
    postFilter: (t) => t.attempts_left === 0,
  },
]

const STATUSES: TaskStatus[] = ["pending", "running", "completed", "failed", "cancelled"]

const PAGE_SIZE = 80

export function TasksPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  const view = (searchParams.get("view") as ViewId | null) ?? "all"
  const type = searchParams.get("type")
  const status = searchParams.get("status") as TaskStatus | null
  const text = searchParams.get("q") ?? ""
  const idempotency = searchParams.get("idem") as "has" | "none" | null
  const activeIdParam = searchParams.get("id")
  const activeId = activeIdParam ? Number(activeIdParam) : null

  const debouncedText = useDebouncedValue(text, 300)
  const [selected, setSelected] = useState<Set<number>>(new Set())

  const setParam = (key: string, value: string | null) => {
    const next = new URLSearchParams(searchParams)
    if (value == null || value === "") next.delete(key)
    else next.set(key, value)
    if (key !== "id") setSelected(new Set())
    setSearchParams(next, { replace: true })
  }

  const setView = (v: ViewId) => setParam("view", v === "all" ? null : v)
  const setType = (t: string | null) => setParam("type", t)
  const setStatus = (s: TaskStatus | null) => setParam("status", s)
  const setText = (q: string) => setParam("q", q)
  const setIdem = (i: "has" | "none" | null) => setParam("idem", i)
  const setActiveId = (id: number | null) => setParam("id", id == null ? null : String(id))

  const { data: taskTypes } = useQuery({
    queryKey: ["task-types"],
    queryFn: getTaskTypes,
    staleTime: 30_000,
  })

  const viewDef = useMemo(() => VIEWS.find((v) => v.id === view) ?? VIEWS[0], [view])

  const params: TaskListParams = useMemo(() => {
    const statuses: TaskStatus[] = []
    if (viewDef.filter.status) statuses.push(...viewDef.filter.status)
    if (status) statuses.push(status)
    const uniqueStatuses = Array.from(new Set(statuses))

    const p: TaskListParams = {
      limit: PAGE_SIZE,
      order_by: "id",
      order: "DESC",
    }
    if (uniqueStatuses.length > 0) p.status = uniqueStatuses.join(",")
    if (type) p.type = type
    if (debouncedText.trim() && /^\d+$/.test(debouncedText.trim())) {
      p.id = debouncedText.trim()
    }
    if (idempotency) p.idempotency_token = idempotency
    if (viewDef.filter.idempotency_token) {
      p.idempotency_token = viewDef.filter.idempotency_token
    }
    return p
  }, [viewDef, status, type, debouncedText, idempotency])

  const tasksQuery = useQuery({
    queryKey: ["tasks", params],
    queryFn: () => getTasks(params),
    refetchInterval: 5000,
  })

  const tasks = useMemo(() => {
    const list = tasksQuery.data?.tasks ?? []
    let out = list
    if (viewDef.postFilter) out = out.filter(viewDef.postFilter)
    if (debouncedText.trim() && !/^\d+$/.test(debouncedText.trim())) {
      const q = debouncedText.toLowerCase()
      out = out.filter(
        (t) =>
          String(t.id).includes(q) ||
          t.type.toLowerCase().includes(q) ||
          (t.idempotency_token ?? "").toLowerCase().includes(q),
      )
    }
    return out
  }, [tasksQuery.data, viewDef, debouncedText])

  const { data: activeTask } = useQuery({
    queryKey: ["task", activeId],
    queryFn: () =>
      activeId == null ? Promise.reject(new Error("no id")) : getTask(activeId),
    enabled: activeId != null,
    refetchInterval: activeId != null ? 5000 : false,
  })

  const toggleOne = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const tag = (document.activeElement as HTMLElement | null)?.tagName
      if (tag === "INPUT" || tag === "TEXTAREA") return
      // Don't capture modifier-key combos (Cmd+K, Ctrl+K) here.
      if (e.metaKey || e.ctrlKey || e.altKey) return
      if (e.key === "j" || e.key === "k") {
        if (tasks.length === 0) return
        e.preventDefault()
        const idx = activeId == null ? -1 : tasks.findIndex((t) => t.id === activeId)
        const nextIdx =
          e.key === "j"
            ? Math.min(tasks.length - 1, idx < 0 ? 0 : idx + 1)
            : Math.max(0, idx < 0 ? 0 : idx - 1)
        const next = tasks[nextIdx]
        if (next) setActiveId(next.id)
      } else if (e.key === "x" && activeId != null) {
        e.preventDefault()
        toggleOne(activeId)
      } else if (e.key === "Escape" && activeId != null) {
        setActiveId(null)
      }
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tasks, activeId])

  const allChecked = tasks.length > 0 && tasks.every((t) => selected.has(t.id))
  const someChecked = tasks.some((t) => selected.has(t.id))

  const toggleAll = () => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (allChecked) tasks.forEach((t) => next.delete(t.id))
      else tasks.forEach((t) => next.add(t.id))
      return next
    })
  }

  const queryClient = useQueryClient()

  const runBulk = async (action: (id: number) => Promise<unknown>, label: string) => {
    const ids = Array.from(selected)
    if (ids.length === 0) return
    let ok = 0
    let fail = 0
    for (const id of ids) {
      try {
        await action(id)
        ok++
      } catch {
        fail++
      }
    }
    setSelected(new Set())
    queryClient.invalidateQueries({ queryKey: ["tasks"] })
    queryClient.invalidateQueries({ queryKey: ["stats"] })
    alert(`${label}: ${ok} ok, ${fail} failed`)
  }

  const onBulkRetry = () => runBulk(retryTask, "Retry")
  const onBulkCancel = () => runBulk(cancelTask, "Cancel")
  const onBulkDelete = () => {
    if (!confirm(`Delete ${selected.size} task(s)? This cannot be undone.`)) return
    runBulk(deleteTask, "Delete")
  }

  const total = tasksQuery.data?.total ?? 0
  const totalForView: Record<ViewId, number | undefined> = {
    all: total,
    pending: undefined,
    running: undefined,
    failed: undefined,
    "needs-retry": undefined,
    deadletter: undefined,
  }

  return (
    <div>
      <div className="page-head">
        <div>
          <h1>Tasks</h1>
          <div className="sub">
            {fmtN(tasks.length)} of {fmtN(total)} in current page
          </div>
        </div>
        <div className="actions">
          <Button
            icon="rotate-cw"
            variant="ghost"
            data-mobile-hide="true"
            onClick={() => tasksQuery.refetch()}
          >
            Refresh
          </Button>
        </div>
      </div>

      <SavedViews
        views={VIEWS.map((v) => ({ ...v, count: totalForView[v.id] }))}
        active={view}
        onChange={setView}
      />

      <div className="filter-bar">
        <label className="search-bar tasks-search">
          <span className="sr-only">Search tasks</span>
          <Icon name="search" size={14} />
          <input
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="id · type · idempotency token…"
            aria-label="Search tasks by id, type, or idempotency token"
          />
        </label>

        <FilterPill k="type" v={type} options={taskTypes ?? []} onChange={setType} />
        <FilterPill
          k="status"
          v={status}
          options={STATUSES}
          onChange={setStatus}
          kind="status"
        />
        <FilterPill
          k="idempotency"
          v={idempotency}
          options={["has", "none"] as const}
          onChange={(v) => setIdem(v as "has" | "none" | null)}
        />

        <span className="spacer" style={{ flex: 1 }} />
        <span style={{ fontFamily: "var(--font-mono)", fontSize: 11.5, color: "var(--fg-3)" }}>
          <kbd>j</kbd> <kbd>k</kbd> to move · <kbd>x</kbd> select · <kbd>⌘K</kbd> command
        </span>
      </div>

      {selected.size > 0 && (
        <BulkBar
          count={selected.size}
          onRetry={onBulkRetry}
          onCancel={onBulkCancel}
          onDelete={onBulkDelete}
          onClear={() => setSelected(new Set())}
        />
      )}

      <div className="table-wrap">
        <table className="dt">
          <thead>
            <tr>
              <th className="cb" style={{ width: 32 }}>
                <input
                  type="checkbox"
                  className={"cb-input" + (!allChecked && someChecked ? " indet" : "")}
                  checked={allChecked}
                  onChange={toggleAll}
                  aria-label="Select all"
                />
              </th>
              <th style={{ width: 110 }} className="mono">
                id
              </th>
              <th>type</th>
              <th style={{ width: 110 }}>status</th>
              <th style={{ width: 130, textAlign: "right" }} className="mono">
                attempts
              </th>
              <th style={{ width: 130, textAlign: "right" }} className="mono">
                created
              </th>
              <th>last message / payload</th>
              <th style={{ width: 32 }}></th>
            </tr>
          </thead>
          <tbody>
            {tasks.map((t) => (
              <Row
                key={t.id}
                t={t}
                active={activeId === t.id}
                selected={selected.has(t.id)}
                onSelect={() => toggleOne(t.id)}
                onOpen={() => setActiveId(t.id)}
              />
            ))}
            {tasks.length === 0 && !tasksQuery.isLoading && <EmptyRow />}
            {tasksQuery.isLoading && <LoadingRow />}
          </tbody>
        </table>
        {total > tasks.length && (
          <div
            style={{
              padding: "10px 16px",
              borderTop: "1px solid var(--border)",
              fontFamily: "var(--font-mono)",
              fontSize: 11.5,
              color: "var(--fg-3)",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}
          >
            <span>
              Showing {fmtN(tasks.length)} of {fmtN(total)} · narrow filters to see more
            </span>
          </div>
        )}
      </div>

      {activeTask && (
        <TaskDrawer
          key={activeTask.id}
          task={activeTask}
          onClose={() => setActiveId(null)}
          onPrev={() => {
            const idx = tasks.findIndex((t) => t.id === activeId)
            if (idx > 0) setActiveId(tasks[idx - 1].id)
          }}
          onNext={() => {
            const idx = tasks.findIndex((t) => t.id === activeId)
            if (idx >= 0 && idx < tasks.length - 1) setActiveId(tasks[idx + 1].id)
          }}
        />
      )}
    </div>
  )
}

interface RowProps {
  t: TaskSummary
  active: boolean
  selected: boolean
  onSelect: () => void
  onOpen: () => void
}

function Row({ t, active, selected, onSelect, onOpen }: RowProps) {
  const lastMessage = t.messages.length > 0 ? t.messages[t.messages.length - 1] : null
  const finalDuration =
    t.attempted_at && t.finalized_at
      ? new Date(t.finalized_at).getTime() - new Date(t.attempted_at).getTime()
      : null

  return (
    <tr
      className={(active ? "active " : "") + (selected ? "selected" : "")}
      onClick={onOpen}
      onKeyDown={(e) => {
        if (e.key === "Enter") {
          e.preventDefault()
          onOpen()
        }
      }}
      tabIndex={0}
      aria-label={`Task ${t.id}, ${t.type}, ${t.status}`}
    >
      <td className="cb" onClick={(e) => e.stopPropagation()}>
        <input
          type="checkbox"
          className="cb-input"
          checked={selected}
          onChange={onSelect}
          onClick={(e) => e.stopPropagation()}
          aria-label={`Select task ${t.id}`}
        />
      </td>
      <td className="id">#{t.id}</td>
      <td className="type">{t.type}</td>
      <td>
        <StatusChip status={t.status} />
      </td>
      <td className="num">
        <span
          style={{
            color:
              t.attempts_left === 0 && t.status === "failed"
                ? "var(--status-failed)"
                : "var(--fg-2)",
          }}
        >
          {t.attempts_elapsed}/{t.attempts_elapsed + t.attempts_left}
          {finalDuration != null && (
            <span style={{ color: "var(--fg-3)", marginLeft: 6 }}>· {fmtMs(finalDuration)}</span>
          )}
        </span>
      </td>
      <td className="num muted">{ago(t.created_at)} ago</td>
      <td
        className="trunc muted"
        style={{
          fontFamily: lastMessage ? "var(--font-mono)" : "var(--font-sans)",
          color: t.status === "failed" ? "var(--status-failed)" : "var(--fg-3)",
          fontSize: 12.5,
        }}
      >
        {lastMessage ?? (t.idempotency_token ? `tok ${t.idempotency_token}` : `payload ${fmtN(t.payload_size)}B`)}
      </td>
      <td className="act">
        <Icon name="chevron-right" size={14} style={{ color: "var(--fg-4)" }} />
      </td>
    </tr>
  )
}

function EmptyRow() {
  return (
    <tr>
      <td colSpan={8} style={{ padding: 0, borderBottom: "none" }}>
        <div className="empty">
          <div className="h">No tasks match your filters.</div>
          <div>Loosen the filter or pick another saved view.</div>
        </div>
      </td>
    </tr>
  )
}

function LoadingRow() {
  return (
    <tr>
      <td colSpan={8} style={{ padding: 0, borderBottom: "none" }}>
        <div className="empty">
          <div className="h">Loading…</div>
        </div>
      </td>
    </tr>
  )
}
