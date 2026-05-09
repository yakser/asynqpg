import { useEffect, useId, useMemo, useRef, useState } from "react"
import { useNavigate } from "react-router"
import { useQuery } from "@tanstack/react-query"
import { Icon } from "@/components/Icon"
import { useTheme } from "@/contexts/theme"
import { getConfig, getTasks, getTaskTypes } from "@/api/client"
import type { TaskSummary } from "@/api/types"

interface CmdKProps {
  open: boolean
  onClose: () => void
}

type ItemKind = "task" | "nav" | "action" | "type"

interface CmdItem {
  kind: ItemKind
  icon: string
  label: string
  sub?: string
  action: () => void
}

interface IndexedItem extends CmdItem {
  idx: number
}

interface GroupProps {
  title: string
  items: IndexedItem[]
  active: number
  setActive: (idx: number) => void
  onPick: () => void
  listboxId: string
}

function Group({ title, items, active, setActive, onPick, listboxId }: GroupProps): React.JSX.Element {
  return (
    <>
      <div className="cmdk-grp" aria-hidden="true">
        {title}
      </div>
      {items.map((it) => (
        <button
          key={it.idx}
          type="button"
          id={`${listboxId}-opt-${it.idx}`}
          role="option"
          aria-selected={it.idx === active}
          className={"cmdk-it" + (it.idx === active ? " active" : "")}
          onMouseEnter={() => setActive(it.idx)}
          onClick={() => {
            it.action()
            onPick()
          }}
        >
          <Icon name={it.icon} size={15} />
          <span>{it.label}</span>
          {it.sub && <span className="sub">{it.sub}</span>}
        </button>
      ))}
    </>
  )
}

const TASK_ID_RE = /^#?\d{3,}$/

export function CmdK({ open, onClose }: CmdKProps): React.JSX.Element | null {
  const navigate = useNavigate()
  const { theme, setTheme } = useTheme()
  const [q, setQ] = useState("")
  const [active, setActive] = useState(0)
  const inputRef = useRef<HTMLInputElement | null>(null)
  const listboxId = useId()
  const titleId = useId()

  // Focus the input on mount. Parent remounts CmdK on open via key prop,
  // so this effect runs each time the palette is shown.
  useEffect(() => {
    const t = setTimeout(() => inputRef.current?.focus(), 10)
    return () => clearTimeout(t)
  }, [])

  // Restore focus to the previously focused element when closing.
  useEffect(() => {
    if (!open) return undefined
    const previouslyFocused = document.activeElement as HTMLElement | null
    return () => {
      previouslyFocused?.focus?.()
    }
  }, [open])

  const { data: taskTypes } = useQuery({
    queryKey: ["task-types"],
    queryFn: getTaskTypes,
  })

  const { data: config } = useQuery({
    queryKey: ["config"],
    queryFn: getConfig,
  })

  const trimmed = q.trim()
  const isIdQuery = TASK_ID_RE.test(trimmed)
  const idQuery = isIdQuery ? trimmed.replace(/[^\d]/g, "") : ""

  const { data: idMatches } = useQuery({
    queryKey: ["tasks", "byId", idQuery],
    queryFn: () => getTasks({ id: idQuery }),
    enabled: open && isIdQuery,
  })

  const items = useMemo<CmdItem[]>(() => {
    const navs: CmdItem[] = [
      {
        kind: "nav",
        icon: "layout-dashboard",
        label: "Go to Overview",
        sub: "G O",
        action: () => navigate("/overview"),
      },
      {
        kind: "nav",
        icon: "list",
        label: "Go to Tasks",
        sub: "G T",
        action: () => navigate("/tasks"),
      },
      {
        kind: "nav",
        icon: "cpu",
        label: "Go to Workers",
        sub: "G W",
        action: () => navigate("/workers"),
      },
      {
        kind: "nav",
        icon: "wrench",
        label: "Go to Maintenance",
        sub: "G M",
        action: () => navigate("/maintenance"),
      },
      {
        kind: "nav",
        icon: "user",
        label: "Go to Profile & Settings",
        sub: "theme · density",
        action: () => navigate("/profile"),
      },
    ]

    const types: CmdItem[] = (taskTypes ?? []).map((t) => ({
      kind: "type",
      icon: "filter",
      label: "Filter tasks: " + t,
      sub: t,
      action: () => navigate("/tasks?type=" + encodeURIComponent(t)),
    }))

    const actions: CmdItem[] = [
      {
        kind: "action",
        icon: "moon",
        label: "Toggle theme",
        sub: "dark ↔ light",
        action: () => setTheme(theme === "dark" ? "light" : "dark"),
      },
      {
        kind: "action",
        icon: "sun",
        label: "Use light theme",
        sub: "settings",
        action: () => setTheme("light"),
      },
      {
        kind: "action",
        icon: "moon",
        label: "Use dark theme",
        sub: "settings",
        action: () => setTheme("dark"),
      },
      {
        kind: "action",
        icon: "user",
        label: "Use system theme",
        sub: "follow OS preference",
        action: () => setTheme("system"),
      },
    ]

    let out: CmdItem[] = [...navs, ...actions, ...types]

    if (isIdQuery && idMatches?.tasks) {
      const matches = idMatches.tasks.slice(0, 5)
      const taskItems: CmdItem[] = matches.map((t: TaskSummary) => ({
        kind: "task",
        icon: "hash",
        label: "Open task #" + t.id,
        sub: t.type + " · " + t.status,
        action: () => navigate("/tasks/" + t.id),
      }))
      out = [...taskItems, ...out]
    }

    if (trimmed) {
      const ql = trimmed.toLowerCase()
      out = out.filter(
        (it) =>
          it.label.toLowerCase().includes(ql) ||
          (it.sub ?? "").toLowerCase().includes(ql),
      )
    }

    return out
  }, [navigate, taskTypes, isIdQuery, idMatches, trimmed, theme, setTheme])

  useEffect(() => {
    if (!open) return undefined
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "ArrowDown") {
        e.preventDefault()
        setActive((a) => Math.min(items.length - 1, a + 1))
      } else if (e.key === "ArrowUp") {
        e.preventDefault()
        setActive((a) => Math.max(0, a - 1))
      } else if (e.key === "Enter") {
        e.preventDefault()
        const it = items[active]
        if (it) {
          it.action()
          onClose()
        }
      } else if (e.key === "Escape") {
        onClose()
      }
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [open, items, active, onClose])

  const grouped = useMemo(() => {
    const g: Record<ItemKind, IndexedItem[]> = { task: [], nav: [], action: [], type: [] }
    items.forEach((it, idx) => {
      g[it.kind].push({ ...it, idx })
    })
    return g
  }, [items])

  if (!open) return null

  const activeId = items[active] ? `${listboxId}-opt-${active}` : undefined

  return (
    <div
      className="cmdk-scrim"
      onClick={onClose}
      role="presentation"
    >
      <div
        className="cmdk"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
      >
        <h2 id={titleId} className="sr-only">Command palette</h2>
        <div className="cmdk-input">
          <Icon name="terminal" size={16} />
          <input
            ref={inputRef}
            value={q}
            onChange={(e) => {
              setQ(e.target.value)
              setActive(0)
            }}
            placeholder="Type a command, navigate, search task #id…"
            aria-label="Command palette query"
            aria-controls={listboxId}
            aria-activedescendant={activeId}
            role="combobox"
            aria-expanded="true"
            aria-autocomplete="list"
          />
          <kbd aria-hidden="true">esc</kbd>
        </div>
        <div
          id={listboxId}
          role="listbox"
          aria-label="Command results"
          className="cmdk-list"
        >
          {grouped.task.length > 0 && (
            <Group
              title="Tasks"
              items={grouped.task}
              active={active}
              setActive={setActive}
              onPick={onClose}
              listboxId={listboxId}
            />
          )}
          {grouped.nav.length > 0 && (
            <Group
              title="Navigate"
              items={grouped.nav}
              active={active}
              setActive={setActive}
              onPick={onClose}
              listboxId={listboxId}
            />
          )}
          {grouped.action.length > 0 && (
            <Group
              title="Actions"
              items={grouped.action}
              active={active}
              setActive={setActive}
              onPick={onClose}
              listboxId={listboxId}
            />
          )}
          {grouped.type.length > 0 && (
            <Group
              title="Filter by task type"
              items={grouped.type}
              active={active}
              setActive={setActive}
              onPick={onClose}
              listboxId={listboxId}
            />
          )}
          {items.length === 0 && (
            <div
              role="status"
              style={{
                padding: "24px 16px",
                color: "var(--fg-3)",
                fontSize: 13,
                textAlign: "center",
              }}
            >
              No matches · try a task id or queue name
            </div>
          )}
        </div>
        <div className="cmdk-foot">
          <span className="h">
            <kbd aria-hidden="true">↵</kbd> select
          </span>
          <span className="h">
            <kbd aria-hidden="true">↑</kbd>
            <kbd aria-hidden="true">↓</kbd> move
          </span>
          <span className="h">
            <kbd aria-hidden="true">esc</kbd> close
          </span>
          <span style={{ flex: 1 }} aria-hidden="true" />
          <span className="h">
            asynqpg{" "}
            {config?.version && (
              <span style={{ color: "var(--fg-2)" }}>{config.version}</span>
            )}
          </span>
        </div>
      </div>
    </div>
  )
}
