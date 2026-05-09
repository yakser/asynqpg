import { useEffect, useId, useRef, useState } from "react"
import type { TaskStatus } from "@/api/types"
import { Icon } from "./Icon"
import { StatusDot } from "./StatusChip"

interface FilterPillProps<T extends string> {
  k: string
  v: T | null
  options: T[] | readonly T[]
  onChange?: (v: T | null) => void
  kind?: "status" | "default"
}

export function FilterPill<T extends string>({
  k,
  v,
  options,
  onChange,
  kind = "default",
}: FilterPillProps<T>) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)
  const menuId = useId()

  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener("mousedown", onDoc)
    return () => document.removeEventListener("mousedown", onDoc)
  }, [])

  useEffect(() => {
    if (!open) return undefined
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setOpen(false)
        buttonRef.current?.focus()
      }
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [open])

  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button
        ref={buttonRef}
        type="button"
        className={"filter-pill" + (v ? " set" : "")}
        onClick={() => setOpen((o) => !o)}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={menuId}
        aria-label={v ? `${k}: ${v}. Click to change.` : `Filter by ${k}`}
      >
        <Icon name={v ? "check" : "plus"} size={12} />
        <span className="k">{k}</span>
        {v ? (
          <>
            <span style={{ color: "var(--fg-3)" }} aria-hidden="true">
              =
            </span>
            <span className="v">{v}</span>
          </>
        ) : null}
        {v && (
          <span
            className="x"
            role="button"
            tabIndex={0}
            aria-label={`Clear ${k} filter`}
            onClick={(e) => {
              e.stopPropagation()
              onChange?.(null)
            }}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault()
                e.stopPropagation()
                onChange?.(null)
              }
            }}
          >
            <Icon name="x" size={11} />
          </span>
        )}
      </button>
      {open && (
        <ul
          id={menuId}
          role="listbox"
          aria-label={`${k} options`}
          style={{
            position: "absolute",
            top: "calc(100% + 4px)",
            left: 0,
            zIndex: 10,
            minWidth: 200,
            maxWidth: 280,
            background: "var(--surface-2)",
            border: "1px solid var(--border-strong)",
            borderRadius: "var(--r-3)",
            boxShadow: "var(--shadow-overlay)",
            padding: 4,
            fontSize: 12.5,
            maxHeight: 320,
            overflow: "auto",
            listStyle: "none",
            margin: 0,
          }}
        >
          {options.map((o) => {
            const selected = v === o
            return (
              <li
                key={o}
                role="option"
                aria-selected={selected}
                tabIndex={0}
                onClick={() => {
                  onChange?.(o)
                  setOpen(false)
                  buttonRef.current?.focus()
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault()
                    onChange?.(o)
                    setOpen(false)
                    buttonRef.current?.focus()
                  }
                }}
                style={{
                  padding: "6px 10px",
                  borderRadius: "var(--r-2)",
                  cursor: "pointer",
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  fontFamily:
                    kind === "status" ? "var(--font-sans)" : "var(--font-mono)",
                  color: selected ? "var(--brand-600)" : "var(--fg-1)",
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.background =
                    "color-mix(in oklch, var(--brand-600) 10%, transparent)"
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.background = "transparent"
                }}
              >
                {kind === "status" && <StatusDot status={o as TaskStatus} />}
                <span>{o}</span>
                {selected && (
                  <Icon
                    name="check"
                    size={12}
                    style={{ marginLeft: "auto", color: "var(--brand-600)" }}
                  />
                )}
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}
