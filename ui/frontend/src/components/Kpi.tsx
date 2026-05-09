import type { ReactNode } from "react"

interface KpiProps {
  label: string
  value: ReactNode
  color?: string
  live?: boolean
  sub?: ReactNode
}

export function Kpi({ label, value, color, live, sub }: KpiProps) {
  return (
    <div className="kpi">
      <div className="lbl">
        {color && (
          <span
            className="dot"
            style={{ background: color, width: 6, height: 6, borderRadius: 999, display: "inline-block" }}
          />
        )}
        {label}
        {live && <span className="live-pulse" aria-hidden />}
      </div>
      <div className="num" style={color && !sub ? undefined : { color }}>
        {value}
      </div>
      {sub && (
        <div style={{ fontSize: 12, color: "var(--fg-3)", fontFamily: "var(--font-mono)" }}>
          {sub}
        </div>
      )}
    </div>
  )
}
