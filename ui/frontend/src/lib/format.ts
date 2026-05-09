// Compact number formatting: 1.2k, 4.8M.
export function fmtN(n: number | null | undefined): string {
  if (n == null) return "—"
  if (n >= 1e6) return (n / 1e6).toFixed(n >= 10e6 ? 0 : 1) + "M"
  if (n >= 1e3) return (n / 1e3).toFixed(n >= 10e3 ? 0 : 1) + "k"
  return String(n)
}

// Duration formatting: 180ms / 1.20s / 4.5m.
export function fmtMs(ms: number | null | undefined): string {
  if (ms == null) return "—"
  if (ms < 1000) return ms + "ms"
  if (ms < 60_000) return (ms / 1000).toFixed(ms < 10_000 ? 2 : 1) + "s"
  return (ms / 60_000).toFixed(1) + "m"
}

// Byte formatting: 200B / 1.5KB / 2.0MB.
export function fmtBytes(b: number | null | undefined): string {
  if (b == null) return "—"
  if (b < 1024) return b + "B"
  if (b < 1024 * 1024) return (b / 1024).toFixed(1) + "KB"
  return (b / 1024 / 1024).toFixed(1) + "MB"
}

// UTC ISO-ish timestamp: 2026-05-08 10:00:00Z.
export function fmtTime(d: Date | string | null | undefined): string {
  if (d == null) return "—"
  const date = d instanceof Date ? d : new Date(d)
  if (isNaN(date.getTime())) return "—"
  const pad = (n: number) => String(n).padStart(2, "0")
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}Z`
}

// Relative duration: "12s", "5m 12s", "3h 4m", "2d".
export function ago(d: Date | string | null | undefined, now: number = Date.now()): string {
  if (d == null) return "—"
  const date = d instanceof Date ? d : new Date(d)
  if (isNaN(date.getTime())) return "—"
  const s = Math.max(1, Math.floor((now - date.getTime()) / 1000))
  if (s < 60) return s + "s"
  if (s < 3600) return Math.floor(s / 60) + "m " + (s % 60) + "s"
  if (s < 86400) return Math.floor(s / 3600) + "h " + Math.floor((s % 3600) / 60) + "m"
  return Math.floor(s / 86400) + "d"
}
