import { useEffect } from "react"

function isTextInput(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false
  const tag = target.tagName
  return tag === "INPUT" || tag === "TEXTAREA" || target.isContentEditable
}

interface ShortcutHandlers {
  onCmdK?: () => void
  onSlash?: () => void
  onJ?: () => void
  onK?: () => void
  onX?: () => void
  onEscape?: () => void
  onG?: (next: string) => void
}

// Wires a global keyboard listener. INPUT/TEXTAREA focus suppresses the
// non-modifier shortcuts so typing in fields does not trigger nav.
export function useKeyboardShortcuts(handlers: ShortcutHandlers): void {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault()
        handlers.onCmdK?.()
        return
      }

      if (isTextInput(document.activeElement)) return

      if (e.key === "/") {
        e.preventDefault()
        handlers.onSlash?.()
      } else if (e.key === "j") {
        e.preventDefault()
        handlers.onJ?.()
      } else if (e.key === "k") {
        e.preventDefault()
        handlers.onK?.()
      } else if (e.key === "x") {
        e.preventDefault()
        handlers.onX?.()
      } else if (e.key === "Escape") {
        handlers.onEscape?.()
      } else if (e.key === "g" && handlers.onG) {
        const onSecond = (ev: KeyboardEvent) => {
          window.removeEventListener("keydown", onSecond, true)
          handlers.onG?.(ev.key)
          ev.preventDefault()
        }
        window.addEventListener("keydown", onSecond, true)
        setTimeout(() => window.removeEventListener("keydown", onSecond, true), 800)
      }
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [handlers])
}
