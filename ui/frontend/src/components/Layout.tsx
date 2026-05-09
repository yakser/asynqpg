import { useEffect, useState } from "react"
import { Outlet, useLocation, useNavigate } from "react-router"
import { Sidebar } from "./Sidebar"
import { Topbar } from "./Topbar"
import { CmdK } from "./CmdK"
import { useKeyboardShortcuts } from "@/hooks/useKeyboardShortcuts"

const MOBILE_BP = 900

function isMobileViewport(): boolean {
  if (typeof window === "undefined") return false
  return window.matchMedia(`(max-width: ${MOBILE_BP}px)`).matches
}

export function Layout() {
  const [cmdkOpen, setCmdkOpen] = useState(false)
  const [navOpen, setNavOpen] = useState(false)
  const [isMobile, setIsMobile] = useState(isMobileViewport)
  const navigate = useNavigate()
  const location = useLocation()

  useKeyboardShortcuts({
    onCmdK: () => setCmdkOpen((o) => !o),
    onSlash: () => setCmdkOpen(true),
    onG: (next) => {
      if (next === "o") navigate("/overview")
      else if (next === "t") navigate("/tasks")
      else if (next === "w") navigate("/workers")
      else if (next === "m") navigate("/maintenance")
    },
  })

  // Close the mobile drawer when the route changes (covers browser back /
  // forward navigation; in-app clicks already invoke onNavigate).
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setNavOpen(false)
  }, [location.key])

  // Track viewport so we can mark the off-canvas sidebar inert on mobile
  // and close it automatically when the viewport grows past the breakpoint.
  useEffect(() => {
    const mq = window.matchMedia(`(max-width: ${MOBILE_BP}px)`)
    const handler = () => {
      setIsMobile(mq.matches)
      if (!mq.matches) setNavOpen(false)
    }
    handler()
    mq.addEventListener("change", handler)
    return () => mq.removeEventListener("change", handler)
  }, [])

  // Lock body scroll when the drawer is open on mobile.
  useEffect(() => {
    if (!navOpen) return undefined
    const prev = document.body.style.overflow
    document.body.style.overflow = "hidden"
    return () => {
      document.body.style.overflow = prev
    }
  }, [navOpen])

  // Escape closes the mobile drawer.
  useEffect(() => {
    if (!navOpen) return undefined
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setNavOpen(false)
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [navOpen])

  // Pause CSS pulse animations while the tab is hidden — the dashboard
  // refreshes at 5s intervals, so leaving 1.6s pulses running in a
  // background tab just burns paint cycles.
  useEffect(() => {
    const sync = () => {
      if (document.visibilityState === "hidden") {
        document.documentElement.setAttribute("data-page-hidden", "")
      } else {
        document.documentElement.removeAttribute("data-page-hidden")
      }
    }
    sync()
    document.addEventListener("visibilitychange", sync)
    return () => document.removeEventListener("visibilitychange", sync)
  }, [])

  const onToggleNav = () => {
    if (isMobileViewport()) setNavOpen((o) => !o)
  }

  return (
    <div className="shell">
      <a className="skip-link sr-only" href="#main-content">
        Skip to main content
      </a>
      {navOpen && (
        <div
          className="sidebar-scrim"
          onClick={() => setNavOpen(false)}
          aria-hidden="true"
        />
      )}
      <Sidebar
        open={navOpen}
        collapsed={isMobile && !navOpen}
        onNavigate={() => setNavOpen(false)}
      />
      <div className="main-col">
        <Topbar onCmdK={() => setCmdkOpen(true)} onToggleNav={onToggleNav} />
        <main className="main" id="main-content" tabIndex={-1}>
          <Outlet />
        </main>
      </div>
      <CmdK
        key={cmdkOpen ? "cmdk-open" : "cmdk-closed"}
        open={cmdkOpen}
        onClose={() => setCmdkOpen(false)}
      />
    </div>
  )
}
