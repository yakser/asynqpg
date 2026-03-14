import { useState, useEffect } from "react"
import { Link, Outlet, useLocation } from "react-router"
import { LayoutDashboard, ListTodo, AlertTriangle, Settings, Menu, X } from "lucide-react"
import { cn } from "@/lib/utils"
import { UserMenu } from "@/components/UserMenu"

const navItems = [
  { to: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { to: "/tasks", label: "Tasks", icon: ListTodo },
  { to: "/tasks?status=failed", label: "Dead Letter Queue", icon: AlertTriangle },
  { to: "/settings", label: "Settings", icon: Settings },
]

function useIsNavActive(to: string): boolean {
  const { pathname, search } = useLocation()
  const [itemPath, itemSearch] = to.split("?")

  if (pathname !== itemPath) return false

  // DLQ link is active only when ?status=failed is present.
  if (itemSearch) return search.includes(itemSearch)

  // "Tasks" link is active when on /tasks WITHOUT ?status=failed.
  return !search.includes("status=failed")
}

function NavItem({ to, label, icon: Icon, onClick }: (typeof navItems)[number] & { onClick?: () => void }) {
  const active = useIsNavActive(to)
  return (
    <Link
      to={to}
      onClick={onClick}
      aria-current={active ? "page" : undefined}
      className={cn(
        "flex items-center gap-2 px-3 py-2 rounded-md text-sm font-medium transition-colors",
        active
          ? "bg-gray-200 text-gray-900 dark:bg-gray-700 dark:text-gray-100"
          : "text-gray-600 hover:bg-gray-100 hover:text-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-gray-100",
      )}
    >
      <Icon className="h-4 w-4" />
      {label}
    </Link>
  )
}

export function Layout() {
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const location = useLocation()

  useEffect(() => {
    setSidebarOpen(false)
  }, [location])

  return (
    <div className="flex flex-col md:flex-row h-screen">
      {/* Skip link */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:z-[100] focus:top-2 focus:left-2 focus:px-4 focus:py-2 focus:rounded-md focus:bg-white focus:text-gray-900 focus:shadow-lg focus:border focus:border-gray-300 dark:focus:bg-gray-900 dark:focus:text-gray-100 dark:focus:border-gray-600"
      >
        Skip to main content
      </a>

      {/* Mobile top bar */}
      <header className="md:hidden flex items-center h-14 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 px-4 flex-shrink-0">
        <button
          onClick={() => setSidebarOpen(true)}
          aria-label="Open navigation menu"
          className="p-2 rounded-md text-gray-600 hover:bg-gray-100 dark:text-gray-400 dark:hover:bg-gray-800"
        >
          <Menu className="h-5 w-5" />
        </button>
        <Link to="/dashboard" className="flex-1 text-center font-bold tracking-tight text-base">
          asynqpg
        </Link>
        <div className="w-9" aria-hidden="true" />
      </header>

      {/* Backdrop overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/50 md:hidden"
          onClick={() => setSidebarOpen(false)}
          aria-hidden="true"
        />
      )}

      {/* Sidebar */}
      <aside
        className={cn(
          "fixed inset-y-0 left-0 z-50 w-64 border-r border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 flex flex-col transition-transform duration-200",
          "md:static md:w-56 md:translate-x-0 md:flex-shrink-0",
          sidebarOpen ? "translate-x-0" : "-translate-x-full",
        )}
      >
        <div className="flex items-center justify-between border-b border-gray-200 dark:border-gray-700">
          <Link
            to="/dashboard"
            className="flex-1 block px-4 py-5 hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
          >
            <h1 className="text-lg font-bold tracking-tight">asynqpg</h1>
            <p className="text-xs text-gray-500 dark:text-gray-400">Task Queue Dashboard</p>
          </Link>
          <button
            onClick={() => setSidebarOpen(false)}
            aria-label="Close navigation menu"
            className="md:hidden p-2 mr-2 rounded-md text-gray-600 hover:bg-gray-100 dark:text-gray-400 dark:hover:bg-gray-800"
          >
            <X className="h-5 w-5" />
          </button>
        </div>
        <nav aria-label="Main navigation" className="flex-1 px-2 py-3 space-y-1">
          {navItems.map((item) => (
            <NavItem key={item.to} {...item} onClick={() => setSidebarOpen(false)} />
          ))}
        </nav>
        <UserMenu />
      </aside>

      <main id="main-content" className="flex-1 overflow-auto p-4 md:p-6 bg-white dark:bg-gray-950">
        <Outlet />
      </main>
    </div>
  )
}
