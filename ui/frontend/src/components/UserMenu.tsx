import { useState, useRef, useEffect } from "react"
import { Link } from "react-router"
import { useAuth } from "@/contexts/AuthContext"
import { LogOut, User as UserIcon, ChevronUp } from "lucide-react"

export function UserMenu() {
  const { user, authMode, logout } = useAuth()
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  const triggerRef = useRef<HTMLButtonElement>(null)

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener("mousedown", handleClickOutside)
    return () => document.removeEventListener("mousedown", handleClickOutside)
  }, [])

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape" && open) {
        setOpen(false)
        triggerRef.current?.focus()
      }
    }
    document.addEventListener("keydown", handleKeyDown)
    return () => document.removeEventListener("keydown", handleKeyDown)
  }, [open])

  if (authMode !== "oauth" || !user) {
    return (
      <div className="px-4 py-3 border-t border-gray-200 dark:border-gray-700 text-xs text-gray-400 dark:text-gray-300">
        Powered by asynqpg
      </div>
    )
  }

  const initials = user.name
    ? user.name
        .split(" ")
        .map((n) => n[0])
        .slice(0, 2)
        .join("")
        .toUpperCase()
    : "?"

  return (
    <div ref={ref} className="relative border-t border-gray-200 dark:border-gray-700">
      <button
        ref={triggerRef}
        onClick={() => setOpen((prev) => !prev)}
        aria-expanded={open}
        aria-haspopup="true"
        className="flex items-center gap-2 w-full px-4 py-3 min-h-[44px] hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors text-left"
      >
        {user.avatar_url ? (
          <img
            src={user.avatar_url}
            alt={user.name}
            className="h-7 w-7 rounded-full flex-shrink-0"
          />
        ) : (
          <div className="h-7 w-7 rounded-full bg-gray-300 dark:bg-gray-600 flex items-center justify-center text-xs font-medium text-gray-600 dark:text-gray-300 flex-shrink-0">
            {initials}
          </div>
        )}
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">{user.name}</p>
        </div>
        <ChevronUp
          className={`h-4 w-4 text-gray-400 dark:text-gray-500 transition-transform ${open ? "" : "rotate-180"}`}
        />
      </button>

      {open && (
        <div
          role="menu"
          className="absolute bottom-full left-0 right-0 mb-1 mx-2 bg-white dark:bg-gray-800 rounded-md shadow-lg border border-gray-200 dark:border-gray-700 py-1 z-50"
        >
          <Link
            to="/profile"
            role="menuitem"
            onClick={() => setOpen(false)}
            className="flex items-center gap-2 px-3 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700"
          >
            <UserIcon className="h-4 w-4" />
            Profile
          </Link>
          <div className="border-t border-gray-100 dark:border-gray-700 my-1" />
          <button
            role="menuitem"
            onClick={() => {
              setOpen(false)
              logout()
            }}
            className="flex items-center gap-2 w-full px-3 py-2 text-sm text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-950"
          >
            <LogOut className="h-4 w-4" />
            Log out
          </button>
        </div>
      )}
    </div>
  )
}
