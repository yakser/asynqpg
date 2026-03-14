import { Link } from "react-router"
import { useAuth } from "@/contexts/AuthContext"
import { LogOut, Settings } from "lucide-react"

export function ProfilePage() {
  const { user, logout } = useAuth()

  if (!user) {
    return null
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
    <div className="max-w-lg mx-auto">
      <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-6">Profile</h2>

      <div className="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-6">
        <div className="flex items-center gap-4 mb-6">
          {user.avatar_url ? (
            <img
              src={user.avatar_url}
              alt={user.name}
              className="h-16 w-16 rounded-full"
            />
          ) : (
            <div className="h-16 w-16 rounded-full bg-gray-200 dark:bg-gray-700 flex items-center justify-center text-lg font-medium text-gray-600 dark:text-gray-300">
              {initials}
            </div>
          )}
          <div>
            <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100">{user.name}</h3>
            {user.email && (
              <p className="text-sm text-gray-500 dark:text-gray-400">{user.email}</p>
            )}
          </div>
        </div>

        <div className="space-y-3 border-t border-gray-100 dark:border-gray-800 pt-4">
          <div className="flex justify-between text-sm">
            <span className="text-gray-500 dark:text-gray-400">Provider</span>
            <span className="text-gray-900 dark:text-gray-100 capitalize">{user.provider}</span>
          </div>
          <div className="flex justify-between text-sm">
            <span className="text-gray-500 dark:text-gray-400">User ID</span>
            <span className="text-gray-900 dark:text-gray-100 font-mono text-xs">{user.id}</span>
          </div>
        </div>

        <div className="border-t border-gray-100 dark:border-gray-800 mt-6 pt-4 flex flex-wrap items-center gap-3">
          <Link
            to="/settings"
            className="flex items-center gap-2 px-4 py-2.5 rounded-md border border-gray-200 dark:border-gray-700 text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            <Settings className="h-4 w-4" />
            Settings
          </Link>

          <button
            onClick={logout}
            className="flex items-center gap-2 px-4 py-2.5 rounded-md border border-red-200 dark:border-red-800 text-sm font-medium text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-950 transition-colors"
          >
            <LogOut className="h-4 w-4" />
            Log out
          </button>
        </div>
      </div>
    </div>
  )
}
