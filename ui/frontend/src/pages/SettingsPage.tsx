import { Sun, Moon, Monitor } from "lucide-react"
import { useTheme } from "@/contexts/ThemeContext"
import { cn } from "@/lib/utils"

const themeOptions = [
  { value: "light" as const, label: "Light", icon: Sun },
  { value: "dark" as const, label: "Dark", icon: Moon },
  { value: "system" as const, label: "System", icon: Monitor },
]

export function SettingsPage() {
  const { theme, setTheme } = useTheme()

  return (
    <div className="max-w-lg mx-auto">
      <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-6">Settings</h2>

      <div className="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-6">
        <h3 className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-4">Appearance</h3>
        <div role="group" aria-label="Theme selection" className="grid grid-cols-3 gap-3">
          {themeOptions.map(({ value, label, icon: Icon }) => (
            <button
              key={value}
              onClick={() => setTheme(value)}
              aria-pressed={theme === value}
              className={cn(
                "flex flex-col items-center gap-2 rounded-lg border px-4 py-3 text-sm font-medium transition-colors",
                theme === value
                  ? "border-gray-900 bg-gray-100 text-gray-900 dark:border-gray-100 dark:bg-gray-800 dark:text-gray-100"
                  : "border-gray-200 text-gray-600 hover:bg-gray-50 dark:border-gray-700 dark:text-gray-400 dark:hover:bg-gray-800",
              )}
            >
              <Icon className="h-5 w-5" />
              {label}
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}
