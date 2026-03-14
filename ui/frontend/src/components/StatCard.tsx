import { cn } from "@/lib/utils"

interface StatCardProps {
  title: string
  value: number | string
  className?: string
  icon?: React.ReactNode
}

export function StatCard({ title, value, className, icon }: StatCardProps) {
  return (
    <div className={cn("rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 p-4 shadow-sm", className)}>
      <div className="flex items-center justify-between">
        <p className="text-sm font-medium text-gray-500 dark:text-gray-400">{title}</p>
        {icon && <div className="text-gray-400 dark:text-gray-500">{icon}</div>}
      </div>
      <p className="mt-2 text-2xl font-bold tracking-tight">{value}</p>
    </div>
  )
}
