import { useCallback } from "react"
import { useNavigate } from "react-router"
import { useQuery } from "@tanstack/react-query"
import { getStats } from "@/api/client"
import type { TaskStatus } from "@/api/types"
import { StatCard } from "@/components/StatCard"
import { DataTable } from "@/components/DataTable"
import type { ColumnDef } from "@tanstack/react-table"
import { Activity, CheckCircle, XCircle, Clock, Ban } from "lucide-react"

const statusIcons: Record<TaskStatus, React.ReactNode> = {
  pending: <Clock className="h-5 w-5 text-amber-500" />,
  running: <Activity className="h-5 w-5 text-blue-500" />,
  completed: <CheckCircle className="h-5 w-5 text-green-500" />,
  failed: <XCircle className="h-5 w-5 text-red-500" />,
  cancelled: <Ban className="h-5 w-5 text-gray-400" />,
}

interface TypeRow {
  type: string
  total: number
  pending: number
  running: number
  completed: number
  failed: number
  cancelled: number
}

function StatusCell({ value, status, type }: { value: number; status: TaskStatus; type: string }) {
  const navigate = useNavigate()
  if (value === 0) return <span className="text-gray-300 dark:text-gray-600">0</span>

  const colorMap: Record<TaskStatus, string> = {
    pending: "text-amber-600",
    running: "text-blue-600",
    completed: "text-green-600",
    failed: "text-red-600",
    cancelled: "text-gray-500",
  }

  return (
    <button
      onClick={(e) => {
        e.stopPropagation()
        const params = new URLSearchParams({ status, type })
        navigate(`/tasks?${params.toString()}`)
      }}
      className={`font-medium ${colorMap[status]} hover:underline`}
    >
      {value}
    </button>
  )
}

function makeTypeColumns(): ColumnDef<TypeRow, unknown>[] {
  return [
    { accessorKey: "type", header: "Task Type" },
    { accessorKey: "total", header: "Total" },
    {
      accessorKey: "pending",
      header: "Pending",
      cell: ({ getValue, row }) => (
        <StatusCell value={getValue() as number} status="pending" type={row.original.type} />
      ),
    },
    {
      accessorKey: "running",
      header: "Running",
      cell: ({ getValue, row }) => (
        <StatusCell value={getValue() as number} status="running" type={row.original.type} />
      ),
    },
    {
      accessorKey: "completed",
      header: "Completed",
      cell: ({ getValue, row }) => (
        <StatusCell value={getValue() as number} status="completed" type={row.original.type} />
      ),
    },
    {
      accessorKey: "failed",
      header: "Failed",
      cell: ({ getValue, row }) => (
        <StatusCell value={getValue() as number} status="failed" type={row.original.type} />
      ),
    },
    {
      accessorKey: "cancelled",
      header: "Cancelled",
      cell: ({ getValue, row }) => (
        <StatusCell value={getValue() as number} status="cancelled" type={row.original.type} />
      ),
    },
  ]
}

const typeColumns = makeTypeColumns()

export function DashboardPage() {
  const navigate = useNavigate()
  const { data: stats, isLoading, error } = useQuery({
    queryKey: ["stats"],
    queryFn: getStats,
    refetchInterval: 5_000,
  })

  const handleRowClick = useCallback(
    (row: TypeRow) => {
      navigate(`/tasks?type=${encodeURIComponent(row.type)}`)
    },
    [navigate],
  )

  if (isLoading) {
    return (
      <div role="status" aria-live="polite" className="flex items-center justify-center h-64">
        <p className="text-gray-400">Loading...</p>
      </div>
    )
  }

  if (error || !stats) {
    return (
      <div role="alert" className="flex items-center justify-center h-64">
        <p className="text-red-500">Failed to load statistics</p>
      </div>
    )
  }

  const statuses: TaskStatus[] = ["pending", "running", "completed", "failed", "cancelled"]

  const typeRows: TypeRow[] = stats.by_type.map((t) => ({
    type: t.type,
    total: t.total,
    pending: t.by_status.pending ?? 0,
    running: t.by_status.running ?? 0,
    completed: t.by_status.completed ?? 0,
    failed: t.by_status.failed ?? 0,
    cancelled: t.by_status.cancelled ?? 0,
  }))

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-bold tracking-tight">Dashboard</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">Overview of your task queues</p>
      </div>

      <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
        <StatCard title="Total" value={stats.total} className="col-span-1" />
        {statuses.map((s) => (
          <StatCard
            key={s}
            title={s.charAt(0).toUpperCase() + s.slice(1)}
            value={stats.by_status[s] ?? 0}
            icon={statusIcons[s]}
          />
        ))}
      </div>

      <div>
        <h3 className="text-lg font-semibold mb-3">By Task Type</h3>
        <DataTable columns={typeColumns} data={typeRows} onRowClick={handleRowClick} ariaLabel="Tasks by type" />
      </div>
    </div>
  )
}
