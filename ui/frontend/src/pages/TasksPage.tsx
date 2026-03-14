import { useState, useCallback, useEffect } from "react"
import { useNavigate, useSearchParams } from "react-router"
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { getTasks, getTaskTypes, bulkRetry, bulkDelete, retryTask, cancelTask, deleteTask } from "@/api/client"
import type { TaskStatus, TaskSummary } from "@/api/types"
import { DataTable } from "@/components/DataTable"
import { StatusBadge } from "@/components/StatusBadge"
import { Pagination } from "@/components/Pagination"
import { useDebouncedValue } from "@/hooks/useDebouncedValue"
import type { ColumnDef } from "@tanstack/react-table"
import { RefreshCw, Trash2, XCircle, Search } from "lucide-react"

const DEFAULT_PAGE_SIZE = 25
const SORTABLE_COLUMNS = ["id", "created_at", "updated_at"]

function toLocalDatetime(iso: string): string {
  if (!iso) return ""
  const d = new Date(iso)
  const offset = d.getTimezoneOffset()
  const local = new Date(d.getTime() - offset * 60000)
  return local.toISOString().slice(0, 16)
}

function fromLocalDatetime(local: string): string {
  if (!local) return ""
  return new Date(local).toISOString()
}

export function TasksPage() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const queryClient = useQueryClient()

  const statusFilter = (searchParams.get("status") ?? "") as TaskStatus | ""
  const typeFilter = searchParams.get("type") ?? ""
  const orderBy = searchParams.get("order_by") || "created_at"
  const order = (searchParams.get("order") || "DESC") as "ASC" | "DESC"

  // Local state for debounced inputs – instant UI, delayed API calls.
  const [idInput, setIdInput] = useState(searchParams.get("id") ?? "")
  const [dateAfterInput, setDateAfterInput] = useState(searchParams.get("created_after") ?? "")
  const [dateBeforeInput, setDateBeforeInput] = useState(searchParams.get("created_before") ?? "")

  const debouncedId = useDebouncedValue(idInput, 300)
  const debouncedDateAfter = useDebouncedValue(dateAfterInput, 500)
  const debouncedDateBefore = useDebouncedValue(dateBeforeInput, 500)

  const [offset, setOffset] = useState(0)
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE)

  // Sync debounced values to URL params.
  useEffect(() => {
    setOffset(0)
    setSearchParams((prev) => {
      if (debouncedId) prev.set("id", debouncedId); else prev.delete("id")
      if (debouncedDateAfter) prev.set("created_after", debouncedDateAfter); else prev.delete("created_after")
      if (debouncedDateBefore) prev.set("created_before", debouncedDateBefore); else prev.delete("created_before")
      return prev
    })
  }, [debouncedId, debouncedDateAfter, debouncedDateBefore, setSearchParams])

  const { data: taskTypes } = useQuery({
    queryKey: ["taskTypes"],
    queryFn: getTaskTypes,
  })

  const { data, isLoading, error } = useQuery({
    queryKey: ["tasks", statusFilter, typeFilter, debouncedId, orderBy, order, debouncedDateAfter, debouncedDateBefore, offset, pageSize],
    queryFn: () =>
      getTasks({
        status: statusFilter || undefined,
        type: typeFilter || undefined,
        id: debouncedId || undefined,
        limit: pageSize,
        offset,
        order_by: orderBy,
        order,
        created_after: debouncedDateAfter || undefined,
        created_before: debouncedDateBefore || undefined,
      }),
    refetchInterval: 5_000,
  })

  const invalidateAll = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: ["tasks"] })
    queryClient.invalidateQueries({ queryKey: ["stats"] })
  }, [queryClient])

  const retryMutation = useMutation({
    mutationFn: () => bulkRetry(typeFilter || undefined),
    onSuccess: () => {
      deleteMutation.reset()
      invalidateAll()
    },
  })

  const deleteMutation = useMutation({
    mutationFn: () => bulkDelete(typeFilter || undefined),
    onSuccess: () => {
      retryMutation.reset()
      invalidateAll()
    },
  })

  const rowRetryMut = useMutation({
    mutationFn: (id: number) => retryTask(id),
    onSuccess: invalidateAll,
  })
  const rowCancelMut = useMutation({
    mutationFn: (id: number) => cancelTask(id),
    onSuccess: invalidateAll,
  })
  const rowDeleteMut = useMutation({
    mutationFn: (id: number) => deleteTask(id),
    onSuccess: invalidateAll,
  })

  const rowActionPending = rowRetryMut.isPending || rowCancelMut.isPending || rowDeleteMut.isPending

  const handleRowClick = useCallback(
    (row: TaskSummary) => {
      navigate(`/tasks/${row.id}`)
    },
    [navigate],
  )

  const updateFilter = useCallback(
    (key: string, value: string) => {
      setOffset(0)
      setSearchParams((prev) => {
        if (value) {
          prev.set(key, value)
        } else {
          prev.delete(key)
        }
        return prev
      })
    },
    [setSearchParams],
  )

  const handleSort = useCallback(
    (column: string) => {
      setSearchParams((prev) => {
        if (prev.get("order_by") === column) {
          prev.set("order", prev.get("order") === "ASC" ? "DESC" : "ASC")
        } else {
          prev.set("order_by", column)
          prev.set("order", "DESC")
        }
        return prev
      })
      setOffset(0)
    },
    [setSearchParams],
  )

  const handlePageSizeChange = useCallback(
    (newSize: number) => {
      setPageSize(newSize)
      setOffset(0)
    },
    [],
  )

  const isDLQ = statusFilter === "failed"

  const columns: ColumnDef<TaskSummary, unknown>[] = [
    { accessorKey: "id", header: "ID", size: 80 },
    { accessorKey: "type", header: "Type" },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ getValue }) => <StatusBadge status={getValue() as TaskStatus} />,
    },
    {
      accessorKey: "attempts_elapsed",
      header: "Attempts",
      cell: ({ row }) => (
        <span>
          {row.original.attempts_elapsed} / {row.original.attempts_elapsed + row.original.attempts_left}
        </span>
      ),
    },
    {
      accessorKey: "created_at",
      header: "Created",
      cell: ({ getValue }) => new Date(getValue() as string).toLocaleString(),
    },
    {
      accessorKey: "updated_at",
      header: "Updated",
      cell: ({ getValue }) => new Date(getValue() as string).toLocaleString(),
    },
    {
      id: "actions",
      header: () => <span className="sr-only">Actions</span>,
      size: 100,
      cell: ({ row }) => {
        const task = row.original
        const s = task.status
        return (
          <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
            {(s === "failed" || s === "cancelled") && (
              <button
                onClick={() => rowRetryMut.mutate(task.id)}
                disabled={rowActionPending}
                aria-label="Retry task"
                title="Retry"
                className="rounded p-2 text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-950 disabled:opacity-50"
              >
                <RefreshCw className="h-3.5 w-3.5" />
              </button>
            )}
            {(s === "pending" || s === "running") && (
              <button
                onClick={() => rowCancelMut.mutate(task.id)}
                disabled={rowActionPending}
                aria-label="Cancel task"
                title="Cancel"
                className="rounded p-2 text-amber-600 hover:bg-amber-50 dark:hover:bg-amber-950 disabled:opacity-50"
              >
                <XCircle className="h-3.5 w-3.5" />
              </button>
            )}
            {s !== "running" && (
              <button
                onClick={() => {
                  if (window.confirm(`Delete task #${task.id}?`)) {
                    rowDeleteMut.mutate(task.id)
                  }
                }}
                disabled={rowActionPending}
                aria-label="Delete task"
                title="Delete"
                className="rounded p-2 text-red-600 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50"
              >
                <Trash2 className="h-3.5 w-3.5" />
              </button>
            )}
          </div>
        )
      },
    },
  ]

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="text-xl font-bold tracking-tight">
            {isDLQ ? "Dead Letter Queue" : "Tasks"}
          </h2>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            {isDLQ
              ? "Failed tasks that exhausted all retry attempts"
              : "Browse and manage all tasks"}
          </p>
        </div>
        {isDLQ && (
          <div className="flex flex-wrap gap-2">
            <button
              onClick={() => retryMutation.mutate()}
              disabled={retryMutation.isPending}
              className="inline-flex items-center gap-1.5 rounded-md bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
            >
              <RefreshCw className="h-3.5 w-3.5" />
              Retry All
            </button>
            <button
              onClick={() => {
                if (window.confirm("Delete all failed tasks? This cannot be undone.")) {
                  deleteMutation.mutate()
                }
              }}
              disabled={deleteMutation.isPending}
              className="inline-flex items-center gap-1.5 rounded-md bg-red-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-red-700 disabled:opacity-50"
            >
              <Trash2 className="h-3.5 w-3.5" />
              Delete All
            </button>
          </div>
        )}
      </div>

      <div className="flex flex-wrap gap-3 items-end">
        <div className="flex items-center gap-1.5 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-2 py-1 w-full sm:w-auto">
          <Search className="h-3.5 w-3.5 text-gray-400 flex-shrink-0" />
          <input
            type="text"
            inputMode="numeric"
            placeholder="Task ID"
            value={idInput}
            onChange={(e) => setIdInput(e.target.value.replace(/\D/g, ""))}
            className="w-full sm:w-24 border-0 bg-transparent text-sm outline-none placeholder:text-gray-400"
          />
        </div>

        <select
          aria-label="Filter by status"
          value={statusFilter}
          onChange={(e) => updateFilter("status", e.target.value)}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-1.5 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 w-full sm:w-auto"
        >
          <option value="">All Statuses</option>
          <option value="pending">Pending</option>
          <option value="running">Running</option>
          <option value="completed">Completed</option>
          <option value="failed">Failed</option>
          <option value="cancelled">Cancelled</option>
        </select>

        <select
          aria-label="Filter by task type"
          value={typeFilter}
          onChange={(e) => updateFilter("type", e.target.value)}
          className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-1.5 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 w-full sm:w-auto"
        >
          <option value="">All Types</option>
          {taskTypes?.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>

        <div className="flex items-center gap-1.5">
          <label htmlFor="date-from" className="text-xs text-gray-500 dark:text-gray-400">From</label>
          <input
            id="date-from"
            type="datetime-local"
            value={dateAfterInput ? toLocalDatetime(dateAfterInput) : ""}
            onChange={(e) => setDateAfterInput(e.target.value ? fromLocalDatetime(e.target.value) : "")}
            className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-2 py-1 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
          />
        </div>

        <div className="flex items-center gap-1.5">
          <label htmlFor="date-to" className="text-xs text-gray-500 dark:text-gray-400">To</label>
          <input
            id="date-to"
            type="datetime-local"
            value={dateBeforeInput ? toLocalDatetime(dateBeforeInput) : ""}
            onChange={(e) => setDateBeforeInput(e.target.value ? fromLocalDatetime(e.target.value) : "")}
            className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-2 py-1 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
          />
        </div>
      </div>

      {error && (
        <div role="alert" className="rounded-md bg-red-50 dark:bg-red-950 p-3 text-sm text-red-700 dark:text-red-400">
          Failed to load tasks
        </div>
      )}

      {isLoading ? (
        <div role="status" aria-live="polite" className="flex items-center justify-center h-32">
          <p className="text-gray-400">Loading...</p>
        </div>
      ) : (
        <>
          <DataTable
            columns={columns}
            data={data?.tasks ?? []}
            onRowClick={handleRowClick}
            sortColumn={orderBy}
            sortDirection={order}
            onSort={handleSort}
            sortableColumns={SORTABLE_COLUMNS}
            ariaLabel="Tasks"
          />
          <Pagination
            offset={offset}
            limit={pageSize}
            total={data?.total ?? 0}
            onPageChange={setOffset}
            onPageSizeChange={handlePageSizeChange}
          />
        </>
      )}

      {(retryMutation.isSuccess || deleteMutation.isSuccess) && (
        <div role="status" className="rounded-md bg-green-50 dark:bg-green-950 p-3 text-sm text-green-700 dark:text-green-400">
          {retryMutation.isSuccess &&
            `Retried ${retryMutation.data.affected} task(s).`}
          {deleteMutation.isSuccess &&
            `Deleted ${deleteMutation.data.affected} task(s).`}
        </div>
      )}
    </div>
  )
}
