import { useState, useCallback } from "react"
import { useParams, useNavigate } from "react-router"
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { getTask, getTaskPayload, getConfig, retryTask, cancelTask, deleteTask } from "@/api/client"
import { StatusBadge } from "@/components/StatusBadge"
import { ArrowLeft, RefreshCw, XCircle, Trash2, Copy, Check, Eye, EyeOff } from "lucide-react"

function formatPayload(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
}

export function TaskDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const taskId = Number(id)

  const [payloadRevealed, setPayloadRevealed] = useState(false)
  const [copied, setCopied] = useState(false)

  const { data: config } = useQuery({
    queryKey: ["config"],
    queryFn: getConfig,
    staleTime: 60_000,
  })

  const { data: task, isLoading, error } = useQuery({
    queryKey: ["task", taskId],
    queryFn: () => getTask(taskId),
    enabled: !isNaN(taskId),
    refetchInterval: 5_000,
  })

  const hidePayload = config?.hide_payload_by_default ?? false

  const { data: payload, isLoading: payloadLoading } = useQuery({
    queryKey: ["taskPayload", taskId],
    queryFn: () => getTaskPayload(taskId),
    enabled: !isNaN(taskId) && (!hidePayload || payloadRevealed),
  })

  const invalidate = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: ["task", taskId] })
    queryClient.invalidateQueries({ queryKey: ["tasks"] })
    queryClient.invalidateQueries({ queryKey: ["stats"] })
  }, [queryClient, taskId])

  const retryMut = useMutation({ mutationFn: () => retryTask(taskId), onSuccess: invalidate })
  const cancelMut = useMutation({ mutationFn: () => cancelTask(taskId), onSuccess: invalidate })
  const deleteMut = useMutation({
    mutationFn: () => deleteTask(taskId),
    onSuccess: () => {
      invalidate()
      navigate("/tasks")
    },
  })

  const handleCopy = useCallback(async () => {
    if (!payload) return
    await navigator.clipboard.writeText(payload)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }, [payload])

  if (isLoading) {
    return (
      <div role="status" aria-live="polite" className="flex items-center justify-center h-64">
        <p className="text-gray-400">Loading...</p>
      </div>
    )
  }

  if (error || !task) {
    return (
      <div className="space-y-4">
        <button onClick={() => navigate(-1)} className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300">
          <ArrowLeft className="h-4 w-4" /> Back
        </button>
        <div className="rounded-md bg-red-50 dark:bg-red-950 p-4 text-sm text-red-700 dark:text-red-400">
          Task not found
        </div>
      </div>
    )
  }

  const canRetry = task.status === "failed" || task.status === "cancelled"
  const canCancel = task.status === "pending" || task.status === "running"
  const showPayload = !hidePayload || payloadRevealed

  return (
    <div className="space-y-6 max-w-3xl">
      <div className="flex flex-wrap items-center gap-3">
        <button
          onClick={() => navigate(-1)}
          className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
        >
          <ArrowLeft className="h-4 w-4" /> Back
        </button>
        <h2 className="text-xl font-bold tracking-tight">Task #{task.id}</h2>
        <StatusBadge status={task.status} />
      </div>

      <div className="flex flex-wrap gap-2">
        {canRetry && (
          <button
            onClick={() => retryMut.mutate()}
            disabled={retryMut.isPending}
            className="inline-flex items-center gap-1.5 rounded-md bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          >
            <RefreshCw className="h-3.5 w-3.5" /> Retry
          </button>
        )}
        {canCancel && (
          <button
            onClick={() => cancelMut.mutate()}
            disabled={cancelMut.isPending}
            className="inline-flex items-center gap-1.5 rounded-md bg-amber-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50"
          >
            <XCircle className="h-3.5 w-3.5" /> Cancel
          </button>
        )}
        <button
          onClick={() => {
            if (window.confirm("Delete this task permanently?")) {
              deleteMut.mutate()
            }
          }}
          disabled={deleteMut.isPending}
          className="inline-flex items-center gap-1.5 rounded-md bg-red-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-red-700 disabled:opacity-50"
        >
          <Trash2 className="h-3.5 w-3.5" /> Delete
        </button>
      </div>

      <div className="rounded-lg border border-gray-200 dark:border-gray-700 divide-y divide-gray-200 dark:divide-gray-700">
        <DetailRow label="ID" value={String(task.id)} />
        <DetailRow label="Type" value={task.type} />
        <DetailRow label="Status" value={<StatusBadge status={task.status} />} />
        <DetailRow label="Idempotency Token" value={task.idempotency_token ?? "–"} />
        <DetailRow
          label="Attempts"
          value={`${task.attempts_elapsed} elapsed / ${task.attempts_left} remaining`}
        />
        <DetailRow label="Created" value={new Date(task.created_at).toLocaleString()} />
        <DetailRow label="Updated" value={new Date(task.updated_at).toLocaleString()} />
        <DetailRow
          label="Finalized"
          value={task.finalized_at ? new Date(task.finalized_at).toLocaleString() : "–"}
        />
        <DetailRow
          label="Attempted"
          value={task.attempted_at ? new Date(task.attempted_at).toLocaleString() : "–"}
        />
        <DetailRow
          label="Blocked Until"
          value={new Date(task.blocked_till).toLocaleString()}
        />
      </div>

      {task.messages && task.messages.length > 0 && (
        <div>
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Messages / Errors</h3>
          <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 p-3 space-y-1.5">
            {[...task.messages].reverse().map((msg, i) => (
              <div key={i} className="text-sm text-gray-700 dark:text-gray-300 font-mono bg-white dark:bg-gray-900 rounded px-2 py-1 border border-gray-100 dark:border-gray-700">
                <span className="text-gray-400 dark:text-gray-400 mr-2">#{task.messages.length - i}</span>
                {msg}
              </div>
            ))}
          </div>
        </div>
      )}

      <div>
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
            Payload
            {task.payload_size > 0 && (
              <span className="ml-1.5 font-normal text-gray-400 dark:text-gray-400">
                ({task.payload_size.toLocaleString()} bytes)
              </span>
            )}
          </h3>
          <div className="flex items-center gap-1.5">
            {hidePayload && (
              <button
                onClick={() => setPayloadRevealed(!payloadRevealed)}
                aria-label={payloadRevealed ? "Hide payload" : "Reveal payload"}
                className="inline-flex items-center gap-1 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-2 py-1 text-xs font-medium text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700"
              >
                {payloadRevealed ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
                {payloadRevealed ? "Hide" : "Reveal"}
              </button>
            )}
            {showPayload && payload && (
              <button
                onClick={handleCopy}
                aria-label={copied ? "Payload copied" : "Copy payload"}
                className="inline-flex items-center gap-1 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-2 py-1 text-xs font-medium text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700"
              >
                {copied ? <Check className="h-3.5 w-3.5 text-green-500" /> : <Copy className="h-3.5 w-3.5" />}
                {copied ? "Copied" : "Copy"}
              </button>
            )}
          </div>
        </div>
        {!showPayload ? (
          <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 p-4 text-sm text-gray-500 dark:text-gray-400 text-center">
            Payload hidden. Click "Reveal" to show.
          </div>
        ) : payloadLoading ? (
          <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 p-4 text-sm text-gray-400 text-center">
            Loading payload...
          </div>
        ) : payload ? (
          <pre className="rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 p-4 text-sm font-mono overflow-x-auto whitespace-pre-wrap break-all max-h-96 overflow-y-auto">
            {formatPayload(payload)}
          </pre>
        ) : (
          <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 p-4 text-sm text-gray-400 text-center">
            Empty payload
          </div>
        )}
      </div>
    </div>
  )
}

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1 px-4 py-2.5 sm:flex-row">
      <dt className="sm:w-40 sm:flex-shrink-0 text-sm font-medium text-gray-500 dark:text-gray-400">{label}</dt>
      <dd className="text-sm text-gray-900 dark:text-gray-100">{value}</dd>
    </div>
  )
}
