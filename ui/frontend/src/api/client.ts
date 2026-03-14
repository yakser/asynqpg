import type {
  ApiResponse,
  AuthProviderInfo,
  AuthUser,
  BulkResponse,
  ConfigResponse,
  StatsResponse,
  TaskDetail,
  TaskListParams,
  TaskListResponse,
} from "./types"

class ApiError extends Error {
  code: string
  status: number

  constructor(code: string, message: string, status: number) {
    super(message)
    this.name = "ApiError"
    this.code = code
    this.status = status
  }
}

async function fetchAPI<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`/api${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  })

  const json = (await response.json()) as ApiResponse<T>

  if (json.error) {
    throw new ApiError(json.error.code, json.error.message, response.status)
  }

  return json.data
}

export function getConfig(): Promise<ConfigResponse> {
  return fetchAPI<ConfigResponse>("/config")
}

export function getStats(): Promise<StatsResponse> {
  return fetchAPI<StatsResponse>("/stats")
}

export function getTaskTypes(): Promise<string[]> {
  return fetchAPI<string[]>("/task-types")
}

export function getTasks(params: TaskListParams): Promise<TaskListResponse> {
  const searchParams = new URLSearchParams()

  if (params.status) searchParams.set("status", params.status)
  if (params.type) searchParams.set("type", params.type)
  if (params.id) searchParams.set("id", params.id)
  if (params.limit) searchParams.set("limit", String(params.limit))
  if (params.offset) searchParams.set("offset", String(params.offset))
  if (params.order_by) searchParams.set("order_by", params.order_by)
  if (params.order) searchParams.set("order", params.order)
  if (params.created_after) searchParams.set("created_after", params.created_after)
  if (params.created_before) searchParams.set("created_before", params.created_before)

  const qs = searchParams.toString()
  return fetchAPI<TaskListResponse>(`/tasks${qs ? `?${qs}` : ""}`)
}

export function getTask(id: number): Promise<TaskDetail> {
  return fetchAPI<TaskDetail>(`/tasks/${id}`)
}

export function getTaskPayload(id: number): Promise<string> {
  return fetch(`/api/tasks/${id}/payload`).then((r) => r.text())
}

export function retryTask(id: number): Promise<TaskDetail> {
  return fetchAPI<TaskDetail>(`/tasks/${id}/retry`, { method: "POST" })
}

export function cancelTask(id: number): Promise<TaskDetail> {
  return fetchAPI<TaskDetail>(`/tasks/${id}/cancel`, { method: "POST" })
}

export function deleteTask(id: number): Promise<TaskDetail> {
  return fetchAPI<TaskDetail>(`/tasks/${id}`, { method: "DELETE" })
}

export function bulkRetry(type?: string): Promise<BulkResponse> {
  return fetchAPI<BulkResponse>("/tasks/bulk/retry", {
    method: "POST",
    body: JSON.stringify(type ? { type } : {}),
  })
}

export function bulkDelete(type?: string): Promise<BulkResponse> {
  return fetchAPI<BulkResponse>("/tasks/bulk/delete", {
    method: "POST",
    body: JSON.stringify(type ? { type } : {}),
  })
}

export function getAuthProviders(): Promise<AuthProviderInfo[]> {
  return fetchAPI<AuthProviderInfo[]>("/auth/providers")
}

export async function getMe(): Promise<AuthUser | null> {
  try {
    return await fetchAPI<AuthUser>("/auth/me")
  } catch (err) {
    if (err instanceof ApiError && err.status === 401) {
      return null
    }
    throw err
  }
}

export function logout(): Promise<void> {
  return fetchAPI<void>("/auth/logout", { method: "POST" })
}

export { ApiError }
