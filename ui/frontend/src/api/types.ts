export type TaskStatus = "pending" | "running" | "completed" | "failed" | "cancelled"

export interface TaskSummary {
  id: number
  type: string
  status: TaskStatus
  payload_size: number
  idempotency_token: string | null
  messages: string[]
  blocked_till: string
  attempts_left: number
  attempts_elapsed: number
  created_at: string
  updated_at: string
  finalized_at: string | null
  attempted_at: string | null
}

export interface TaskDetail extends TaskSummary {
  payload: string | null
}

export interface TaskListResponse {
  tasks: TaskSummary[]
  total: number
}

export interface StatsResponse {
  total: number
  by_status: Record<TaskStatus, number>
  by_type: Array<{
    type: string
    total: number
    by_status: Record<TaskStatus, number>
  }>
}

export interface BulkResponse {
  affected: number
}

export interface ConfigResponse {
  prefix: string
  hide_payload_by_default: boolean
  version: string
  auth_mode: "none" | "basic" | "oauth"
}

export interface AuthUser {
  id: string
  provider: string
  name: string
  avatar_url: string
  email: string
}

export interface AuthProviderInfo {
  id: string
  name: string
  icon_url: string
  login_url: string
}

export interface ApiError {
  code: string
  message: string
}

export interface ApiResponse<T> {
  data: T
  error: ApiError | null
}

export interface TaskListParams {
  status?: string
  type?: string
  id?: string
  limit?: number
  offset?: number
  order_by?: string
  order?: "ASC" | "DESC"
  created_after?: string
  created_before?: string
}
