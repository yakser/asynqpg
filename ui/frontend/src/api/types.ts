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
  idempotency_token?: "has" | "none"
}

export interface LeaderInfo {
  leader_id: string
  elected_at: string | null
  expires_at: string | null
  lease_ttl_seconds: number
}

export interface ConsumerTaskType {
  type: string
  workers: number
  busy: number
  max_attempts: number
  timeout_ms: number
}

export interface ConsumerInfo {
  running: boolean
  id: string
  is_leader: boolean
  maintenance_enabled: boolean
  started_at: string | null
  fetch_interval_ms: number
  janitor_interval_ms: number
  stuck_threshold_ms: number
  shutdown_timeout_ms: number
  task_types: ConsumerTaskType[]
}

export interface MaintenanceInfo {
  config: {
    source: "configured" | "defaults"
    stuck_threshold_ms: number
    completed_retention_ms: number
    failed_retention_ms: number
    cancelled_retention_ms: number
    interval_ms: number
  }
  rescuable: {
    total: number
    retriable: number
    discardable: number
  }
  cleanable: {
    total: number
    completed: number
    failed: number
    cancelled: number
  }
  queue_health: {
    oldest_pending_ms: number | null
    completed_last_15m: number
    failed_last_15m: number
  }
}
