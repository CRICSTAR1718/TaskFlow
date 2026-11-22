// Types mirror internal/domain and internal/api. If the Go structs change, this
// file is the one place the dashboard has to change too.

export type Status =
  | "PENDING"
  | "QUEUED"
  | "RUNNING"
  | "SUCCEEDED"
  | "RETRYING"
  | "DEAD"
  | "CANCELLED";

export type Priority = "high" | "normal" | "low";

export const STATUSES: Status[] = [
  "PENDING",
  "QUEUED",
  "RUNNING",
  "SUCCEEDED",
  "RETRYING",
  "DEAD",
  "CANCELLED",
];

export const PRIORITIES: Priority[] = ["high", "normal", "low"];

export interface Job {
  id: string;
  type: string;
  payload: unknown;
  status: Status;
  priority: Priority;
  attempt: number;
  max_retries: number;
  timeout_seconds: number;
  cron_expr?: string | null;
  run_at: string;
  next_run_at?: string | null;
  created_at: string;
  updated_at: string;
  last_error?: string | null;
}

export interface JobRun {
  id: string;
  job_id: string;
  worker_id: string;
  attempt: number;
  status: Status;
  started_at: string;
  finished_at?: string | null;
  error?: string | null;
  output?: unknown;
}

export interface Worker {
  id: string;
  hostname: string;
  concurrency: number;
  started_at: string;
  last_heartbeat: string;
  alive: boolean;
}

export interface Stats {
  jobs_by_status: Partial<Record<Status, number>>;
  queue_depths: Record<Priority, number>;
  runs_last_hour: number;
  avg_run_duration_ms_last_hour: number;
  ws_clients: number;
}

/** The event the worker publishes on every status change. */
export interface JobEvent {
  job_id: string;
  status: Status;
  worker_id?: string;
  attempt: number;
  /** Always RFC 3339 in UTC - Event.MarshalJSON pins it server-side. */
  at: string;
  error?: string;
}

/** Message envelope on GET /ws. */
export type LiveMessage =
  | { type: "job_event"; data: JobEvent }
  | { type: "stats"; data: Stats };

export interface JobListResponse {
  jobs: Job[];
  count: number;
}

export interface JobDetailResponse {
  job: Job;
  runs: JobRun[];
}

export interface CreateJobRequest {
  type: string;
  payload?: unknown;
  priority?: Priority;
  run_at?: string;
  cron_expr?: string;
  timeout_seconds?: number;
  max_retries?: number;
}

export interface HandlersResponse {
  handlers: string[];
  count: number;
}

export interface JobFilters {
  status?: Status | "";
  priority?: Priority | "";
  type?: string;
  limit?: number;
  cursor?: string;
}

export interface ApiErrorBody {
  error: { code: string; message: string };
}
