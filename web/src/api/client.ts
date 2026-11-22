import type {
  CreateJobRequest,
  HandlersResponse,
  Job,
  JobDetailResponse,
  JobFilters,
  JobListResponse,
  Stats,
  Worker,
} from "./types";

/**
 * Base URL for the API. Defaults to a relative path so the build is portable:
 * in dev Vite proxies /api, in production Caddy does.
 */
export const API_BASE = (import.meta.env.VITE_API_URL as string | undefined) ?? "";

/**
 * The API key. It is read from VITE_API_KEY, falling back to a value the user
 * can set in the browser (localStorage), because baking a key into a static
 * bundle that anyone can download is not a secret - it is a speed bump. See
 * README.md, "About the API key".
 */
const STORAGE_KEY = "taskflow.apiKey";

export function getApiKey(): string {
  const fromEnv = import.meta.env.VITE_API_KEY as string | undefined;
  if (fromEnv) return fromEnv;
  try {
    return window.localStorage.getItem(STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

export function setApiKey(key: string): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, key);
  } catch {
    /* private-mode browsers throw; the key is then only used for this page life */
  }
}

/** Raised for any non-2xx response, carrying the server's {code,message}. */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");
  const key = getApiKey();
  if (key) headers.set("X-API-Key", key);
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  let res: Response;
  try {
    res = await fetch(`${API_BASE}${path}`, { ...init, headers });
  } catch {
    // fetch only rejects on network-level failures, which is the common
    // "is the API actually running?" mistake during a demo.
    throw new ApiError(0, "network_error", `cannot reach the API at ${API_BASE || "this origin"}`);
  }

  if (res.status === 204) return undefined as T;

  const text = await res.text();
  let body: unknown = undefined;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = undefined;
    }
  }

  if (!res.ok) {
    const err = (body as { error?: { code?: string; message?: string } } | undefined)?.error;
    throw new ApiError(
      res.status,
      err?.code ?? "http_error",
      err?.message ?? `request failed with ${res.status}`,
    );
  }
  return body as T;
}

function query(params: Record<string, string | number | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") q.set(k, String(v));
  }
  const s = q.toString();
  return s ? `?${s}` : "";
}

export const api = {
  health: () => request<{ status: string }>("/healthz"),

  listJobs: (f: JobFilters = {}) =>
    request<JobListResponse>(
      `/api/v1/jobs${query({
        status: f.status,
        priority: f.priority,
        type: f.type,
        limit: f.limit,
        cursor: f.cursor,
      })}`,
    ),

  getJob: (id: string) => request<JobDetailResponse>(`/api/v1/jobs/${id}`),

  createJob: (body: CreateJobRequest) =>
    request<Job>("/api/v1/jobs", { method: "POST", body: JSON.stringify(body) }),

  cancelJob: (id: string) =>
    request<Job>(`/api/v1/jobs/${id}/cancel`, { method: "POST" }),

  retryJob: (id: string) =>
    request<Job>(`/api/v1/jobs/${id}/retry`, { method: "POST" }),

  stats: () => request<Stats>("/api/v1/stats"),

  workers: () => request<{ workers: Worker[]; count: number }>("/api/v1/workers"),

  handlers: () => request<HandlersResponse>("/api/v1/handlers"),
};

/**
 * Absolute websocket URL for GET /ws.
 *
 * The key travels in the query string because browsers cannot set headers on a
 * websocket handshake - that is why /ws is registered outside the X-API-Key
 * middleware and validates ?key= itself.
 */
export function liveSocketUrl(): string {
  const explicit = import.meta.env.VITE_WS_URL as string | undefined;
  if (explicit) {
    const url = new URL(explicit);
    const key = getApiKey();
    if (key) url.searchParams.set("key", key);
    return url.toString();
  }
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  const url = new URL(`${proto}//${window.location.host}/ws`);
  const key = getApiKey();
  if (key) url.searchParams.set("key", key);
  return url.toString();
}
