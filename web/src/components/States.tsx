import type { ReactNode } from "react";
import { ApiError } from "../api/client";

export function Loading({ what = "data" }: { what?: string }) {
  return (
    <div className="card animate-pulse text-sm text-slate-400">loading {what}â€¦</div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="card text-sm text-slate-400">{children}</div>;
}

/**
 * Errors get a human sentence, not a stack trace. The two that actually happen
 * during a demo are "API is not running" and "wrong API key", so name them.
 */
export function ErrorState({ error }: { error: unknown }) {
  let title = "Something went wrong";
  const detail = String((error as Error)?.message ?? error);
  let hint: string | null = null;

  if (error instanceof ApiError) {
    if (error.status === 0) {
      title = "Cannot reach the API";
      hint = "Is `go run ./cmd/api` (or the api container) running?";
    } else if (error.status === 401) {
      title = "API key rejected";
      hint = "Set VITE_API_KEY in web/.env, or paste the key on the Submit Job page.";
    } else if (error.status >= 500) {
      title = "The API returned an error";
      hint = "Check the api container logs - Postgres or Redis may be down.";
    }
  }

  return (
    <div className="card border-dead/40 bg-dead/5">
      <div className="text-sm font-semibold text-dead">{title}</div>
      <div className="mt-1 text-sm text-slate-300">{detail}</div>
      {hint && <div className="mt-2 text-xs text-slate-400">{hint}</div>}
    </div>
  );
}
