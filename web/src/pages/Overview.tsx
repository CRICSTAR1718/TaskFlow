import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { PRIORITIES, STATUSES, type Stats } from "../api/types";
import StatusBadge from "../components/StatusBadge";
import { ErrorState, Loading } from "../components/States";
import { ConnectionBadge } from "../components/Layout";
import type { LiveEvents } from "../hooks/useLiveEvents";

function StatCard({ label, value, accent }: { label: string; value: number | string; accent?: string }) {
  return (
    <div className="card">
      <div className="label">{label}</div>
      <div className={`mt-2 text-3xl font-semibold tabular-nums ${accent ?? ""}`}>{value}</div>
    </div>
  );
}

export default function Overview({ live }: { live: LiveEvents }) {
  const statsQuery = useQuery({ queryKey: ["stats"], queryFn: api.stats });
  const workersQuery = useQuery({ queryKey: ["workers"], queryFn: api.workers });

  if (statsQuery.isLoading) return <Loading what="stats" />;
  if (statsQuery.error) return <ErrorState error={statsQuery.error} />;

  // The websocket snapshot and the REST payload are the same shape, so prefer
  // whichever arrived most recently.
  const stats: Stats = live.stats ?? statsQuery.data!;
  const byStatus = stats.jobs_by_status;
  const total = Object.values(byStatus).reduce((a, b) => a + (b ?? 0), 0);
  const alive = workersQuery.data?.workers.filter((w) => w.alive).length ?? 0;
  const allWorkers = workersQuery.data?.workers.length ?? 0;

  return (
    <div className="space-y-6">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Overview</h1>
          <p className="mt-1 text-sm text-slate-400">
            {total} job{total === 1 ? "" : "s"} tracked · {stats.ws_clients} dashboard
            {stats.ws_clients === 1 ? "" : "s"} connected
          </p>
        </div>
        <ConnectionBadge state={live.state} reconnects={live.reconnects} />
      </header>

      <section className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        {STATUSES.filter((s) => s !== "CANCELLED").map((s) => (
          <StatCard key={s} label={s.toLowerCase()} value={byStatus[s] ?? 0} />
        ))}
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        <div className="card lg:col-span-2">
          <div className="label mb-4">queue depth by priority</div>
          <div className="space-y-3">
            {PRIORITIES.map((p) => {
              const depth = stats.queue_depths?.[p] ?? 0;
              const max = Math.max(1, ...PRIORITIES.map((q) => stats.queue_depths?.[q] ?? 0));
              return (
                <div key={p} className="flex items-center gap-3">
                  <span className="w-16 text-sm text-slate-400">{p}</span>
                  <div className="h-2 flex-1 overflow-hidden rounded-full bg-slate-800">
                    <div
                      className="h-full rounded-full bg-sky-500 transition-all duration-500"
                      style={{ width: `${Math.round((depth / max) * 100)}%` }}
                    />
                  </div>
                  <span className="w-10 text-right text-sm tabular-nums text-slate-300">{depth}</span>
                </div>
              );
            })}
          </div>
          <div className="mt-5 grid grid-cols-2 gap-3 border-t border-slate-800 pt-4 text-sm">
            <div>
              <div className="label">runs last hour</div>
              <div className="mt-1 text-xl tabular-nums">{stats.runs_last_hour}</div>
            </div>
            <div>
              <div className="label">avg run</div>
              <div className="mt-1 text-xl tabular-nums">
                {stats.avg_run_duration_ms_last_hour >= 1000
                  ? `${(stats.avg_run_duration_ms_last_hour / 1000).toFixed(1)}s`
                  : `${stats.avg_run_duration_ms_last_hour}ms`}
              </div>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="flex items-baseline justify-between">
            <div className="label">workers</div>
            <span className="text-sm text-slate-400" title={`${allWorkers} worker ids have ever registered`}>
              {alive} alive
              {allWorkers > alive && (
                <span className="text-slate-600"> · {allWorkers - alive} dead</span>
              )}
            </span>
          </div>
          <div className="mt-4 space-y-2">
            {(workersQuery.data?.workers ?? []).slice(0, 6).map((w) => (
              <div key={w.id} className="flex items-center justify-between gap-2 text-sm">
                <span className="flex min-w-0 items-center gap-2">
                  <span className={`h-2 w-2 shrink-0 rounded-full ${w.alive ? "bg-succeeded" : "bg-dead"}`} />
                  <span className="truncate font-mono text-xs text-slate-300">{w.id}</span>
                </span>
                <span className="shrink-0 text-xs text-slate-500">×{w.concurrency}</span>
              </div>
            ))}
            {allWorkers === 0 && <div className="text-sm text-slate-500">no workers registered yet</div>}
            {allWorkers > 0 && alive === 0 && (
              <div className="text-sm text-retrying">no worker is alive — is the stack still up?</div>
            )}
          </div>
        </div>
      </section>

      <section className="card">
        <div className="label mb-3">live events</div>
        {live.events.length === 0 ? (
          <p className="text-sm text-slate-500">
            {live.state === "open"
              ? "connected - submit a job and its status changes will appear here."
              : "waiting for a connection to /ws…"}
          </p>
        ) : (
          <ul className="space-y-1.5 font-mono text-xs">
            {live.events
              .slice(-12)
              .reverse()
              .map((e, i) => (
                <li key={`${e.job_id}-${e.at}-${i}`} className="flex flex-wrap items-center gap-2 text-slate-400">
                  <span className="text-slate-600">{new Date(e.at).toLocaleTimeString()}</span>
                  <StatusBadge status={e.status} />
                  <span className="text-slate-300">{e.job_id.slice(0, 8)}</span>
                  {e.worker_id && <span className="text-slate-600">on {e.worker_id}</span>}
                  <span className="text-slate-600">attempt {e.attempt}</span>
                  {e.error && <span className="text-dead">{e.error}</span>}
                </li>
              ))}
          </ul>
        )}
      </section>
    </div>
  );
}
