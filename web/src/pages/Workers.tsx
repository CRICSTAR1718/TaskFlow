import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { Empty, ErrorState, Loading } from "../components/States";

function ago(iso: string): string {
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  return `${Math.round(s / 3600)}h ago`;
}

export default function Workers() {
  const workersQuery = useQuery({
    queryKey: ["workers"],
    queryFn: api.workers,
    refetchInterval: 5_000, // heartbeats are the one thing worth polling fast
  });

  if (workersQuery.isLoading) return <Loading what="workers" />;
  if (workersQuery.error) return <ErrorState error={workersQuery.error} />;

  const workers = workersQuery.data?.workers ?? [];

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-semibold tracking-tight">Workers</h1>
        <p className="mt-1 text-sm text-slate-400">
          {workers.filter((w) => w.alive).length} of {workers.length} alive Â· a worker is dead when its
          heartbeat is older than the TTL
        </p>
      </header>

      {workers.length === 0 && (
        <Empty>
          No workers have registered. Start one with <code className="text-slate-300">docker compose up -d
          worker</code> or <code className="text-slate-300">go run ./cmd/worker</code>.
        </Empty>
      )}

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {workers.map((w) => (
          <div key={w.id} className={`card ${w.alive ? "" : "opacity-70"}`}>
            <div className="flex items-center justify-between gap-2">
              <span className="truncate font-mono text-xs text-slate-300">{w.id}</span>
              <span
                className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs ring-1 ring-inset ${
                  w.alive
                    ? "bg-succeeded/15 text-succeeded ring-succeeded/30"
                    : "bg-dead/15 text-dead ring-dead/30"
                }`}
              >
                <span className={`h-1.5 w-1.5 rounded-full ${w.alive ? "bg-succeeded" : "bg-dead"}`} />
                {w.alive ? "alive" : "dead"}
              </span>
            </div>
            <dl className="mt-4 space-y-2 text-sm">
              <div className="flex justify-between">
                <dt className="text-slate-500">host</dt>
                <dd className="truncate text-slate-300">{w.hostname}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">concurrency</dt>
                <dd className="tabular-nums text-slate-300">{w.concurrency}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">last heartbeat</dt>
                <dd className="text-slate-300">{ago(w.last_heartbeat)}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">started</dt>
                <dd className="text-slate-300">{ago(w.started_at)}</dd>
              </div>
            </dl>
          </div>
        ))}
      </div>
    </div>
  );
}
