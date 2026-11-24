import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "../api/client";
import { PRIORITIES, STATUSES, type JobFilters, type Priority, type Status } from "../api/types";
import StatusBadge from "../components/StatusBadge";
import { Empty, ErrorState, Loading } from "../components/States";
import type { LiveEvents } from "../hooks/useLiveEvents";

function ago(iso: string): string {
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  return `${Math.round(s / 3600)}h ago`;
}

export default function Jobs({ live }: { live: LiveEvents }) {
  const [filters, setFilters] = useState<JobFilters>({ limit: 50 });
  const qc = useQueryClient();

  const jobsQuery = useQuery({
    queryKey: ["jobs", filters],
    queryFn: () => api.listJobs(filters),
  });

  // Live events nudge the cache; this only triggers the refetch. Keeping the
  // mutation here rather than in the table means Cancel/Retry work regardless of
  // whether the socket is up.
  const act = useMutation({
    mutationFn: ({ id, action }: { id: string; action: "cancel" | "retry" }) =>
      action === "cancel" ? api.cancelJob(id) : api.retryJob(id),
    onSettled: () => {
      void qc.invalidateQueries({ queryKey: ["jobs"] });
      void qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });

  const jobs = jobsQuery.data?.jobs ?? [];

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-semibold tracking-tight">Jobs</h1>
        <p className="mt-1 text-sm text-slate-400">
          {jobsQuery.data?.count ?? 0} shown
          {live.state === "open" && " · updating live"}
        </p>
      </header>

      <div className="flex flex-wrap items-center gap-2">
        <select
          className="input max-w-40"
          value={filters.status ?? ""}
          onChange={(e) => setFilters((f) => ({ ...f, status: e.target.value as Status | "" }))}
        >
          <option value="">all statuses</option>
          {STATUSES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>

        <select
          className="input max-w-36"
          value={filters.priority ?? ""}
          onChange={(e) => setFilters((f) => ({ ...f, priority: e.target.value as Priority | "" }))}
        >
          <option value="">all priorities</option>
          {PRIORITIES.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>

        <input
          className="input max-w-44"
          placeholder="job type"
          value={filters.type ?? ""}
          onChange={(e) => setFilters((f) => ({ ...f, type: e.target.value }))}
        />

        <button className="btn-ghost" onClick={() => void jobsQuery.refetch()}>
          refresh
        </button>
      </div>

      {act.error && <ErrorState error={act.error} />}
      {jobsQuery.isLoading && <Loading what="jobs" />}
      {jobsQuery.error && <ErrorState error={jobsQuery.error} />}

      {!jobsQuery.isLoading && !jobsQuery.error && jobs.length === 0 && (
        <Empty>
          No jobs match. Submit one from the{" "}
          <a className="text-sky-400 underline" href="/submit">
            Submit Job
          </a>{" "}
          page.
        </Empty>
      )}

      {jobs.length > 0 && (
        <div className="overflow-x-auto rounded-xl border border-slate-800">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-900/60 text-left text-xs uppercase tracking-wide text-slate-400">
              <tr>
                <th className="px-4 py-3">id</th>
                <th className="px-4 py-3">type</th>
                <th className="px-4 py-3">priority</th>
                <th className="px-4 py-3">status</th>
                <th className="px-4 py-3">attempt</th>
                <th className="px-4 py-3">updated</th>
                <th className="px-4 py-3 text-right">actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {jobs.map((job) => {
                const busy = act.isPending && act.variables?.id === job.id;
                return (
                  <tr key={job.id} className="hover:bg-slate-900/40">
                    <td className="px-4 py-3 font-mono text-xs text-slate-400">{job.id.slice(0, 8)}</td>
                    <td className="px-4 py-3">{job.type}</td>
                    <td className="px-4 py-3 text-slate-400">{job.priority}</td>
                    <td className="px-4 py-3">
                      <StatusBadge status={job.status} />
                    </td>
                    <td className="px-4 py-3 tabular-nums text-slate-400">
                      {job.attempt}/{job.max_retries}
                    </td>
                    <td className="px-4 py-3 text-slate-500">{ago(job.updated_at)}</td>
                    <td className="px-4 py-3">
                      <div className="flex justify-end gap-2">
                        {job.status === "DEAD" && (
                          <button
                            className="btn-ghost px-2 py-1 text-xs"
                            disabled={busy}
                            onClick={() => act.mutate({ id: job.id, action: "retry" })}
                          >
                            retry
                          </button>
                        )}
                        {["PENDING", "QUEUED", "RETRYING"].includes(job.status) && (
                          <button
                            className="btn-ghost px-2 py-1 text-xs"
                            disabled={busy}
                            onClick={() => act.mutate({ id: job.id, action: "cancel" })}
                          >
                            cancel
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {act.error instanceof ApiError && act.error.status === 409 && (
        <p className="text-xs text-slate-500">
          A 409 means the job moved on before the request landed - refresh to see its current state.
        </p>
      )}
    </div>
  );
}
