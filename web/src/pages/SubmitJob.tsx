import { useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError, getApiKey, setApiKey } from "../api/client";
import { PRIORITIES, type CreateJobRequest, type Priority } from "../api/types";
import StatusBadge from "../components/StatusBadge";
import { ErrorState } from "../components/States";

type ScheduleMode = "now" | "delay" | "cron";

const DEFAULT_PAYLOADS: Record<string, string> = {
  sleep: '{\n  "seconds": 2\n}',
  fail: '{\n  "message": "boom"\n}',
};

export default function SubmitJob() {
  const qc = useQueryClient();
  const handlersQuery = useQuery({ queryKey: ["handlers"], queryFn: api.handlers });

  const [type, setType] = useState("sleep");
  const [payloadText, setPayloadText] = useState(DEFAULT_PAYLOADS.sleep);
  const [priority, setPriority] = useState<Priority>("normal");
  const [mode, setMode] = useState<ScheduleMode>("now");
  const [delay, setDelay] = useState("10");
  const [cron, setCron] = useState("*/10 * * * * *");
  const [timeoutSeconds, setTimeoutSeconds] = useState("");
  const [maxRetries, setMaxRetries] = useState("");
  const [apiKeyInput, setApiKeyInput] = useState(getApiKey());
  const [localError, setLocalError] = useState<string | null>(null);

  const create = useMutation({
    mutationFn: (body: CreateJobRequest) => api.createJob(body),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["jobs"] });
      void qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });

  const handlers = handlersQuery.data?.handlers ?? [];

  const onTypeChange = (next: string) => {
    setType(next);
    // Only prefill when the user has not written something of their own.
    if (DEFAULT_PAYLOADS[next] && Object.values(DEFAULT_PAYLOADS).includes(payloadText)) {
      setPayloadText(DEFAULT_PAYLOADS[next]);
    }
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    setLocalError(null);

    let payload: unknown;
    try {
      payload = payloadText.trim() === "" ? {} : JSON.parse(payloadText);
    } catch (err) {
      setLocalError(`payload is not valid JSON: ${(err as Error).message}`);
      return;
    }

    const body: CreateJobRequest = { type, payload, priority };

    if (mode === "delay") {
      const seconds = Number(delay);
      if (!Number.isFinite(seconds) || seconds < 0) {
        setLocalError("delay must be a number of seconds (0 or more)");
        return;
      }
      body.run_at = new Date(Date.now() + seconds * 1000).toISOString();
    }
    if (mode === "cron") {
      if (!cron.trim()) {
        setLocalError("cron expression is required");
        return;
      }
      body.cron_expr = cron.trim();
    }
    if (timeoutSeconds.trim()) {
      const n = Number(timeoutSeconds);
      if (!Number.isInteger(n) || n < 1 || n > 3600) {
        setLocalError("timeout must be a whole number of seconds between 1 and 3600");
        return;
      }
      body.timeout_seconds = n;
    }
    if (maxRetries.trim()) {
      const n = Number(maxRetries);
      if (!Number.isInteger(n) || n < 0 || n > 10) {
        setLocalError("max retries must be a whole number between 0 and 10");
        return;
      }
      body.max_retries = n;
    }

    create.mutate(body);
  };

  const saveKey = () => {
    setApiKey(apiKeyInput.trim());
    setLocalError(null);
    void qc.invalidateQueries();
  };

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-semibold tracking-tight">Submit Job</h1>
        <p className="mt-1 text-sm text-slate-400">
          Creates a job over <code className="text-slate-300">POST /api/v1/jobs</code>. Its status changes
          appear on the Overview page as they happen.
        </p>
      </header>

      {(localError || create.error) && <ErrorState error={localError ?? create.error} />}

      {create.isSuccess && (
        <div className="card border-succeeded/40 bg-succeeded/5">
          <div className="flex items-center gap-3 text-sm">
            <span className="font-semibold text-succeeded">created</span>
            <span className="font-mono text-xs text-slate-300">{create.data.id}</span>
            <StatusBadge status={create.data.status} />
          </div>
        </div>
      )}

      <form onSubmit={submit} className="grid gap-4 lg:grid-cols-3">
        <div className="card space-y-4 lg:col-span-2">
          <div>
            <label className="label" htmlFor="type">
              type
            </label>
            <select id="type" className="input mt-1" value={type} onChange={(e) => onTypeChange(e.target.value)}>
              {(handlers.length > 0 ? handlers : ["sleep", "fail"]).map((h) => (
                <option key={h} value={h}>
                  {h}
                </option>
              ))}
            </select>
            {handlersQuery.error && (
              <p className="mt-1 text-xs text-slate-500">
                could not load /handlers - showing the built-in types
              </p>
            )}
          </div>

          <div>
            <label className="label" htmlFor="payload">
              payload (JSON)
            </label>
            <textarea
              id="payload"
              className="input mt-1 min-h-40 font-mono text-xs leading-relaxed"
              spellCheck={false}
              value={payloadText}
              onChange={(e) => setPayloadText(e.target.value)}
            />
          </div>

          <div className="grid gap-3 sm:grid-cols-3">
            <div>
              <label className="label" htmlFor="priority">
                priority
              </label>
              <select
                id="priority"
                className="input mt-1"
                value={priority}
                onChange={(e) => setPriority(e.target.value as Priority)}
              >
                {PRIORITIES.map((p) => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="label" htmlFor="timeout">
                timeout (s)
              </label>
              <input
                id="timeout"
                className="input mt-1"
                inputMode="numeric"
                placeholder="default"
                value={timeoutSeconds}
                onChange={(e) => setTimeoutSeconds(e.target.value)}
              />
            </div>
            <div>
              <label className="label" htmlFor="retries">
                max retries
              </label>
              <input
                id="retries"
                className="input mt-1"
                inputMode="numeric"
                placeholder="3"
                value={maxRetries}
                onChange={(e) => setMaxRetries(e.target.value)}
              />
            </div>
          </div>
        </div>

        <div className="space-y-4">
          <div className="card space-y-3">
            <div className="label">schedule</div>
            {(
              [
                ["now", "run immediately"],
                ["delay", "after a delay"],
                ["cron", "on a cron schedule"],
              ] as const
            ).map(([value, label]) => (
              <label key={value} className="flex cursor-pointer items-center gap-2 text-sm text-slate-300">
                <input
                  type="radio"
                  name="mode"
                  checked={mode === value}
                  onChange={() => setMode(value)}
                  className="accent-sky-500"
                />
                {label}
              </label>
            ))}

            {mode === "delay" && (
              <div>
                <label className="label" htmlFor="delay">
                  seconds from now
                </label>
                <input
                  id="delay"
                  className="input mt-1"
                  inputMode="numeric"
                  value={delay}
                  onChange={(e) => setDelay(e.target.value)}
                />
              </div>
            )}

            {mode === "cron" && (
              <div>
                <label className="label" htmlFor="cron">
                  cron expression
                </label>
                <input
                  id="cron"
                  className="input mt-1 font-mono text-xs"
                  value={cron}
                  onChange={(e) => setCron(e.target.value)}
                />
                <p className="mt-1 text-xs text-slate-500">
                  six fields with seconds first: <code>*/10 * * * * *</code> is every 10 seconds.
                </p>
              </div>
            )}
          </div>

          <div className="card space-y-2">
            <label className="label" htmlFor="apikey">
              api key
            </label>
            <input
              id="apikey"
              className="input font-mono text-xs"
              type="password"
              placeholder="X-API-Key"
              value={apiKeyInput}
              onChange={(e) => setApiKeyInput(e.target.value)}
            />
            <button type="button" className="btn-ghost w-full" onClick={saveKey}>
              save key
            </button>
          </div>

          <button type="submit" className="btn-primary w-full" disabled={create.isPending}>
            {create.isPending ? "submitting…" : "submit job"}
          </button>
        </div>
      </form>

      {create.error instanceof ApiError && create.error.status === 401 && (
        <p className="text-xs text-slate-500">The key must match API_KEY in the API's environment.</p>
      )}
    </div>
  );
}
