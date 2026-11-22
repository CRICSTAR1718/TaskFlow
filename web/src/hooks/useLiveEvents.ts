import { useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { liveSocketUrl } from "../api/client";
import type { Job, JobEvent, LiveMessage, Stats } from "../api/types";

export type ConnectionState = "connecting" | "open" | "closed";

export interface LiveEvents {
  state: ConnectionState;
  /** Stats pushed by the server every 2s while at least one client is open. */
  stats: Stats | null;
  /** Last event per job id, newest last. Capped so a long demo cannot leak. */
  events: JobEvent[];
  /** Epoch ms of the most recent frame of any kind. */
  lastMessageAt: number | null;
  /** How many times the socket has reconnected - useful in the UI, and it
   *  proves the backoff works during a demo without opening devtools. */
  reconnects: number;
}

const MAX_EVENTS = 200;
const BASE_DELAY_MS = 500;
const MAX_DELAY_MS = 15_000;

/**
 * Subscribes to GET /ws and keeps the TanStack Query cache in step with it.
 *
 * Reconnect policy: exponential backoff (0.5s, 1s, 2s, ... capped at 15s) with
 * the cap reset as soon as a connection is established, so a flapping API cannot
 * turn into a reconnect storm. Any live message also triggers a refetch of the
 * affected queries, which makes the socket a hint rather than the only source of
 * truth - if /ws never connects, the dashboard still works on polling alone.
 */
export function useLiveEvents(enabled = true): LiveEvents {
  const queryClient = useQueryClient();
  const [state, setState] = useState<ConnectionState>("connecting");
  const [stats, setStats] = useState<Stats | null>(null);
  const [events, setEvents] = useState<JobEvent[]>([]);
  const [lastMessageAt, setLastMessageAt] = useState<number | null>(null);
  const [reconnects, setReconnects] = useState(0);

  // Kept in a ref so the effect below never has to re-run (and therefore never
  // tears down a healthy socket) when these change.
  const attempt = useRef(0);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const socket = useRef<WebSocket | null>(null);

  useEffect(() => {
    if (!enabled) {
      setState("closed");
      return;
    }
    // Per-run, not a shared ref: React 18 StrictMode mounts, unmounts and
    // remounts this effect in dev. A shared "closed by us" flag is reset by the
    // second mount, so the first socket's late onclose would look like a real
    // disconnect and open a second connection - two sockets, duplicate events
    // and a phantom reconnect. A flag captured by this run cannot do that.
    let disposed = false;

    const connect = () => {
      if (disposed) return;
      setState("connecting");
      let ws: WebSocket;
      try {
        ws = new WebSocket(liveSocketUrl());
      } catch {
        scheduleReconnect();
        return;
      }
      socket.current = ws;

      ws.onopen = () => {
        attempt.current = 0;
        setState("open");
      };

      ws.onmessage = (raw) => {
        if (disposed) return;
        setLastMessageAt(Date.now());
        let msg: LiveMessage;
        try {
          msg = JSON.parse(raw.data as string) as LiveMessage;
        } catch {
          return; // a malformed frame must never break the feed
        }

        if (msg.type === "stats") {
          setStats(msg.data);
          queryClient.setQueryData(["stats"], msg.data);
          return;
        }

        if (msg.type === "job_event") {
          const ev = msg.data;
          setEvents((prev) => [...prev.slice(-(MAX_EVENTS - 1)), ev]);

          // Patch the cached lists so a row flips colour the instant the event
          // lands, without waiting for the refetch below.
          queryClient.setQueriesData<{ jobs: Job[]; count: number }>(
            { queryKey: ["jobs"] },
            (old) =>
              old
                ? {
                    ...old,
                    jobs: old.jobs.map((j) =>
                      j.id === ev.job_id
                        ? { ...j, status: ev.status, attempt: ev.attempt }
                        : j,
                    ),
                  }
                : old,
          );
          // The event does not carry the whole job, so refetch lazily: the
          // socket stays a hint, the API stays the truth.
          void queryClient.invalidateQueries({ queryKey: ["jobs"] });
          void queryClient.invalidateQueries({ queryKey: ["job", ev.job_id] });
          void queryClient.invalidateQueries({ queryKey: ["stats"] });
          void queryClient.invalidateQueries({ queryKey: ["workers"] });
        }
      };

      ws.onerror = () => {
        // onclose always follows; nothing to do here but let it run.
      };

      ws.onclose = () => {
        if (socket.current === ws) socket.current = null;
        if (disposed) return;
        setState("closed");
        scheduleReconnect();
      };
    };

    const scheduleReconnect = () => {
      if (disposed) return;
      attempt.current += 1;
      setReconnects((n) => n + 1);
      const delay = Math.min(BASE_DELAY_MS * 2 ** (attempt.current - 1), MAX_DELAY_MS);
      // Jitter stops several tabs reconnecting in lockstep after an API restart.
      const jittered = delay * (0.8 + Math.random() * 0.4);
      timer.current = setTimeout(connect, jittered);
    };

    connect();

    return () => {
      disposed = true;
      if (timer.current) clearTimeout(timer.current);
      timer.current = null;
      // close() on a socket that is still CONNECTING aborts the handshake, which
      // is what we want - no half-open connection is left behind on the server.
      socket.current?.close();
      socket.current = null;
    };
  }, [enabled, queryClient]);

  return { state, stats, events, lastMessageAt, reconnects };
}
