import { NavLink, Outlet } from "react-router-dom";
import type { ConnectionState } from "../hooks/useLiveEvents";

const NAV = [
  { to: "/", label: "Overview", end: true },
  { to: "/jobs", label: "Jobs" },
  { to: "/workers", label: "Workers" },
  { to: "/submit", label: "Submit Job" },
];

const DOT: Record<ConnectionState, string> = {
  open: "bg-succeeded",
  connecting: "bg-running",
  closed: "bg-dead",
};

const TEXT: Record<ConnectionState, string> = {
  open: "live",
  connecting: "connecting",
  closed: "polling",
};

export function ConnectionBadge({ state, reconnects }: { state: ConnectionState; reconnects: number }) {
  return (
    <div className="flex items-center gap-2 text-xs text-slate-400" title="Live updates over /ws - falls back to polling when it drops">
      <span className={`h-2 w-2 rounded-full ${DOT[state]} ${state === "open" ? "animate-pulse" : ""}`} />
      <span>{TEXT[state]}</span>
      {reconnects > 0 && <span className="text-slate-600">Â· {reconnects} reconnect{reconnects === 1 ? "" : "s"}</span>}
    </div>
  );
}

export default function Layout({ state, reconnects }: { state: ConnectionState; reconnects: number }) {
  return (
    <div className="flex h-full min-h-screen">
      <aside className="hidden w-56 shrink-0 flex-col border-r border-slate-800 bg-slate-900/40 p-4 sm:flex">
        <div className="mb-6 px-2">
          <div className="text-lg font-semibold tracking-tight">TaskFlow</div>
          <div className="text-xs text-slate-500">job scheduler</div>
        </div>
        <nav className="flex flex-col gap-1">
          {NAV.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                `rounded-lg px-3 py-2 text-sm transition ${
                  isActive ? "bg-sky-600/15 text-sky-300" : "text-slate-300 hover:bg-slate-800/70"
                }`
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
        <div className="mt-auto px-2 pt-6">
          <ConnectionBadge state={state} reconnects={reconnects} />
        </div>
      </aside>

      <main className="min-w-0 flex-1 overflow-y-auto">
        {/* Mobile nav: the sidebar is hidden under sm, so keep the tabs reachable. */}
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3 sm:hidden">
          <span className="font-semibold">TaskFlow</span>
          <ConnectionBadge state={state} reconnects={reconnects} />
        </div>
        <div className="mx-auto max-w-6xl p-4 sm:p-8">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
