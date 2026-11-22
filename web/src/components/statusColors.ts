import type { Status } from "../api/types";

// Kept apart from StatusBadge.tsx so the file only exports a component, which
// is what the react-refresh lint rule wants.
export const STATUS_COLOR_CLASSES: Record<Status, string> = {
  PENDING: "bg-pending/15 text-pending ring-pending/30",
  QUEUED: "bg-queued/15 text-queued ring-queued/30",
  RUNNING: "bg-running/15 text-running ring-running/30",
  SUCCEEDED: "bg-succeeded/15 text-succeeded ring-succeeded/30",
  RETRYING: "bg-retrying/15 text-retrying ring-retrying/30",
  DEAD: "bg-dead/15 text-dead ring-dead/30",
  CANCELLED: "bg-cancelled/15 text-cancelled ring-cancelled/30",
};
