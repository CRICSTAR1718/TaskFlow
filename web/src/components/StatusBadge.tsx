import type { Status } from "../api/types";
import { STATUS_COLOR_CLASSES } from "./statusColors";



export default function StatusBadge({ status }: { status: Status }) {
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${
        STATUS_COLOR_CLASSES[status] ?? STATUS_COLOR_CLASSES.PENDING
      }`}
    >
      {status}
    </span>
  );
}
