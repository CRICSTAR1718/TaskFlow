import { Link } from "react-router-dom";

export default function NotFound() {
  return (
    <div className="card">
      <h1 className="text-lg font-semibold">Page not found</h1>
      <p className="mt-1 text-sm text-slate-400">
        <Link className="text-sky-400 underline" to="/">
          back to the overview
        </Link>
      </p>
    </div>
  );
}
