/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // Status palette, kept in one place so badges, charts and cards agree.
        pending: "#94a3b8",
        queued: "#38bdf8",
        running: "#fbbf24",
        succeeded: "#34d399",
        retrying: "#fb923c",
        dead: "#f87171",
        cancelled: "#a1a1aa",
      },
    },
  },
  plugins: [],
};
