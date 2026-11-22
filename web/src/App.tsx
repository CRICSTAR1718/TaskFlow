import { Route, Routes } from "react-router-dom";
import Layout from "./components/Layout";
import Overview from "./pages/Overview";
import Jobs from "./pages/Jobs";
import Workers from "./pages/Workers";
import SubmitJob from "./pages/SubmitJob";
import NotFound from "./pages/NotFound";
import { useLiveEvents } from "./hooks/useLiveEvents";

export default function App() {
  // One socket for the whole app: the sidebar badge, the overview cards and the
  // jobs table all read from it, so opening a page never opens a second socket.
  const live = useLiveEvents();

  return (
    <Routes>
      <Route element={<Layout state={live.state} reconnects={live.reconnects} />}>
        <Route index element={<Overview live={live} />} />
        <Route path="jobs" element={<Jobs live={live} />} />
        <Route path="workers" element={<Workers />} />
        <Route path="submit" element={<SubmitJob />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
