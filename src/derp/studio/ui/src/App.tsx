import { Route, Routes } from "react-router-dom";

import { ConfigPage } from "./routes/ConfigPage";
import { LandingPage } from "./routes/LandingPage";

export default function App(): JSX.Element {
  return (
    <Routes>
      <Route path="/" element={<LandingPage />} />
      <Route path="/config" element={<ConfigPage />} />
    </Routes>
  );
}
