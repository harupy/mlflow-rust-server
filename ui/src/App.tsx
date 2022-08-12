import React from "react";
import { ExperimentTable } from "./ExperimentTable";
import { ExperimentPage } from "./ExperimentPage";
import { RunPage } from "./RunPage";
import "antd/dist/antd.min.css";
import { Routes, Route } from "react-router-dom";

function App() {
  return (
    <div>
      <Routes>
        <Route path="/" element={<ExperimentTable />} />
        <Route
          path="/experiments/:experiment_id"
          element={<ExperimentPage />}
        />
        <Route
          path="/experiments/:experiment_id/runs/:run_id"
          element={<RunPage />}
        />
      </Routes>
    </div>
  );
}

export default App;
