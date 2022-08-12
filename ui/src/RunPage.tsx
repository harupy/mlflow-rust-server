import React, { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import { Skeleton } from "antd";
import { getRun } from "./api";

type RunInfo = {
  name: string;
  run_id: string;
};
type RunData = {};
type Run = {
  data: RunData;
  info: RunInfo;
};

export const RunPage = () => {
  const [run, setRun] = useState<Run>();
  const { run_id } = useParams();

  useEffect(() => {
    if (run_id === undefined) {
      return;
    }
    getRun({ run_id }).then(async resp => {
      const { run } = await resp.json();
      setRun(run);
    });
  }, [run_id]);

  if (run === undefined) {
    return <Skeleton active />;
  }

  return <div>{run.info.name || run.info.run_id}</div>;
};
