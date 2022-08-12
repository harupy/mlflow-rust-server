import React, { useState, useEffect } from "react";
import { Link, useParams } from "react-router-dom";
import { Table } from "antd";
import { searchRuns } from "./api";
import { isString } from "./validation";

type RunInfo = {
  name: string;
  run_id: string;
};
type RunData = {};
type Run = {
  data: RunData;
  info: RunInfo;
};

export const ExperimentPage = () => {
  const [runs, setRuns] = useState<Run[]>([]);
  const { experiment_id } = useParams();

  useEffect(() => {
    const experiment_ids = [experiment_id].filter(isString);
    searchRuns({ experiment_ids }).then(async resp => {
      const { runs } = await resp.json();
      setRuns(runs);
    });
  }, [experiment_id]);

  const columns = [
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      render: (text: string, { run_id }: RunInfo) => (
        <Link to={`/experiments/${experiment_id}/runs/${run_id}`}>
          {text || run_id}
        </Link>
      ),
    },
  ];

  return <Table dataSource={runs.map(({ info }) => info)} columns={columns} />;
};
