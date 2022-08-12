import React, { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { Table } from "antd";
import { searchExperiments } from "./api";

type Experiment = {
  experiment_id: string;
  name: string;
};

export const ExperimentTable = () => {
  const [experiments, setExperiments] = useState<Experiment[]>([]);

  useEffect(() => {
    searchExperiments({}).then(async resp => {
      const { experiments } = await resp.json();
      setExperiments(experiments);
    });
  }, []);

  const columns = [
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      render: (text: string, { experiment_id }: Experiment) => (
        <Link to={`/experiments/${experiment_id}`}>{text}</Link>
      ),
    },
  ];

  return <Table dataSource={experiments} columns={columns} />;
};
