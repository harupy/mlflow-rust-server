export const post = async (url: string, data: object): Promise<Response> => {
  const method = "POST";
  const body = JSON.stringify(data);
  const headers = {
    Accept: "application/json",
    "Content-Type": "application/json",
  };
  return fetch(url, { method, headers, body });
};

export const get = async (
  url: string,
  params: Record<string, string>
): Promise<Response> => {
  const searchParams = new URLSearchParams(params);
  return fetch(`${url}?${searchParams.toString()}`);
};

const API_PREFIX = "/api/2.0/mlflow";

type GetRunRequest = {
  run_id: string;
};

export const getRun = async (params: GetRunRequest): Promise<Response> => {
  return get(`${API_PREFIX}/runs/get`, params);
};

type SearchRunsRequest = {
  experiment_ids: string[];
};

export const searchRuns = async (
  data: SearchRunsRequest
): Promise<Response> => {
  return post(`${API_PREFIX}/runs/search`, data);
};

type SearchExperimentsRequest = {
  max_results?: number;
};

export const searchExperiments = async (
  data: SearchExperimentsRequest
): Promise<Response> => {
  return post(`${API_PREFIX}/experiments/search`, data);
};
