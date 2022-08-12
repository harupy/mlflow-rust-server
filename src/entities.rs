use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(FromRow)]
pub struct SqlExperiment {
    pub experiment_id: i32,
    pub name: String,
    pub artifact_location: String,
    pub lifecycle_stage: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Experiment {
    pub experiment_id: String,
    pub name: String,
    pub artifact_location: String,
    pub lifecycle_stage: String,
    pub tags: Vec<ExperimentTag>,
}

#[derive(FromRow)]
pub struct SqlExperimentTag {
    pub experiment_id: i32,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExperimentTag {
    pub experiment_id: String,
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize)]
pub struct Param {
    pub key: String,
    pub value: String,
}
#[derive(Serialize, Deserialize)]
pub struct Metric {
    pub key: String,
    pub value: f64,
    pub timestamp: i64,
    pub step: i64,
}

#[derive(Serialize, Deserialize)]
pub struct RunTag {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize)]
pub struct RunData {
    pub params: Vec<Param>,
    pub metrics: Vec<Metric>,
    pub tags: Vec<RunTag>,
}

#[derive(Serialize, Deserialize)]
pub struct RunInfo {
    pub name: String,
    pub run_uuid: String,
    pub run_id: String,
    pub experiment_id: String,
    pub user_id: String,
    pub status: String,
    pub start_time: i64,
    pub end_time: i64,
    pub lifecycle_stage: String,
    pub artifact_uri: String,
}

#[derive(FromRow)]
pub struct SqlRun {
    pub run_uuid: String,
    pub name: String,
    pub source_type: String,
    pub source_name: String,
    pub entry_point_name: String,
    pub user_id: String,
    pub status: String,
    pub start_time: i64,
    pub end_time: i64,
    pub source_version: String,
    pub lifecycle_stage: String,
    pub artifact_uri: String,
    pub experiment_id: i32,
}

#[derive(Serialize, Deserialize)]
pub struct Run {
    pub info: RunInfo,
    pub data: RunData,
}
