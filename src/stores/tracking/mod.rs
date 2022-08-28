pub mod error;
pub mod postgres;
pub mod sqlite;

use crate::config::ServerConfig;
use crate::entities::{Experiment, ExperimentTag, Run};
use crate::parser::order_by;
use async_trait::async_trait;
use error::MlflowError;
use postgres::PostgresStore;
use sqlite::SqliteStore;
use url::Url;

#[async_trait]
pub trait Store {
    async fn teardown(&self);
    async fn search_runs(&self, experiment_ids: Vec<&str>) -> Result<Vec<Run>, MlflowError>;
    async fn get_run(&self, run_id: &str) -> Result<Run, MlflowError>;
    async fn list_experiments(&self) -> Result<Vec<Experiment>, MlflowError>;
    async fn search_experiments(
        &self,
        max_results: Option<i64>,
        filter_string: Option<&str>,
        order_by: Option<Vec<&str>>,
    ) -> Result<Vec<Experiment>, MlflowError>;
    async fn get_experiment(&self, experiment_id: &str) -> Result<Experiment, MlflowError>;
    async fn create_experiment(
        &self,
        name: &str,
        artifact_location: Option<&str>,
        tags: Option<Vec<&ExperimentTag>>,
    ) -> Result<Experiment, MlflowError>;
    async fn delete_experiment(&self, experiment_id: &str) -> Result<Experiment, MlflowError>;
    async fn restore_experiment(&self, experiment_id: &str) -> Result<Experiment, MlflowError>;
    async fn update_experiment(
        &self,
        experiment_id: &str,
        new_name: &str,
    ) -> Result<Experiment, MlflowError>;
}

pub async fn get_store(
    uri: &str,
    default_artifact_root: &str,
) -> Result<Box<dyn Store>, MlflowError> {
    let parsed = Url::parse(uri).unwrap();
    match parsed.scheme() {
        "postgresql" => Ok(Box::new(
            PostgresStore::new(uri, default_artifact_root).await?,
        )),
        "sqlite" => Ok(Box::new(
            SqliteStore::new(uri, default_artifact_root).await?,
        )),
        _ => panic!("Unsupported URI: {}", uri),
    }
}

pub async fn get_store_from_server_config(
    server_config: &ServerConfig,
) -> Result<Box<dyn Store>, MlflowError> {
    get_store(
        server_config.backend_store_uri.as_str(),
        server_config.default_artifact_root.as_str(),
    )
    .await
}
