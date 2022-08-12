use crate::entities::{
    Experiment, ExperimentTag, Run, RunData, RunInfo, SqlExperiment, SqlExperimentTag, SqlRun,
};
use crate::env::{DEFAULT_ARTIFACT_ROOT, MLFLOW_TRACKING_URI};
use crate::stores::tracking::error::MlflowError;
use crate::stores::tracking::Store;
use async_trait::async_trait;
use sqlx::sqlite::{Sqlite, SqlitePoolOptions};
use sqlx::Pool;
use std::env;

pub struct SqliteStore {
    pub connection: Pool<Sqlite>,
    pub default_artifact_root: String,
}

async fn get_connection_pool(db_uri: &str) -> Result<Pool<Sqlite>, MlflowError> {
    Ok(SqlitePoolOptions::new().connect(db_uri).await?)
}

impl SqliteStore {
    pub async fn new(
        db_uri: &str,
        default_artifact_root: &str,
    ) -> Result<SqliteStore, MlflowError> {
        let connection = get_connection_pool(db_uri).await?;
        Ok(SqliteStore {
            connection,
            default_artifact_root: default_artifact_root.to_string(),
        })
    }

    pub async fn from_env() -> Result<SqliteStore, MlflowError> {
        let db_uri = env::var(MLFLOW_TRACKING_URI)
            .unwrap_or_else(|_| panic!("{} must be set", MLFLOW_TRACKING_URI));
        let default_artifact_root = env::var(DEFAULT_ARTIFACT_ROOT)
            .unwrap_or_else(|_| panic!("{} must be set", DEFAULT_ARTIFACT_ROOT));
        let connection = get_connection_pool(&db_uri).await?;
        Ok(SqliteStore {
            connection,
            default_artifact_root,
        })
    }

    async fn get_experiment_tags(
        &self,
        experiment_id: i32,
    ) -> Result<Vec<ExperimentTag>, MlflowError> {
        let tags: Vec<SqlExperimentTag> =
            sqlx::query_as(r#"SELECT * FROM experiment_tags WHERE experiment_id = $1"#)
                .bind(experiment_id)
                .fetch_all(&self.connection)
                .await
                .unwrap();
        Ok(tags
            .into_iter()
            .map(|t| ExperimentTag {
                experiment_id: t.experiment_id.to_string(),
                key: t.key,
                value: t.value,
            })
            .collect())
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn teardown(&self) {
        self.connection.close().await;
    }

    async fn search_runs(&self, experiment_ids: Vec<&str>) -> Result<Vec<Run>, MlflowError> {
        let experiment_id_filter = format!("experiment_id IN ({})", experiment_ids.join(","));
        let query = format!(r#"SELECT * FROM runs WHERE {}"#, experiment_id_filter);
        let runs: Vec<SqlRun> = sqlx::query_as(query.as_str())
            .fetch_all(&self.connection)
            .await
            .unwrap();
        Ok(runs
            .into_iter()
            .map(|r| Run {
                info: RunInfo {
                    name: r.name,
                    run_uuid: r.run_uuid.clone(),
                    run_id: r.run_uuid,
                    experiment_id: r.experiment_id.to_string(),
                    user_id: r.user_id,
                    status: r.status,
                    start_time: r.start_time,
                    end_time: r.end_time,
                    lifecycle_stage: r.lifecycle_stage,
                    artifact_uri: r.artifact_uri,
                },
                data: RunData {
                    params: vec![],
                    metrics: vec![],
                    tags: vec![],
                },
            })
            .collect())
    }

    async fn get_run(&self, run_id: &str) -> Result<Run, MlflowError> {
        let r: SqlRun = sqlx::query_as(r#"SELECT * FROM runs WHERE run_uuid = $1"#)
            .bind(run_id)
            .fetch_one(&self.connection)
            .await
            .unwrap();
        Ok(Run {
            info: RunInfo {
                name: r.name,
                run_uuid: r.run_uuid.clone(),
                run_id: r.run_uuid,
                experiment_id: r.experiment_id.to_string(),
                user_id: r.user_id,
                status: r.status,
                start_time: r.start_time,
                end_time: r.end_time,
                lifecycle_stage: r.lifecycle_stage,
                artifact_uri: r.artifact_uri,
            },
            data: RunData {
                params: vec![],
                metrics: vec![],
                tags: vec![],
            },
        })
    }

    async fn list_experiments(&self) -> Result<Vec<Experiment>, MlflowError> {
        let sql_experiments: Vec<SqlExperiment> = sqlx::query_as(r#"SELECT * FROM experiments"#)
            .fetch_all(&self.connection)
            .await?;
        let mut experiments: Vec<Experiment> = vec![];
        for e in sql_experiments {
            experiments.push(Experiment {
                experiment_id: e.experiment_id.to_string(),
                name: e.name,
                artifact_location: e.artifact_location,
                lifecycle_stage: e.lifecycle_stage,
                tags: self.get_experiment_tags(e.experiment_id).await?,
            })
        }

        Ok(experiments)
    }

    async fn search_experiments(
        &self,
        max_results: Option<i64>,
    ) -> Result<Vec<Experiment>, MlflowError> {
        let sql_experiments: Vec<SqlExperiment> = sqlx::query_as(
            r#"
            SELECT * FROM experiments
            ORDER BY experiment_id DESC
            LIMIT $1
            "#,
        )
        .bind(max_results.unwrap_or(-1))
        .fetch_all(&self.connection)
        .await
        .unwrap();
        let mut experiments: Vec<Experiment> = vec![];
        for e in sql_experiments {
            experiments.push(Experiment {
                experiment_id: e.experiment_id.to_string(),
                name: e.name,
                artifact_location: e.artifact_location,
                lifecycle_stage: e.lifecycle_stage,
                tags: self.get_experiment_tags(e.experiment_id).await?,
            })
        }
        Ok(experiments)
    }

    async fn get_experiment(&self, experiment_id: &str) -> Result<Experiment, MlflowError> {
        let experiment: SqlExperiment =
            sqlx::query_as(r#"SELECT * FROM experiments WHERE experiment_id = $1"#)
                .bind(experiment_id.parse::<i32>().unwrap())
                .fetch_one(&self.connection)
                .await
                .unwrap();
        Ok(Experiment {
            experiment_id: experiment.experiment_id.to_string(),
            name: experiment.name,
            artifact_location: experiment.artifact_location,
            lifecycle_stage: experiment.lifecycle_stage,
            tags: self.get_experiment_tags(experiment.experiment_id).await?,
        })
    }

    async fn create_experiment(
        &self,
        name: &str,
        artifact_location: Option<&str>,
    ) -> Result<Experiment, MlflowError> {
        sqlx::query(
            r#"INSERT INTO experiments (name, artifact_location, lifecycle_stage) VALUES ($1, '', 'active')"#,
        )
        .bind(name)
        .execute(&self.connection)
        .await
        .unwrap();
        let experiment: SqlExperiment =
            sqlx::query_as(r#"SELECT * FROM experiments WHERE name = $1"#)
                .bind(name)
                .fetch_one(&self.connection)
                .await
                .unwrap();
        let default_location = format!(
            "{}/{}",
            self.default_artifact_root, experiment.experiment_id
        );
        let artifact_loc = artifact_location.unwrap_or(&default_location);
        sqlx::query(r#"UPDATE experiments SET artifact_location = $1 WHERE name = $2"#)
            .bind(&artifact_loc)
            .bind(name)
            .execute(&self.connection)
            .await
            .unwrap();
        Ok(Experiment {
            experiment_id: experiment.experiment_id.to_string(),
            name: experiment.name,
            artifact_location: default_location,
            lifecycle_stage: experiment.lifecycle_stage,
            tags: self.get_experiment_tags(experiment.experiment_id).await?,
        })
    }

    async fn delete_experiment(&self, experiment_id: &str) -> Result<Experiment, MlflowError> {
        sqlx::query(
            r#"UPDATE experiments SET lifecycle_stage = 'deleted' WHERE experiment_id = $1"#,
        )
        .bind(experiment_id.parse::<i32>().unwrap())
        .execute(&self.connection)
        .await
        .unwrap();
        let experiment: SqlExperiment =
            sqlx::query_as(r#"SELECT * FROM experiments WHERE experiment_id = $1"#)
                .bind(experiment_id.parse::<i32>().unwrap())
                .fetch_one(&self.connection)
                .await
                .unwrap();
        Ok(Experiment {
            experiment_id: experiment.experiment_id.to_string(),
            name: experiment.name,
            artifact_location: experiment.artifact_location,
            lifecycle_stage: experiment.lifecycle_stage,
            tags: self.get_experiment_tags(experiment.experiment_id).await?,
        })
    }

    async fn restore_experiment(&self, experiment_id: &str) -> Result<Experiment, MlflowError> {
        sqlx::query(
            r#"UPDATE experiments SET lifecycle_stage = 'active' WHERE experiment_id = $1"#,
        )
        .bind(experiment_id.parse::<i32>().unwrap())
        .execute(&self.connection)
        .await
        .unwrap();
        let experiment: SqlExperiment =
            sqlx::query_as(r#"SELECT * FROM experiments WHERE experiment_id = $1"#)
                .bind(experiment_id.parse::<i32>().unwrap())
                .fetch_one(&self.connection)
                .await
                .unwrap();
        Ok(Experiment {
            experiment_id: experiment.experiment_id.to_string(),
            name: experiment.name,
            artifact_location: experiment.artifact_location,
            lifecycle_stage: experiment.lifecycle_stage,
            tags: self.get_experiment_tags(experiment.experiment_id).await?,
        })
    }

    async fn update_experiment(
        &self,
        experiment_id: &str,
        new_name: &str,
    ) -> Result<Experiment, MlflowError> {
        sqlx::query(r#"UPDATE experiments SET name = $1 WHERE experiment_id = $2"#)
            .bind(new_name)
            .bind(experiment_id.parse::<i32>().unwrap())
            .execute(&self.connection)
            .await
            .unwrap();
        let experiment: SqlExperiment =
            sqlx::query_as(r#"SELECT * FROM experiments WHERE experiment_id = $1"#)
                .bind(experiment_id.parse::<i32>().unwrap())
                .fetch_one(&self.connection)
                .await
                .unwrap();
        Ok(Experiment {
            experiment_id: experiment.experiment_id.to_string(),
            name: experiment.name,
            artifact_location: experiment.artifact_location,
            lifecycle_stage: experiment.lifecycle_stage,
            tags: self.get_experiment_tags(experiment.experiment_id).await?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteStore;
    use crate::stores::tracking::Store;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_create_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name = Uuid::new_v4().to_string();
        store.create_experiment(&name, None).await.unwrap();
        let experiments = store.list_experiments().await.unwrap();
        assert!(experiments
            .into_iter()
            .map(|e| e.name)
            .collect::<Vec<String>>()
            .contains(&name));
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_list_experiments() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name1 = Uuid::new_v4().to_string();
        let name2 = Uuid::new_v4().to_string();
        store.create_experiment(&name1, None).await.unwrap();
        store.create_experiment(&name2, None).await.unwrap();
        let experiments = store.list_experiments().await.unwrap();
        let experiment_names = experiments
            .into_iter()
            .map(|e| e.name)
            .collect::<Vec<String>>();
        assert!(experiment_names.contains(&name1));
        assert!(experiment_names.contains(&name2));
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_search_experiments() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name1 = Uuid::new_v4().to_string();
        let name2 = Uuid::new_v4().to_string();
        store.create_experiment(&name1, None).await.unwrap();
        store.create_experiment(&name2, None).await.unwrap();
        let experiments = store.search_experiments(Some(1)).await.unwrap();
        assert_eq!(experiments.len(), 1);
        let experiments = store.search_experiments(Some(2)).await.unwrap();
        assert_eq!(experiments.len(), 2);
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_delete_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name = Uuid::new_v4().to_string();
        let experiment = store.create_experiment(&name, None).await.unwrap();
        let deleted_experiment = store
            .delete_experiment(&experiment.experiment_id)
            .await
            .unwrap();
        assert_eq!(deleted_experiment.lifecycle_stage, "deleted");
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_restore_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name = Uuid::new_v4().to_string();
        let experiment = store.create_experiment(&name, None).await.unwrap();
        let deleted_experiment = store
            .delete_experiment(&experiment.experiment_id)
            .await
            .unwrap();
        let restored_experiment = store
            .restore_experiment(&deleted_experiment.experiment_id)
            .await
            .unwrap();
        assert_eq!(restored_experiment.lifecycle_stage, "active");
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_update_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name = Uuid::new_v4().to_string();
        let experiment = store.create_experiment(&name, None).await.unwrap();
        let new_name = Uuid::new_v4().to_string();
        let updated_experiment = store
            .update_experiment(&experiment.experiment_id, &new_name)
            .await
            .unwrap();
        assert_eq!(updated_experiment.name, new_name);
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_search_runs() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let runs = store.search_runs(vec!["0"]).await.unwrap();
        assert!(runs.len() > 0);
        store.teardown().await;
    }
}
