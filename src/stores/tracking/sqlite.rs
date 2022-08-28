use crate::entities::{
    Experiment, ExperimentTag, Run, RunData, RunInfo, SqlExperiment, SqlExperimentTag, SqlRun,
};
use crate::env::{DEFAULT_ARTIFACT_ROOT, MLFLOW_TRACKING_URI};
use crate::parser::common::{Entity, Identifier};
use crate::parser::filter::parse_filter;
use crate::parser::order_by::{parse_order_by, OrderBy, OrderByDirection};
use crate::stores::tracking::error::MlflowError;
use crate::stores::tracking::Store;
use async_trait::async_trait;
use sqlx::sqlite::{Sqlite, SqlitePoolOptions};
use sqlx::{Pool, QueryBuilder};
use std::collections::HashMap;
use std::env;

pub struct SqliteStore {
    pub connection: Pool<Sqlite>,
    pub default_artifact_root: String,
}

async fn get_connection_pool(db_uri: &str) -> Result<Pool<Sqlite>, sqlx::Error> {
    Ok(SqlitePoolOptions::new().connect(db_uri).await?)
}

async fn get_connection_pool_from_env() -> Result<Pool<Sqlite>, sqlx::Error> {
    let db_uri = env::var(MLFLOW_TRACKING_URI)
        .unwrap_or_else(|_| panic!("{} must be set", MLFLOW_TRACKING_URI));
    get_connection_pool(&db_uri).await
}

async fn initialize_database() -> Result<(), sqlx::Error> {
    let connection = get_connection_pool_from_env().await?;
    let mut tx = connection.begin().await?;
    sqlx::query("DELETE FROM params").execute(&mut tx).await?;
    sqlx::query("DELETE FROM latest_metrics")
        .execute(&mut tx)
        .await?;
    sqlx::query("DELETE FROM metrics").execute(&mut tx).await?;
    sqlx::query("DELETE FROM tags").execute(&mut tx).await?;
    sqlx::query("DELETE FROM runs").execute(&mut tx).await?;
    sqlx::query("DELETE FROM experiment_tags")
        .execute(&mut tx)
        .await?;
    sqlx::query("DELETE FROM experiments WHERE experiment_id != 0")
        .execute(&mut tx)
        .await?;
    tx.commit().await?;
    connection.close().await;
    Ok(())
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
            .await?;
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
            .await?;
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
        filter_string: Option<&str>,
        order_by: Option<Vec<&str>>,
    ) -> Result<Vec<Experiment>, MlflowError> {
        let (remaining, comparisons) = parse_filter(filter_string.unwrap_or("").trim())
            .map_err(|e| MlflowError::InvalidParameter(e.to_string()))?;

        if !remaining.is_empty() {
            return Err(MlflowError::InvalidParameter(format!(
                "Invalid filter: {}",
                remaining
            )));
        }

        if comparisons
            .iter()
            .any(|c| !(c.left.entity == Entity::Attribute || c.left.entity == Entity::Tag))
        {
            return Err(MlflowError::InvalidParameter(
                "Experiment search only supports filtering by attribute or tag".to_string(),
            ));
        }

        let mut order_by_clauses: Vec<OrderBy> = vec![];
        for ob in order_by.unwrap_or_else(|| vec![]) {
            let (remaining, ob) = parse_order_by(ob.trim())
                .map_err(|e| MlflowError::InvalidParameter(e.to_string()))?;

            if !remaining.is_empty() {
                return Err(MlflowError::InvalidParameter(format!(
                    "Invalid order by clause: {}",
                    remaining
                )));
            }

            if ob.identifier.entity != Entity::Attribute {
                return Err(MlflowError::InvalidParameter(
                    "Experiment search only supports ordering by attribute".to_string(),
                ));
            }
            order_by_clauses.push(ob);
        }

        // Add experiment_id to order_by as a tie-breaker if it's not present
        if !order_by_clauses
            .iter()
            .any(|ob| ob.identifier.key == "experiment_id")
        {
            order_by_clauses.push(OrderBy {
                identifier: Identifier {
                    entity: Entity::Attribute,
                    key: "experiment_id".to_string(),
                },
                ascending: OrderByDirection::Ascending,
            });
        }
        let order_by = order_by_clauses
            .into_iter()
            .map(|ob| format!("{} {}", ob.identifier.key, ob.ascending))
            .collect::<Vec<_>>()
            .join(", ");

        let attribute_comparisons = comparisons
            .iter()
            .filter(|c| c.left.entity == Entity::Attribute)
            .collect::<Vec<_>>();
        let attribute_filter = if attribute_comparisons.is_empty() {
            "1 = 1".to_string()
        } else {
            attribute_comparisons
                .iter()
                .map(|c| format!("{} {} {}", c.left.key, c.operator, c.right))
                .collect::<Vec<_>>()
                .join(" AND ")
        };

        let tag_comparisons = comparisons
            .iter()
            .filter(|c| c.left.entity == Entity::Tag)
            .collect::<Vec<_>>();

        let query = if tag_comparisons.is_empty() {
            format!(
                r#"
                SELECT * FROM experiments
                WHERE {}
                ORDER BY {}
                LIMIT $1
                "#,
                attribute_filter, order_by
            )
        } else {
            let mut tag_filters: HashMap<&str, Vec<String>> = HashMap::new();
            for tag_comp in tag_comparisons {
                if tag_filters.contains_key(tag_comp.left.key.as_str()) {
                    tag_filters
                        .get_mut(tag_comp.left.key.as_str())
                        .unwrap()
                        .push(format!("value {} {}", tag_comp.operator, tag_comp.right));
                } else {
                    tag_filters.insert(
                        tag_comp.left.key.as_str(),
                        vec![
                            format!("key = '{}'", tag_comp.left.key),
                            format!("value {} {}", tag_comp.operator, tag_comp.right),
                        ],
                    );
                }
            }
            let num_unique_tags = tag_filters.len();
            let filtered_tags = format!(
                r#"
                SELECT experiment_id
                FROM experiment_tags
                WHERE {}
                GROUP BY experiment_id
                HAVING COUNT(*) >= {}
                "#,
                tag_filters
                    .iter()
                    .map(|(_, v)| { format!("({})", v.join(" AND ")) })
                    .collect::<Vec<_>>()
                    .join(" OR "),
                num_unique_tags,
            );

            format!(
                r#"
                SELECT *
                FROM experiments
                INNER JOIN ({}) ft ON ft.experiment_id = experiments.experiment_id
                WHERE {}
                ORDER BY {}
                LIMIT $1
                "#,
                filtered_tags, attribute_filter, order_by
            )
        };
        let sql_experiments: Vec<SqlExperiment> = sqlx::query_as(query.as_str())
            .bind(max_results.unwrap_or(-1))
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

    async fn get_experiment(&self, experiment_id: &str) -> Result<Experiment, MlflowError> {
        let experiment: SqlExperiment =
            sqlx::query_as(r#"SELECT * FROM experiments WHERE experiment_id = $1"#)
                .bind(experiment_id.parse::<i32>().unwrap())
                .fetch_one(&self.connection)
                .await?;
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
        tags: Option<Vec<&ExperimentTag>>,
    ) -> Result<Experiment, MlflowError> {
        let mut tx = self.connection.begin().await?;
        let experiment = if let Some(artifact_location) = artifact_location {
            let experiment: SqlExperiment = sqlx::query_as(
                r#"
                INSERT INTO experiments (name, artifact_location, lifecycle_stage)
                VALUES ($1, $2, 'active')
                RETURNING *
                "#,
            )
            .bind(name)
            .bind(artifact_location)
            .fetch_one(&mut tx)
            .await?;
            experiment
        } else {
            let experiment: SqlExperiment = sqlx::query_as(
                r#"
                INSERT INTO experiments (name, artifact_location, lifecycle_stage)
                VALUES ($1, '', 'active')
                RETURNING *
                "#,
            )
            .bind(name)
            .fetch_one(&mut tx)
            .await?;

            let artifact_loc = format!(
                "{}/{}",
                self.default_artifact_root, experiment.experiment_id
            );
            let experiment: SqlExperiment = sqlx::query_as(
                r#"
                UPDATE experiments SET artifact_location = $1 WHERE name = $2
                RETURNING *
                "#,
            )
            .bind(&artifact_loc)
            .bind(name)
            .fetch_one(&mut tx)
            .await?;
            experiment
        };

        if let Some(tags) = tags {
            let mut query_builder: QueryBuilder<Sqlite> =
                QueryBuilder::new("INSERT INTO experiment_tags (experiment_id, key, value) ");

            query_builder.push_values(tags, |mut b, tag| {
                b.push_bind(experiment.experiment_id)
                    .push_bind(&tag.key)
                    .push_bind(&tag.value);
            });

            let query = query_builder.build();
            query.execute(&mut tx).await?;
        };
        tx.commit().await?;

        Ok(Experiment {
            experiment_id: experiment.experiment_id.to_string(),
            name: experiment.name,
            artifact_location: experiment.artifact_location,
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
        .await?;
        let experiment: SqlExperiment =
            sqlx::query_as(r#"SELECT * FROM experiments WHERE experiment_id = $1"#)
                .bind(experiment_id.parse::<i32>().unwrap())
                .fetch_one(&self.connection)
                .await?;
        Ok(Experiment {
            experiment_id: experiment.experiment_id.to_string(),
            name: experiment.name,
            artifact_location: experiment.artifact_location,
            lifecycle_stage: experiment.lifecycle_stage,
            tags: self.get_experiment_tags(experiment.experiment_id).await?,
        })
    }

    async fn restore_experiment(&self, experiment_id: &str) -> Result<Experiment, MlflowError> {
        let experiment: SqlExperiment = sqlx::query_as(
            r#"
            UPDATE experiments SET lifecycle_stage = 'active'
            WHERE experiment_id = $1
            RETURNING *
            "#,
        )
        .bind(experiment_id.parse::<i32>().unwrap())
        .fetch_one(&self.connection)
        .await?;
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
        let experiment: SqlExperiment = sqlx::query_as(
            r#"
            UPDATE experiments SET name = $1
            WHERE experiment_id = $2
            RETURNING *
            "#,
        )
        .bind(new_name)
        .bind(experiment_id.parse::<i32>().unwrap())
        .fetch_one(&self.connection)
        .await?;
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
    use super::{initialize_database, SqliteStore};
    use crate::{entities::ExperimentTag, stores::tracking::Store};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_get_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name = Uuid::new_v4().to_string();
        let experiment = store.create_experiment(&name, None, None).await.unwrap();
        let experiment = store
            .get_experiment(&experiment.experiment_id)
            .await
            .unwrap();
        assert_eq!(experiment.name, name);
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_create_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name = Uuid::new_v4().to_string();
        store.create_experiment(&name, None, None).await.unwrap();
        let experiments = store.list_experiments().await.unwrap();
        assert!(experiments
            .into_iter()
            .map(|e| e.name)
            .collect::<Vec<String>>()
            .contains(&name));
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_create_experiment_tags() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name = Uuid::new_v4().to_string();
        let experiment = store
            .create_experiment(
                &name,
                None,
                Some(vec![&ExperimentTag {
                    key: "key".to_string(),
                    value: "value".to_string(),
                }]),
            )
            .await
            .unwrap();
        assert_eq!(
            experiment.tags,
            vec![ExperimentTag {
                key: "key".to_string(),
                value: "value".to_string(),
            }]
        );
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_list_experiments() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name1 = Uuid::new_v4().to_string();
        let name2 = Uuid::new_v4().to_string();
        store.create_experiment(&name1, None, None).await.unwrap();
        store.create_experiment(&name2, None, None).await.unwrap();
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
        store.create_experiment(&name1, None, None).await.unwrap();
        store.create_experiment(&name2, None, None).await.unwrap();
        let experiments = store.search_experiments(Some(1), None, None).await.unwrap();
        assert_eq!(experiments.len(), 1);
        let experiments = store.search_experiments(Some(2), None, None).await.unwrap();
        assert_eq!(experiments.len(), 2);
        // Filter string
        let filter_string = format!("name = '{}'", name1.as_str());
        let experiments = store
            .search_experiments(None, Some(filter_string.as_str()), None)
            .await
            .unwrap();
        assert_eq!(experiments.len(), 1);
        let experiment = experiments.first().unwrap();
        assert_eq!(experiment.name, name1);

        let filter_string = format!("name LIKE '{}%'", &name1[..6]);
        let experiments = store
            .search_experiments(None, Some(filter_string.as_str()), None)
            .await
            .unwrap();
        assert_eq!(experiments.len(), 1);
        let experiment = experiments.first().unwrap();
        assert_eq!(experiment.name, name1);
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_search_experiments_filter_by_tags() {
        dotenv::from_filename(".env_dev").ok();
        initialize_database().await.unwrap();
        let store = SqliteStore::from_env().await.unwrap();
        let name1 = Uuid::new_v4().to_string();
        let name2 = Uuid::new_v4().to_string();
        let tags1 = vec![ExperimentTag {
            key: "key".to_string(),
            value: "value1".to_string(),
        }];
        let tags2 = vec![ExperimentTag {
            key: "key".to_string(),
            value: "value2".to_string(),
        }];
        store
            .create_experiment(&name1, None, Some(tags1.iter().collect()))
            .await
            .unwrap();
        store
            .create_experiment(&name2, None, Some(tags2.iter().collect()))
            .await
            .unwrap();
        let experiments = store
            .search_experiments(Some(1), Some("tag.key = 'value1'"), None)
            .await
            .unwrap();
        assert_eq!(experiments.len(), 1);
        let experiment = experiments.first().unwrap();
        assert_eq!(experiment.tags, tags1);

        let experiments = store
            .search_experiments(Some(1), Some("tag.key = 'value2'"), None)
            .await
            .unwrap();
        assert_eq!(experiments.len(), 1);
        let experiment = experiments.first().unwrap();
        assert_eq!(experiment.tags, tags2);

        let experiments = store
            .search_experiments(
                Some(1),
                Some("tag.key LIKE 'val%' AND tag.key LIKE '%ue1'"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(experiments.len(), 1);
        let experiment = experiments.first().unwrap();
        assert_eq!(experiment.tags, tags1);

        let experiments = store
            .search_experiments(Some(1), Some("tag.nonexistent_key LIKE 'val%'"), None)
            .await
            .unwrap();
        assert!(experiments.is_empty());

        store.teardown().await;
    }

    #[tokio::test]
    async fn test_search_experiments_order_by() {
        dotenv::from_filename(".env_dev").ok();
        initialize_database().await.unwrap();
        let store = SqliteStore::from_env().await.unwrap();
        let name1 = "a";
        let name2 = "b";
        store.create_experiment(&name1, None, None).await.unwrap();
        store.create_experiment(&name2, None, None).await.unwrap();
        let experiments = store
            .search_experiments(None, None, Some(vec!["name"]))
            .await
            .unwrap();
        assert_eq!(experiments.len(), 2);
        assert_eq!(experiments[0].name, "a");
        assert_eq!(experiments[1].name, "b");

        let experiments = store
            .search_experiments(None, None, Some(vec!["name DESC"]))
            .await
            .unwrap();
        assert_eq!(experiments.len(), 2);
        assert_eq!(experiments[0].name, "b");
        assert_eq!(experiments[1].name, "a");

        store.teardown().await;
    }

    #[tokio::test]
    async fn test_search_experiments_filter_string_containing_invalid_entities() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let filter_string = format!("param.key = 'value'");
        let res = store
            .search_experiments(None, Some(filter_string.as_str()), None)
            .await;
        assert!(res.is_err());
        store.teardown().await;
    }

    #[tokio::test]
    async fn test_delete_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let store = SqliteStore::from_env().await.unwrap();
        let name = Uuid::new_v4().to_string();
        let experiment = store.create_experiment(&name, None, None).await.unwrap();
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
        let experiment = store.create_experiment(&name, None, None).await.unwrap();
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
        let experiment = store.create_experiment(&name, None, None).await.unwrap();
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
