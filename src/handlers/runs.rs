use crate::config::ServerConfig;
use crate::entities::Run;
use crate::stores::tracking::get_store_from_server_config;
use actix_web::{web, Responder, Result, Scope};
use serde::{Deserialize, Serialize};

// use regex::Regex;
// use crate::proto::mlflow::RunStatus;
// use crate::proto::mlflow::{
//     search_runs::Response as SearchRunsResponse, Metric, Param, Run, RunData, RunInfo, RunTag,
//     SearchRuns, ViewType,
// };

// #[derive(Debug, sqlx::FromRow, Serialize)]
// struct ExperimentTag {
//     run_uuid: String,
//     key: String,
//     value: String,
// }

// impl ViewType {
//     pub fn from_str<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         let s: &str = Deserialize::deserialize(deserializer)?;
//         match s.to_lowercase().as_str() {
//             "ACTIVE_ONLY" => Ok(Some(Self::ActiveOnly as i32)),
//             "DELETED_ONLY" => Ok(Some(Self::DeletedOnly as i32)),
//             "ALL" => Ok(Some(Self::All as i32)),
//             _ => Err(serde::de::Error::custom(format!("Invalid ViewType: {}", s))),
//         }
//     }
// }

// impl<'r> FromRow<'r, PgRow> for Run {
//     fn from_row(row: &'r PgRow) -> Result<Self, Error> {
//         let run_uuid: String = row.try_get("run_uuid")?;
//         let user_id = row.try_get("user_id")?;
//         let status = row.try_get("status")?;
//         let start_time = row.try_get("start_time")?;
//         let end_time = row.try_get("end_time")?;
//         let lifecycle_stage = row.try_get("lifecycle_stage")?;
//         let artifact_uri = row.try_get("artifact_uri")?;
//         let experiment_id: i32 = row.try_get("experiment_id")?;

//         let info = RunInfo {
//             run_uuid: Some(run_uuid.clone()),
//             run_id: Some(run_uuid.clone()),
//             user_id,
//             status,
//             start_time,
//             end_time,
//             lifecycle_stage,
//             artifact_uri,
//             experiment_id: Some(experiment_id.to_string()),
//         };
//         let data = RunData {
//             params: vec![],
//             metrics: vec![],
//             tags: vec![],
//         };
//         Ok(Run {
//             info: Some(info),
//             data: Some(data),
//         })
//     }
// }

// impl<'r> FromRow<'r, PgRow> for Param {
//     fn from_row(row: &'r PgRow) -> Result<Self, Error> {
//         let key: String = row.try_get("key")?;
//         let value: String = row.try_get("value")?;
//         Ok(Param {
//             key: Some(key),
//             value: Some(value),
//         })
//     }
// }

// impl<'r> FromRow<'r, PgRow> for Metric {
//     fn from_row(row: &'r PgRow) -> Result<Self, Error> {
//         let key: String = row.try_get("key")?;
//         let value: f64 = row.try_get("value")?;
//         let timestamp: i64 = row.try_get("timestamp")?;
//         let step: i64 = row.try_get("step")?;
//         Ok(Metric {
//             key: Some(key),
//             value: Some(value),
//             timestamp: Some(timestamp),
//             step: Some(step),
//         })
//     }
// }

// impl<'r> FromRow<'r, PgRow> for RunTag {
//     fn from_row(row: &'r PgRow) -> Result<Self, Error> {
//         let key: String = row.try_get("key")?;
//         let value: String = row.try_get("value")?;
//         Ok(RunTag {
//             key: Some(key),
//             value: Some(value),
//         })
//     }
// }

// pub async fn list_runs(pool: web::Data<PgPool>) -> Result<impl Responder> {
//     let runs: Vec<Run> = sqlx::query_as(r#"SELECT * FROM runs"#)
//         .fetch_all(pool.get_ref())
//         .await
//         .unwrap();
//     Ok(web::Json(runs))
// }

// #[derive(Debug, sqlx::FromRow, Serialize)]
// struct SqlRun {
//     run_uuid: String,
//     name: String,
//     source_type: String,
//     source_name: String,
//     entry_point_name: String,
//     user_id: String,
//     status: String,
//     start_time: i64,
//     end_time: i64,
//     source_version: String,
//     lifecycle_stage: String,
//     artifact_uri: String,
//     experiment_id: i32,
// }

// fn view_type_to_stages(view_type: ViewType) -> Vec<&'static str> {
//     match view_type {
//         ViewType::ActiveOnly => vec!["active"],
//         ViewType::DeletedOnly => vec!["deleted"],
//         ViewType::All => vec!["active", "deleted"],
//     }
// }

// struct Comparison<'a> {
//     key: &'a str,
//     comparator: &'a str,
//     value: &'a str,
// }

// impl<'a> Comparison<'a> {
//     fn to_query(&self) -> String {
//         format!(
//             "key = '{}' AND value {} {}",
//             self.key, self.comparator, self.value
//         )
//     }
// }

// {
//     let s: &str = serde::Deserialize::deserialize(deserializer)?;
//     match s {
//         "RUNNING" => Ok(Some(RunStatus::Running as i32)),
//         "SCHEDULE" => Ok(Some(RunStatus::Scheduled as i32)),
//         "FINISHED" => Ok(Some(RunStatus::Finished as i32)),
//         "FAILED" => Ok(Some(RunStatus::Failed as i32)),
//         "KILLED" => Ok(Some(RunStatus::Killed as i32)),
//         _ => Err(serde::de::Error::custom(format!("Invalid ViewType: {}", s))),
//     }
// }

// fn run_status_string_to_int(status: &str) -> Result<i32, Box<dyn std::error::Error>> {
//     match status {
//         "RUNNING" => Ok(RunStatus::Running as i32),
//         "SCHEDULED" => Ok(RunStatus::Scheduled as i32),
//         "FINISHED" => Ok(RunStatus::Finished as i32),
//         "FAILED" => Ok(RunStatus::Failed as i32),
//         "KILLED" => Ok(RunStatus::Killed as i32),
//         _ => Err(format!("Invalid RunStatus: {}", status).into()),
//     }
// }

// pub async fn search_runs(
//     pool: web::Data<PgPool>,
//     payload: web::Json<SearchRuns>,
// ) -> Result<impl Responder> {
//     let stages =
//         view_type_to_stages(ViewType::from_i32(payload.run_view_type.unwrap_or(0)).unwrap());
//     let re = Regex::new(
//         r"((?P<type>(params|metrics|tags|attributes))\.)?(?P<key>.+) (?P<comparator>(=|!=|<|<=|>|>=|LIKE|ILIKE)) (?P<value>.+)",
//     )
//     .unwrap();
//     let filter_query = match &payload.filter {
//         Some(filter) if (filter != "") => {
//             let clauses = filter.split("AND").map(|x| x.trim()).collect::<Vec<_>>();
//             let query = clauses.into_iter().fold("runs".to_string(), |acc, clause| {
//                 let caps = re.captures(clause).unwrap();
//                 let typ = caps.name("type").unwrap().as_str();
//                 let comparison = Comparison {
//                     key: caps.name("key").unwrap().as_str(),
//                     comparator: caps.name("comparator").unwrap().as_str(),
//                     value: caps.name("value").unwrap().as_str(),
//                 };
//                 let query = comparison.to_query();
//                 format!(
//                     r#"
//                         (
//                             SELECT
//                                 runs.*
//                             FROM
//                                 {}
//                             JOIN (
//                                 SELECT
//                                     DISTINCT ON (run_uuid) run_uuid
//                                 FROM
//                                     {}
//                                 WHERE
//                                     {}
//                             ) filtered
//                             ON
//                                 filtered.run_uuid = runs.run_uuid
//                         ) runs
//                         "#,
//                     acc, typ, query
//                 )
//             });
//             query
//         }
//         None => "runs".to_string(),
//         _ => "runs".to_string(),
//     };

//     let sql_runs: Vec<SqlRun> = sqlx::query_as(&format!(
//         r#"
// SELECT *
// FROM {}
// WHERE lifecycle_stage = ANY($1) LIMIT $2
// "#,
//         filter_query
//     ))
//     .bind(stages)
//     .bind(payload.max_results.unwrap_or(3))
//     .fetch_all(pool.get_ref())
//     .await
//     .unwrap();

//     let mut runs: Vec<Run> = vec![];
//     for sql_run in sql_runs {
//         let params: Vec<Param> = sqlx::query_as(r#"SELECT * FROM params WHERE run_uuid = $1"#)
//             .bind(sql_run.run_uuid.as_str())
//             .fetch_all(pool.get_ref())
//             .await
//             .unwrap();
//         let metrics: Vec<Metric> = sqlx::query_as(r#"SELECT * FROM metrics WHERE run_uuid = $1"#)
//             .bind(sql_run.run_uuid.as_str())
//             .fetch_all(pool.get_ref())
//             .await
//             .unwrap();
//         let tags: Vec<RunTag> = sqlx::query_as(r#"SELECT * FROM tags WHERE run_uuid = $1"#)
//             .bind(sql_run.run_uuid.as_str())
//             .fetch_all(pool.get_ref())
//             .await
//             .unwrap();
//         let run = Run {
//             info: Some(RunInfo {
//                 run_uuid: Some(sql_run.run_uuid.clone()),
//                 run_id: Some(sql_run.run_uuid),
//                 experiment_id: Some(sql_run.experiment_id.to_string()),
//                 user_id: Some(sql_run.user_id),
//                 status: Some(run_status_string_to_int(&sql_run.status).unwrap()),
//                 start_time: Some(sql_run.start_time),
//                 end_time: Some(sql_run.end_time),
//                 lifecycle_stage: Some(sql_run.lifecycle_stage),
//                 artifact_uri: Some(sql_run.artifact_uri),
//             }),
//             data: Some(RunData {
//                 params,
//                 metrics,
//                 tags,
//             }),
//         };
//         runs.push(run);
//     }

//     Ok(web::Json(SearchRunsResponse {
//         runs,
//         next_page_token: None,
//     }))
// }

// #[derive(Deserialize)]
// pub struct GetRunParams {
//     run_id: String,
// }

// #[derive(Serialize)]
// struct GetRunResponse {
//     run: Run,
// }

// pub async fn get_run(
//     pool: web::Data<PgPool>,
//     params: web::Query<GetRunParams>,
// ) -> Result<impl Responder> {
//     let run: SqlRun = sqlx::query_as(r#"SELECT * FROM runs WHERE run_uuid = $1"#)
//         .bind(params.run_id.as_str())
//         .fetch_one(pool.get_ref())
//         .await
//         .unwrap();
//     let params: Vec<Param> = sqlx::query_as(r#"SELECT * FROM params WHERE run_uuid = $1"#)
//         .bind(run.run_uuid.as_str())
//         .fetch_all(pool.get_ref())
//         .await
//         .unwrap();
//     let metrics: Vec<Metric> = sqlx::query_as(r#"SELECT * FROM metrics WHERE run_uuid = $1"#)
//         .bind(run.run_uuid.as_str())
//         .fetch_all(pool.get_ref())
//         .await
//         .unwrap();
//     let tags: Vec<RunTag> = sqlx::query_as(r#"SELECT * FROM tags WHERE run_uuid = $1"#)
//         .bind(run.run_uuid.as_str())
//         .fetch_all(pool.get_ref())
//         .await
//         .unwrap();
//     let run = Run {
//         info: Some(RunInfo {
//             run_uuid: Some(run.run_uuid.clone()),
//             run_id: Some(run.run_uuid),
//             user_id: Some(run.user_id),
//             status: Some(run.status.parse().unwrap()),
//             start_time: Some(run.start_time),
//             end_time: Some(run.end_time),
//             artifact_uri: Some(run.artifact_uri),
//             experiment_id: Some(run.experiment_id.to_string()),
//             lifecycle_stage: Some(run.lifecycle_stage),
//         }),
//         data: Some(RunData {
//             params,
//             metrics,
//             tags,
//         }),
//     };
//     Ok(web::Json(GetRunResponse { run }))
// }

#[derive(Serialize, Deserialize)]
pub struct SearchRunsRequest {
    experiment_ids: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct SearchRunsResponse {
    runs: Vec<Run>,
    next_page_token: Option<String>,
}

pub async fn search_runs(
    server_config: web::Data<ServerConfig>,
    data: web::Json<SearchRunsRequest>,
) -> Result<impl Responder> {
    let store = get_store_from_server_config(&server_config).await?;
    let runs = store
        .search_runs(data.experiment_ids.iter().map(|s| &**s).collect())
        .await?;
    store.teardown().await;
    Ok(web::Json(SearchRunsResponse {
        runs,
        next_page_token: None,
    }))
}

#[derive(Deserialize)]
pub struct GetRunParams {
    pub run_id: String,
}

#[derive(Serialize)]
pub struct GetRunResponse {
    pub run: Run,
}

async fn get_run(
    server_config: web::Data<ServerConfig>,
    params: web::Query<GetRunParams>,
) -> Result<impl Responder> {
    let store = get_store_from_server_config(&server_config).await?;
    let run = store.get_run(&params.run_id.to_string()).await?;
    store.teardown().await;
    Ok(web::Json(GetRunResponse { run }))
}

pub fn get_scope() -> Scope {
    web::scope("runs")
        .route("search", web::post().to(search_runs))
        .route("get", web::get().to(get_run))
}

#[cfg(test)]
mod tests {
    use super::{SearchRunsRequest, SearchRunsResponse};
    use crate::config::ServerConfig;
    use crate::handlers::{get_api_endpoint, get_service};
    use actix_web::test;
    use actix_web::{web, App};

    #[tokio::test]
    async fn test_search_runs() {
        dotenv::from_filename(".env_dev").ok();
        let server_config = ServerConfig::from_env();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server_config.clone()))
                .service(get_service()),
        )
        .await;
        let req = test::TestRequest::post()
            .uri(get_api_endpoint("/runs/search").as_str())
            .set_json(&SearchRunsRequest {
                experiment_ids: vec!["0".to_string()],
            })
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let search_runs_resp: SearchRunsResponse = serde_json::from_str(&result).unwrap();
        assert!(search_runs_resp.runs.len() > 0);
    }
}
