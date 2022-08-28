use crate::config::ServerConfig;
use crate::entities::Experiment;
use crate::stores::tracking::get_store_from_server_config;
use actix_web::{web, HttpResponse, Responder, Result, Scope};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ListExperimentsResponse {
    pub experiments: Vec<Experiment>,
    pub next_page_token: Option<String>,
}

pub async fn list_experiments(server_config: web::Data<ServerConfig>) -> Result<impl Responder> {
    let store = get_store_from_server_config(&server_config).await?;
    let experiments = store.list_experiments().await?;
    store.teardown().await;
    Ok(web::Json(ListExperimentsResponse {
        experiments,
        next_page_token: None,
    }))
}

#[derive(Deserialize)]
pub struct SearchExperimentsRequest {
    max_results: Option<i64>,
    filter_string: Option<String>,
}

#[derive(Serialize)]
struct SearchExperimentsResponse {
    experiments: Vec<Experiment>,
    next_page_token: Option<String>,
}

pub async fn search_experiments(
    server_config: web::Data<ServerConfig>,
    params: web::Json<SearchExperimentsRequest>,
) -> Result<HttpResponse, actix_web::Error> {
    let store = get_store_from_server_config(&server_config).await?;
    let experiments = store
        .search_experiments(params.max_results, params.filter_string.as_deref(), None)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    store.teardown().await;
    Ok(HttpResponse::Ok().json(SearchExperimentsResponse {
        experiments,
        next_page_token: None,
    }))
}

#[derive(Deserialize)]
struct GetExperimentRequest {
    experiment_id: i32,
}

#[derive(Serialize)]
struct GetExperimentResponse {
    experiment: Experiment,
}

async fn get_experiment(
    server_config: web::Data<ServerConfig>,
    params: web::Query<GetExperimentRequest>,
) -> Result<impl Responder> {
    let store = get_store_from_server_config(&server_config).await?;
    let experiment = store
        .get_experiment(params.experiment_id.to_string().as_str())
        .await?;
    store.teardown().await;
    Ok(web::Json(GetExperimentResponse { experiment }))
}

#[derive(Serialize, Deserialize)]
pub struct CreateExperimentRequest {
    pub name: String,
    pub artifact_location: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct CreateExperimentResponse {
    pub experiment: Experiment,
}

async fn create_experiment(
    server_config: web::Data<ServerConfig>,
    data: web::Json<CreateExperimentRequest>,
) -> Result<impl Responder> {
    let store = get_store_from_server_config(&server_config).await?;
    let experiment = store
        .create_experiment(data.name.as_str(), data.artifact_location.as_deref(), None)
        .await?;
    store.teardown().await;
    Ok(web::Json(GetExperimentResponse { experiment }))
}

#[derive(Serialize, Deserialize)]
pub struct DeleteExperimentRequest {
    pub experiment_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct DeleteExperimentResponse {
    pub experiment: Experiment,
}

async fn delete_experiment(
    server_config: web::Data<ServerConfig>,
    data: web::Json<DeleteExperimentRequest>,
) -> Result<impl Responder> {
    let store = get_store_from_server_config(&server_config).await?;
    let experiment = store.delete_experiment(data.experiment_id.as_str()).await?;
    store.teardown().await;
    Ok(web::Json(DeleteExperimentResponse { experiment }))
}

#[derive(Serialize, Deserialize)]
pub struct RestoreExperimentRequest {
    pub experiment_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct RestoreExperimentResponse {
    pub experiment: Experiment,
}

async fn restore_experiment(
    server_config: web::Data<ServerConfig>,
    data: web::Json<RestoreExperimentRequest>,
) -> Result<impl Responder> {
    let store = get_store_from_server_config(&server_config).await?;
    let experiment = store
        .restore_experiment(data.experiment_id.as_str())
        .await?;
    store.teardown().await;
    Ok(web::Json(RestoreExperimentResponse { experiment }))
}

#[derive(Serialize, Deserialize)]
pub struct UpdateExperimentRequest {
    pub experiment_id: String,
    pub new_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateExperimentResponse {
    pub experiment: Experiment,
}

async fn update_experiment(
    server_config: web::Data<ServerConfig>,
    data: web::Json<UpdateExperimentRequest>,
) -> Result<impl Responder> {
    let store = get_store_from_server_config(&server_config).await?;
    let experiment = store
        .update_experiment(data.experiment_id.as_str(), data.new_name.as_str())
        .await?;
    store.teardown().await;
    Ok(web::Json(UpdateExperimentResponse { experiment }))
}

pub fn get_scope() -> Scope {
    web::scope("experiments")
        .route("list", web::get().to(list_experiments))
        .route("search", web::post().to(search_experiments))
        .route("get", web::get().to(get_experiment))
        .route("create", web::post().to(create_experiment))
        .route("delete", web::post().to(delete_experiment))
        .route("restore", web::post().to(restore_experiment))
        .route("update", web::post().to(update_experiment))
}

#[cfg(test)]
mod tests {
    use super::{
        CreateExperimentRequest, CreateExperimentResponse, DeleteExperimentRequest,
        DeleteExperimentResponse, ListExperimentsResponse, RestoreExperimentRequest,
        RestoreExperimentResponse, UpdateExperimentRequest, UpdateExperimentResponse,
    };
    use crate::config::ServerConfig;
    use crate::handlers::{get_api_endpoint, get_service};
    use crate::utils::random_string;
    use actix_web::test;
    use actix_web::{web, App};

    #[tokio::test]
    async fn test_list_experiments() {
        dotenv::from_filename(".env_dev").ok();
        let server_config = ServerConfig::from_env();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server_config.clone()))
                .service(get_service()),
        )
        .await;
        let req = test::TestRequest::with_uri(get_api_endpoint("/experiments/list").as_str())
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let list_experiments_resp: ListExperimentsResponse = serde_json::from_str(&result).unwrap();
        assert!(list_experiments_resp.experiments.len() > 0);
    }

    #[tokio::test]
    async fn test_create_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let server_config = ServerConfig::from_env();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server_config.clone()))
                .service(get_service()),
        )
        .await;
        let name = random_string();
        let req = test::TestRequest::post()
            .uri(get_api_endpoint("/experiments/create").as_str())
            .set_json(&CreateExperimentRequest {
                name: name.clone(),
                artifact_location: None,
            })
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let create_experiment_resp: CreateExperimentResponse =
            serde_json::from_str(&result).unwrap();
        assert_eq!(create_experiment_resp.experiment.name, name);
    }

    #[tokio::test]
    async fn test_delete_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let server_config = ServerConfig::from_env();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server_config.clone()))
                .service(get_service()),
        )
        .await;
        let name = random_string();
        let req = test::TestRequest::post()
            .uri(get_api_endpoint("/experiments/create").as_str())
            .set_json(&CreateExperimentRequest {
                name: name.clone(),
                artifact_location: None,
            })
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let create_experiment_resp: CreateExperimentResponse =
            serde_json::from_str(&result).unwrap();

        let req = test::TestRequest::post()
            .uri(get_api_endpoint("/experiments/delete").as_str())
            .set_json(&DeleteExperimentRequest {
                experiment_id: create_experiment_resp.experiment.experiment_id,
            })
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let delete_experiment_resp: DeleteExperimentResponse =
            serde_json::from_str(&result).unwrap();
        assert_eq!(delete_experiment_resp.experiment.lifecycle_stage, "deleted");
    }

    #[tokio::test]
    async fn test_restore_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let server_config = ServerConfig::from_env();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server_config.clone()))
                .service(get_service()),
        )
        .await;

        let name = random_string();
        let req = test::TestRequest::post()
            .uri(get_api_endpoint("/experiments/create").as_str())
            .set_json(&CreateExperimentRequest {
                name: name.clone(),
                artifact_location: None,
            })
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let create_experiment_resp: CreateExperimentResponse =
            serde_json::from_str(&result).unwrap();

        let req = test::TestRequest::post()
            .uri(get_api_endpoint("/experiments/delete").as_str())
            .set_json(&DeleteExperimentRequest {
                experiment_id: create_experiment_resp.experiment.experiment_id,
            })
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let delete_experiment_resp: DeleteExperimentResponse =
            serde_json::from_str(&result).unwrap();
        assert_eq!(delete_experiment_resp.experiment.lifecycle_stage, "deleted");

        let req = test::TestRequest::post()
            .uri(get_api_endpoint("/experiments/restore").as_str())
            .set_json(&RestoreExperimentRequest {
                experiment_id: delete_experiment_resp.experiment.experiment_id,
            })
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let restore_experiment_resp: RestoreExperimentResponse =
            serde_json::from_str(&result).unwrap();
        assert_eq!(restore_experiment_resp.experiment.lifecycle_stage, "active");
    }

    #[tokio::test]
    async fn test_update_experiment() {
        dotenv::from_filename(".env_dev").ok();
        let server_config = ServerConfig::from_env();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server_config.clone()))
                .service(get_service()),
        )
        .await;

        let name = random_string();
        let req = test::TestRequest::post()
            .uri(get_api_endpoint("/experiments/create").as_str())
            .set_json(&CreateExperimentRequest {
                name: name.clone(),
                artifact_location: None,
            })
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let create_experiment_resp: CreateExperimentResponse =
            serde_json::from_str(&result).unwrap();

        let new_name = random_string();
        let req = test::TestRequest::post()
            .uri(get_api_endpoint("/experiments/update").as_str())
            .set_json(&UpdateExperimentRequest {
                experiment_id: create_experiment_resp.experiment.experiment_id,
                new_name: new_name.clone(),
            })
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        let update_experiment_resp: UpdateExperimentResponse =
            serde_json::from_str(&result).unwrap();
        assert_eq!(update_experiment_resp.experiment.name, new_name);
    }
}
