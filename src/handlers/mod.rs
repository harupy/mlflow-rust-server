pub mod experiments;
pub mod runs;
use actix_web::{web, Scope};

const API_PREFIX: &str = "/api/2.0/mlflow";

pub fn get_service() -> Scope {
    web::scope(API_PREFIX)
        .service(experiments::get_scope())
        .service(runs::get_scope())
}

pub fn get_api_endpoint(endpoint: &str) -> String {
    format!("{}{}", API_PREFIX, endpoint)
}
