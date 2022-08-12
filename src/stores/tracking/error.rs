use actix_web::{http::header::ContentType, http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;

#[derive(Debug)]
pub enum MlflowError {
    DatabaseError(sqlx::Error),
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub message: String,
}

impl From<sqlx::Error> for MlflowError {
    fn from(e: sqlx::Error) -> Self {
        Self::DatabaseError(e)
    }
}

impl std::fmt::Display for MlflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ResponseError for MlflowError {
    fn status_code(&self) -> actix_web::http::StatusCode {
        match *self {
            Self::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .json(ErrorResponse {
                message: self.to_string(),
            })
    }
}
