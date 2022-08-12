use crate::env::{DEFAULT_ARTIFACT_ROOT, MLFLOW_TRACKING_URI};
use std::env;

#[derive(Clone)]
pub struct ServerConfig {
    pub backend_store_uri: String,
    pub default_artifact_root: String,
}

impl ServerConfig {
    pub fn from_env() -> Self {
        Self {
            backend_store_uri: env::var(MLFLOW_TRACKING_URI)
                .unwrap_or_else(|_| panic!("{} must be set", MLFLOW_TRACKING_URI)),
            default_artifact_root: env::var(DEFAULT_ARTIFACT_ROOT)
                .unwrap_or_else(|_| panic!("{} must be set", DEFAULT_ARTIFACT_ROOT)),
        }
    }
}
