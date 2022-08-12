use actix_web::{middleware::Logger, web, App, HttpServer};
use clap::Parser;
use mlflow_rust_server::cli::Args;
use mlflow_rust_server::config::ServerConfig;
use mlflow_rust_server::handlers::get_service;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let server_config = ServerConfig {
        backend_store_uri: args.backend_store_uri,
        default_artifact_root: args.default_artifact_root,
    };

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("debug"));
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(server_config.clone()))
            .wrap(Logger::default())
            .service(get_service())
    })
    .bind((args.host, args.port))?
    .run()
    .await
}
