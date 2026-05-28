use std::env;
use std::net::SocketAddr;

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use durable_evals_core::{
    BeginStepRequest, CompleteStepRequest, Error as StoreError, ErrorInfo, FailStepRequest, Health,
    Runtime, SqliteStore, StepOutcome,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let db_path = env::var("DURABLE_EVALS_DB").unwrap_or_else(|_| ".durable/evals.sqlite".into());
    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let runtime = Runtime::new(SqliteStore::open(db_path)?);
    let app = Router::new()
        .route("/health", get(health))
        .route("/steps/begin", post(begin_step))
        .route("/steps/complete", post(complete_step))
        .route("/steps/fail", post(fail_step))
        .with_state(runtime);

    let addr: SocketAddr = env::var("DURABLE_EVALS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:0".into())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    println!("{local_addr}");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

async fn begin_step(
    State(runtime): State<Runtime>,
    Json(req): Json<BeginStepRequest>,
) -> Result<Json<StepOutcome>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.begin_step(req).map(Json).map_err(store_error)
}

async fn complete_step(
    State(runtime): State<Runtime>,
    Json(req): Json<CompleteStepRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .complete_step(req)
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
}

async fn fail_step(
    State(runtime): State<Runtime>,
    Json(req): Json<FailStepRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .fail_step(req)
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
}

fn internal_server_error<E: std::fmt::Display>(
    error: E,
) -> (axum::http::StatusCode, Json<ErrorInfo>) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorInfo {
            error_type: "InternalServerError".to_string(),
            message: error.to_string(),
        }),
    )
}

fn store_error(error: StoreError) -> (axum::http::StatusCode, Json<ErrorInfo>) {
    match error {
        StoreError::StepNotFound => (
            axum::http::StatusCode::NOT_FOUND,
            Json(ErrorInfo {
                error_type: "StepNotFound".to_string(),
                message: error.to_string(),
            }),
        ),
        error => internal_server_error(error),
    }
}
