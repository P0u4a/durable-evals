use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use durable_evals_core::{Error as StoreError, *};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let db = env::var("DURABLE_EVALS_DB").unwrap_or_else(|_| ".durable/evals.sqlite".into());
    let runtime = build_runtime(&db)?;
    // Optional shared-secret auth. Unset (the default) leaves the API open, which is fine
    // for the loopback-bound local runtime but should be set whenever DURABLE_EVALS_ADDR
    // exposes the server beyond localhost.
    let token = env::var("DURABLE_EVALS_TOKEN")
        .ok()
        .filter(|value| !value.is_empty())
        .map(Arc::new);
    let app = Router::new()
        .route("/health", get(health))
        .route("/runs/register", post(register_run))
        .route("/runs/summary", post(summary))
        .route("/runs/export", post(export_run))
        .route("/steps/begin", post(begin_step))
        .route("/steps/complete", post(complete_step))
        .route("/steps/fail", post(fail_step))
        .route("/steps/heartbeat", post(heartbeat_step))
        .route("/datasets/register", post(register_dataset))
        .route("/datasets/tasks/list", post(list_tasks))
        .route("/datasets/tasks/complete", post(complete_task))
        .route("/datasets/tasks/fail", post(fail_task))
        .route("/variants/register", post(register_variants))
        .route("/variants/:run_id", get(list_variants))
        .route("/traces/events", post(add_trace_event))
        .route(
            "/traces/:run_id/:dataset_name/:task_id",
            get(list_trace_events),
        )
        .route("/memos/get", post(memo_get))
        .route("/memos/put", post(memo_put))
        .layer(middleware::from_fn_with_state(token, require_token))
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

/// Reject requests without a matching `Authorization: Bearer <token>` when a token is
/// configured. `/health` stays open so liveness probes don't need the secret.
async fn require_token(
    State(token): State<Option<Arc<String>>>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(expected) = token.as_deref() {
        if req.uri().path() != "/health" {
            let provided = req
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "));
            if provided != Some(expected.as_str()) {
                return (
                    axum::http::StatusCode::UNAUTHORIZED,
                    Json(ErrorInfo {
                        error_type: "Unauthorized".to_string(),
                        message: "missing or invalid bearer token".to_string(),
                        failure_class: FailureClass::DurableHarnessError,
                        stack: None,
                        retryable: Some(false),
                    }),
                )
                    .into_response();
            }
        }
    }
    next.run(req).await
}

fn build_runtime(db: &str) -> anyhow::Result<Runtime> {
    if let Some(parent) = std::path::Path::new(db).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    Ok(Runtime::new(SqliteStore::open(db)?))
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

async fn register_run(
    State(runtime): State<Runtime>,
    Json(req): Json<RegisterRunRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .register_run(req)
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
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

async fn heartbeat_step(
    State(runtime): State<Runtime>,
    Json(req): Json<HeartbeatStepRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .heartbeat_step(req)
        .map(|retained| Json(Health { ok: retained }))
        .map_err(store_error)
}

async fn register_dataset(
    State(runtime): State<Runtime>,
    Json(req): Json<RegisterDatasetRequest>,
) -> Result<Json<DatasetSummary>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.register_dataset(req).map(Json).map_err(store_error)
}

async fn list_tasks(
    State(runtime): State<Runtime>,
    Json(req): Json<ListTasksRequest>,
) -> Result<Json<Vec<TaskRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.list_tasks(req).map(Json).map_err(store_error)
}

async fn complete_task(
    State(runtime): State<Runtime>,
    Json(req): Json<CompleteTaskRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .complete_task(req)
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
}

async fn fail_task(
    State(runtime): State<Runtime>,
    Json(req): Json<FailTaskRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .fail_task(req)
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
}

async fn register_variants(
    State(runtime): State<Runtime>,
    Json(req): Json<RegisterVariantsRequest>,
) -> Result<Json<Vec<VariantRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .register_variants(req)
        .map(Json)
        .map_err(store_error)
}

async fn list_variants(
    State(runtime): State<Runtime>,
    Path(run_id): Path<String>,
) -> Result<Json<Vec<VariantRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .list_variants(&run_id)
        .map(Json)
        .map_err(store_error)
}

async fn add_trace_event(
    State(runtime): State<Runtime>,
    Json(req): Json<TraceEventRequest>,
) -> Result<Json<TraceEventRecord>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.add_trace_event(req).map(Json).map_err(store_error)
}

async fn list_trace_events(
    State(runtime): State<Runtime>,
    Path((run_id, dataset_name, task_id)): Path<(String, String, String)>,
) -> Result<Json<Vec<TraceEventRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .list_trace_events(&run_id, &dataset_name, &task_id)
        .map(Json)
        .map_err(store_error)
}

async fn memo_get(
    State(runtime): State<Runtime>,
    Json(req): Json<MemoGetRequest>,
) -> Result<Json<MemoGetResponse>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.memo_get(req).map(Json).map_err(store_error)
}

async fn memo_put(
    State(runtime): State<Runtime>,
    Json(req): Json<MemoPutRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .memo_put(req)
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
}

async fn summary(
    State(runtime): State<Runtime>,
    Json(req): Json<SummaryRequest>,
) -> Result<Json<RunSummary>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.summary(req).map(Json).map_err(store_error)
}

async fn export_run(
    State(runtime): State<Runtime>,
    Json(req): Json<ExportRequest>,
) -> Result<Json<ExportResponse>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.export(req).map(Json).map_err(store_error)
}

fn internal_server_error<E: std::fmt::Display>(
    error: E,
) -> (axum::http::StatusCode, Json<ErrorInfo>) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorInfo {
            error_type: "InternalServerError".to_string(),
            message: error.to_string(),
            failure_class: FailureClass::DurableHarnessError,
            stack: None,
            retryable: Some(false),
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
                failure_class: FailureClass::DurableHarnessError,
                stack: None,
                retryable: Some(false),
            }),
        ),
        StoreError::TaskNotFound => (
            axum::http::StatusCode::NOT_FOUND,
            Json(ErrorInfo {
                error_type: "TaskNotFound".to_string(),
                message: error.to_string(),
                failure_class: FailureClass::DurableHarnessError,
                stack: None,
                retryable: Some(false),
            }),
        ),
        error => internal_server_error(error),
    }
}
