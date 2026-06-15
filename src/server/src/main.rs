use std::env;
use std::net::SocketAddr;

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use durable_evals_core::{Error as StoreError, *};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let db = env::var("DURABLE_EVALS_DB").unwrap_or_else(|_| ".durable/evals.sqlite".into());
    let runtime = build_runtime(&db)?;
    let app = Router::new()
        .route("/health", get(health))
        .route("/runs/register", post(register_run))
        .route("/runs/summary", post(summary))
        .route("/runs/export", post(export_run))
        .route("/steps/begin", post(begin_step))
        .route("/steps/complete", post(complete_step))
        .route("/steps/fail", post(fail_step))
        .route("/steps/heartbeat", post(heartbeat_step))
        .route("/batches/register", post(register_batch))
        .route("/batches/cases/start", post(start_case))
        .route("/batches/cases/list", post(list_cases))
        .route("/batches/cases/complete", post(complete_case))
        .route("/batches/cases/fail", post(fail_case))
        .route("/variants/register", post(register_variants))
        .route("/variants/:run_id", get(list_variants))
        .route("/workers/register", post(register_worker))
        .route("/workers", get(list_workers))
        .route("/traces/events", post(add_trace_event))
        .route(
            "/traces/:run_id/:batch_name/:case_id",
            get(list_trace_events),
        )
        .route("/memos/get", post(memo_get))
        .route("/memos/put", post(memo_put))
        .route("/reviews", post(mark_reviewed))
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

fn build_runtime(db: &str) -> anyhow::Result<Runtime> {
    if db.starts_with("postgres://") || db.starts_with("postgresql://") {
        #[cfg(feature = "postgres")]
        {
            return Ok(Runtime::new(PostgresStore::connect(db)?));
        }
        #[cfg(not(feature = "postgres"))]
        {
            anyhow::bail!(
                "DURABLE_EVALS_DB requests postgres but the server was built without the `postgres` feature"
            );
        }
    }
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
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
}

async fn register_batch(
    State(runtime): State<Runtime>,
    Json(req): Json<RegisterBatchRequest>,
) -> Result<Json<BatchSummary>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.register_batch(req).map(Json).map_err(store_error)
}

async fn start_case(
    State(runtime): State<Runtime>,
    Json(req): Json<ListCasesRequest>,
) -> Result<Json<Option<CaseRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.start_case(req).map(Json).map_err(store_error)
}

async fn list_cases(
    State(runtime): State<Runtime>,
    Json(req): Json<ListCasesRequest>,
) -> Result<Json<Vec<CaseRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.list_cases(req).map(Json).map_err(store_error)
}

async fn complete_case(
    State(runtime): State<Runtime>,
    Json(req): Json<CompleteCaseRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .complete_case(req)
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
}

async fn fail_case(
    State(runtime): State<Runtime>,
    Json(req): Json<FailCaseRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .fail_case(req)
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

async fn register_worker(
    State(runtime): State<Runtime>,
    Json(req): Json<RegisterWorkerRequest>,
) -> Result<Json<WorkerRecord>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.register_worker(req).map(Json).map_err(store_error)
}

async fn list_workers(
    State(runtime): State<Runtime>,
) -> Result<Json<Vec<WorkerRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.list_workers().map(Json).map_err(store_error)
}

async fn add_trace_event(
    State(runtime): State<Runtime>,
    Json(req): Json<TraceEventRequest>,
) -> Result<Json<TraceEventRecord>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.add_trace_event(req).map(Json).map_err(store_error)
}

async fn list_trace_events(
    State(runtime): State<Runtime>,
    Path((run_id, batch_name, case_id)): Path<(String, String, String)>,
) -> Result<Json<Vec<TraceEventRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .list_trace_events(&run_id, &batch_name, &case_id)
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

async fn mark_reviewed(
    State(runtime): State<Runtime>,
    Json(req): Json<ReviewRequest>,
) -> Result<Json<ReviewRecord>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.mark_reviewed(req).map(Json).map_err(store_error)
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
            failure_class: FailureClass::RunnerError,
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
                failure_class: FailureClass::RunnerError,
                stack: None,
                retryable: Some(false),
            }),
        ),
        StoreError::CaseNotFound => (
            axum::http::StatusCode::NOT_FOUND,
            Json(ErrorInfo {
                error_type: "CaseNotFound".to_string(),
                message: error.to_string(),
                failure_class: FailureClass::RunnerError,
                stack: None,
                retryable: Some(false),
            }),
        ),
        error => internal_server_error(error),
    }
}
