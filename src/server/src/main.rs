use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use durable_evals_core::{Error as StoreError, *};

const DEFAULT_DB: &str = ".durable/evals.sqlite";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("run") => run_cmd(args.collect()).await,
        Some("serve") | None => serve_cmd().await,
        Some(other) => {
            eprintln!(
                "unknown subcommand `{other}`\n\
                 usage:\n  \
                 durable-eval run [--fresh] [--db PATH] <harness> [args...]\n  \
                 durable-eval serve"
            );
            std::process::exit(2);
        }
    }
}

/// Run the HTTP runtime in the foreground, printing its bound address on the first
/// stdout line so a parent process can connect.
async fn serve_cmd() -> anyhow::Result<()> {
    let db = env::var("DURABLE_EVALS_DB").unwrap_or_else(|_| DEFAULT_DB.into());
    let runtime = build_runtime(&db)?;
    // Optional shared-secret auth. Unset (the default) leaves the API open, which is fine
    // for the loopback-bound local runtime but should be set whenever DURABLE_EVALS_ADDR
    // exposes the server beyond localhost.
    let token = env::var("DURABLE_EVALS_TOKEN")
        .ok()
        .filter(|value| !value.is_empty())
        .map(Arc::new);
    let addr: SocketAddr = env::var("DURABLE_EVALS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:0".into())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("{}", listener.local_addr()?);
    axum::serve(listener, router(runtime, token)).await?;
    Ok(())
}

/// Launch a user eval harness with a managed runtime: start the server on a loopback
/// port, point the harness's client at it via DURABLE_EVALS_RUNTIME_URL, and exit with
/// the harness's status. Re-running resumes from the same database.
async fn run_cmd(args: Vec<String>) -> anyhow::Result<()> {
    let mut fresh = false;
    let mut db: Option<String> = None;
    let mut harness: Option<String> = None;
    let mut passthrough: Vec<String> = Vec::new();
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        if harness.is_some() {
            passthrough.push(arg);
        } else {
            match arg.as_str() {
                "--fresh" => fresh = true,
                "--db" => db = it.next(),
                _ => harness = Some(arg),
            }
        }
    }
    let harness = harness.ok_or_else(|| {
        anyhow::anyhow!("usage: durable-eval run [--fresh] [--db PATH] <harness> [args...]")
    })?;
    let db = db.unwrap_or_else(|| DEFAULT_DB.into());
    if fresh {
        let _ = std::fs::remove_file(&db);
    }

    let runtime = build_runtime(&db)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    // The harness is a child process; the runtime lives for that child's lifetime.
    tokio::spawn(async move {
        let _ = axum::serve(listener, router(runtime, None)).await;
    });

    let (program, mut cmd_args) = interpreter_for(&harness)?;
    cmd_args.push(harness);
    cmd_args.extend(passthrough);
    let status = tokio::process::Command::new(program)
        .args(&cmd_args)
        .env("DURABLE_EVALS_RUNTIME_URL", format!("http://{addr}"))
        .env("DURABLE_EVALS_DB", &db)
        .status()
        .await?;
    std::process::exit(status.code().unwrap_or(1));
}

/// Pick the interpreter for a harness by file extension. Python and JS run directly;
/// TypeScript is run through the `tsx` loader, which must be installed.
fn interpreter_for(harness: &str) -> anyhow::Result<(String, Vec<String>)> {
    let ext = std::path::Path::new(harness)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    match ext {
        "py" => Ok((
            env::var("DURABLE_EVALS_PYTHON").unwrap_or_else(|_| "python".into()),
            vec![],
        )),
        "js" | "mjs" | "cjs" => Ok(("node".into(), vec![])),
        "ts" | "mts" => Ok(("node".into(), vec!["--import".into(), "tsx".into()])),
        _ => Err(anyhow::anyhow!(
            "don't know how to run `{harness}` (expected a .py, .js, or .ts harness)"
        )),
    }
}

fn router(runtime: Runtime, token: Option<Arc<String>>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/runs/register", post(register_run))
        .route("/runs/summary", post(summary))
        .route("/runs/export", post(export_run))
        .route("/tasks/begin", post(begin))
        .route("/tasks/complete", post(complete))
        .route("/tasks/fail", post(fail))
        .route("/tasks/list", post(list))
        .route("/tasks/heartbeat", post(heartbeat))
        .route("/datasets/register", post(register_dataset))
        .route("/variants/register", post(register_variants))
        .route("/variants/:run_id", get(list_variants))
        .route("/traces/events", post(add_trace_event))
        .route("/traces/:run_id/:kind/:task_id", get(list_trace_events))
        .route("/memos/get", post(memo_get))
        .route("/memos/put", post(memo_put))
        .layer(middleware::from_fn_with_state(token, require_token))
        .with_state(runtime)
}

fn build_runtime(db: &str) -> anyhow::Result<Runtime> {
    if let Some(parent) = std::path::Path::new(db).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    Ok(Runtime::new(SqliteStore::open(db)?))
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

async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

async fn begin(
    State(runtime): State<Runtime>,
    Json(req): Json<BeginRequest>,
) -> Result<Json<Outcome>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.begin(req).map(Json).map_err(store_error)
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

async fn complete(
    State(runtime): State<Runtime>,
    Json(req): Json<CompleteRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .complete(req)
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
}

async fn fail(
    State(runtime): State<Runtime>,
    Json(req): Json<FailRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .fail(req)
        .map(|_| Json(Health { ok: true }))
        .map_err(store_error)
}

async fn heartbeat(
    State(runtime): State<Runtime>,
    Json(req): Json<HeartbeatRequest>,
) -> Result<Json<Health>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .heartbeat(req)
        .map(|retained| Json(Health { ok: retained }))
        .map_err(store_error)
}

async fn register_dataset(
    State(runtime): State<Runtime>,
    Json(req): Json<RegisterDatasetRequest>,
) -> Result<Json<DatasetSummary>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.register_dataset(req).map(Json).map_err(store_error)
}

async fn list(
    State(runtime): State<Runtime>,
    Json(req): Json<ListRequest>,
) -> Result<Json<Vec<TaskRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime.list(req).map(Json).map_err(store_error)
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
    Path((run_id, kind, task_id)): Path<(String, String, String)>,
) -> Result<Json<Vec<TraceEventRecord>>, (axum::http::StatusCode, Json<ErrorInfo>)> {
    runtime
        .list_trace_events(&run_id, &kind, &task_id)
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

fn store_error(error: StoreError) -> (axum::http::StatusCode, Json<ErrorInfo>) {
    let (status, error_type) = match error {
        StoreError::TaskNotFound => (axum::http::StatusCode::NOT_FOUND, "TaskNotFound"),
        _ => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
        ),
    };
    (
        status,
        Json(ErrorInfo {
            error_type: error_type.to_string(),
            message: error.to_string(),
            failure_class: FailureClass::DurableHarnessError,
            stack: None,
            retryable: Some(false),
        }),
    )
}
