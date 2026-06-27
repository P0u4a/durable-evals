use crate::types::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("task not found")]
    TaskNotFound,
    #[error("task claim no longer active")]
    TaskClaimLost,
}

/// Backend-agnostic durable store, implemented by [`crate::SqliteStore`]. Every unit of
/// work is a task keyed by `(run_id, kind, input_digest)`; a single begin/complete/fail
/// path serves both steps (a group of one) and dataset tasks (a pre-registered set).
/// Kept synchronous to match the runtime; the backend serializes its own access.
pub trait Store: Send + Sync + 'static {
    fn register_run(&self, req: RegisterRunRequest) -> Result<()>;
    fn register_dataset(&self, req: RegisterDatasetRequest) -> Result<DatasetSummary>;
    fn begin(&self, req: BeginRequest) -> Result<Outcome>;
    fn complete(&self, req: CompleteRequest) -> Result<()>;
    fn fail(&self, req: FailRequest) -> Result<()>;
    fn list(&self, req: ListRequest) -> Result<Vec<TaskRecord>>;
    /// Returns `true` if the heartbeat refreshed a lease still owned by the worker,
    /// `false` if the lease was lost or the task no longer exists.
    fn heartbeat(&self, req: HeartbeatRequest) -> Result<bool>;
    fn add_trace_event(&self, req: TraceEventRequest) -> Result<TraceEventRecord>;
    fn list_trace_events(&self, req: ListTraceEventsRequest) -> Result<Vec<TraceEventRecord>>;
    fn memo_get(&self, req: MemoGetRequest) -> Result<MemoGetResponse>;
    fn memo_put(&self, req: MemoPutRequest) -> Result<()>;
    fn summary(&self, req: SummaryRequest) -> Result<RunSummary>;
    fn export(&self, req: ExportRequest) -> Result<ExportResponse>;
}
