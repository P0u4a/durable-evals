use crate::types::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("step state not found")]
    StepNotFound,
    #[error("task not found")]
    TaskNotFound,
}

/// Backend-agnostic durable store, implemented by [`crate::SqliteStore`]. Kept
/// synchronous to match the existing runtime; the backend serializes its own access.
pub trait Store: Send + Sync + 'static {
    fn register_run(&self, req: RegisterRunRequest) -> Result<()>;
    fn begin_step(&self, req: BeginStepRequest) -> Result<StepOutcome>;
    fn complete_step(&self, req: CompleteStepRequest) -> Result<()>;
    fn fail_step(&self, req: FailStepRequest) -> Result<()>;
    fn register_dataset(&self, req: RegisterDatasetRequest) -> Result<DatasetSummary>;
    fn complete_task(&self, req: CompleteTaskRequest) -> Result<()>;
    fn fail_task(&self, req: FailTaskRequest) -> Result<()>;
    fn list_tasks(&self, req: ListTasksRequest) -> Result<Vec<TaskRecord>>;
    fn register_variants(&self, req: RegisterVariantsRequest) -> Result<Vec<VariantRecord>>;
    fn list_variants(&self, run_id: &str) -> Result<Vec<VariantRecord>>;
    /// Returns `true` if the heartbeat refreshed a lease still owned by the worker,
    /// `false` if the lease was lost or the step no longer exists.
    fn heartbeat_step(&self, req: HeartbeatStepRequest) -> Result<bool>;
    fn add_trace_event(&self, req: TraceEventRequest) -> Result<TraceEventRecord>;
    fn list_trace_events(
        &self,
        run_id: &str,
        dataset_name: &str,
        task_id: &str,
    ) -> Result<Vec<TraceEventRecord>>;
    fn memo_get(&self, req: MemoGetRequest) -> Result<MemoGetResponse>;
    fn memo_put(&self, req: MemoPutRequest) -> Result<()>;
    fn summary(&self, req: SummaryRequest) -> Result<RunSummary>;
    fn export(&self, req: ExportRequest) -> Result<ExportResponse>;
}
