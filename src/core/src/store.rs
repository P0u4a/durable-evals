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
    #[error("case not found")]
    CaseNotFound,
    #[cfg(feature = "postgres")]
    #[error("postgres error: {0}")]
    Postgres(String),
}

/// Backend-agnostic durable store. Implemented by [`crate::SqliteStore`] and,
/// behind the `postgres` feature, `crate::PostgresStore`. Kept synchronous to
/// match the existing runtime; backends serialize their own access.
pub trait Store: Send + Sync + 'static {
    fn register_run(&self, req: RegisterRunRequest) -> Result<()>;
    fn begin_step(&self, req: BeginStepRequest) -> Result<StepOutcome>;
    fn complete_step(&self, req: CompleteStepRequest) -> Result<()>;
    fn fail_step(&self, req: FailStepRequest) -> Result<()>;
    fn register_batch(&self, req: RegisterBatchRequest) -> Result<BatchSummary>;
    fn complete_case(&self, req: CompleteCaseRequest) -> Result<()>;
    fn fail_case(&self, req: FailCaseRequest) -> Result<()>;
    fn list_cases(&self, req: ListCasesRequest) -> Result<Vec<CaseRecord>>;
    fn register_variants(&self, req: RegisterVariantsRequest) -> Result<Vec<VariantRecord>>;
    fn list_variants(&self, run_id: &str) -> Result<Vec<VariantRecord>>;
    fn register_worker(&self, req: RegisterWorkerRequest) -> Result<WorkerRecord>;
    fn list_workers(&self) -> Result<Vec<WorkerRecord>>;
    /// Returns `true` if the heartbeat refreshed a lease still owned by the worker,
    /// `false` if the lease was lost or the step no longer exists.
    fn heartbeat_step(&self, req: HeartbeatStepRequest) -> Result<bool>;
    fn add_trace_event(&self, req: TraceEventRequest) -> Result<TraceEventRecord>;
    fn list_trace_events(
        &self,
        run_id: &str,
        batch_name: &str,
        case_id: &str,
    ) -> Result<Vec<TraceEventRecord>>;
    fn mark_reviewed(&self, req: ReviewRequest) -> Result<ReviewRecord>;
    fn memo_get(&self, req: MemoGetRequest) -> Result<MemoGetResponse>;
    fn memo_put(&self, req: MemoPutRequest) -> Result<()>;
    fn summary(&self, req: SummaryRequest) -> Result<RunSummary>;
    fn export(&self, req: ExportRequest) -> Result<ExportResponse>;
}
