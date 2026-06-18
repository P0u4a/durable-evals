use std::sync::Arc;

use crate::store::{Result, Store};
use crate::types::*;

#[derive(Clone)]
pub struct Runtime {
    store: Arc<dyn Store>,
}

impl Runtime {
    pub fn new(store: impl Store) -> Self {
        Self {
            store: Arc::new(store),
        }
    }

    pub fn register_run(&self, req: RegisterRunRequest) -> Result<()> {
        self.store.register_run(req)
    }

    pub fn register_dataset(&self, req: RegisterDatasetRequest) -> Result<DatasetSummary> {
        self.store.register_dataset(req)
    }

    pub fn begin(&self, req: BeginRequest) -> Result<Outcome> {
        self.store.begin(req)
    }

    pub fn complete(&self, req: CompleteRequest) -> Result<()> {
        self.store.complete(req)
    }

    pub fn fail(&self, req: FailRequest) -> Result<()> {
        self.store.fail(req)
    }

    pub fn list(&self, req: ListRequest) -> Result<Vec<TaskRecord>> {
        self.store.list(req)
    }

    pub fn register_variants(
        &self,
        req: RegisterVariantsRequest,
    ) -> Result<Vec<VariantRecord>> {
        self.store.register_variants(req)
    }

    pub fn list_variants(&self, run_id: &str) -> Result<Vec<VariantRecord>> {
        self.store.list_variants(run_id)
    }

    pub fn heartbeat(&self, req: HeartbeatRequest) -> Result<bool> {
        self.store.heartbeat(req)
    }

    pub fn add_trace_event(&self, req: TraceEventRequest) -> Result<TraceEventRecord> {
        self.store.add_trace_event(req)
    }

    pub fn list_trace_events(
        &self,
        run_id: &str,
        kind: &str,
        task_id: &str,
    ) -> Result<Vec<TraceEventRecord>> {
        self.store.list_trace_events(run_id, kind, task_id)
    }

    pub fn memo_get(&self, req: MemoGetRequest) -> Result<MemoGetResponse> {
        self.store.memo_get(req)
    }

    pub fn memo_put(&self, req: MemoPutRequest) -> Result<()> {
        self.store.memo_put(req)
    }

    pub fn summary(&self, req: SummaryRequest) -> Result<RunSummary> {
        self.store.summary(req)
    }

    pub fn export(&self, req: ExportRequest) -> Result<ExportResponse> {
        self.store.export(req)
    }
}
