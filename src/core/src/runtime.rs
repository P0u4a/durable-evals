use crate::sqlite::SqliteStore;
use crate::types::*;

#[derive(Clone)]
pub struct Runtime {
    store: SqliteStore,
}

impl Runtime {
    pub fn new(store: SqliteStore) -> Self {
        Self { store }
    }

    pub fn begin_step(&self, req: BeginStepRequest) -> crate::sqlite::Result<StepOutcome> {
        self.store.begin_step(req)
    }

    pub fn complete_step(&self, req: CompleteStepRequest) -> crate::sqlite::Result<()> {
        self.store.complete_step(req)
    }

    pub fn fail_step(&self, req: FailStepRequest) -> crate::sqlite::Result<()> {
        self.store.fail_step(req)
    }

    pub fn register_run(&self, req: RegisterRunRequest) -> crate::sqlite::Result<()> {
        self.store.register_run(req)
    }

    pub fn register_batch(&self, req: RegisterBatchRequest) -> crate::sqlite::Result<BatchSummary> {
        self.store.register_batch(req)
    }

    pub fn start_case(&self, req: ListCasesRequest) -> crate::sqlite::Result<Option<CaseRecord>> {
        self.store.start_case(req)
    }

    pub fn complete_case(&self, req: CompleteCaseRequest) -> crate::sqlite::Result<()> {
        self.store.complete_case(req)
    }

    pub fn fail_case(&self, req: FailCaseRequest) -> crate::sqlite::Result<()> {
        self.store.fail_case(req)
    }

    pub fn list_cases(&self, req: ListCasesRequest) -> crate::sqlite::Result<Vec<CaseRecord>> {
        self.store.list_cases(req)
    }

    pub fn register_variants(
        &self,
        req: RegisterVariantsRequest,
    ) -> crate::sqlite::Result<Vec<VariantRecord>> {
        self.store.register_variants(req)
    }

    pub fn list_variants(&self, run_id: &str) -> crate::sqlite::Result<Vec<VariantRecord>> {
        self.store.list_variants(run_id)
    }

    pub fn register_worker(
        &self,
        req: RegisterWorkerRequest,
    ) -> crate::sqlite::Result<WorkerRecord> {
        self.store.register_worker(req)
    }

    pub fn list_workers(&self) -> crate::sqlite::Result<Vec<WorkerRecord>> {
        self.store.list_workers()
    }

    pub fn heartbeat_step(&self, req: HeartbeatStepRequest) -> crate::sqlite::Result<()> {
        self.store.heartbeat_step(req)
    }

    pub fn add_trace_event(
        &self,
        req: TraceEventRequest,
    ) -> crate::sqlite::Result<TraceEventRecord> {
        self.store.add_trace_event(req)
    }

    pub fn list_trace_events(
        &self,
        run_id: &str,
        batch_name: &str,
        case_id: &str,
    ) -> crate::sqlite::Result<Vec<TraceEventRecord>> {
        self.store.list_trace_events(run_id, batch_name, case_id)
    }

    pub fn mark_reviewed(&self, req: ReviewRequest) -> crate::sqlite::Result<ReviewRecord> {
        self.store.mark_reviewed(req)
    }

    pub fn summary(&self, req: SummaryRequest) -> crate::sqlite::Result<RunSummary> {
        self.store.summary(req)
    }

    pub fn export(&self, req: ExportRequest) -> crate::sqlite::Result<ExportResponse> {
        self.store.export(req)
    }
}
