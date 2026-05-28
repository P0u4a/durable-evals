use crate::sqlite::SqliteStore;
use crate::types::{BeginStepRequest, CompleteStepRequest, FailStepRequest, StepOutcome};

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
}
