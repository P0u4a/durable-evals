use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::types::{
    BeginStepRequest, CompleteStepRequest, ErrorInfo, FailStepRequest, StepOutcome,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("step state not found")]
    StepNotFound,
}

#[derive(Clone)]
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn begin_step(&self, req: BeginStepRequest) -> Result<StepOutcome> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let existing = conn
            .query_row(
                "SELECT status, attempt, output_json, error_json
                 FROM step_states
                 WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
                params![req.run_id, req.step_name, req.input_digest],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u32>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;

        match existing {
            Some((status, _, output_json, _)) if status == "succeeded" => {
                let output = output_json
                    .map(|json| serde_json::from_str(&json))
                    .transpose()?
                    .unwrap_or(Value::Null);
                Ok(StepOutcome::SkipCompleted { output })
            }
            Some((status, _, _, error_json)) if status == "failed_terminal" => {
                let error = error_json
                    .map(|json| serde_json::from_str(&json))
                    .transpose()?
                    .unwrap_or(ErrorInfo {
                        error_type: "StepFailed".to_string(),
                        message: "step failed".to_string(),
                    });
                Ok(StepOutcome::FailedTerminal { error })
            }
            Some((status, _, _, _)) if status == "running" => Ok(StepOutcome::InProgress),
            Some((_, attempt, _, _)) => {
                let next_attempt = attempt + 1;
                conn.execute(
                    "UPDATE step_states
                     SET status = 'running', attempt = ?4, updated_at = datetime('now')
                     WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
                    params![req.run_id, req.step_name, req.input_digest, next_attempt],
                )?;
                Ok(StepOutcome::Execute {
                    attempt: next_attempt,
                })
            }
            None => {
                conn.execute(
                    "INSERT INTO step_states
                     (run_id, step_name, input_digest, status, attempt, updated_at)
                     VALUES (?1, ?2, ?3, 'running', 1, datetime('now'))",
                    params![req.run_id, req.step_name, req.input_digest],
                )?;
                Ok(StepOutcome::Execute { attempt: 1 })
            }
        }
    }

    pub fn complete_step(&self, req: CompleteStepRequest) -> Result<()> {
        let output_json = serde_json::to_string(&req.output)?;
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let updated = conn.execute(
            "UPDATE step_states
             SET status = 'succeeded', output_json = ?4, error_json = NULL, updated_at = datetime('now')
             WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
            params![req.run_id, req.step_name, req.input_digest, output_json],
        )?;
        if updated == 0 {
            return Err(Error::StepNotFound);
        }
        Ok(())
    }

    pub fn fail_step(&self, req: FailStepRequest) -> Result<()> {
        let error_json = serde_json::to_string(&req.error)?;
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let updated = conn.execute(
            "UPDATE step_states
             SET status = 'failed', error_json = ?4, updated_at = datetime('now')
             WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
            params![req.run_id, req.step_name, req.input_digest, error_json],
        )?;
        if updated == 0 {
            return Err(Error::StepNotFound);
        }
        Ok(())
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS step_states (
                run_id TEXT NOT NULL,
                step_name TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                output_json TEXT,
                error_json TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, step_name, input_digest)
            );",
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BeginStepRequest, CompleteStepRequest, FailStepRequest};
    use serde_json::json;

    fn store() -> SqliteStore {
        SqliteStore::open(":memory:").expect("open sqlite store")
    }

    fn begin_req() -> BeginStepRequest {
        BeginStepRequest {
            run_id: "run".to_string(),
            step_name: "step".to_string(),
            input_digest: "digest".to_string(),
        }
    }

    fn complete_req() -> CompleteStepRequest {
        CompleteStepRequest {
            run_id: "run".to_string(),
            step_name: "step".to_string(),
            input_digest: "digest".to_string(),
            output: json!({"ok": true}),
        }
    }

    fn fail_req() -> FailStepRequest {
        FailStepRequest {
            run_id: "run".to_string(),
            step_name: "step".to_string(),
            input_digest: "digest".to_string(),
            error: ErrorInfo {
                error_type: "RuntimeError".to_string(),
                message: "temporary failure".to_string(),
            },
        }
    }

    #[test]
    fn running_step_reports_in_progress() {
        let store = store();

        let first = store.begin_step(begin_req()).expect("begin first step");
        assert!(matches!(first, StepOutcome::Execute { attempt: 1 }));

        let second = store.begin_step(begin_req()).expect("begin running step");
        assert!(matches!(second, StepOutcome::InProgress));
    }

    #[test]
    fn failed_step_retries_on_next_begin() {
        let store = store();

        store.begin_step(begin_req()).expect("begin step");
        store.fail_step(fail_req()).expect("mark step failed");

        let retry = store.begin_step(begin_req()).expect("begin failed step");
        assert!(matches!(retry, StepOutcome::Execute { attempt: 2 }));
    }

    #[test]
    fn completed_step_skips_with_output() {
        let store = store();

        store.begin_step(begin_req()).expect("begin step");
        store.complete_step(complete_req()).expect("complete step");

        let outcome = store.begin_step(begin_req()).expect("begin completed step");
        assert!(matches!(
            outcome,
            StepOutcome::SkipCompleted { output } if output == json!({"ok": true})
        ));
    }

    #[test]
    fn completing_missing_step_errors() {
        let store = store();

        let error = store
            .complete_step(complete_req())
            .expect_err("missing completion should fail");
        assert!(matches!(error, Error::StepNotFound));
    }

    #[test]
    fn failing_missing_step_errors() {
        let store = store();

        let error = store
            .fail_step(fail_req())
            .expect_err("missing failure should fail");
        assert!(matches!(error, Error::StepNotFound));
    }
}
