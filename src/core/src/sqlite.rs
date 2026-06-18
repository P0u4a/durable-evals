use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::store::{Error, Result, Store};
use crate::types::*;

/// Tasks are content-addressed: edits to an input create a new row under a new digest.
/// Rows from earlier registrations are kept (so reverting an input reuses its old
/// output) but only the latest registration's generation counts as the dataset.
const CURRENT_GENERATION: &str = "tasks.generation = COALESCE(
    (SELECT d.generation FROM datasets d
     WHERE d.run_id = tasks.run_id AND d.dataset_name = tasks.dataset_name),
    tasks.generation)";

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

    pub fn register_run(&self, req: RegisterRunRequest) -> Result<()> {
        let config_json = serde_json::to_string(&req.config)?;
        let digest = digest_json(&req.config)?;
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        conn.execute(
            "INSERT INTO runs (run_id, name, config_json, config_digest, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, datetime('now'), datetime('now'))
             ON CONFLICT(run_id) DO UPDATE SET
                name = excluded.name,
                config_json = excluded.config_json,
                config_digest = excluded.config_digest,
                updated_at = datetime('now')",
            params![req.run_id, req.name, config_json, digest],
        )?;
        Ok(())
    }

    pub fn begin_step(&self, req: BeginStepRequest) -> Result<StepOutcome> {
        let config_json = serde_json::to_string(&req.config)?;
        let dependencies_json = serde_json::to_string(&req.dependencies)?;
        let variants_json = serde_json::to_string(&req.variants)?;
        let retry_json = serde_json::to_string(&req.retry)?;
        let worker_id = req.worker_id.clone();
        let lease_seconds = req.lease_seconds.unwrap_or(300);
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        // The process-wide mutex serializes access for the single-process (default)
        // deployment; the transaction keeps the read-modify-write atomic against the
        // connection so a panic can't leave a half-applied state.
        let tx = conn.unchecked_transaction()?;
        let existing = tx
            .query_row(
                "SELECT status, attempt, output_json, error_json, lease_expires_at, retry_at
                 FROM step_states
                 WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
                params![req.run_id, req.step_name, req.input_digest],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u32>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?;

        match existing {
            Some((status, _, output_json, _, _, _)) if status == "succeeded" => {
                Ok(StepOutcome::SkipCompleted {
                    output: parse_optional(output_json)?.unwrap_or(Value::Null),
                })
            }
            Some((status, _, _, error_json, _, _)) if status == "terminal" => {
                Ok(StepOutcome::FailedTerminal {
                    error: parse_optional(error_json)?.unwrap_or(ErrorInfo {
                        error_type: "StepFailed".to_string(),
                        message: "step failed".to_string(),
                        failure_class: FailureClass::DurableHarnessError,
                        stack: None,
                        retryable: Some(false),
                    }),
                })
            }
            Some((status, _, _, _, lease_expires_at, _))
                if status == "running" && lease_active(lease_expires_at.as_deref()) =>
            {
                Ok(StepOutcome::InProgress)
            }
            Some((_, attempt, _, _, _, _)) if attempt >= req.retry.max_attempts => {
                tx.execute(
                    "UPDATE step_states
                     SET status = 'terminal', updated_at = datetime('now')
                     WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
                    params![req.run_id, req.step_name, req.input_digest],
                )?;
                tx.commit()?;
                Ok(StepOutcome::FailedTerminal {
                    error: ErrorInfo {
                        error_type: "MaxAttemptsExceeded".to_string(),
                        message: "step exceeded retry policy max_attempts".to_string(),
                        failure_class: FailureClass::DurableHarnessError,
                        stack: None,
                        retryable: Some(false),
                    },
                })
            }
            // A failed step whose backoff window has not elapsed yet must wait.
            Some((status, _, _, _, _, Some(retry_at)))
                if status == "failed" && retry_pending(&retry_at) =>
            {
                Ok(StepOutcome::RetryLater { retry_at })
            }
            Some((_, attempt, _, _, _, _)) => {
                let next_attempt = attempt + 1;
                tx.execute(
                    "UPDATE step_states
                     SET status = 'running',
                         attempt = ?4,
                         config_json = ?5,
                         dependencies_json = ?6,
                         variants_json = ?7,
                         retry_json = ?8,
                         lease_owner = ?9,
                         lease_acquired_at = datetime('now'),
                         lease_expires_at = datetime('now', '+' || ?10 || ' seconds'),
                         heartbeat_at = datetime('now'),
                         retry_at = NULL,
                         started_at = COALESCE(started_at, datetime('now')),
                         updated_at = datetime('now')
                     WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
                    params![
                        req.run_id,
                        req.step_name,
                        req.input_digest,
                        next_attempt,
                        config_json,
                        dependencies_json,
                        variants_json,
                        retry_json,
                        worker_id,
                        lease_seconds
                    ],
                )?;
                tx.commit()?;
                Ok(StepOutcome::Execute {
                    attempt: next_attempt,
                })
            }
            None => {
                tx.execute(
                    "INSERT INTO step_states
                     (run_id, step_name, input_digest, status, attempt, config_json,
                      dependencies_json, variants_json, retry_json, lease_owner,
                      lease_acquired_at, lease_expires_at, heartbeat_at, started_at, updated_at)
                     VALUES (?1, ?2, ?3, 'running', 1, ?4, ?5, ?6, ?7, ?8,
                             datetime('now'), datetime('now', '+' || ?9 || ' seconds'),
                             datetime('now'), datetime('now'), datetime('now'))",
                    params![
                        req.run_id,
                        req.step_name,
                        req.input_digest,
                        config_json,
                        dependencies_json,
                        variants_json,
                        retry_json,
                        worker_id,
                        lease_seconds
                    ],
                )?;
                tx.commit()?;
                Ok(StepOutcome::Execute { attempt: 1 })
            }
        }
    }

    pub fn complete_step(&self, req: CompleteStepRequest) -> Result<()> {
        let output_json = serde_json::to_string(&req.output)?;
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let tx = conn.unchecked_transaction()?;
        let updated = tx.execute(
            "UPDATE step_states
             SET status = 'succeeded', output_json = ?4, error_json = NULL,
                 lease_owner = NULL, lease_expires_at = NULL, completed_at = datetime('now'),
                 updated_at = datetime('now')
             WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
            params![req.run_id, req.step_name, req.input_digest, output_json],
        )?;
        if updated == 0 {
            return Err(Error::StepNotFound);
        }
        for artifact in req.artifacts {
            tx.execute(
                "INSERT OR REPLACE INTO artifacts
                 (artifact_id, run_id, step_name, input_digest, name, kind, path,
                  inline_json, sha256, size, valid, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, datetime('now'))",
                params![
                    artifact.artifact_id,
                    req.run_id,
                    req.step_name,
                    req.input_digest,
                    artifact.name,
                    artifact.kind,
                    artifact.path,
                    optional_json(artifact.inline_json)?,
                    artifact.sha256,
                    artifact.size,
                    artifact.valid
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn fail_step(&self, req: FailStepRequest) -> Result<()> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let (attempt, retry_json) = conn
            .query_row(
                "SELECT attempt, retry_json FROM step_states
                 WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
                params![req.run_id, req.step_name, req.input_digest],
                |row| Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or(Error::StepNotFound)?;
        let retry_policy: RetryPolicy = serde_json::from_str(&retry_json).unwrap_or_default();

        let retryable = req.error.retryable.unwrap_or_else(|| {
            retry_policy.retryable.contains(&req.error.failure_class)
                && !retry_policy.terminal.contains(&req.error.failure_class)
        });
        let status = if retryable { "failed" } else { "terminal" };
        let error_json = serde_json::to_string(&req.error)?;
        // Schedule the next eligible retry per the backoff policy; 0 means retry immediately.
        let delay_secs = retryable
            .then(|| {
                let delay_ms = retry_policy.backoff_delay_ms(attempt, jitter_seed());
                delay_ms.div_ceil(1000)
            })
            .filter(|secs| *secs > 0);
        conn.execute(
            "UPDATE step_states
             SET status = ?4, error_json = ?5, lease_owner = NULL, lease_expires_at = NULL,
                 retry_at = CASE WHEN ?6 IS NULL THEN NULL
                                 ELSE datetime('now', '+' || ?6 || ' seconds') END,
                 completed_at = datetime('now'), updated_at = datetime('now')
             WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
            params![
                req.run_id,
                req.step_name,
                req.input_digest,
                status,
                error_json,
                delay_secs
            ],
        )?;
        let failure_class = failure_class_to_str(&req.error.failure_class);
        conn.execute(
            "INSERT INTO failure_records
             (run_id, step_name, input_digest, attempt, failure_class, error_type,
              message, stack, retryable, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, datetime('now'))",
            params![
                req.run_id,
                req.step_name,
                req.input_digest,
                attempt,
                failure_class,
                req.error.error_type,
                req.error.message,
                req.error.stack,
                retryable
            ],
        )?;
        Ok(())
    }

    pub fn register_dataset(&self, req: RegisterDatasetRequest) -> Result<DatasetSummary> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO datasets (run_id, dataset_name, generation)
             VALUES (?1, ?2, 1)
             ON CONFLICT(run_id, dataset_name) DO UPDATE SET generation = generation + 1",
            params![req.run_id, req.dataset_name],
        )?;
        let generation: u32 = tx.query_row(
            "SELECT generation FROM datasets WHERE run_id = ?1 AND dataset_name = ?2",
            params![req.run_id, req.dataset_name],
            |row| row.get(0),
        )?;
        // Identical inputs collapse to one content-addressed task; first occurrence wins.
        let mut seen = std::collections::BTreeSet::new();
        for (idx, task) in req.tasks.iter().enumerate() {
            if !seen.insert(task.input_digest.clone()) {
                continue;
            }
            tx.execute(
                "INSERT INTO tasks
                 (run_id, dataset_name, input_digest, label, category, position, generation,
                  input_json, status, attempt, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'pending', 0, datetime('now'), datetime('now'))
                 ON CONFLICT(run_id, dataset_name, input_digest) DO UPDATE SET
                    label = excluded.label,
                    category = excluded.category,
                    position = excluded.position,
                    generation = excluded.generation,
                    updated_at = datetime('now')",
                params![
                    req.run_id,
                    req.dataset_name,
                    task.input_digest,
                    task.label,
                    task.category,
                    idx as u32,
                    generation,
                    serde_json::to_string(&task.input)?
                ],
            )?;
        }
        tx.commit()?;
        drop(conn); // release before dataset_summary re-locks the connection
        self.dataset_summary(&req.run_id, &req.dataset_name)
    }

    pub fn complete_task(&self, req: CompleteTaskRequest) -> Result<()> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let updated = conn.execute(
            "UPDATE tasks
             SET status = 'succeeded', output_json = ?4, error_json = NULL,
                 completed_at = datetime('now'), updated_at = datetime('now')
             WHERE run_id = ?1 AND dataset_name = ?2 AND input_digest = ?3",
            params![
                req.run_id,
                req.dataset_name,
                req.input_digest,
                serde_json::to_string(&req.output)?
            ],
        )?;
        if updated == 0 {
            return Err(Error::TaskNotFound);
        }
        Ok(())
    }

    pub fn fail_task(&self, req: FailTaskRequest) -> Result<()> {
        let retryable = req.error.retryable.unwrap_or(matches!(
            req.error.failure_class,
            FailureClass::Transient
                | FailureClass::ResourceUnavailable
                | FailureClass::EvalException
        ));
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let tx = conn.unchecked_transaction()?;
        // Count this failed run as a consumed attempt and stop retrying once the
        // ceiling is reached, mirroring the step retry policy.
        let attempt: u32 = tx
            .query_row(
                "SELECT attempt FROM tasks
                 WHERE run_id = ?1 AND dataset_name = ?2 AND input_digest = ?3",
                params![req.run_id, req.dataset_name, req.input_digest],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(Error::TaskNotFound)?;
        let next_attempt = attempt + 1;
        let exhausted = next_attempt >= req.max_attempts;
        let status = if retryable && !exhausted {
            "failed"
        } else {
            "terminal"
        };
        tx.execute(
            "UPDATE tasks
             SET status = ?4, attempt = ?5, error_json = ?6,
                 completed_at = datetime('now'), updated_at = datetime('now')
             WHERE run_id = ?1 AND dataset_name = ?2 AND input_digest = ?3",
            params![
                req.run_id,
                req.dataset_name,
                req.input_digest,
                status,
                next_attempt,
                serde_json::to_string(&req.error)?
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn list_tasks(&self, req: ListTasksRequest) -> Result<Vec<TaskRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn.prepare(&format!(
            "SELECT run_id, dataset_name, input_digest, label, category, status, attempt,
                    input_json, output_json, error_json, started_at, completed_at
             FROM tasks
             WHERE run_id = ?1 AND dataset_name = ?2
               AND (?3 = '[]' OR status IN (SELECT value FROM json_each(?3)))
               AND (?4 = '[]' OR category IN (SELECT value FROM json_each(?4)))
               AND {CURRENT_GENERATION}
             ORDER BY position"
        ))?;
        let statuses = serde_json::to_string(&req.statuses)?;
        let categories = serde_json::to_string(&req.categories)?;
        let rows = stmt.query_map(
            params![req.run_id, req.dataset_name, statuses, categories],
            task_from_row,
        )?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Sqlite)
    }

    pub fn dataset_summary(&self, run_id: &str, dataset_name: &str) -> Result<DatasetSummary> {
        let counts = self.counts(
            &format!(
                "SELECT status, COUNT(*) FROM tasks
                 WHERE run_id = ?1 AND dataset_name = ?2 AND {CURRENT_GENERATION}
                 GROUP BY status"
            ),
            params![run_id, dataset_name],
        )?;
        Ok(DatasetSummary {
            total: counts.values().sum(),
            pending: *counts.get("pending").unwrap_or(&0),
            running: *counts.get("running").unwrap_or(&0),
            succeeded: *counts.get("succeeded").unwrap_or(&0),
            failed: *counts.get("failed").unwrap_or(&0),
            terminal: *counts.get("terminal").unwrap_or(&0),
        })
    }

    pub fn register_variants(&self, req: RegisterVariantsRequest) -> Result<Vec<VariantRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM variants WHERE run_id = ?1 AND dimension = ?2",
            params![req.run_id, req.dimension],
        )?;
        for variant in &req.variants {
            tx.execute(
                "INSERT INTO variants (run_id, dimension, name, config_json, digest)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    req.run_id,
                    req.dimension,
                    variant.name,
                    serde_json::to_string(&variant.config)?,
                    variant.digest
                ],
            )?;
        }
        tx.commit()?;
        self.list_variants(&req.run_id)
    }

    pub fn list_variants(&self, run_id: &str) -> Result<Vec<VariantRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT run_id, dimension, name, config_json, digest
             FROM variants WHERE run_id = ?1 ORDER BY dimension, name",
        )?;
        let rows = stmt.query_map(params![run_id], |row| {
            Ok(VariantRecord {
                run_id: row.get(0)?,
                dimension: row.get(1)?,
                name: row.get(2)?,
                config: parse_required(row.get::<_, String>(3)?)?,
                digest: row.get(4)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Sqlite)
    }

    pub fn heartbeat_step(&self, req: HeartbeatStepRequest) -> Result<bool> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let updated = conn.execute(
            "UPDATE step_states
             SET heartbeat_at = datetime('now'), lease_expires_at = datetime('now', '+' || ?5 || ' seconds')
             WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3 AND lease_owner = ?4
               AND status = 'running'",
            params![
                req.run_id,
                req.step_name,
                req.input_digest,
                req.worker_id,
                req.lease_seconds
            ],
        )?;
        Ok(updated > 0)
    }

    pub fn add_trace_event(&self, req: TraceEventRequest) -> Result<TraceEventRecord> {
        let payload = serde_json::to_string(&req.payload)?;
        let artifact_ids = serde_json::to_string(&req.artifact_ids)?;
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let event_index: u32 = conn.query_row(
            "SELECT COALESCE(MAX(event_index), 0) + 1
             FROM trace_events
             WHERE run_id = ?1 AND dataset_name = ?2 AND task_id = ?3 AND attempt = ?4",
            params![req.run_id, req.dataset_name, req.task_id, req.attempt],
            |row| row.get(0),
        )?;
        conn.execute(
            "INSERT INTO trace_events
             (run_id, dataset_name, task_id, attempt, event_index, event_type,
              payload_json, artifact_ids_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, datetime('now'))",
            params![
                req.run_id,
                req.dataset_name,
                req.task_id,
                req.attempt,
                event_index,
                req.event_type,
                payload,
                artifact_ids
            ],
        )?;
        Ok(TraceEventRecord {
            id: conn.last_insert_rowid(),
            run_id: req.run_id,
            dataset_name: req.dataset_name,
            task_id: req.task_id,
            attempt: req.attempt,
            event_index,
            event_type: req.event_type,
            payload: req.payload,
            artifact_ids: req.artifact_ids,
            created_at: now(),
        })
    }

    pub fn list_trace_events(
        &self,
        run_id: &str,
        dataset_name: &str,
        task_id: &str,
    ) -> Result<Vec<TraceEventRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, run_id, dataset_name, task_id, attempt, event_index, event_type,
                    payload_json, artifact_ids_json, created_at
             FROM trace_events
             WHERE run_id = ?1 AND dataset_name = ?2 AND task_id = ?3
             ORDER BY attempt, event_index",
        )?;
        let rows = stmt.query_map(params![run_id, dataset_name, task_id], trace_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Sqlite)
    }

    pub fn memo_get(&self, req: MemoGetRequest) -> Result<MemoGetResponse> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let value: Option<String> = conn
            .query_row(
                "SELECT value_json FROM memos WHERE run_id = ?1 AND key_digest = ?2",
                params![req.run_id, req.key_digest],
                |row| row.get(0),
            )
            .optional()?;
        match value {
            Some(json) => Ok(MemoGetResponse {
                found: true,
                value: Some(parse_required(json)?),
            }),
            None => Ok(MemoGetResponse {
                found: false,
                value: None,
            }),
        }
    }

    pub fn memo_put(&self, req: MemoPutRequest) -> Result<()> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        conn.execute(
            "INSERT INTO memos (run_id, key_digest, key_json, value_json, created_at)
             VALUES (?1, ?2, ?3, ?4, datetime('now'))
             ON CONFLICT(run_id, key_digest) DO UPDATE SET
                key_json = excluded.key_json,
                value_json = excluded.value_json",
            params![
                req.run_id,
                req.key_digest,
                serde_json::to_string(&req.key)?,
                serde_json::to_string(&req.value)?
            ],
        )?;
        Ok(())
    }

    pub fn summary(&self, req: SummaryRequest) -> Result<RunSummary> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let run = conn
            .query_row(
                "SELECT name, config_json, config_digest, created_at FROM runs WHERE run_id = ?1",
                params![req.run_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        let (name, config_json, config_digest, started_at) =
            run.unwrap_or_else(|| (None, "null".to_string(), digest_bytes(b"null"), Some(now())));
        drop(conn);
        Ok(RunSummary {
            run_id: req.run_id.clone(),
            name,
            config: parse_required(config_json)?,
            config_digest,
            step_counts: self.counts(
                "SELECT status, COUNT(*) FROM step_states WHERE run_id = ?1 GROUP BY status",
                params![req.run_id.clone()],
            )?,
            task_counts: self.counts(
                &format!(
                    "SELECT status, COUNT(*) FROM tasks
                     WHERE run_id = ?1 AND {CURRENT_GENERATION}
                     GROUP BY status"
                ),
                params![req.run_id.clone()],
            )?,
            artifact_count: self.scalar_count(
                "SELECT COUNT(*) FROM artifacts WHERE run_id = ?1",
                params![req.run_id.clone()],
            )?,
            failure_counts: self.counts(
                "SELECT failure_class, COUNT(*) FROM failure_records WHERE run_id = ?1 GROUP BY failure_class",
                params![req.run_id],
            )?,
            started_at,
            completed_at: None,
        })
    }

    pub fn export(&self, req: ExportRequest) -> Result<ExportResponse> {
        match req.kind {
            ExportKind::ManifestJson => Ok(ExportResponse {
                content_type: "application/json".to_string(),
                body: serde_json::to_string_pretty(
                    &self.summary(SummaryRequest { run_id: req.run_id })?,
                )?,
            }),
            ExportKind::TaskResultsJsonl => {
                let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
                let mut stmt = conn.prepare(&format!(
                    "SELECT run_id, dataset_name, input_digest, label, category, status, attempt,
                            input_json, output_json, error_json, started_at, completed_at
                     FROM tasks WHERE run_id = ?1 AND {CURRENT_GENERATION}
                     ORDER BY dataset_name, position"
                ))?;
                let rows = stmt.query_map(params![req.run_id], task_from_row)?;
                let mut body = String::new();
                for row in rows {
                    body.push_str(&serde_json::to_string(&row?)?);
                    body.push('\n');
                }
                Ok(ExportResponse {
                    content_type: "application/x-ndjson".to_string(),
                    body,
                })
            }
            ExportKind::FailureReportJson => Ok(ExportResponse {
                content_type: "application/json".to_string(),
                body: serde_json::to_string_pretty(&self.failure_records(&req.run_id)?)?,
            }),
            ExportKind::FailureReportCsv => {
                let mut body =
                    "run_id,step_name,input_digest,attempt,failure_class,error_type,message,timestamp\n"
                        .to_string();
                for failure in self.failure_records(&req.run_id)? {
                    body.push_str(&csv_row([
                        &failure.run_id,
                        &failure.step_name,
                        &failure.input_digest,
                        &failure.attempt.to_string(),
                        &format!("{:?}", failure.failure_class),
                        &failure.error_type,
                        &failure.message,
                        &failure.timestamp,
                    ]));
                }
                Ok(ExportResponse {
                    content_type: "text/csv".to_string(),
                    body,
                })
            }
            ExportKind::AggregateMetricsJson => Ok(ExportResponse {
                content_type: "application/json".to_string(),
                body: serde_json::to_string_pretty(&json!({
                    "summary": self.summary(SummaryRequest { run_id: req.run_id })?
                }))?,
            }),
            ExportKind::AggregateMetricsCsv => {
                let summary = self.summary(SummaryRequest { run_id: req.run_id })?;
                let mut body = "metric,status,count\n".to_string();
                for (status, count) in summary.step_counts {
                    body.push_str(&format!("steps,{status},{count}\n"));
                }
                for (status, count) in summary.task_counts {
                    body.push_str(&format!("tasks,{status},{count}\n"));
                }
                Ok(ExportResponse {
                    content_type: "text/csv".to_string(),
                    body,
                })
            }
        }
    }

    fn failure_records(&self, run_id: &str) -> Result<Vec<FailureRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT run_id, step_name, input_digest, attempt, failure_class, error_type,
                    message, stack, retryable, timestamp
             FROM failure_records WHERE run_id = ?1 ORDER BY timestamp",
        )?;
        let rows = stmt.query_map(params![run_id], |row| {
            Ok(FailureRecord {
                run_id: row.get(0)?,
                step_name: row.get(1)?,
                input_digest: row.get(2)?,
                attempt: row.get(3)?,
                failure_class: parse_failure_class(row.get::<_, String>(4)?),
                error_type: row.get(5)?,
                message: row.get(6)?,
                stack: row.get(7)?,
                retryable: row.get(8)?,
                timestamp: row.get(9)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Sqlite)
    }

    fn counts<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<BTreeMap<String, u32>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(params, |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
        })?;
        let mut counts = BTreeMap::new();
        for row in rows {
            let (status, count) = row?;
            counts.insert(status, count);
        }
        Ok(counts)
    }

    fn scalar_count<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<u32> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        Ok(conn.query_row(sql, params, |row| row.get(0))?)
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                name TEXT,
                config_json TEXT NOT NULL,
                config_digest TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS step_states (
                run_id TEXT NOT NULL,
                step_name TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                config_json TEXT NOT NULL DEFAULT 'null',
                dependencies_json TEXT NOT NULL DEFAULT '[]',
                variants_json TEXT NOT NULL DEFAULT '{}',
                retry_json TEXT NOT NULL DEFAULT '{}',
                output_json TEXT,
                error_json TEXT,
                lease_owner TEXT,
                lease_acquired_at TEXT,
                lease_expires_at TEXT,
                heartbeat_at TEXT,
                retry_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, step_name, input_digest)
            );
            CREATE TABLE IF NOT EXISTS failure_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                step_name TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                failure_class TEXT NOT NULL,
                error_type TEXT NOT NULL,
                message TEXT NOT NULL,
                stack TEXT,
                retryable INTEGER NOT NULL,
                timestamp TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS artifacts (
                artifact_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                step_name TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                name TEXT NOT NULL,
                kind TEXT NOT NULL,
                path TEXT,
                inline_json TEXT,
                sha256 TEXT NOT NULL,
                size INTEGER NOT NULL,
                valid INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS datasets (
                run_id TEXT NOT NULL,
                dataset_name TEXT NOT NULL,
                generation INTEGER NOT NULL,
                PRIMARY KEY (run_id, dataset_name)
            );
            CREATE TABLE IF NOT EXISTS tasks (
                run_id TEXT NOT NULL,
                dataset_name TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                label TEXT,
                category TEXT,
                position INTEGER NOT NULL,
                generation INTEGER NOT NULL DEFAULT 1,
                input_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                output_json TEXT,
                error_json TEXT,
                started_at TEXT,
                completed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, dataset_name, input_digest)
            );
            CREATE TABLE IF NOT EXISTS memos (
                run_id TEXT NOT NULL,
                key_digest TEXT NOT NULL,
                key_json TEXT NOT NULL,
                value_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_id, key_digest)
            );
            CREATE TABLE IF NOT EXISTS variants (
                run_id TEXT NOT NULL,
                dimension TEXT NOT NULL,
                name TEXT NOT NULL,
                config_json TEXT NOT NULL,
                digest TEXT NOT NULL,
                PRIMARY KEY (run_id, dimension, name)
            );
            CREATE TABLE IF NOT EXISTS trace_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                dataset_name TEXT NOT NULL,
                task_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                event_index INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                artifact_ids_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_failure_records_run ON failure_records (run_id);
            CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts (run_id);
            CREATE INDEX IF NOT EXISTS idx_trace_events_task
                ON trace_events (run_id, dataset_name, task_id, attempt, event_index);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_trace_events_index
                ON trace_events (run_id, dataset_name, task_id, attempt, event_index);",
        )?;
        Ok(())
    }
}

impl Store for SqliteStore {
    fn register_run(&self, req: RegisterRunRequest) -> Result<()> {
        self.register_run(req)
    }
    fn begin_step(&self, req: BeginStepRequest) -> Result<StepOutcome> {
        self.begin_step(req)
    }
    fn complete_step(&self, req: CompleteStepRequest) -> Result<()> {
        self.complete_step(req)
    }
    fn fail_step(&self, req: FailStepRequest) -> Result<()> {
        self.fail_step(req)
    }
    fn register_dataset(&self, req: RegisterDatasetRequest) -> Result<DatasetSummary> {
        self.register_dataset(req)
    }
    fn complete_task(&self, req: CompleteTaskRequest) -> Result<()> {
        self.complete_task(req)
    }
    fn fail_task(&self, req: FailTaskRequest) -> Result<()> {
        self.fail_task(req)
    }
    fn list_tasks(&self, req: ListTasksRequest) -> Result<Vec<TaskRecord>> {
        self.list_tasks(req)
    }
    fn register_variants(&self, req: RegisterVariantsRequest) -> Result<Vec<VariantRecord>> {
        self.register_variants(req)
    }
    fn list_variants(&self, run_id: &str) -> Result<Vec<VariantRecord>> {
        self.list_variants(run_id)
    }
    fn heartbeat_step(&self, req: HeartbeatStepRequest) -> Result<bool> {
        self.heartbeat_step(req)
    }
    fn add_trace_event(&self, req: TraceEventRequest) -> Result<TraceEventRecord> {
        self.add_trace_event(req)
    }
    fn list_trace_events(
        &self,
        run_id: &str,
        dataset_name: &str,
        task_id: &str,
    ) -> Result<Vec<TraceEventRecord>> {
        self.list_trace_events(run_id, dataset_name, task_id)
    }
    fn memo_get(&self, req: MemoGetRequest) -> Result<MemoGetResponse> {
        self.memo_get(req)
    }
    fn memo_put(&self, req: MemoPutRequest) -> Result<()> {
        self.memo_put(req)
    }
    fn summary(&self, req: SummaryRequest) -> Result<RunSummary> {
        self.summary(req)
    }
    fn export(&self, req: ExportRequest) -> Result<ExportResponse> {
        self.export(req)
    }
}

fn task_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRecord> {
    Ok(TaskRecord {
        run_id: row.get(0)?,
        dataset_name: row.get(1)?,
        input_digest: row.get(2)?,
        label: row.get(3)?,
        category: row.get(4)?,
        status: row.get(5)?,
        attempt: row.get(6)?,
        input: parse_required(row.get::<_, String>(7)?)?,
        output: parse_optional(row.get::<_, Option<String>>(8)?)?,
        error: parse_optional(row.get::<_, Option<String>>(9)?)?,
        started_at: row.get(10)?,
        completed_at: row.get(11)?,
    })
}

fn trace_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TraceEventRecord> {
    Ok(TraceEventRecord {
        id: row.get(0)?,
        run_id: row.get(1)?,
        dataset_name: row.get(2)?,
        task_id: row.get(3)?,
        attempt: row.get(4)?,
        event_index: row.get(5)?,
        event_type: row.get(6)?,
        payload: parse_required(row.get::<_, String>(7)?)?,
        artifact_ids: parse_required(row.get::<_, String>(8)?)?,
        created_at: row.get(9)?,
    })
}

fn parse_required<T: DeserializeOwned>(json: String) -> rusqlite::Result<T> {
    serde_json::from_str(&json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
    })
}

fn parse_optional<T: DeserializeOwned>(json: Option<String>) -> rusqlite::Result<Option<T>> {
    json.map(parse_required).transpose()
}

fn optional_json(value: Option<Value>) -> Result<Option<String>> {
    value
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .map_err(Error::Json)
}

fn digest_json(value: &Value) -> Result<String> {
    Ok(digest_bytes(serde_json::to_string(value)?.as_bytes()))
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Matches SQLite's `datetime('now')` text format so values we synthesize in Rust
/// (e.g. heartbeat timestamps returned to clients) read back identically to the ones
/// the database wrote.
fn now() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Parse a `datetime('now')`-formatted timestamp and report whether it is still in
/// the future relative to now.
fn in_future(timestamp: &str) -> bool {
    let normalized = format!("{}Z", timestamp.replace(' ', "T"));
    chrono::DateTime::parse_from_rfc3339(&normalized)
        .map(|dt| dt.with_timezone(&Utc) > Utc::now())
        .unwrap_or(false)
}

fn lease_active(lease_expires_at: Option<&str>) -> bool {
    lease_expires_at.map(in_future).unwrap_or(false)
}

fn retry_pending(retry_at: &str) -> bool {
    in_future(retry_at)
}

/// A monotonic-ish seed for retry jitter without taking on a random-number dependency.
fn jitter_seed() -> u64 {
    Utc::now().timestamp_subsec_nanos() as u64
}

fn csv_row<const N: usize>(fields: [&str; N]) -> String {
    let mut line = String::with_capacity(64);
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            line.push(',');
        }
        line.push_str(&csv_field(field));
    }
    line.push('\n');
    line
}

/// Quote a CSV field per RFC 4180 when it contains a comma, quote, or newline.
fn csv_field(value: &str) -> String {
    if value.contains(|c| matches!(c, ',' | '"' | '\n' | '\r')) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn failure_class_to_str(class: &FailureClass) -> &'static str {
    match class {
        FailureClass::Transient => "transient",
        FailureClass::ResourceUnavailable => "resource_unavailable",
        FailureClass::EvalException => "eval_exception",
        FailureClass::DurableHarnessError => "durable_harness_error",
        FailureClass::ArtifactError => "artifact_error",
    }
}

fn parse_failure_class(value: String) -> FailureClass {
    match value.as_str() {
        "transient" => FailureClass::Transient,
        "resource_unavailable" => FailureClass::ResourceUnavailable,
        "durable_harness_error" => FailureClass::DurableHarnessError,
        "artifact_error" => FailureClass::ArtifactError,
        _ => FailureClass::EvalException,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn store() -> SqliteStore {
        SqliteStore::open(":memory:").expect("open sqlite store")
    }

    fn begin_req() -> BeginStepRequest {
        BeginStepRequest {
            run_id: "run".to_string(),
            step_name: "step".to_string(),
            input_digest: "digest".to_string(),
            config: Value::Null,
            dependencies: vec![],
            variants: BTreeMap::new(),
            retry: RetryPolicy::default(),
            worker_id: None,
            lease_seconds: None,
        }
    }

    #[test]
    fn running_step_reports_in_progress() {
        let store = store();
        assert!(matches!(
            store.begin_step(begin_req()).expect("begin first step"),
            StepOutcome::Execute { attempt: 1 }
        ));
        assert!(matches!(
            store.begin_step(begin_req()).expect("begin running step"),
            StepOutcome::InProgress
        ));
    }

    #[test]
    fn failed_step_retries_on_next_begin() {
        let store = store();
        store.begin_step(begin_req()).expect("begin step");
        store
            .fail_step(FailStepRequest {
                run_id: "run".to_string(),
                step_name: "step".to_string(),
                input_digest: "digest".to_string(),
                error: ErrorInfo {
                    error_type: "RuntimeError".to_string(),
                    message: "temporary failure".to_string(),
                    failure_class: FailureClass::Transient,
                    stack: None,
                    retryable: Some(true),
                },
            })
            .expect("mark step failed");
        assert!(matches!(
            store.begin_step(begin_req()).expect("retry failed step"),
            StepOutcome::Execute { attempt: 2 }
        ));
    }

    #[test]
    fn completed_step_skips_with_output() {
        let store = store();
        store.begin_step(begin_req()).expect("begin step");
        store
            .complete_step(CompleteStepRequest {
                run_id: "run".to_string(),
                step_name: "step".to_string(),
                input_digest: "digest".to_string(),
                output: json!({"ok": true}),
                artifacts: vec![],
            })
            .expect("complete step");
        assert!(matches!(
            store.begin_step(begin_req()).expect("begin completed step"),
            StepOutcome::SkipCompleted { output } if output == json!({"ok": true})
        ));
    }

    fn task(digest: &str, input: Value) -> DatasetTaskInput {
        DatasetTaskInput {
            input_digest: digest.to_string(),
            input,
            label: None,
            category: None,
        }
    }

    fn categorized(digest: &str, input: Value, category: &str) -> DatasetTaskInput {
        DatasetTaskInput {
            input_digest: digest.to_string(),
            input,
            label: None,
            category: Some(category.to_string()),
        }
    }

    fn register(store: &SqliteStore, tasks: Vec<DatasetTaskInput>) -> DatasetSummary {
        store
            .register_dataset(RegisterDatasetRequest {
                run_id: "run".to_string(),
                dataset_name: "dataset".to_string(),
                tasks,
            })
            .expect("register dataset")
    }

    fn list(store: &SqliteStore, statuses: Vec<String>, categories: Vec<String>) -> Vec<TaskRecord> {
        store
            .list_tasks(ListTasksRequest {
                run_id: "run".to_string(),
                dataset_name: "dataset".to_string(),
                statuses,
                categories,
            })
            .expect("list tasks")
    }

    #[test]
    fn identical_inputs_collapse_to_one_task() {
        let store = store();
        let summary = register(
            &store,
            vec![task("a", json!({"x": 1})), task("a", json!({"x": 1}))],
        );
        assert_eq!(summary.total, 1);
        assert_eq!(summary.pending, 1);
    }

    #[test]
    fn changed_input_invalidates_and_revert_reuses_output() {
        let store = store();
        register(&store, vec![task("a", json!({"x": 1}))]);
        store
            .complete_task(CompleteTaskRequest {
                run_id: "run".to_string(),
                dataset_name: "dataset".to_string(),
                input_digest: "a".to_string(),
                output: json!({"ok": true}),
            })
            .expect("complete task");

        // Edited input gets a fresh pending task; the stale success no longer counts.
        let summary = register(&store, vec![task("b", json!({"x": 2}))]);
        assert_eq!(summary.total, 1);
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.succeeded, 0);

        // Reverting to the original input salvages its completed output.
        let summary = register(&store, vec![task("a", json!({"x": 1}))]);
        assert_eq!(summary.total, 1);
        assert_eq!(summary.succeeded, 1);
        let tasks = list(&store, vec![], vec![]);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].output, Some(json!({"ok": true})));
    }

    #[test]
    fn list_tasks_filters_by_category() {
        let store = store();
        register(
            &store,
            vec![
                categorized("a", json!({"x": 1}), "math"),
                categorized("b", json!({"x": 2}), "code"),
                categorized("c", json!({"x": 3}), "math"),
            ],
        );
        let math = list(&store, vec![], vec!["math".to_string()]);
        assert_eq!(math.len(), 2);
        assert!(math.iter().all(|t| t.category.as_deref() == Some("math")));
        let all = list(&store, vec![], vec![]);
        assert_eq!(all.len(), 3);
    }

    fn fail_task_req(max_attempts: u32) -> FailTaskRequest {
        FailTaskRequest {
            run_id: "run".to_string(),
            dataset_name: "dataset".to_string(),
            input_digest: "a".to_string(),
            error: ErrorInfo {
                error_type: "RuntimeError".to_string(),
                message: "boom".to_string(),
                failure_class: FailureClass::Transient,
                stack: None,
                retryable: Some(true),
            },
            max_attempts,
        }
    }

    #[test]
    fn task_goes_terminal_after_max_attempts() {
        let store = store();
        register(&store, vec![task("a", json!({"x": 1}))]);

        store.fail_task(fail_task_req(2)).expect("first failure");
        let failed = list(&store, vec!["failed".to_string()], vec![]);
        assert_eq!(failed.len(), 1, "first failure stays retryable");
        assert_eq!(failed[0].attempt, 1);

        store.fail_task(fail_task_req(2)).expect("second failure");
        let terminal = list(&store, vec!["terminal".to_string()], vec![]);
        assert_eq!(terminal.len(), 1, "ceiling reached marks terminal");
        assert_eq!(terminal[0].attempt, 2);
    }

    #[test]
    fn failed_step_with_backoff_reports_retry_later() {
        let store = store();
        let mut req = begin_req();
        req.retry = RetryPolicy {
            max_attempts: 3,
            base_delay_ms: 60_000,
            ..RetryPolicy::default()
        };
        assert!(matches!(
            store.begin_step(req.clone()).expect("begin step"),
            StepOutcome::Execute { attempt: 1 }
        ));
        store
            .fail_step(FailStepRequest {
                run_id: "run".to_string(),
                step_name: "step".to_string(),
                input_digest: "digest".to_string(),
                error: ErrorInfo {
                    error_type: "RuntimeError".to_string(),
                    message: "temporary failure".to_string(),
                    failure_class: FailureClass::Transient,
                    stack: None,
                    retryable: Some(true),
                },
            })
            .expect("mark step failed");
        assert!(
            matches!(
                store.begin_step(req).expect("begin during backoff"),
                StepOutcome::RetryLater { .. }
            ),
            "backoff window should defer the retry"
        );
    }

    #[test]
    fn memo_roundtrip() {
        let store = store();
        let miss = store
            .memo_get(MemoGetRequest {
                run_id: "run".to_string(),
                key_digest: "k".to_string(),
            })
            .expect("memo get miss");
        assert!(!miss.found);
        store
            .memo_put(MemoPutRequest {
                run_id: "run".to_string(),
                key_digest: "k".to_string(),
                key: json!({"turn": 1}),
                value: json!({"answer": 42}),
            })
            .expect("memo put");
        let hit = store
            .memo_get(MemoGetRequest {
                run_id: "run".to_string(),
                key_digest: "k".to_string(),
            })
            .expect("memo get hit");
        assert!(hit.found);
        assert_eq!(hit.value, Some(json!({"answer": 42})));
    }
}
