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

/// Cases are content-addressed: edits to an input create a new row under a new digest.
/// Rows from earlier registrations are kept (so reverting an input reuses its old
/// output) but only the latest registration's generation counts as the batch.
const CURRENT_GENERATION: &str = "batch_cases.generation = COALESCE(
    (SELECT b.generation FROM batches b
     WHERE b.run_id = batch_cases.run_id AND b.batch_name = batch_cases.batch_name),
    batch_cases.generation)";

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
                        failure_class: FailureClass::TerminalEval,
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
                        failure_class: FailureClass::TerminalEval,
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
        let failure_class = serde_json::to_string(&req.error.failure_class)?
            .trim_matches('"')
            .to_string();
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

    pub fn register_batch(&self, req: RegisterBatchRequest) -> Result<BatchSummary> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO batches (run_id, batch_name, generation)
             VALUES (?1, ?2, 1)
             ON CONFLICT(run_id, batch_name) DO UPDATE SET generation = generation + 1",
            params![req.run_id, req.batch_name],
        )?;
        let generation: u32 = tx.query_row(
            "SELECT generation FROM batches WHERE run_id = ?1 AND batch_name = ?2",
            params![req.run_id, req.batch_name],
            |row| row.get(0),
        )?;
        // Identical inputs collapse to one content-addressed case; first occurrence wins.
        let mut seen = std::collections::BTreeSet::new();
        for (idx, case) in req.cases.iter().enumerate() {
            if !seen.insert(case.input_digest.clone()) {
                continue;
            }
            tx.execute(
                "INSERT INTO batch_cases
                 (run_id, batch_name, input_digest, label, position, generation, input_json,
                  status, attempt, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', 0, datetime('now'), datetime('now'))
                 ON CONFLICT(run_id, batch_name, input_digest) DO UPDATE SET
                    label = excluded.label,
                    position = excluded.position,
                    generation = excluded.generation,
                    updated_at = datetime('now')",
                params![
                    req.run_id,
                    req.batch_name,
                    case.input_digest,
                    case.label,
                    idx as u32,
                    generation,
                    serde_json::to_string(&case.input)?
                ],
            )?;
        }
        tx.commit()?;
        drop(conn); // release before batch_summary re-locks the connection
        self.batch_summary(&req.run_id, &req.batch_name)
    }

    pub fn complete_case(&self, req: CompleteCaseRequest) -> Result<()> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let updated = conn.execute(
            "UPDATE batch_cases
             SET status = 'succeeded', output_json = ?4, error_json = NULL,
                 completed_at = datetime('now'), updated_at = datetime('now')
             WHERE run_id = ?1 AND batch_name = ?2 AND input_digest = ?3",
            params![
                req.run_id,
                req.batch_name,
                req.input_digest,
                serde_json::to_string(&req.output)?
            ],
        )?;
        if updated == 0 {
            return Err(Error::CaseNotFound);
        }
        Ok(())
    }

    pub fn fail_case(&self, req: FailCaseRequest) -> Result<()> {
        let retryable = req.error.retryable.unwrap_or(matches!(
            req.error.failure_class,
            FailureClass::Transient
                | FailureClass::ResourceUnavailable
                | FailureClass::UserCodeError
        ));
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let tx = conn.unchecked_transaction()?;
        // Count this failed run as a consumed attempt and stop retrying once the
        // ceiling is reached, mirroring the step retry policy.
        let attempt: u32 = tx
            .query_row(
                "SELECT attempt FROM batch_cases
                 WHERE run_id = ?1 AND batch_name = ?2 AND input_digest = ?3",
                params![req.run_id, req.batch_name, req.input_digest],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(Error::CaseNotFound)?;
        let next_attempt = attempt + 1;
        let exhausted = next_attempt >= req.max_attempts;
        let status = if retryable && !exhausted {
            "failed"
        } else {
            "terminal"
        };
        tx.execute(
            "UPDATE batch_cases
             SET status = ?4, attempt = ?5, error_json = ?6,
                 completed_at = datetime('now'), updated_at = datetime('now')
             WHERE run_id = ?1 AND batch_name = ?2 AND input_digest = ?3",
            params![
                req.run_id,
                req.batch_name,
                req.input_digest,
                status,
                next_attempt,
                serde_json::to_string(&req.error)?
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn list_cases(&self, req: ListCasesRequest) -> Result<Vec<CaseRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn.prepare(&format!(
            "SELECT run_id, batch_name, input_digest, label, status, attempt, input_json,
                    output_json, error_json, started_at, completed_at
             FROM batch_cases
             WHERE run_id = ?1 AND batch_name = ?2
               AND (?3 = '[]' OR status IN (SELECT value FROM json_each(?3)))
               AND {CURRENT_GENERATION}
             ORDER BY position"
        ))?;
        let statuses = serde_json::to_string(&req.statuses)?;
        let rows = stmt.query_map(params![req.run_id, req.batch_name, statuses], case_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Sqlite)
    }

    pub fn batch_summary(&self, run_id: &str, batch_name: &str) -> Result<BatchSummary> {
        let counts = self.counts(
            &format!(
                "SELECT status, COUNT(*) FROM batch_cases
                 WHERE run_id = ?1 AND batch_name = ?2 AND {CURRENT_GENERATION}
                 GROUP BY status"
            ),
            params![run_id, batch_name],
        )?;
        Ok(BatchSummary {
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

    pub fn register_worker(&self, req: RegisterWorkerRequest) -> Result<WorkerRecord> {
        let resources = serde_json::to_string(&req.resources)?;
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        conn.execute(
            "INSERT INTO workers (worker_id, hostname, process_id, resources_json, heartbeat_at)
             VALUES (?1, ?2, ?3, ?4, datetime('now'))
             ON CONFLICT(worker_id) DO UPDATE SET
                hostname = excluded.hostname,
                process_id = excluded.process_id,
                resources_json = excluded.resources_json,
                heartbeat_at = datetime('now')",
            params![req.worker_id, req.hostname, req.process_id, resources],
        )?;
        Ok(WorkerRecord {
            worker_id: req.worker_id,
            hostname: req.hostname,
            process_id: req.process_id,
            resources: req.resources,
            heartbeat_at: now(),
        })
    }

    pub fn list_workers(&self) -> Result<Vec<WorkerRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT worker_id, hostname, process_id, resources_json, heartbeat_at
             FROM workers ORDER BY worker_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(WorkerRecord {
                worker_id: row.get(0)?,
                hostname: row.get(1)?,
                process_id: row.get(2)?,
                resources: parse_required(row.get::<_, String>(3)?)?,
                heartbeat_at: row.get(4)?,
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
             WHERE run_id = ?1 AND batch_name = ?2 AND case_id = ?3 AND attempt = ?4",
            params![req.run_id, req.batch_name, req.case_id, req.attempt],
            |row| row.get(0),
        )?;
        conn.execute(
            "INSERT INTO trace_events
             (run_id, batch_name, case_id, attempt, event_index, event_type,
              payload_json, artifact_ids_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, datetime('now'))",
            params![
                req.run_id,
                req.batch_name,
                req.case_id,
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
            batch_name: req.batch_name,
            case_id: req.case_id,
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
        batch_name: &str,
        case_id: &str,
    ) -> Result<Vec<TraceEventRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, run_id, batch_name, case_id, attempt, event_index, event_type,
                    payload_json, artifact_ids_json, created_at
             FROM trace_events
             WHERE run_id = ?1 AND batch_name = ?2 AND case_id = ?3
             ORDER BY attempt, event_index",
        )?;
        let rows = stmt.query_map(params![run_id, batch_name, case_id], trace_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Sqlite)
    }

    pub fn mark_reviewed(&self, req: ReviewRequest) -> Result<ReviewRecord> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        conn.execute(
            "INSERT INTO reviews
             (run_id, batch_name, case_id, reviewer, decision, note, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'))",
            params![
                req.run_id,
                req.batch_name,
                req.case_id,
                req.reviewer,
                review_to_str(&req.decision),
                req.note
            ],
        )?;
        Ok(ReviewRecord {
            run_id: req.run_id,
            batch_name: req.batch_name,
            case_id: req.case_id,
            reviewer: req.reviewer,
            decision: req.decision,
            note: req.note,
            timestamp: now(),
        })
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
            case_counts: self.counts(
                &format!(
                    "SELECT status, COUNT(*) FROM batch_cases
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
            ExportKind::CaseResultsJsonl => {
                let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
                let mut stmt = conn.prepare(&format!(
                    "SELECT run_id, batch_name, input_digest, label, status, attempt, input_json,
                            output_json, error_json, started_at, completed_at
                     FROM batch_cases WHERE run_id = ?1 AND {CURRENT_GENERATION}
                     ORDER BY batch_name, position"
                ))?;
                let rows = stmt.query_map(params![req.run_id], case_from_row)?;
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
                for (status, count) in summary.case_counts {
                    body.push_str(&format!("cases,{status},{count}\n"));
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
        // Legacy schema keyed batch_cases by user-supplied case_id; rename it aside so the
        // content-addressed table can be created, then carry rows over below.
        let legacy_batch_cases: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('batch_cases') WHERE name = 'case_id'")?
            .exists([])?;
        if legacy_batch_cases {
            conn.execute_batch("ALTER TABLE batch_cases RENAME TO batch_cases_legacy;")?;
        }
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
            CREATE TABLE IF NOT EXISTS batches (
                run_id TEXT NOT NULL,
                batch_name TEXT NOT NULL,
                generation INTEGER NOT NULL,
                PRIMARY KEY (run_id, batch_name)
            );
            CREATE TABLE IF NOT EXISTS batch_cases (
                run_id TEXT NOT NULL,
                batch_name TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                label TEXT,
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
                PRIMARY KEY (run_id, batch_name, input_digest)
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
            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                hostname TEXT,
                process_id INTEGER,
                resources_json TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trace_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                batch_name TEXT NOT NULL,
                case_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                event_index INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                artifact_ids_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                batch_name TEXT NOT NULL,
                case_id TEXT NOT NULL,
                reviewer TEXT NOT NULL,
                decision TEXT NOT NULL,
                note TEXT,
                timestamp TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_failure_records_run ON failure_records (run_id);
            CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts (run_id);
            CREATE INDEX IF NOT EXISTS idx_trace_events_case
                ON trace_events (run_id, batch_name, case_id, attempt, event_index);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_trace_events_index
                ON trace_events (run_id, batch_name, case_id, attempt, event_index);
            CREATE INDEX IF NOT EXISTS idx_reviews_run ON reviews (run_id);",
        )?;
        // Older databases predate the retry_at backoff column; add it in place.
        let has_retry_at: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('step_states') WHERE name = 'retry_at'")?
            .exists([])?;
        if !has_retry_at {
            conn.execute_batch("ALTER TABLE step_states ADD COLUMN retry_at TEXT;")?;
        }
        if legacy_batch_cases {
            conn.execute_batch(
                "INSERT OR IGNORE INTO batch_cases
                   (run_id, batch_name, input_digest, label, position, generation, input_json,
                    status, attempt, output_json, error_json, started_at, completed_at,
                    created_at, updated_at)
                 SELECT run_id, batch_name, input_digest, case_id, position, 1, input_json,
                        status, attempt, output_json, error_json, started_at, completed_at,
                        created_at, updated_at
                 FROM batch_cases_legacy ORDER BY position;
                 INSERT OR IGNORE INTO batches (run_id, batch_name, generation)
                 SELECT DISTINCT run_id, batch_name, 1 FROM batch_cases_legacy;
                 DROP TABLE batch_cases_legacy;",
            )?;
        }
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
    fn register_batch(&self, req: RegisterBatchRequest) -> Result<BatchSummary> {
        self.register_batch(req)
    }
    fn complete_case(&self, req: CompleteCaseRequest) -> Result<()> {
        self.complete_case(req)
    }
    fn fail_case(&self, req: FailCaseRequest) -> Result<()> {
        self.fail_case(req)
    }
    fn list_cases(&self, req: ListCasesRequest) -> Result<Vec<CaseRecord>> {
        self.list_cases(req)
    }
    fn register_variants(&self, req: RegisterVariantsRequest) -> Result<Vec<VariantRecord>> {
        self.register_variants(req)
    }
    fn list_variants(&self, run_id: &str) -> Result<Vec<VariantRecord>> {
        self.list_variants(run_id)
    }
    fn register_worker(&self, req: RegisterWorkerRequest) -> Result<WorkerRecord> {
        self.register_worker(req)
    }
    fn list_workers(&self) -> Result<Vec<WorkerRecord>> {
        self.list_workers()
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
        batch_name: &str,
        case_id: &str,
    ) -> Result<Vec<TraceEventRecord>> {
        self.list_trace_events(run_id, batch_name, case_id)
    }
    fn mark_reviewed(&self, req: ReviewRequest) -> Result<ReviewRecord> {
        self.mark_reviewed(req)
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

fn case_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CaseRecord> {
    Ok(CaseRecord {
        run_id: row.get(0)?,
        batch_name: row.get(1)?,
        input_digest: row.get(2)?,
        label: row.get(3)?,
        status: row.get(4)?,
        attempt: row.get(5)?,
        input: parse_required(row.get::<_, String>(6)?)?,
        output: parse_optional(row.get::<_, Option<String>>(7)?)?,
        error: parse_optional(row.get::<_, Option<String>>(8)?)?,
        started_at: row.get(9)?,
        completed_at: row.get(10)?,
    })
}

fn trace_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TraceEventRecord> {
    Ok(TraceEventRecord {
        id: row.get(0)?,
        run_id: row.get(1)?,
        batch_name: row.get(2)?,
        case_id: row.get(3)?,
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

fn parse_failure_class(value: String) -> FailureClass {
    match value.as_str() {
        "transient" => FailureClass::Transient,
        "resource_unavailable" => FailureClass::ResourceUnavailable,
        "terminal_eval" => FailureClass::TerminalEval,
        "runner_error" => FailureClass::RunnerError,
        "artifact_error" => FailureClass::ArtifactError,
        _ => FailureClass::UserCodeError,
    }
}

fn review_to_str(decision: &ReviewDecision) -> &'static str {
    match decision {
        ReviewDecision::PendingReview => "pending_review",
        ReviewDecision::ReviewedPass => "reviewed_pass",
        ReviewDecision::ReviewedFail => "reviewed_fail",
        ReviewDecision::ReviewedExcluded => "reviewed_excluded",
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

    fn case(digest: &str, input: Value) -> BatchCaseInput {
        BatchCaseInput {
            input_digest: digest.to_string(),
            input,
            label: None,
        }
    }

    fn register(store: &SqliteStore, cases: Vec<BatchCaseInput>) -> BatchSummary {
        store
            .register_batch(RegisterBatchRequest {
                run_id: "run".to_string(),
                batch_name: "batch".to_string(),
                cases,
            })
            .expect("register batch")
    }

    #[test]
    fn identical_inputs_collapse_to_one_case() {
        let store = store();
        let summary = register(
            &store,
            vec![case("a", json!({"x": 1})), case("a", json!({"x": 1}))],
        );
        assert_eq!(summary.total, 1);
        assert_eq!(summary.pending, 1);
    }

    #[test]
    fn changed_input_invalidates_and_revert_reuses_output() {
        let store = store();
        register(&store, vec![case("a", json!({"x": 1}))]);
        store
            .complete_case(CompleteCaseRequest {
                run_id: "run".to_string(),
                batch_name: "batch".to_string(),
                input_digest: "a".to_string(),
                output: json!({"ok": true}),
            })
            .expect("complete case");

        // Edited input gets a fresh pending case; the stale success no longer counts.
        let summary = register(&store, vec![case("b", json!({"x": 2}))]);
        assert_eq!(summary.total, 1);
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.succeeded, 0);

        // Reverting to the original input salvages its completed output.
        let summary = register(&store, vec![case("a", json!({"x": 1}))]);
        assert_eq!(summary.total, 1);
        assert_eq!(summary.succeeded, 1);
        let cases = store
            .list_cases(ListCasesRequest {
                run_id: "run".to_string(),
                batch_name: "batch".to_string(),
                statuses: vec![],
            })
            .expect("list cases");
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].output, Some(json!({"ok": true})));
    }

    fn fail_case_req(max_attempts: u32) -> FailCaseRequest {
        FailCaseRequest {
            run_id: "run".to_string(),
            batch_name: "batch".to_string(),
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
    fn case_goes_terminal_after_max_attempts() {
        let store = store();
        register(&store, vec![case("a", json!({"x": 1}))]);

        store.fail_case(fail_case_req(2)).expect("first failure");
        let failed = store
            .list_cases(ListCasesRequest {
                run_id: "run".to_string(),
                batch_name: "batch".to_string(),
                statuses: vec!["failed".to_string()],
            })
            .expect("list failed");
        assert_eq!(failed.len(), 1, "first failure stays retryable");
        assert_eq!(failed[0].attempt, 1);

        store.fail_case(fail_case_req(2)).expect("second failure");
        let terminal = store
            .list_cases(ListCasesRequest {
                run_id: "run".to_string(),
                batch_name: "batch".to_string(),
                statuses: vec!["terminal".to_string()],
            })
            .expect("list terminal");
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
