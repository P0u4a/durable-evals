use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

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
    #[error("duplicate case id: {0}")]
    DuplicateCaseId(String),
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
        let existing = conn
            .query_row(
                "SELECT status, attempt, output_json, error_json, lease_expires_at
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
                    ))
                },
            )
            .optional()?;

        match existing {
            Some((status, _, output_json, _, _)) if status == "succeeded" => {
                Ok(StepOutcome::SkipCompleted {
                    output: parse_optional(output_json)?.unwrap_or(Value::Null),
                })
            }
            Some((status, _, _, error_json, _)) if status == "terminal" => {
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
            Some((status, _, _, _, lease_expires_at))
                if status == "running" && lease_active(lease_expires_at.as_deref()) =>
            {
                Ok(StepOutcome::InProgress)
            }
            Some((_, attempt, _, _, _)) if attempt >= req.retry.max_attempts => {
                conn.execute(
                    "UPDATE step_states
                     SET status = 'terminal', updated_at = datetime('now')
                     WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
                    params![req.run_id, req.step_name, req.input_digest],
                )?;
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
            Some((_, attempt, _, _, _)) => {
                let next_attempt = attempt + 1;
                conn.execute(
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
                Ok(StepOutcome::Execute {
                    attempt: next_attempt,
                })
            }
            None => {
                conn.execute(
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
        conn.execute(
            "UPDATE step_states
             SET status = ?4, error_json = ?5, lease_owner = NULL, lease_expires_at = NULL,
                 completed_at = datetime('now'), updated_at = datetime('now')
             WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3",
            params![
                req.run_id,
                req.step_name,
                req.input_digest,
                status,
                error_json
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
        let mut seen = std::collections::BTreeSet::new();
        for case in &req.cases {
            if !seen.insert(case.case_id.clone()) {
                return Err(Error::DuplicateCaseId(case.case_id.clone()));
            }
        }
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let tx = conn.unchecked_transaction()?;
        for (idx, case) in req.cases.iter().enumerate() {
            tx.execute(
                "INSERT INTO batch_cases
                 (run_id, batch_name, case_id, position, input_digest, input_json, status,
                  attempt, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', 0, datetime('now'), datetime('now'))
                 ON CONFLICT(run_id, batch_name, case_id) DO UPDATE SET
                    position = excluded.position,
                    input_digest = excluded.input_digest,
                    input_json = excluded.input_json,
                    updated_at = datetime('now')",
                params![
                    req.run_id,
                    req.batch_name,
                    case.case_id,
                    idx as u32,
                    case.input_digest,
                    serde_json::to_string(&case.input)?
                ],
            )?;
        }
        tx.commit()?;
        self.batch_summary(&req.run_id, &req.batch_name)
    }

    pub fn start_case(&self, req: ListCasesRequest) -> Result<Option<CaseRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let record = conn
            .query_row(
                "SELECT run_id, batch_name, case_id, input_digest, status, attempt, input_json,
                        output_json, error_json, started_at, completed_at
                 FROM batch_cases
                 WHERE run_id = ?1 AND batch_name = ?2
                   AND (?3 = '[]' OR status IN (SELECT value FROM json_each(?3)))
                 ORDER BY position
                 LIMIT 1",
                params![
                    req.run_id,
                    req.batch_name,
                    serde_json::to_string(&req.statuses)?
                ],
                case_from_row,
            )
            .optional()?;
        if let Some(case) = record {
            conn.execute(
                "UPDATE batch_cases
                 SET status = 'running', attempt = attempt + 1,
                     started_at = COALESCE(started_at, datetime('now')),
                     updated_at = datetime('now')
                 WHERE run_id = ?1 AND batch_name = ?2 AND case_id = ?3",
                params![case.run_id, case.batch_name, case.case_id],
            )?;
            Ok(Some(CaseRecord {
                status: "running".to_string(),
                attempt: case.attempt + 1,
                started_at: case.started_at.or_else(|| Some(now())),
                ..case
            }))
        } else {
            Ok(None)
        }
    }

    pub fn complete_case(&self, req: CompleteCaseRequest) -> Result<()> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let updated = conn.execute(
            "UPDATE batch_cases
             SET status = 'succeeded', output_json = ?4, error_json = NULL,
                 completed_at = datetime('now'), updated_at = datetime('now')
             WHERE run_id = ?1 AND batch_name = ?2 AND case_id = ?3",
            params![
                req.run_id,
                req.batch_name,
                req.case_id,
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
        let status = if retryable { "failed" } else { "terminal" };
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let updated = conn.execute(
            "UPDATE batch_cases
             SET status = ?4, error_json = ?5, completed_at = datetime('now'), updated_at = datetime('now')
             WHERE run_id = ?1 AND batch_name = ?2 AND case_id = ?3",
            params![
                req.run_id,
                req.batch_name,
                req.case_id,
                status,
                serde_json::to_string(&req.error)?
            ],
        )?;
        if updated == 0 {
            return Err(Error::CaseNotFound);
        }
        Ok(())
    }

    pub fn list_cases(&self, req: ListCasesRequest) -> Result<Vec<CaseRecord>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT run_id, batch_name, case_id, input_digest, status, attempt, input_json,
                    output_json, error_json, started_at, completed_at
             FROM batch_cases
             WHERE run_id = ?1 AND batch_name = ?2
               AND (?3 = '[]' OR status IN (SELECT value FROM json_each(?3)))
             ORDER BY position",
        )?;
        let statuses = serde_json::to_string(&req.statuses)?;
        let rows = stmt.query_map(params![req.run_id, req.batch_name, statuses], case_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::Sqlite)
    }

    pub fn batch_summary(&self, run_id: &str, batch_name: &str) -> Result<BatchSummary> {
        let counts = self.counts(
            "SELECT status, COUNT(*) FROM batch_cases WHERE run_id = ?1 AND batch_name = ?2 GROUP BY status",
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

    pub fn heartbeat_step(&self, req: HeartbeatStepRequest) -> Result<()> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        conn.execute(
            "UPDATE step_states
             SET heartbeat_at = datetime('now'), lease_expires_at = datetime('now', '+' || ?5 || ' seconds')
             WHERE run_id = ?1 AND step_name = ?2 AND input_digest = ?3 AND lease_owner = ?4",
            params![
                req.run_id,
                req.step_name,
                req.input_digest,
                req.worker_id,
                req.lease_seconds
            ],
        )?;
        Ok(())
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
                "SELECT status, COUNT(*) FROM batch_cases WHERE run_id = ?1 GROUP BY status",
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
                let mut stmt = conn.prepare(
                    "SELECT run_id, batch_name, case_id, input_digest, status, attempt, input_json,
                            output_json, error_json, started_at, completed_at
                     FROM batch_cases WHERE run_id = ?1 ORDER BY batch_name, position",
                )?;
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
                    body.push_str(&format!(
                        "{},{},{},{},{:?},{},{},{}\n",
                        failure.run_id,
                        failure.step_name,
                        failure.input_digest,
                        failure.attempt,
                        failure.failure_class,
                        failure.error_type,
                        failure.message.replace(',', " "),
                        failure.timestamp
                    ));
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
            CREATE TABLE IF NOT EXISTS batch_cases (
                run_id TEXT NOT NULL,
                batch_name TEXT NOT NULL,
                case_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                input_digest TEXT NOT NULL,
                input_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                output_json TEXT,
                error_json TEXT,
                started_at TEXT,
                completed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, batch_name, case_id)
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
            );",
        )?;
        Ok(())
    }
}

fn case_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CaseRecord> {
    Ok(CaseRecord {
        run_id: row.get(0)?,
        batch_name: row.get(1)?,
        case_id: row.get(2)?,
        input_digest: row.get(3)?,
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

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn lease_active(lease_expires_at: Option<&str>) -> bool {
    lease_expires_at
        .map(|expires| {
            let sql = format!("{}Z", expires.replace(' ', "T"));
            chrono::DateTime::parse_from_rfc3339(&sql)
                .map(|dt| dt.with_timezone(&Utc) > Utc::now())
                .unwrap_or(false)
        })
        .unwrap_or(false)
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

    #[test]
    fn duplicate_batch_case_ids_are_rejected() {
        let store = store();
        let err = store
            .register_batch(RegisterBatchRequest {
                run_id: "run".to_string(),
                batch_name: "batch".to_string(),
                cases: vec![
                    BatchCaseInput {
                        case_id: "case".to_string(),
                        input_digest: "a".to_string(),
                        input: json!({}),
                    },
                    BatchCaseInput {
                        case_id: "case".to_string(),
                        input_digest: "b".to_string(),
                        input: json!({}),
                    },
                ],
            })
            .expect_err("duplicate ids should fail");
        assert!(matches!(err, Error::DuplicateCaseId(_)));
    }
}
