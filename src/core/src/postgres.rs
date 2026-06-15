use std::collections::BTreeMap;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use postgres::types::ToSql;
use postgres::{NoTls, Row};
use r2d2_postgres::PostgresConnectionManager;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::store::{Error, Result, Store};
use crate::types::*;

impl From<r2d2::Error> for Error {
    fn from(err: r2d2::Error) -> Self {
        Error::Postgres(err.to_string())
    }
}

impl From<postgres::Error> for Error {
    fn from(err: postgres::Error) -> Self {
        Error::Postgres(err.to_string())
    }
}

/// Cases are content-addressed: edits to an input create a new row under a new digest.
/// Rows from earlier registrations are kept (so reverting an input reuses its old
/// output) but only the latest registration's generation counts as the batch.
const CURRENT_GENERATION: &str = "batch_cases.generation = COALESCE(
    (SELECT b.generation FROM batches b
     WHERE b.run_id = batch_cases.run_id AND b.batch_name = batch_cases.batch_name),
    batch_cases.generation)";

type Pool = r2d2::Pool<PostgresConnectionManager<NoTls>>;

#[derive(Clone)]
pub struct PostgresStore {
    pool: Pool,
}

impl PostgresStore {
    pub fn connect(url: &str) -> Result<Self> {
        let config: postgres::Config = url
            .parse()
            .map_err(|err: postgres::Error| Error::Postgres(err.to_string()))?;
        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = r2d2::Pool::new(manager)?;
        let store = Self { pool };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<()> {
        let mut client = self.pool.get()?;
        client.batch_execute(
            "CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                name TEXT,
                config_json TEXT NOT NULL,
                config_digest TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS step_states (
                run_id TEXT NOT NULL,
                step_name TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt BIGINT NOT NULL,
                config_json TEXT NOT NULL DEFAULT 'null',
                dependencies_json TEXT NOT NULL DEFAULT '[]',
                variants_json TEXT NOT NULL DEFAULT '{}',
                retry_json TEXT NOT NULL DEFAULT '{}',
                output_json TEXT,
                error_json TEXT,
                lease_owner TEXT,
                lease_acquired_at TIMESTAMPTZ,
                lease_expires_at TIMESTAMPTZ,
                heartbeat_at TIMESTAMPTZ,
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (run_id, step_name, input_digest)
            );
            CREATE TABLE IF NOT EXISTS failure_records (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                run_id TEXT NOT NULL,
                step_name TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                attempt BIGINT NOT NULL,
                failure_class TEXT NOT NULL,
                error_type TEXT NOT NULL,
                message TEXT NOT NULL,
                stack TEXT,
                retryable BOOLEAN NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL
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
                size BIGINT NOT NULL,
                valid BOOLEAN NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS batches (
                run_id TEXT NOT NULL,
                batch_name TEXT NOT NULL,
                generation BIGINT NOT NULL,
                PRIMARY KEY (run_id, batch_name)
            );
            CREATE TABLE IF NOT EXISTS batch_cases (
                run_id TEXT NOT NULL,
                batch_name TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                label TEXT,
                position BIGINT NOT NULL,
                generation BIGINT NOT NULL DEFAULT 1,
                input_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt BIGINT NOT NULL,
                output_json TEXT,
                error_json TEXT,
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (run_id, batch_name, input_digest)
            );
            CREATE TABLE IF NOT EXISTS memos (
                run_id TEXT NOT NULL,
                key_digest TEXT NOT NULL,
                key_json TEXT NOT NULL,
                value_json TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
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
                process_id BIGINT,
                resources_json TEXT NOT NULL,
                heartbeat_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trace_events (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                run_id TEXT NOT NULL,
                batch_name TEXT NOT NULL,
                case_id TEXT NOT NULL,
                attempt BIGINT NOT NULL,
                event_index BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                artifact_ids_json TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS reviews (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                run_id TEXT NOT NULL,
                batch_name TEXT NOT NULL,
                case_id TEXT NOT NULL,
                reviewer TEXT NOT NULL,
                decision TEXT NOT NULL,
                note TEXT,
                timestamp TIMESTAMPTZ NOT NULL
            );",
        )?;
        Ok(())
    }

    fn register_run(&self, req: RegisterRunRequest) -> Result<()> {
        let config_json = serde_json::to_string(&req.config)?;
        let digest = digest_json(&req.config)?;
        let mut client = self.pool.get()?;
        client.execute(
            "INSERT INTO runs (run_id, name, config_json, config_digest, created_at, updated_at)
             VALUES ($1, $2, $3, $4, now(), now())
             ON CONFLICT (run_id) DO UPDATE SET
                name = excluded.name,
                config_json = excluded.config_json,
                config_digest = excluded.config_digest,
                updated_at = now()",
            &[&req.run_id, &req.name, &config_json, &digest],
        )?;
        Ok(())
    }

    fn begin_step(&self, req: BeginStepRequest) -> Result<StepOutcome> {
        let config_json = serde_json::to_string(&req.config)?;
        let dependencies_json = serde_json::to_string(&req.dependencies)?;
        let variants_json = serde_json::to_string(&req.variants)?;
        let retry_json = serde_json::to_string(&req.retry)?;
        let worker_id = req.worker_id.clone();
        let lease_seconds = i32::try_from(req.lease_seconds.unwrap_or(300)).unwrap_or(i32::MAX);
        let mut client = self.pool.get()?;
        let existing = client
            .query_opt(
                "SELECT status, attempt, output_json, error_json,
                        (lease_expires_at > now()) AS lease_active
                 FROM step_states
                 WHERE run_id = $1 AND step_name = $2 AND input_digest = $3",
                &[&req.run_id, &req.step_name, &req.input_digest],
            )?
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    row.get::<_, i64>(1) as u32,
                    row.get::<_, Option<String>>(2),
                    row.get::<_, Option<String>>(3),
                    row.get::<_, Option<bool>>(4).unwrap_or(false),
                )
            });

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
            Some((status, _, _, _, lease_active))
                if status == "running" && lease_active =>
            {
                Ok(StepOutcome::InProgress)
            }
            Some((_, attempt, _, _, _)) if attempt >= req.retry.max_attempts => {
                client.execute(
                    "UPDATE step_states
                     SET status = 'terminal', updated_at = now()
                     WHERE run_id = $1 AND step_name = $2 AND input_digest = $3",
                    &[&req.run_id, &req.step_name, &req.input_digest],
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
                let next_attempt = i64::from(attempt + 1);
                client.execute(
                    "UPDATE step_states
                     SET status = 'running',
                         attempt = $4,
                         config_json = $5,
                         dependencies_json = $6,
                         variants_json = $7,
                         retry_json = $8,
                         lease_owner = $9,
                         lease_acquired_at = now(),
                         lease_expires_at = now() + make_interval(secs => $10::int),
                         heartbeat_at = now(),
                         started_at = COALESCE(started_at, now()),
                         updated_at = now()
                     WHERE run_id = $1 AND step_name = $2 AND input_digest = $3",
                    &[
                        &req.run_id,
                        &req.step_name,
                        &req.input_digest,
                        &next_attempt,
                        &config_json,
                        &dependencies_json,
                        &variants_json,
                        &retry_json,
                        &worker_id,
                        &lease_seconds,
                    ],
                )?;
                Ok(StepOutcome::Execute {
                    attempt: attempt + 1,
                })
            }
            None => {
                client.execute(
                    "INSERT INTO step_states
                     (run_id, step_name, input_digest, status, attempt, config_json,
                      dependencies_json, variants_json, retry_json, lease_owner,
                      lease_acquired_at, lease_expires_at, heartbeat_at, started_at, updated_at)
                     VALUES ($1, $2, $3, 'running', 1, $4, $5, $6, $7, $8,
                             now(), now() + make_interval(secs => $9::int),
                             now(), now(), now())",
                    &[
                        &req.run_id,
                        &req.step_name,
                        &req.input_digest,
                        &config_json,
                        &dependencies_json,
                        &variants_json,
                        &retry_json,
                        &worker_id,
                        &lease_seconds,
                    ],
                )?;
                Ok(StepOutcome::Execute { attempt: 1 })
            }
        }
    }

    fn complete_step(&self, req: CompleteStepRequest) -> Result<()> {
        let output_json = serde_json::to_string(&req.output)?;
        let mut client = self.pool.get()?;
        let mut tx = client.transaction()?;
        let updated = tx.execute(
            "UPDATE step_states
             SET status = 'succeeded', output_json = $4, error_json = NULL,
                 lease_owner = NULL, lease_expires_at = NULL, completed_at = now(),
                 updated_at = now()
             WHERE run_id = $1 AND step_name = $2 AND input_digest = $3",
            &[&req.run_id, &req.step_name, &req.input_digest, &output_json],
        )?;
        if updated == 0 {
            return Err(Error::StepNotFound);
        }
        for artifact in req.artifacts {
            let inline = optional_json(artifact.inline_json)?;
            let size = i64::try_from(artifact.size).unwrap_or(i64::MAX);
            tx.execute(
                "INSERT INTO artifacts
                 (artifact_id, run_id, step_name, input_digest, name, kind, path,
                  inline_json, sha256, size, valid, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
                 ON CONFLICT (artifact_id) DO UPDATE SET
                    run_id = excluded.run_id,
                    step_name = excluded.step_name,
                    input_digest = excluded.input_digest,
                    name = excluded.name,
                    kind = excluded.kind,
                    path = excluded.path,
                    inline_json = excluded.inline_json,
                    sha256 = excluded.sha256,
                    size = excluded.size,
                    valid = excluded.valid,
                    created_at = excluded.created_at",
                &[
                    &artifact.artifact_id,
                    &req.run_id,
                    &req.step_name,
                    &req.input_digest,
                    &artifact.name,
                    &artifact.kind,
                    &artifact.path,
                    &inline,
                    &artifact.sha256,
                    &size,
                    &artifact.valid,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn fail_step(&self, req: FailStepRequest) -> Result<()> {
        let mut client = self.pool.get()?;
        let row = client
            .query_opt(
                "SELECT attempt, retry_json FROM step_states
                 WHERE run_id = $1 AND step_name = $2 AND input_digest = $3",
                &[&req.run_id, &req.step_name, &req.input_digest],
            )?
            .ok_or(Error::StepNotFound)?;
        let attempt = row.get::<_, i64>(0);
        let retry_json = row.get::<_, String>(1);
        let retry_policy: RetryPolicy = serde_json::from_str(&retry_json).unwrap_or_default();

        let retryable = req.error.retryable.unwrap_or_else(|| {
            retry_policy.retryable.contains(&req.error.failure_class)
                && !retry_policy.terminal.contains(&req.error.failure_class)
        });
        let status = if retryable { "failed" } else { "terminal" };
        let error_json = serde_json::to_string(&req.error)?;
        client.execute(
            "UPDATE step_states
             SET status = $4, error_json = $5, lease_owner = NULL, lease_expires_at = NULL,
                 completed_at = now(), updated_at = now()
             WHERE run_id = $1 AND step_name = $2 AND input_digest = $3",
            &[
                &req.run_id,
                &req.step_name,
                &req.input_digest,
                &status,
                &error_json,
            ],
        )?;
        let failure_class = serde_json::to_string(&req.error.failure_class)?
            .trim_matches('"')
            .to_string();
        client.execute(
            "INSERT INTO failure_records
             (run_id, step_name, input_digest, attempt, failure_class, error_type,
              message, stack, retryable, timestamp)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())",
            &[
                &req.run_id,
                &req.step_name,
                &req.input_digest,
                &attempt,
                &failure_class,
                &req.error.error_type,
                &req.error.message,
                &req.error.stack,
                &retryable,
            ],
        )?;
        Ok(())
    }

    fn register_batch(&self, req: RegisterBatchRequest) -> Result<BatchSummary> {
        let mut client = self.pool.get()?;
        let mut tx = client.transaction()?;
        tx.execute(
            "INSERT INTO batches (run_id, batch_name, generation)
             VALUES ($1, $2, 1)
             ON CONFLICT (run_id, batch_name) DO UPDATE SET generation = batches.generation + 1",
            &[&req.run_id, &req.batch_name],
        )?;
        let generation: i64 = tx
            .query_one(
                "SELECT generation FROM batches WHERE run_id = $1 AND batch_name = $2",
                &[&req.run_id, &req.batch_name],
            )?
            .get(0);
        // Identical inputs collapse to one content-addressed case; first occurrence wins.
        let mut seen = std::collections::BTreeSet::new();
        for (idx, case) in req.cases.iter().enumerate() {
            if !seen.insert(case.input_digest.clone()) {
                continue;
            }
            let position = idx as i64;
            let input_json = serde_json::to_string(&case.input)?;
            tx.execute(
                "INSERT INTO batch_cases
                 (run_id, batch_name, input_digest, label, position, generation, input_json,
                  status, attempt, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', 0, now(), now())
                 ON CONFLICT (run_id, batch_name, input_digest) DO UPDATE SET
                    label = excluded.label,
                    position = excluded.position,
                    generation = excluded.generation,
                    updated_at = now()",
                &[
                    &req.run_id,
                    &req.batch_name,
                    &case.input_digest,
                    &case.label,
                    &position,
                    &generation,
                    &input_json,
                ],
            )?;
        }
        tx.commit()?;
        self.batch_summary(&req.run_id, &req.batch_name)
    }

    fn start_case(&self, req: ListCasesRequest) -> Result<Option<CaseRecord>> {
        let mut client = self.pool.get()?;
        let record = client
            .query_opt(
                &format!(
                    "SELECT run_id, batch_name, input_digest, label, status, attempt, input_json,
                            output_json, error_json, {STARTED_AT}, {COMPLETED_AT}
                     FROM batch_cases
                     WHERE run_id = $1 AND batch_name = $2
                       AND (cardinality($3::text[]) = 0 OR status = ANY($3::text[]))
                       AND {CURRENT_GENERATION}
                     ORDER BY position
                     LIMIT 1",
                    STARTED_AT = ts_text("started_at"),
                    COMPLETED_AT = ts_text("completed_at"),
                ),
                &[&req.run_id, &req.batch_name, &req.statuses],
            )?
            .map(case_from_row)
            .transpose()?;
        if let Some(case) = record {
            client.execute(
                "UPDATE batch_cases
                 SET status = 'running', attempt = attempt + 1,
                     started_at = COALESCE(started_at, now()),
                     updated_at = now()
                 WHERE run_id = $1 AND batch_name = $2 AND input_digest = $3",
                &[&case.run_id, &case.batch_name, &case.input_digest],
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

    fn complete_case(&self, req: CompleteCaseRequest) -> Result<()> {
        let output_json = serde_json::to_string(&req.output)?;
        let mut client = self.pool.get()?;
        let updated = client.execute(
            "UPDATE batch_cases
             SET status = 'succeeded', output_json = $4, error_json = NULL,
                 completed_at = now(), updated_at = now()
             WHERE run_id = $1 AND batch_name = $2 AND input_digest = $3",
            &[&req.run_id, &req.batch_name, &req.input_digest, &output_json],
        )?;
        if updated == 0 {
            return Err(Error::CaseNotFound);
        }
        Ok(())
    }

    fn fail_case(&self, req: FailCaseRequest) -> Result<()> {
        let retryable = req.error.retryable.unwrap_or(matches!(
            req.error.failure_class,
            FailureClass::Transient
                | FailureClass::ResourceUnavailable
                | FailureClass::UserCodeError
        ));
        let status = if retryable { "failed" } else { "terminal" };
        let error_json = serde_json::to_string(&req.error)?;
        let mut client = self.pool.get()?;
        let updated = client.execute(
            "UPDATE batch_cases
             SET status = $4, error_json = $5, completed_at = now(), updated_at = now()
             WHERE run_id = $1 AND batch_name = $2 AND input_digest = $3",
            &[&req.run_id, &req.batch_name, &req.input_digest, &status, &error_json],
        )?;
        if updated == 0 {
            return Err(Error::CaseNotFound);
        }
        Ok(())
    }

    fn list_cases(&self, req: ListCasesRequest) -> Result<Vec<CaseRecord>> {
        let mut client = self.pool.get()?;
        let rows = client.query(
            &format!(
                "SELECT run_id, batch_name, input_digest, label, status, attempt, input_json,
                        output_json, error_json, {STARTED_AT}, {COMPLETED_AT}
                 FROM batch_cases
                 WHERE run_id = $1 AND batch_name = $2
                   AND (cardinality($3::text[]) = 0 OR status = ANY($3::text[]))
                   AND {CURRENT_GENERATION}
                 ORDER BY position",
                STARTED_AT = ts_text("started_at"),
                COMPLETED_AT = ts_text("completed_at"),
            ),
            &[&req.run_id, &req.batch_name, &req.statuses],
        )?;
        rows.into_iter().map(case_from_row).collect()
    }

    fn batch_summary(&self, run_id: &str, batch_name: &str) -> Result<BatchSummary> {
        let counts = self.counts(
            &format!(
                "SELECT status, COUNT(*) FROM batch_cases
                 WHERE run_id = $1 AND batch_name = $2 AND {CURRENT_GENERATION}
                 GROUP BY status"
            ),
            &[&run_id, &batch_name],
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

    fn register_variants(&self, req: RegisterVariantsRequest) -> Result<Vec<VariantRecord>> {
        let mut client = self.pool.get()?;
        let mut tx = client.transaction()?;
        tx.execute(
            "DELETE FROM variants WHERE run_id = $1 AND dimension = $2",
            &[&req.run_id, &req.dimension],
        )?;
        for variant in &req.variants {
            let config_json = serde_json::to_string(&variant.config)?;
            tx.execute(
                "INSERT INTO variants (run_id, dimension, name, config_json, digest)
                 VALUES ($1, $2, $3, $4, $5)",
                &[
                    &req.run_id,
                    &req.dimension,
                    &variant.name,
                    &config_json,
                    &variant.digest,
                ],
            )?;
        }
        tx.commit()?;
        self.list_variants(&req.run_id)
    }

    fn list_variants(&self, run_id: &str) -> Result<Vec<VariantRecord>> {
        let mut client = self.pool.get()?;
        let rows = client.query(
            "SELECT run_id, dimension, name, config_json, digest
             FROM variants WHERE run_id = $1 ORDER BY dimension, name",
            &[&run_id],
        )?;
        rows.into_iter()
            .map(|row| {
                Ok(VariantRecord {
                    run_id: row.get(0),
                    dimension: row.get(1),
                    name: row.get(2),
                    config: parse_required(row.get::<_, String>(3))?,
                    digest: row.get(4),
                })
            })
            .collect()
    }

    fn register_worker(&self, req: RegisterWorkerRequest) -> Result<WorkerRecord> {
        let resources = serde_json::to_string(&req.resources)?;
        let process_id = req.process_id.map(i64::from);
        let mut client = self.pool.get()?;
        client.execute(
            "INSERT INTO workers (worker_id, hostname, process_id, resources_json, heartbeat_at)
             VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (worker_id) DO UPDATE SET
                hostname = excluded.hostname,
                process_id = excluded.process_id,
                resources_json = excluded.resources_json,
                heartbeat_at = now()",
            &[&req.worker_id, &req.hostname, &process_id, &resources],
        )?;
        Ok(WorkerRecord {
            worker_id: req.worker_id,
            hostname: req.hostname,
            process_id: req.process_id,
            resources: req.resources,
            heartbeat_at: now(),
        })
    }

    fn list_workers(&self) -> Result<Vec<WorkerRecord>> {
        let mut client = self.pool.get()?;
        let rows = client.query(
            "SELECT worker_id, hostname, process_id, resources_json, heartbeat_at
             FROM workers ORDER BY worker_id",
            &[],
        )?;
        rows.into_iter()
            .map(|row| {
                Ok(WorkerRecord {
                    worker_id: row.get(0),
                    hostname: row.get(1),
                    process_id: row.get::<_, Option<i64>>(2).map(|v| v as u32),
                    resources: parse_required(row.get::<_, String>(3))?,
                    heartbeat_at: ts_to_rfc3339(row.get(4)),
                })
            })
            .collect()
    }

    fn heartbeat_step(&self, req: HeartbeatStepRequest) -> Result<()> {
        let lease_seconds = i32::try_from(req.lease_seconds).unwrap_or(i32::MAX);
        let mut client = self.pool.get()?;
        client.execute(
            "UPDATE step_states
             SET heartbeat_at = now(), lease_expires_at = now() + make_interval(secs => $5::int)
             WHERE run_id = $1 AND step_name = $2 AND input_digest = $3 AND lease_owner = $4",
            &[
                &req.run_id,
                &req.step_name,
                &req.input_digest,
                &req.worker_id,
                &lease_seconds,
            ],
        )?;
        Ok(())
    }

    fn add_trace_event(&self, req: TraceEventRequest) -> Result<TraceEventRecord> {
        let payload = serde_json::to_string(&req.payload)?;
        let artifact_ids = serde_json::to_string(&req.artifact_ids)?;
        let attempt = i64::from(req.attempt);
        let mut client = self.pool.get()?;
        let event_index: i64 = client
            .query_one(
                "SELECT COALESCE(MAX(event_index), 0) + 1
                 FROM trace_events
                 WHERE run_id = $1 AND batch_name = $2 AND case_id = $3 AND attempt = $4",
                &[&req.run_id, &req.batch_name, &req.case_id, &attempt],
            )?
            .get(0);
        let id: i64 = client
            .query_one(
                "INSERT INTO trace_events
                 (run_id, batch_name, case_id, attempt, event_index, event_type,
                  payload_json, artifact_ids_json, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
                 RETURNING id",
                &[
                    &req.run_id,
                    &req.batch_name,
                    &req.case_id,
                    &attempt,
                    &event_index,
                    &req.event_type,
                    &payload,
                    &artifact_ids,
                ],
            )?
            .get(0);
        Ok(TraceEventRecord {
            id,
            run_id: req.run_id,
            batch_name: req.batch_name,
            case_id: req.case_id,
            attempt: req.attempt,
            event_index: event_index as u32,
            event_type: req.event_type,
            payload: req.payload,
            artifact_ids: req.artifact_ids,
            created_at: now(),
        })
    }

    fn list_trace_events(
        &self,
        run_id: &str,
        batch_name: &str,
        case_id: &str,
    ) -> Result<Vec<TraceEventRecord>> {
        let mut client = self.pool.get()?;
        let rows = client.query(
            &format!(
                "SELECT id, run_id, batch_name, case_id, attempt, event_index, event_type,
                        payload_json, artifact_ids_json, {CREATED_AT}
                 FROM trace_events
                 WHERE run_id = $1 AND batch_name = $2 AND case_id = $3
                 ORDER BY attempt, event_index",
                CREATED_AT = ts_text("created_at"),
            ),
            &[&run_id, &batch_name, &case_id],
        )?;
        rows.into_iter().map(trace_from_row).collect()
    }

    fn mark_reviewed(&self, req: ReviewRequest) -> Result<ReviewRecord> {
        let decision = review_to_str(&req.decision);
        let mut client = self.pool.get()?;
        client.execute(
            "INSERT INTO reviews
             (run_id, batch_name, case_id, reviewer, decision, note, timestamp)
             VALUES ($1, $2, $3, $4, $5, $6, now())",
            &[
                &req.run_id,
                &req.batch_name,
                &req.case_id,
                &req.reviewer,
                &decision,
                &req.note,
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

    fn memo_get(&self, req: MemoGetRequest) -> Result<MemoGetResponse> {
        let mut client = self.pool.get()?;
        let value: Option<String> = client
            .query_opt(
                "SELECT value_json FROM memos WHERE run_id = $1 AND key_digest = $2",
                &[&req.run_id, &req.key_digest],
            )?
            .map(|row| row.get(0));
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

    fn memo_put(&self, req: MemoPutRequest) -> Result<()> {
        let key_json = serde_json::to_string(&req.key)?;
        let value_json = serde_json::to_string(&req.value)?;
        let mut client = self.pool.get()?;
        client.execute(
            "INSERT INTO memos (run_id, key_digest, key_json, value_json, created_at)
             VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (run_id, key_digest) DO UPDATE SET
                key_json = excluded.key_json,
                value_json = excluded.value_json",
            &[&req.run_id, &req.key_digest, &key_json, &value_json],
        )?;
        Ok(())
    }

    fn summary(&self, req: SummaryRequest) -> Result<RunSummary> {
        let mut client = self.pool.get()?;
        let run = client
            .query_opt(
                &format!(
                    "SELECT name, config_json, config_digest, {CREATED_AT} FROM runs WHERE run_id = $1",
                    CREATED_AT = ts_text("created_at"),
                ),
                &[&req.run_id],
            )?
            .map(|row| {
                (
                    row.get::<_, Option<String>>(0),
                    row.get::<_, String>(1),
                    row.get::<_, String>(2),
                    row.get::<_, Option<String>>(3),
                )
            });
        let (name, config_json, config_digest, started_at) =
            run.unwrap_or_else(|| (None, "null".to_string(), digest_bytes(b"null"), Some(now())));
        drop(client);
        Ok(RunSummary {
            run_id: req.run_id.clone(),
            name,
            config: parse_required(config_json)?,
            config_digest,
            step_counts: self.counts(
                "SELECT status, COUNT(*) FROM step_states WHERE run_id = $1 GROUP BY status",
                &[&req.run_id],
            )?,
            case_counts: self.counts(
                &format!(
                    "SELECT status, COUNT(*) FROM batch_cases
                     WHERE run_id = $1 AND {CURRENT_GENERATION}
                     GROUP BY status"
                ),
                &[&req.run_id],
            )?,
            artifact_count: self.scalar_count(
                "SELECT COUNT(*) FROM artifacts WHERE run_id = $1",
                &[&req.run_id],
            )?,
            failure_counts: self.counts(
                "SELECT failure_class, COUNT(*) FROM failure_records WHERE run_id = $1 GROUP BY failure_class",
                &[&req.run_id],
            )?,
            started_at,
            completed_at: None,
        })
    }

    fn export(&self, req: ExportRequest) -> Result<ExportResponse> {
        match req.kind {
            ExportKind::ManifestJson => Ok(ExportResponse {
                content_type: "application/json".to_string(),
                body: serde_json::to_string_pretty(
                    &self.summary(SummaryRequest { run_id: req.run_id })?,
                )?,
            }),
            ExportKind::CaseResultsJsonl => {
                let mut client = self.pool.get()?;
                let rows = client.query(
                    &format!(
                        "SELECT run_id, batch_name, input_digest, label, status, attempt, input_json,
                                output_json, error_json, {STARTED_AT}, {COMPLETED_AT}
                         FROM batch_cases WHERE run_id = $1 AND {CURRENT_GENERATION}
                         ORDER BY batch_name, position",
                        STARTED_AT = ts_text("started_at"),
                        COMPLETED_AT = ts_text("completed_at"),
                    ),
                    &[&req.run_id],
                )?;
                let mut body = String::new();
                for row in rows {
                    let record = case_from_row(row)?;
                    body.push_str(&serde_json::to_string(&record)?);
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
        let mut client = self.pool.get()?;
        let rows = client.query(
            &format!(
                "SELECT run_id, step_name, input_digest, attempt, failure_class, error_type,
                        message, stack, retryable, {TIMESTAMP}
                 FROM failure_records WHERE run_id = $1 ORDER BY timestamp",
                TIMESTAMP = ts_text("timestamp"),
            ),
            &[&run_id],
        )?;
        rows.into_iter()
            .map(|row| {
                Ok(FailureRecord {
                    run_id: row.get(0),
                    step_name: row.get(1),
                    input_digest: row.get(2),
                    attempt: row.get::<_, i64>(3) as u32,
                    failure_class: parse_failure_class(row.get::<_, String>(4)),
                    error_type: row.get(5),
                    message: row.get(6),
                    stack: row.get(7),
                    retryable: row.get(8),
                    timestamp: row.get(9),
                })
            })
            .collect()
    }

    fn counts(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<BTreeMap<String, u32>> {
        let mut client = self.pool.get()?;
        let rows = client.query(sql, params)?;
        let mut counts = BTreeMap::new();
        for row in rows {
            counts.insert(row.get::<_, String>(0), row.get::<_, i64>(1) as u32);
        }
        Ok(counts)
    }

    fn scalar_count(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u32> {
        let mut client = self.pool.get()?;
        let row = client.query_one(sql, params)?;
        Ok(row.get::<_, i64>(0) as u32)
    }
}

impl Store for PostgresStore {
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
    fn start_case(&self, req: ListCasesRequest) -> Result<Option<CaseRecord>> {
        self.start_case(req)
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
    fn heartbeat_step(&self, req: HeartbeatStepRequest) -> Result<()> {
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

/// Render a `timestamptz` column as RFC3339 text in-DB so row mapping mirrors sqlite's
/// text timestamps. Postgres `to_char` lacks a `T`/`Z` literal escape that round-trips
/// cleanly, so format the offset explicitly to match `chrono`'s `to_rfc3339`.
fn ts_text(col: &str) -> String {
    format!("to_char({col} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"') AS {col}")
}

fn ts_to_rfc3339(ts: SystemTime) -> String {
    DateTime::<Utc>::from(ts).to_rfc3339()
}

fn case_from_row(row: Row) -> Result<CaseRecord> {
    Ok(CaseRecord {
        run_id: row.get(0),
        batch_name: row.get(1),
        input_digest: row.get(2),
        label: row.get(3),
        status: row.get(4),
        attempt: row.get::<_, i64>(5) as u32,
        input: parse_required(row.get::<_, String>(6))?,
        output: parse_optional(row.get::<_, Option<String>>(7))?,
        error: parse_optional(row.get::<_, Option<String>>(8))?,
        started_at: row.get(9),
        completed_at: row.get(10),
    })
}

fn trace_from_row(row: Row) -> Result<TraceEventRecord> {
    Ok(TraceEventRecord {
        id: row.get(0),
        run_id: row.get(1),
        batch_name: row.get(2),
        case_id: row.get(3),
        attempt: row.get::<_, i64>(4) as u32,
        event_index: row.get::<_, i64>(5) as u32,
        event_type: row.get(6),
        payload: parse_required(row.get::<_, String>(7))?,
        artifact_ids: parse_required(row.get::<_, String>(8))?,
        created_at: row.get(9),
    })
}

fn parse_required<T: DeserializeOwned>(json: String) -> Result<T> {
    Ok(serde_json::from_str(&json)?)
}

fn parse_optional<T: DeserializeOwned>(json: Option<String>) -> Result<Option<T>> {
    json.map(|j| parse_required(j)).transpose()
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

    fn store() -> PostgresStore {
        let url = std::env::var("DURABLE_EVALS_TEST_PG_URL")
            .expect("set DURABLE_EVALS_TEST_PG_URL to a live Postgres url");
        let store = PostgresStore::connect(&url).expect("connect postgres store");
        // Each ignored test reuses one database; isolate by run_id, but clear tables first.
        let mut client = store.pool.get().expect("pool get");
        client
            .batch_execute(
                "TRUNCATE runs, step_states, failure_records, artifacts, batches,
                 batch_cases, memos, variants, workers, trace_events, reviews",
            )
            .expect("truncate");
        store
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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

    fn register(store: &PostgresStore, cases: Vec<BatchCaseInput>) -> BatchSummary {
        store
            .register_batch(RegisterBatchRequest {
                run_id: "run".to_string(),
                batch_name: "batch".to_string(),
                cases,
            })
            .expect("register batch")
    }

    #[test]
    #[ignore]
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
    #[ignore]
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

        let summary = register(&store, vec![case("b", json!({"x": 2}))]);
        assert_eq!(summary.total, 1);
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.succeeded, 0);

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

    #[test]
    #[ignore]
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
