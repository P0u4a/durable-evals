use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Health {
    pub ok: bool,
}

/// A unit of work is content-addressed by `(run_id, kind, input_digest)`. A `kind` is
/// the name of either a step (a group of one) or a dataset (a pre-registered set of
/// tasks) — e.g. "score" or "telecom". The same begin/complete/fail path serves both.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginRequest {
    pub run_id: String,
    pub kind: String,
    pub input_digest: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub lease_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub run_id: String,
    pub kind: String,
    pub input_digest: String,
    #[serde(default)]
    pub attempt: Option<u32>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub output: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailRequest {
    pub run_id: String,
    pub kind: String,
    pub input_digest: String,
    #[serde(default)]
    pub attempt: Option<u32>,
    #[serde(default)]
    pub worker_id: Option<String>,
    pub error: ErrorInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRunRequest {
    pub run_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub config: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInput {
    pub input_digest: String,
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
}

/// Pre-register a known set of tasks under a `kind` (a "dataset"). Steps skip this and
/// let `begin` upsert their single task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterDatasetRequest {
    pub run_id: String,
    pub kind: String,
    pub tasks: Vec<TaskInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRequest {
    pub run_id: String,
    pub kind: String,
    #[serde(default)]
    pub statuses: Vec<String>,
    /// Optional category filter; empty means every category.
    #[serde(default)]
    pub categories: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub run_id: String,
    pub kind: String,
    pub input_digest: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    pub status: String,
    pub attempt: u32,
    pub input: Value,
    pub output: Option<Value>,
    pub error: Option<ErrorInfo>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSummary {
    pub total: u32,
    pub pending: u32,
    pub running: u32,
    pub succeeded: u32,
    pub failed: u32,
    pub terminal: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub base_delay_ms: u64,
    #[serde(default)]
    pub max_delay_ms: u64,
    #[serde(default)]
    pub jitter: bool,
    #[serde(default = "default_retryable_classes")]
    pub retryable: Vec<FailureClass>,
    /// Classes forced terminal even if they also appear in `retryable`. Empty by default.
    #[serde(default)]
    pub terminal: Vec<FailureClass>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 2,
            base_delay_ms: 0,
            max_delay_ms: 0,
            jitter: false,
            retryable: default_retryable_classes(),
            terminal: Vec::new(),
        }
    }
}

impl RetryPolicy {
    /// Delay in milliseconds before a failed task on `attempt` (1-based) may retry.
    /// Exponential backoff `base_delay_ms * 2^(attempt-1)`, capped at `max_delay_ms`,
    /// with optional jitter in `[0.5, 1.0)` derived from `jitter_seed` so we avoid
    /// pulling in a random-number dependency. A `base_delay_ms` of 0 disables backoff
    /// and the task retries immediately.
    pub fn backoff_delay_ms(&self, attempt: u32, jitter_seed: u64) -> u64 {
        if self.base_delay_ms == 0 {
            return 0;
        }
        let exp = attempt.saturating_sub(1).min(32);
        let mut delay = self.base_delay_ms.saturating_mul(1u64 << exp);
        if self.max_delay_ms > 0 {
            delay = delay.min(self.max_delay_ms);
        }
        if self.jitter && delay > 0 {
            // Map the seed to a fraction in [0.5, 1.0) and scale the delay.
            let frac = 0.5 + ((jitter_seed % 1000) as f64 / 1000.0) * 0.5;
            delay = ((delay as f64) * frac) as u64;
        }
        delay
    }
}

fn default_max_attempts() -> u32 {
    2
}

fn default_retryable_classes() -> Vec<FailureClass> {
    vec![
        FailureClass::Transient,
        FailureClass::ResourceUnavailable,
        FailureClass::EvalException,
    ]
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    /// An ephemeral hiccup (network blip, HTTP 500). Retrying will likely succeed.
    Transient,
    /// A dependency is temporarily down or exhausted (store unreachable, quota hit,
    /// filesystem unavailable). Retry later.
    ResourceUnavailable,
    /// An exception raised inside the user's eval/step callback.
    EvalException,
    /// A failure in the durable-evals harness itself (runtime/server, serialization,
    /// orchestration). Not the eval's fault; terminal by default.
    DurableHarnessError,
    /// A deterministic artifact failure such as a hash mismatch or corrupt content.
    /// Transient storage outages should be reported as `ResourceUnavailable` instead.
    ArtifactError,
}

impl Default for FailureClass {
    fn default() -> Self {
        Self::EvalException
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub error_type: String,
    pub message: String,
    #[serde(default)]
    pub failure_class: FailureClass,
    #[serde(default)]
    pub stack: Option<String>,
    #[serde(default)]
    pub retryable: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub run_id: String,
    pub kind: String,
    pub input_digest: String,
    pub worker_id: String,
    pub lease_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEventRequest {
    pub run_id: String,
    pub kind: String,
    pub task_id: String,
    pub attempt: u32,
    pub event_type: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub artifact_ids: Vec<String>,
}

/// Query for trace events. `run_id` is required; every other field narrows the
/// result server-side. Omit them all to fetch every trace event for the run, or
/// set `task_id` to fetch a single task's events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTraceEventsRequest {
    pub run_id: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub attempt: Option<u32>,
    #[serde(default)]
    pub event_type: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEventRecord {
    pub id: i64,
    pub run_id: String,
    pub kind: String,
    pub task_id: String,
    pub attempt: u32,
    pub event_index: u32,
    pub event_type: String,
    pub payload: Value,
    pub artifact_ids: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoGetRequest {
    pub run_id: String,
    pub key_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoPutRequest {
    pub run_id: String,
    pub key_digest: String,
    #[serde(default)]
    pub key: Value,
    #[serde(default)]
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoGetResponse {
    pub found: bool,
    #[serde(default)]
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryRequest {
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    pub run_id: String,
    pub name: Option<String>,
    pub config: Value,
    pub config_digest: String,
    pub task_counts: BTreeMap<String, u32>,
    pub failure_counts: BTreeMap<String, u32>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportRequest {
    pub run_id: String,
    pub kind: ExportKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportKind {
    ManifestJson,
    TaskResultsJsonl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportResponse {
    pub content_type: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outcome {
    Execute { attempt: u32 },
    SkipCompleted { output: Value },
    InProgress,
    FailedTerminal { error: ErrorInfo },
    RetryLater { retry_at: String },
}
