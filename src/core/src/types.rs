use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Health {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginStepRequest {
    pub run_id: String,
    pub step_name: String,
    pub input_digest: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub variants: BTreeMap<String, String>,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub lease_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteStepRequest {
    pub run_id: String,
    pub step_name: String,
    pub input_digest: String,
    #[serde(default)]
    pub output: Value,
    #[serde(default)]
    pub artifacts: Vec<ArtifactInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailStepRequest {
    pub run_id: String,
    pub step_name: String,
    pub input_digest: String,
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
pub struct BatchCaseInput {
    pub input_digest: String,
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterBatchRequest {
    pub run_id: String,
    pub batch_name: String,
    pub cases: Vec<BatchCaseInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteCaseRequest {
    pub run_id: String,
    pub batch_name: String,
    pub input_digest: String,
    pub output: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailCaseRequest {
    pub run_id: String,
    pub batch_name: String,
    pub input_digest: String,
    pub error: ErrorInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListCasesRequest {
    pub run_id: String,
    pub batch_name: String,
    #[serde(default)]
    pub statuses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseRecord {
    pub run_id: String,
    pub batch_name: String,
    pub input_digest: String,
    #[serde(default)]
    pub label: Option<String>,
    pub status: String,
    pub attempt: u32,
    pub input: Value,
    pub output: Option<Value>,
    pub error: Option<ErrorInfo>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSummary {
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
    #[serde(default = "default_terminal_classes")]
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
            terminal: default_terminal_classes(),
        }
    }
}

fn default_max_attempts() -> u32 {
    2
}

fn default_retryable_classes() -> Vec<FailureClass> {
    vec![
        FailureClass::Transient,
        FailureClass::ResourceUnavailable,
        FailureClass::UserCodeError,
    ]
}

fn default_terminal_classes() -> Vec<FailureClass> {
    vec![FailureClass::TerminalEval]
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Transient,
    ResourceUnavailable,
    TerminalEval,
    UserCodeError,
    RunnerError,
    ArtifactError,
}

impl Default for FailureClass {
    fn default() -> Self {
        Self::UserCodeError
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
pub struct FailureRecord {
    pub run_id: String,
    pub step_name: String,
    pub input_digest: String,
    pub attempt: u32,
    pub failure_class: FailureClass,
    pub error_type: String,
    pub message: String,
    pub stack: Option<String>,
    pub retryable: bool,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactInput {
    pub artifact_id: String,
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub inline_json: Option<Value>,
    pub sha256: String,
    pub size: u64,
    #[serde(default)]
    pub valid: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub artifact_id: String,
    pub run_id: String,
    pub step_name: String,
    pub input_digest: String,
    pub name: String,
    pub kind: String,
    pub path: Option<String>,
    pub inline_json: Option<Value>,
    pub sha256: String,
    pub size: u64,
    pub valid: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterVariantsRequest {
    pub run_id: String,
    pub dimension: String,
    pub variants: Vec<VariantInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantInput {
    pub name: String,
    #[serde(default)]
    pub config: Value,
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantRecord {
    pub run_id: String,
    pub dimension: String,
    pub name: String,
    pub config: Value,
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterWorkerRequest {
    pub worker_id: String,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub process_id: Option<u32>,
    #[serde(default)]
    pub resources: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRecord {
    pub worker_id: String,
    pub hostname: Option<String>,
    pub process_id: Option<u32>,
    pub resources: Value,
    pub heartbeat_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatStepRequest {
    pub run_id: String,
    pub step_name: String,
    pub input_digest: String,
    pub worker_id: String,
    pub lease_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEventRequest {
    pub run_id: String,
    pub batch_name: String,
    pub case_id: String,
    pub attempt: u32,
    pub event_type: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub artifact_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEventRecord {
    pub id: i64,
    pub run_id: String,
    pub batch_name: String,
    pub case_id: String,
    pub attempt: u32,
    pub event_index: u32,
    pub event_type: String,
    pub payload: Value,
    pub artifact_ids: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewRequest {
    pub run_id: String,
    pub batch_name: String,
    pub case_id: String,
    pub reviewer: String,
    pub decision: ReviewDecision,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDecision {
    PendingReview,
    ReviewedPass,
    ReviewedFail,
    ReviewedExcluded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewRecord {
    pub run_id: String,
    pub batch_name: String,
    pub case_id: String,
    pub reviewer: String,
    pub decision: ReviewDecision,
    pub note: Option<String>,
    pub timestamp: String,
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
    pub step_counts: BTreeMap<String, u32>,
    pub case_counts: BTreeMap<String, u32>,
    pub artifact_count: u32,
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
    CaseResultsJsonl,
    FailureReportJson,
    FailureReportCsv,
    AggregateMetricsJson,
    AggregateMetricsCsv,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportResponse {
    pub content_type: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepOutcome {
    Execute { attempt: u32 },
    SkipCompleted { output: Value },
    InProgress,
    FailedTerminal { error: ErrorInfo },
    RetryLater { retry_at: String },
}
