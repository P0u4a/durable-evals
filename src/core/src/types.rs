use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Health {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginStepRequest {
    pub run_id: String,
    pub step_name: String,
    pub input_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteStepRequest {
    pub run_id: String,
    pub step_name: String,
    pub input_digest: String,
    pub output: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailStepRequest {
    pub run_id: String,
    pub step_name: String,
    pub input_digest: String,
    pub error: ErrorInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub error_type: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepOutcome {
    Execute { attempt: u32 },
    SkipCompleted { output: Value },
    InProgress,
    FailedTerminal { error: ErrorInfo },
}
