use serde::{Deserialize, Serialize};

use super::{ApiType, ProviderDiagnostic, ProviderErrorKind};
use crate::storage::RunId;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_creation_tokens: Option<u64>,
}

impl ProviderUsage {
    pub fn merge_partial(&mut self, other: &Self) {
        for (target, source) in [
            (&mut self.input_tokens, other.input_tokens),
            (&mut self.output_tokens, other.output_tokens),
            (&mut self.cache_read_tokens, other.cache_read_tokens),
            (&mut self.cache_creation_tokens, other.cache_creation_tokens),
        ] {
            if source.is_some() {
                *target = source;
            }
        }
    }

    pub fn add_attempt(&mut self, other: &Self) {
        for (target, source) in [
            (&mut self.input_tokens, other.input_tokens),
            (&mut self.output_tokens, other.output_tokens),
            (&mut self.cache_read_tokens, other.cache_read_tokens),
            (&mut self.cache_creation_tokens, other.cache_creation_tokens),
        ] {
            if let Some(value) = source {
                *target = Some(target.unwrap_or(0).saturating_add(value));
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Started,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderAttempt {
    pub attempt_id: String,
    pub run_id: RunId,
    pub round: u32,
    pub candidate_index: u32,
    pub provider_profile_id: String,
    pub api_type: ApiType,
    pub model: String,
    pub status: AttemptStatus,
    pub error_kind: Option<ProviderErrorKind>,
    pub diagnostic: Option<ProviderDiagnostic>,
    pub retry_after_ms: Option<u64>,
    pub stream_committed: bool,
    pub started_at_ms: i64,
    pub first_event_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub usage: Option<ProviderUsage>,
}
