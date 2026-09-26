use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{ProviderErrorKind, TimeoutPhase};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RetryPolicy {
    pub rate_limit_retries: u32,
    pub transport_retries: u32,
    pub server_retries: u32,
    pub timeout_retries: u32,
    pub protocol_retries: u32,
    pub max_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            rate_limit_retries: 2,
            transport_retries: 2,
            server_retries: 2,
            timeout_retries: 1,
            protocol_retries: 0,
            max_backoff_ms: 5_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    Fail,
    Retry(Duration),
    Fallback,
}

impl RetryPolicy {
    pub fn decide(
        &self,
        kind: ProviderErrorKind,
        prior_retries: u32,
        retry_after_ms: Option<u64>,
        safe_to_replay: bool,
        more_candidates: bool,
    ) -> RetryDecision {
        if !safe_to_replay {
            return RetryDecision::Fail;
        }
        let budget = match kind {
            ProviderErrorKind::RateLimit => self.rate_limit_retries,
            ProviderErrorKind::Transport => self.transport_retries,
            ProviderErrorKind::Server => self.server_retries,
            ProviderErrorKind::Timeout(
                TimeoutPhase::Connect | TimeoutPhase::FirstEvent | TimeoutPhase::StreamIdle,
            ) => self.timeout_retries,
            ProviderErrorKind::Protocol => self.protocol_retries,
            _ => 0,
        };
        if budget > prior_retries {
            let exponential = 100u64.saturating_mul(1u64 << prior_retries.min(6));
            let jitter = (u64::from(prior_retries).wrapping_mul(47) + 31) % 67;
            let delay = retry_after_ms
                .unwrap_or(exponential.saturating_add(jitter))
                .min(self.max_backoff_ms);
            return RetryDecision::Retry(Duration::from_millis(delay));
        }
        let fallback_allowed = matches!(
            kind,
            ProviderErrorKind::RateLimit
                | ProviderErrorKind::Transport
                | ProviderErrorKind::Server
                | ProviderErrorKind::Timeout(
                    TimeoutPhase::Connect | TimeoutPhase::FirstEvent | TimeoutPhase::StreamIdle
                )
        ) || (kind == ProviderErrorKind::Protocol
            && self.protocol_retries > 0);
        if more_candidates && fallback_allowed {
            RetryDecision::Fallback
        } else {
            RetryDecision::Fail
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permanent_errors_never_retry_or_fallback() {
        let policy = RetryPolicy::default();
        for kind in [
            ProviderErrorKind::Auth,
            ProviderErrorKind::AccessDenied,
            ProviderErrorKind::InvalidRequest,
            ProviderErrorKind::ContentPolicy,
            ProviderErrorKind::QuotaExceeded,
        ] {
            assert_eq!(
                policy.decide(kind, 0, None, true, true),
                RetryDecision::Fail
            );
        }
    }

    #[test]
    fn retry_after_and_replay_gate_are_bounded() {
        let policy = RetryPolicy::default();
        assert_eq!(
            policy.decide(ProviderErrorKind::RateLimit, 0, Some(250), true, false),
            RetryDecision::Retry(Duration::from_millis(250))
        );
        assert_eq!(
            policy.decide(ProviderErrorKind::RateLimit, 2, None, true, true),
            RetryDecision::Fallback
        );
        assert_eq!(
            policy.decide(ProviderErrorKind::Server, 0, None, false, true),
            RetryDecision::Fail
        );
    }
}
