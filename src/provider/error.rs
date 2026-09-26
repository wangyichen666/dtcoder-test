use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutPhase {
    Connect,
    FirstEvent,
    StreamIdle,
    Overall,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "phase", rename_all = "snake_case")]
pub enum ProviderErrorKind {
    Auth,
    AccessDenied,
    RateLimit,
    QuotaExceeded,
    InvalidRequest,
    ContextOverflow,
    ContentPolicy,
    Timeout(TimeoutPhase),
    Transport,
    Server,
    Protocol,
    EmptyCompletion,
    ReasoningOnly,
    OutputTruncated,
    Cancelled,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderDiagnostic {
    pub http_status: Option<u16>,
    pub upstream_code: Option<String>,
    pub request_id: Option<String>,
    pub retry_after_ms: Option<u64>,
    pub redacted_message: String,
}

#[derive(Clone, Debug, Error)]
#[error("Provider {kind:?}: {diagnostic_message}")]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub diagnostic: ProviderDiagnostic,
    diagnostic_message: &'static str,
}

impl ProviderError {
    pub fn new(kind: ProviderErrorKind, message: &'static str) -> Self {
        Self {
            kind,
            diagnostic: ProviderDiagnostic {
                redacted_message: message.into(),
                ..Default::default()
            },
            diagnostic_message: message,
        }
    }

    pub fn timeout(phase: TimeoutPhase) -> Self {
        Self::new(ProviderErrorKind::Timeout(phase), "Provider 等待超时")
    }

    pub fn from_reqwest(error: &reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::timeout(TimeoutPhase::Connect)
        } else {
            Self::new(ProviderErrorKind::Transport, "Provider 网络传输失败")
        }
    }

    pub fn from_http(
        status: reqwest::StatusCode,
        headers: &reqwest::header::HeaderMap,
        body: &str,
    ) -> Self {
        let code = serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|json| {
                json.pointer("/error/code")
                    .or_else(|| json.pointer("/error/type"))
                    .or_else(|| json.get("code"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .filter(|code| {
                code.len() <= 64
                    && code
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
            });
        let kind = match status.as_u16() {
            401 => ProviderErrorKind::Auth,
            403 => ProviderErrorKind::AccessDenied,
            429 if code.as_deref().is_some_and(|c| c.contains("quota")) => {
                ProviderErrorKind::QuotaExceeded
            }
            429 => ProviderErrorKind::RateLimit,
            400 if code
                .as_deref()
                .is_some_and(|c| c.contains("context") || c.contains("token")) =>
            {
                ProviderErrorKind::ContextOverflow
            }
            400 if code
                .as_deref()
                .is_some_and(|c| c.contains("content_policy") || c.contains("safety")) =>
            {
                ProviderErrorKind::ContentPolicy
            }
            400..=499 => ProviderErrorKind::InvalidRequest,
            _ => ProviderErrorKind::Server,
        };
        let retry_after_ms = headers
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_retry_after)
            .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64);
        let request_id = headers
            .get("x-request-id")
            .or_else(|| headers.get("request-id"))
            .and_then(|value| value.to_str().ok())
            .filter(|id| {
                id.len() <= 128
                    && id
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
            })
            .map(str::to_owned);
        let message = "Provider HTTP 请求失败";
        Self {
            kind,
            diagnostic: ProviderDiagnostic {
                http_status: Some(status.as_u16()),
                upstream_code: code,
                request_id,
                retry_after_ms,
                redacted_message: message.into(),
            },
            diagnostic_message: message,
        }
    }
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let instant = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    let delta = instant.timestamp().saturating_sub(now as i64);
    Some(Duration::from_secs(delta.max(0) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_http_without_leaking_body_or_key() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "2".parse().unwrap());
        let error = ProviderError::from_http(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            &headers,
            r#"{"error":{"code":"rate_limit","message":"secret-key-test"}}"#,
        );
        assert_eq!(error.kind, ProviderErrorKind::RateLimit);
        assert_eq!(error.diagnostic.retry_after_ms, Some(2000));
        assert!(!format!("{error:?}").contains("secret-key-test"));
    }
}
