use std::sync::Arc;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::circuit::CircuitBreaker;
use super::retry::RetryPolicy;
use super::{ApiType, Provider, ProviderProfile, build_provider_from_profile};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteCandidate {
    pub profile_id: String,
    pub api_type: ApiType,
    pub model: String,
    pub base_url_sha256: String,
}

impl RouteCandidate {
    pub fn from_profile(profile: &ProviderProfile) -> Self {
        Self {
            profile_id: profile.id.clone(),
            api_type: profile.api_type,
            model: profile.model.clone(),
            base_url_sha256: format!("{:x}", Sha256::digest(profile.base_url.as_bytes())),
        }
    }

    pub fn matches(&self, profile: &ProviderProfile) -> bool {
        self.profile_id == profile.id
            && self.api_type == profile.api_type
            && self.model == profile.model
            && self.base_url_sha256 == format!("{:x}", Sha256::digest(profile.base_url.as_bytes()))
    }

    pub fn circuit_key(&self) -> String {
        format!("{}:{}", self.profile_id, self.model)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimeoutPolicy {
    pub connect_ms: u64,
    pub first_event_ms: u64,
    pub stream_idle_ms: u64,
    pub overall_ms: u64,
}

impl Default for TimeoutPolicy {
    fn default() -> Self {
        Self {
            connect_ms: 10_000,
            first_event_ms: 20_000,
            stream_idle_ms: 30_000,
            overall_ms: 300_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContextPolicySnapshot {
    pub token_budget: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteSnapshot {
    pub candidates: Vec<RouteCandidate>,
    pub retry_policy: RetryPolicy,
    pub timeout_policy: TimeoutPolicy,
    pub context_policy: ContextPolicySnapshot,
    pub config_generation: u64,
}

#[derive(Clone)]
pub struct FrozenRoute {
    pub snapshot: RouteSnapshot,
    pub providers: Vec<Arc<dyn Provider>>,
    pub circuit: Arc<CircuitBreaker>,
}

impl FrozenRoute {
    pub fn primary(&self) -> Arc<dyn Provider> {
        self.providers[0].clone()
    }

    pub fn restore(
        snapshot: RouteSnapshot,
        profiles: &[ProviderProfile],
        circuit: Arc<CircuitBreaker>,
    ) -> Result<Self> {
        let mut providers = Vec::new();
        for candidate in &snapshot.candidates {
            let profile = profiles
                .iter()
                .find(|profile| candidate.matches(profile))
                .ok_or_else(|| anyhow::anyhow!("原 run 的 Provider 配置不可用或已改变"))?;
            providers.push(Arc::from(build_provider_from_profile(profile)?));
        }
        if providers.is_empty() {
            bail!("route snapshot 没有候选 Provider");
        }
        Ok(Self {
            snapshot,
            providers,
            circuit,
        })
    }
}
