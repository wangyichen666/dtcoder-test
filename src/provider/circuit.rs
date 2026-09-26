use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

pub struct CircuitBreaker {
    entries: Mutex<HashMap<String, Entry>>,
    open_duration: Duration,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            open_duration: Duration::from_secs(30),
        }
    }
}

struct Entry {
    failures: u32,
    opened_at: Option<Instant>,
    probe_active: bool,
}

impl CircuitBreaker {
    pub fn acquire(&self, key: &str) -> CircuitState {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.entry(key.to_owned()).or_insert(Entry {
            failures: 0,
            opened_at: None,
            probe_active: false,
        });
        match entry.opened_at {
            Some(at) if at.elapsed() < self.open_duration || entry.probe_active => {
                CircuitState::Open
            }
            Some(_) => {
                entry.probe_active = true;
                CircuitState::HalfOpen
            }
            None => CircuitState::Closed,
        }
    }

    pub fn record(&self, key: &str, success: bool, transient_failure: bool) -> CircuitState {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.entry(key.to_owned()).or_insert(Entry {
            failures: 0,
            opened_at: None,
            probe_active: false,
        });
        entry.probe_active = false;
        if success {
            entry.failures = 0;
            entry.opened_at = None;
            return CircuitState::Closed;
        }
        if transient_failure {
            entry.failures = entry.failures.saturating_add(1);
        } else {
            entry.failures = 0;
            entry.opened_at = None;
        }
        if entry.failures >= 3 {
            entry.opened_at = Some(Instant::now());
            CircuitState::Open
        } else {
            CircuitState::Closed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn opens_after_three_transient_failures() {
        let breaker = CircuitBreaker::default();
        for _ in 0..2 {
            assert_eq!(breaker.record("a", false, true), CircuitState::Closed);
        }
        assert_eq!(breaker.record("a", false, true), CircuitState::Open);
        assert_eq!(breaker.acquire("a"), CircuitState::Open);
        assert_eq!(breaker.acquire("b"), CircuitState::Closed);
    }

    #[test]
    fn half_open_allows_one_probe_and_success_closes() {
        let breaker = CircuitBreaker {
            entries: Mutex::new(HashMap::new()),
            open_duration: Duration::from_millis(1),
        };
        for _ in 0..3 {
            breaker.record("a", false, true);
        }
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(breaker.acquire("a"), CircuitState::HalfOpen);
        assert_eq!(breaker.acquire("a"), CircuitState::Open);
        assert_eq!(breaker.record("a", true, false), CircuitState::Closed);
        assert_eq!(breaker.acquire("a"), CircuitState::Closed);
    }
}
