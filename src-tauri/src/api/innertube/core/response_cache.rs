use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use serde_json::Value;

#[derive(Default)]
struct CacheState {
    entries: HashMap<String, (Instant, Arc<Value>)>,
    pending: HashMap<String, Weak<tokio::sync::Mutex<()>>>,
}

pub(crate) struct ResponseCache {
    state: Mutex<CacheState>,
    ttl: Duration,
    capacity: usize,
}

impl ResponseCache {
    pub(crate) fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            state: Mutex::new(CacheState::default()),
            ttl,
            capacity,
        }
    }

    pub(crate) async fn get_or_fetch<F, Fut, E>(&self, key: &str, fetch: F) -> Result<Arc<Value>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Value, E>>,
    {
        let gate = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.entries.retain(|_, (at, _)| at.elapsed() < self.ttl);
            if let Some((_, value)) = state.entries.get(key) {
                return Ok(Arc::clone(value));
            }
            state.pending.retain(|_, gate| gate.strong_count() > 0);
            if let Some(gate) = state.pending.get(key).and_then(Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(tokio::sync::Mutex::new(()));
                state.pending.insert(key.to_owned(), Arc::downgrade(&gate));
                gate
            }
        };

        let _guard = gate.lock().await;
        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((at, value)) = state.entries.get(key) {
                if at.elapsed() < self.ttl {
                    return Ok(Arc::clone(value));
                }
            }
        }

        let value = Arc::new(fetch().await?);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.entries.retain(|_, (at, _)| at.elapsed() < self.ttl);
        if self.capacity > 0 {
            while state.entries.len() >= self.capacity {
                let oldest = state
                    .entries
                    .iter()
                    .min_by_key(|(_, (at, _))| *at)
                    .map(|(key, _)| key.clone());
                if let Some(oldest) = oldest {
                    state.entries.remove(&oldest);
                }
            }
            state
                .entries
                .insert(key.to_owned(), (Instant::now(), Arc::clone(&value)));
        }
        Ok(value)
    }
}

impl Default for ResponseCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), 12)
    }
}
