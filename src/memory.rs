use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Thread-safe in-memory key-value store.

#[derive(Clone)]
pub struct Entry {
    value: String,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expires_at
            .is_some_and(|expiry| expiry <= Instant::now())
    }
}

/// The Mutex is inside the struct so callers just need `&self`.
/// Wrap in `Arc` to share across async tasks.
pub struct Store {
    data: Mutex<HashMap<String, Entry>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut data = self.data.lock().unwrap();
        match data.get(key).cloned() {
            Some(entry) => {
                if entry.is_expired() {
                    data.remove(key);
                    None
                } else {
                    Some(entry.value)
                }
            }
            None => None,
        }
    }

    pub fn set(&self, key: &str, value: &str, expires_after: Option<Duration>) {
        self.data.lock().unwrap().insert(
            key.to_string(),
            Entry {
                value: value.to_string(),
                expires_at: expires_after.map(|duration| Instant::now() + duration),
            },
        );
    }

    pub fn del(&self, key: &str) -> bool {
        let mut data = self.data.lock().unwrap();
        match data.get(key) {
            Some(entry) if entry.is_expired() => {
                data.remove(key);
                false // was already logically gone
            }
            Some(_) => data.remove(key).is_some(),
            None => false,
        }
    }

    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn update_expiry(&self, key: &str, expires_after: Duration) -> bool {
        let mut data = self.data.lock().unwrap();
        match data.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                entry.expires_at = Some(Instant::now() + expires_after);
                true
            }
            Some(_) => {
                data.remove(key);
                false
            } // expired
            None => false,
        }
    }

    pub fn get_pttl(&self, key: &str) -> (bool, Option<Duration>) {
        let mut data = self.data.lock().unwrap();
        match data.get(key).cloned() {
            Some(entry) => match entry.expires_at {
                Some(expires_at) => {
                    let remaining = expires_at.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        data.remove(key);
                        (false, None)
                    } else {
                        (true, Some(remaining))
                    }
                }
                None => (true, None),
            },
            None => (false, None),
        }
    }
}
