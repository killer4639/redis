use std::collections::HashMap;
use std::sync::Mutex;

/// Thread-safe in-memory key-value store.
///
/// The Mutex is inside the struct so callers just need `&self`.
/// Wrap in `Arc` to share across async tasks.
pub struct Store {
    data: Mutex<HashMap<String, String>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.data.lock().unwrap().get(key).cloned()
    }

    pub fn set(&self, key: &str, value: &str) {
        self.data.lock().unwrap().insert(key.to_string(), value.to_string());
    }

    pub fn del(&self, key: &str) -> bool {
        self.data.lock().unwrap().remove(key).is_some()
    }

    pub fn exists(&self, key: &str) -> bool {
        self.data.lock().unwrap().contains_key(key)
    }
}
