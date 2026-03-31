use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::watch::Receiver;

/// Thread-safe in-memory key-value store.

#[derive(Clone)]
pub enum RedisValue {
    String(String),
    List(VecDeque<String>),
    Hash(HashMap<String, String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    WrongType,
}

#[derive(Clone)]
pub struct Entry {
    value: RedisValue,
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

    fn purge_if_expired(data: &mut HashMap<String, Entry>, key: &str) -> bool {
        match data.get(key) {
            Some(entry) if entry.is_expired() => {
                data.remove(key);
                true
            }
            _ => false,
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, StoreError> {
        let mut data = self.data.lock().unwrap();
        Self::purge_if_expired(&mut data, key);

        match data.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::String(value) => Ok(Some(value.clone())),
                _ => Err(StoreError::WrongType),
            },
            None => Ok(None),
        }
    }

    pub fn set(&self, key: &str, value: &str, expires_after: Option<Duration>) {
        self.data.lock().unwrap().insert(
            key.to_string(),
            Entry {
                value: RedisValue::String(value.to_string()),
                expires_at: expires_after.map(|duration| Instant::now() + duration),
            },
        );
    }

    pub fn del(&self, key: &str) -> bool {
        let mut data = self.data.lock().unwrap();
        if Self::purge_if_expired(&mut data, key) {
            return false;
        }

        data.remove(key).is_some()
    }

    pub fn exists(&self, key: &str) -> bool {
        let mut data = self.data.lock().unwrap();
        Self::purge_if_expired(&mut data, key);
        data.contains_key(key)
    }

    pub fn update_expiry(&self, key: &str, expires_after: Duration) -> bool {
        let mut data = self.data.lock().unwrap();
        if Self::purge_if_expired(&mut data, key) {
            return false;
        }

        match data.get_mut(key) {
            Some(entry) => {
                entry.expires_at = Some(Instant::now() + expires_after);
                true
            }
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

    pub fn lpush(&self, key: &str, values: &[String]) -> Result<i64, StoreError> {
        let mut data = self.data.lock().unwrap();
        Self::purge_if_expired(&mut data, key);

        match data.get_mut(key) {
            Some(entry) => match &mut entry.value {
                RedisValue::List(list) => {
                    for value in values {
                        list.push_front(value.clone());
                    }
                    Ok(list.len() as i64)
                }
                _ => Err(StoreError::WrongType),
            },
            None => {
                let mut list = VecDeque::with_capacity(values.len());
                for value in values {
                    list.push_front(value.clone());
                }
                let len = list.len() as i64;
                data.insert(
                    key.to_string(),
                    Entry {
                        value: RedisValue::List(list),
                        expires_at: None,
                    },
                );
                Ok(len)
            }
        }
    }

    pub fn rpush(&self, key: &str, values: &[String]) -> Result<i64, StoreError> {
        let mut data = self.data.lock().unwrap();
        Self::purge_if_expired(&mut data, key);

        match data.get_mut(key) {
            Some(entry) => match &mut entry.value {
                RedisValue::List(list) => {
                    for value in values {
                        list.push_back(value.clone());
                    }
                    Ok(list.len() as i64)
                }
                _ => Err(StoreError::WrongType),
            },
            None => {
                let mut list = VecDeque::with_capacity(values.len());
                for value in values {
                    list.push_back(value.clone());
                }
                let len = list.len() as i64;
                data.insert(
                    key.to_string(),
                    Entry {
                        value: RedisValue::List(list),
                        expires_at: None,
                    },
                );
                Ok(len)
            }
        }
    }

    pub fn lpop(&self, key: &str) -> Result<Option<String>, StoreError> {
        let mut data = self.data.lock().unwrap();
        if Self::purge_if_expired(&mut data, key) {
            return Ok(None);
        }

        let Some(entry) = data.get_mut(key) else {
            return Ok(None);
        };

        let (value, should_remove_key) = match &mut entry.value {
            RedisValue::List(list) => {
                let value = list.pop_front();
                let should_remove_key = list.is_empty();
                (value, should_remove_key)
            }
            _ => return Err(StoreError::WrongType),
        };

        if should_remove_key {
            data.remove(key);
        }

        Ok(value)
    }

    pub fn rpop(&self, key: &str) -> Result<Option<String>, StoreError> {
        let mut data = self.data.lock().unwrap();
        if Self::purge_if_expired(&mut data, key) {
            return Ok(None);
        }

        let Some(entry) = data.get_mut(key) else {
            return Ok(None);
        };

        let (value, should_remove_key) = match &mut entry.value {
            RedisValue::List(list) => {
                let value = list.pop_back();
                let should_remove_key = list.is_empty();
                (value, should_remove_key)
            }
            _ => return Err(StoreError::WrongType),
        };

        if should_remove_key {
            data.remove(key);
        }

        Ok(value)
    }

    pub fn llen(&self, key: &str) -> Result<i64, StoreError> {
        let mut data = self.data.lock().unwrap();
        if Self::purge_if_expired(&mut data, key) {
            return Ok(0);
        }

        match data.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::List(list) => Ok(list.len() as i64),
                _ => Err(StoreError::WrongType),
            },
            None => Ok(0),
        }
    }

    pub fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<String>, StoreError> {
        let mut data = self.data.lock().unwrap();
        if Self::purge_if_expired(&mut data, key) {
            return Ok(Vec::new());
        }

        match data.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::List(list) => {
                    let Some((start, stop)) =
                        Self::normalize_lrange_bounds(list.len(), start, stop)
                    else {
                        return Ok(Vec::new());
                    };

                    Ok(list
                        .iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .cloned()
                        .collect())
                }
                _ => Err(StoreError::WrongType),
            },
            None => Ok(Vec::new()),
        }
    }

    pub fn hset(&self, key: &str, pairs: &[(String, String)]) -> Result<i64, StoreError> {
        let mut data = self.data.lock().unwrap();
        Self::purge_if_expired(&mut data, key);

        match data.get_mut(key) {
            Some(entry) => match &mut entry.value {
                RedisValue::Hash(hash) => {
                    let mut inserted = 0;
                    for (field, value) in pairs {
                        if hash.insert(field.clone(), value.clone()).is_none() {
                            inserted += 1;
                        }
                    }
                    Ok(inserted)
                }
                _ => Err(StoreError::WrongType),
            },
            None => {
                let mut hash = HashMap::with_capacity(pairs.len());
                let mut inserted = 0;
                for (field, value) in pairs {
                    if hash.insert(field.clone(), value.clone()).is_none() {
                        inserted += 1;
                    }
                }
                data.insert(
                    key.to_string(),
                    Entry {
                        value: RedisValue::Hash(hash),
                        expires_at: None,
                    },
                );
                Ok(inserted)
            }
        }
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>, StoreError> {
        let mut data = self.data.lock().unwrap();
        if Self::purge_if_expired(&mut data, key) {
            return Ok(None);
        }

        match data.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::Hash(hash) => Ok(hash.get(field).cloned()),
                _ => Err(StoreError::WrongType),
            },
            None => Ok(None),
        }
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>, StoreError> {
        let mut data = self.data.lock().unwrap();
        if Self::purge_if_expired(&mut data, key) {
            return Ok(Vec::new());
        }

        match data.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::Hash(hash) => Ok(hash
                    .iter()
                    .map(|(field, value)| (field.clone(), value.clone()))
                    .collect()),
                _ => Err(StoreError::WrongType),
            },
            None => Ok(Vec::new()),
        }
    }

    pub fn hdel(&self, key: &str, fields: &[String]) -> Result<i64, StoreError> {
        let mut data = self.data.lock().unwrap();
        if Self::purge_if_expired(&mut data, key) {
            return Ok(0);
        }

        let Some(entry) = data.get_mut(key) else {
            return Ok(0);
        };

        let (removed, should_remove_key) = match &mut entry.value {
            RedisValue::Hash(hash) => {
                let mut removed = 0;
                for field in fields {
                    if hash.remove(field).is_some() {
                        removed += 1;
                    }
                }
                (removed, hash.is_empty())
            }
            _ => return Err(StoreError::WrongType),
        };

        if should_remove_key {
            data.remove(key);
        }

        Ok(removed)
    }

    fn normalize_lrange_bounds(len: usize, start: isize, stop: isize) -> Option<(usize, usize)> {
        if len == 0 {
            return None;
        }

        let len = len as isize;
        let mut start = if start < 0 { len + start } else { start };
        let mut stop = if stop < 0 { len + stop } else { stop };

        if start < 0 {
            start = 0;
        }
        if stop < 0 || start >= len {
            return None;
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop {
            return None;
        }

        Some((start as usize, stop as usize))
    }
}
