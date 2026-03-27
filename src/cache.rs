use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

pub struct Cache<K, V> {
    entries: HashMap<K, V>,
    order: VecDeque<K>,
    capacity: usize,
}

impl<K: Eq + Hash + Clone, V> Cache<K, V> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "cache capacity must be greater than zero");
        Self {
            entries: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if self.entries.contains_key(key) {
            self.promote(key);
            self.entries.get(key)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.entries.contains_key(&key) {
            self.remove_from_order(&key);
        } else if self.entries.len() >= self.capacity {
            self.evict_oldest();
        }

        self.entries.insert(key.clone(), value);
        self.order.push_front(key);
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.entries.remove(key) {
            self.remove_from_order(key);
            Some(value)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn promote(&mut self, key: &K) {
        self.remove_from_order(key);
        self.order.push_front(key.clone());
    }

    fn evict_oldest(&mut self) {
        if let Some(old_key) = self.order.pop_back() {
            self.entries.remove(&old_key);
        }
    }

    fn remove_from_order(&mut self, key: &K) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
    }
}
