// Marabunta - Licensed under the MIT License.
//! LRU (Least Recently Used) cache implementation

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};
use parking_lot::RwLock;

use super::stats::CacheStats;
use super::traits::{Cache, CacheEntry};

/// A node in the LRU linked list
struct LruNode<K, V> {
    key: K,
    entry: CacheEntry<V>,
    prev: Option<usize>,
    next: Option<usize>,
}

/// Internal state of the LRU cache
struct LruState<K, V> {
    /// The storage map (key -> index in nodes)
    map: HashMap<K, usize>,
    /// Doubly-linked list nodes stored in a vector
    nodes: Vec<Option<LruNode<K, V>>>,
    /// Free list indices (reusable slots)
    free_list: Vec<usize>,
    /// Head of the LRU list (most recently used)
    head: Option<usize>,
    /// Tail of the LRU list (least recently used)
    tail: Option<usize>,
    /// Maximum number of entries
    max_size: usize,
}

impl<K: Clone + Eq + Hash, V: Clone> LruState<K, V> {
    fn new(max_size: usize) -> Self {
        Self {
            map: HashMap::with_capacity(max_size),
            nodes: Vec::with_capacity(max_size),
            free_list: Vec::new(),
            head: None,
            tail: None,
            max_size,
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn allocate_index(&mut self) -> usize {
        if let Some(idx) = self.free_list.pop() {
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(None);
            idx
        }
    }

    fn free_index(&mut self, idx: usize) {
        self.nodes[idx] = None;
        self.free_list.push(idx);
    }

    fn remove_from_list(&mut self, idx: usize) {
        let node = self.nodes[idx].as_ref().unwrap();
        let prev = node.prev;
        let next = node.next;

        match prev {
            Some(p) => self.nodes[p].as_mut().unwrap().next = next,
            None => self.head = next,
        }

        match next {
            Some(n) => self.nodes[n].as_mut().unwrap().prev = prev,
            None => self.tail = prev,
        }
    }

    fn push_front(&mut self, idx: usize) {
        if let Some(node) = self.nodes[idx].as_mut() {
            node.prev = None;
            node.next = self.head;
        }

        if let Some(head) = self.head {
            self.nodes[head].as_mut().unwrap().prev = Some(idx);
        }

        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn move_to_front(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }
        self.remove_from_list(idx);
        self.push_front(idx);
    }

    fn evict_lru(&mut self) -> Option<(K, CacheEntry<V>)> {
        let tail_idx = self.tail?;

        self.remove_from_list(tail_idx);

        let node = self.nodes[tail_idx].take()?;
        self.map.remove(&node.key);
        self.free_index(tail_idx);

        Some((node.key, node.entry))
    }
}

/// LRU cache with configurable maximum size
pub struct LruCache<K, V> {
    state: RwLock<LruState<K, V>>,
    stats: CacheStats,
}

impl<K, V> LruCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Create a new LRU cache with the specified maximum size
    pub fn new(max_size: usize) -> Self {
        assert!(max_size > 0, "LRU cache must have a max_size > 0");
        Self {
            state: RwLock::new(LruState::new(max_size)),
            stats: CacheStats::new(),
        }
    }

    /// Get the maximum size of the cache
    pub fn max_size(&self) -> usize {
        self.state.read().max_size
    }

    /// Get the current statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Resize the cache (may cause evictions if shrinking)
    pub fn resize(&self, new_max_size: usize) -> usize {
        assert!(new_max_size > 0, "LRU cache must have a max_size > 0");
        let mut state = self.state.write();
        state.max_size = new_max_size;

        let mut evicted = 0;
        while state.len() > new_max_size {
            if state.evict_lru().is_some() {
                self.stats.record_eviction();
                evicted += 1;
            } else {
                break;
            }
        }
        evicted
    }

    /// Get multiple values at once
    pub fn get_many(&self, keys: &[K]) -> Vec<Option<V>> {
        let start = Instant::now();
        let mut state = self.state.write();
        let results: Vec<Option<V>> = keys
            .iter()
            .map(|key| {
                if let Some(&idx) = state.map.get(key) {
                    if let Some(node) = state.nodes[idx].as_mut() {
                        if node.entry.is_expired() {
                            // Remove expired entry
                            let key = node.key.clone();
                            state.map.remove(&key);
                            state.remove_from_list(idx);
                            state.free_index(idx);
                            self.stats.record_miss();
                            self.stats.record_expiration();
                            None
                        } else {
                            node.entry.record_access();
                            self.stats.record_hit();
                            Some(node.entry.value.clone())
                        }
                    } else {
                        self.stats.record_miss();
                        None
                    }
                } else {
                    self.stats.record_miss();
                    None
                }
            })
            .collect();

        // Move accessed entries to front (batch operation)
        for key in keys {
            if let Some(&idx) = state.map.get(key) {
                state.move_to_front(idx);
            }
        }

        self.stats.record_operation_time(start.elapsed());
        results
    }

    /// Set multiple values at once
    pub fn set_many(&self, entries: Vec<(K, V)>) {
        let start = Instant::now();
        for (key, value) in entries {
            self.set(key, value);
        }
        self.stats.record_operation_time(start.elapsed());
    }

    /// Peek at a value without updating access time
    pub fn peek(&self, key: &K) -> Option<V> {
        let state = self.state.read();
        if let Some(&idx) = state.map.get(key) {
            if let Some(node) = state.nodes[idx].as_ref() {
                if !node.entry.is_expired() {
                    return Some(node.entry.value.clone());
                }
            }
        }
        None
    }

    /// Get all entries (for debugging/testing)
    pub fn entries(&self) -> Vec<(K, V)> {
        let state = self.state.read();
        state
            .map
            .iter()
            .filter_map(|(k, &idx)| {
                state.nodes[idx].as_ref().map(|node| {
                    (k.clone(), node.entry.value.clone())
                })
            })
            .collect()
    }
}

impl<K, V> Cache<K, V> for LruCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Clone + Send + Sync,
{
    fn get(&self, key: &K) -> Option<V> {
        let start = Instant::now();
        let mut state = self.state.write();

        if let Some(&idx) = state.map.get(key) {
            let expired = state.nodes[idx].as_ref().is_some_and(|n| n.entry.is_expired());
            if expired {
                if let Some(node) = state.nodes[idx].as_ref() {
                    let key = node.key.clone();
                    state.map.remove(&key);
                }
                state.remove_from_list(idx);
                state.free_index(idx);
                self.stats.record_miss();
                self.stats.record_expiration();
                self.stats.record_operation_time(start.elapsed());
                return None;
            }

            let value = if let Some(node) = state.nodes[idx].as_mut() {
                node.entry.record_access();
                Some(node.entry.value.clone())
            } else {
                None
            };

            if value.is_some() {
                state.move_to_front(idx);
                self.stats.record_hit();
                self.stats.record_operation_time(start.elapsed());
                return value;
            }
        }

        self.stats.record_miss();
        self.stats.record_operation_time(start.elapsed());
        None
    }

    fn get_entry(&self, key: &K) -> Option<CacheEntry<V>> {
        let start = Instant::now();
        let mut state = self.state.write();

        if let Some(&idx) = state.map.get(key) {
            let expired = state.nodes[idx].as_ref().is_some_and(|n| n.entry.is_expired());
            if expired {
                if let Some(node) = state.nodes[idx].as_ref() {
                    let key = node.key.clone();
                    state.map.remove(&key);
                }
                state.remove_from_list(idx);
                state.free_index(idx);
                self.stats.record_miss();
                self.stats.record_expiration();
                self.stats.record_operation_time(start.elapsed());
                return None;
            }

            let entry = if let Some(node) = state.nodes[idx].as_mut() {
                node.entry.record_access();
                Some(node.entry.clone())
            } else {
                None
            };

            if entry.is_some() {
                state.move_to_front(idx);
                self.stats.record_hit();
                self.stats.record_operation_time(start.elapsed());
                return entry;
            }
        }

        self.stats.record_miss();
        self.stats.record_operation_time(start.elapsed());
        None
    }

    fn set(&self, key: K, value: V) {
        let start = Instant::now();
        let mut state = self.state.write();

        // Check if key already exists
        if let Some(&idx) = state.map.get(&key) {
            if let Some(node) = state.nodes[idx].as_mut() {
                node.entry = CacheEntry::new(value);
                state.move_to_front(idx);
                self.stats.record_insert();
                self.stats.record_operation_time(start.elapsed());
                return;
            }
        }

        // Evict if at capacity
        if state.len() >= state.max_size
            && state.evict_lru().is_some() {
                self.stats.record_eviction();
            }

        // Insert new entry
        let idx = state.allocate_index();
        let node = LruNode {
            key: key.clone(),
            entry: CacheEntry::new(value),
            prev: None,
            next: None,
        };
        state.nodes[idx] = Some(node);
        state.map.insert(key, idx);
        state.push_front(idx);

        self.stats.record_insert();
        self.stats.record_operation_time(start.elapsed());
    }

    fn set_with_ttl(&self, key: K, value: V, ttl: Duration) {
        let start = Instant::now();
        let mut state = self.state.write();

        // Check if key already exists
        if let Some(&idx) = state.map.get(&key) {
            if let Some(node) = state.nodes[idx].as_mut() {
                node.entry = CacheEntry::with_ttl(value, ttl);
                state.move_to_front(idx);
                self.stats.record_insert();
                self.stats.record_operation_time(start.elapsed());
                return;
            }
        }

        // Evict if at capacity
        if state.len() >= state.max_size
            && state.evict_lru().is_some() {
                self.stats.record_eviction();
            }

        // Insert new entry
        let idx = state.allocate_index();
        let node = LruNode {
            key: key.clone(),
            entry: CacheEntry::with_ttl(value, ttl),
            prev: None,
            next: None,
        };
        state.nodes[idx] = Some(node);
        state.map.insert(key, idx);
        state.push_front(idx);

        self.stats.record_insert();
        self.stats.record_operation_time(start.elapsed());
    }

    fn delete(&self, key: &K) -> Option<V> {
        let start = Instant::now();
        let mut state = self.state.write();

        if let Some(idx) = state.map.remove(key) {
            state.remove_from_list(idx);
            if let Some(node) = state.nodes[idx].take() {
                state.free_index(idx);
                self.stats.record_delete();
                self.stats.record_operation_time(start.elapsed());
                return Some(node.entry.value);
            }
        }

        self.stats.record_operation_time(start.elapsed());
        None
    }

    fn clear(&self) {
        let start = Instant::now();
        let mut state = self.state.write();
        let max_size = state.max_size;
        *state = LruState::new(max_size);
        self.stats.record_operation_time(start.elapsed());
    }

    fn len(&self) -> usize {
        self.state.read().len()
    }

    fn keys(&self) -> Vec<K> {
        self.state.read().map.keys().cloned().collect()
    }

    fn evict_expired(&self) -> usize {
        let start = Instant::now();
        let mut state = self.state.write();
        let mut evicted = 0;

        let expired_keys: Vec<K> = state
            .map
            .iter()
            .filter_map(|(key, &idx)| {
                state.nodes[idx]
                    .as_ref()
                    .filter(|node| node.entry.is_expired())
                    .map(|_| key.clone())
            })
            .collect();

        for key in expired_keys {
            if let Some(idx) = state.map.remove(&key) {
                state.remove_from_list(idx);
                state.free_index(idx);
                self.stats.record_expiration();
                evicted += 1;
            }
        }

        self.stats.record_operation_time(start.elapsed());
        evicted
    }
}
