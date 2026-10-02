// Marabunta - Licensed under the MIT License.
//! Two-tier cache with memory + optional disk spillover

use std::collections::HashMap;
use std::hash::Hash;
use std::io::{Read as IoRead, Write as IoWrite};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Serialize};

use super::lru::LruCache;
use super::stats::CacheStats;
use super::traits::{Cache, CacheEntry, CacheError};

/// Configuration for disk spillover
#[derive(Debug, Clone)]
pub struct DiskSpillConfig {
    /// Directory for disk spillover files
    pub directory: PathBuf,
    /// Maximum disk cache size in bytes
    pub max_size_bytes: u64,
    /// Whether to compress disk entries
    pub compress: bool,
    /// File extension for cache files
    pub file_extension: String,
}

impl Default for DiskSpillConfig {
    fn default() -> Self {
        Self {
            directory: std::env::temp_dir().join("marabunta-cache"),
            max_size_bytes: 1024 * 1024 * 1024, // 1GB
            compress: true,
            file_extension: "cache".to_string(),
        }
    }
}

impl DiskSpillConfig {
    /// Create a new disk spill config with the specified directory
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            ..Default::default()
        }
    }

    /// Set the maximum disk cache size
    pub fn with_max_size(mut self, max_size_bytes: u64) -> Self {
        self.max_size_bytes = max_size_bytes;
        self
    }

    /// Enable or disable compression
    pub fn with_compression(mut self, compress: bool) -> Self {
        self.compress = compress;
        self
    }
}

/// Disk cache entry metadata
#[derive(Debug)]
struct DiskEntry {
    file_path: PathBuf,
    size_bytes: u64,
    created_at: Instant,
    ttl: Option<Duration>,
}

impl DiskEntry {
    fn is_expired(&self) -> bool {
        match self.ttl {
            Some(ttl) => self.created_at.elapsed() > ttl,
            None => false,
        }
    }
}

/// Internal disk cache state
struct DiskState<K> {
    /// Map of keys to disk entries
    entries: HashMap<K, DiskEntry>,
    /// Total bytes used on disk
    total_bytes: u64,
    /// Configuration
    config: DiskSpillConfig,
}

impl<K: Clone + Eq + Hash> DiskState<K> {
    fn new(config: DiskSpillConfig) -> std::io::Result<Self> {
        // Create the directory if it doesn't exist
        std::fs::create_dir_all(&config.directory)?;

        Ok(Self {
            entries: HashMap::new(),
            total_bytes: 0,
            config,
        })
    }

    fn evict_oldest(&mut self) -> Option<K> {
        let oldest_key = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.created_at)
            .map(|(k, _)| k.clone());

        if let Some(ref key) = oldest_key {
            if let Some(entry) = self.entries.remove(key) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.size_bytes);
                let _ = std::fs::remove_file(&entry.file_path);
            }
        }

        oldest_key
    }

    fn evict_expired(&mut self) -> usize {
        let expired_keys: Vec<K> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(k, _)| k.clone())
            .collect();

        let count = expired_keys.len();
        for key in expired_keys {
            if let Some(entry) = self.entries.remove(&key) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.size_bytes);
                let _ = std::fs::remove_file(&entry.file_path);
            }
        }
        count
    }
}

/// Two-tier cache with memory (L1) and optional disk (L2) layers
pub struct TwoTierCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Clone + Send + Sync,
{
    /// L1: In-memory LRU cache
    memory: LruCache<K, V>,
    /// L2: Optional disk spillover
    disk: Option<RwLock<DiskState<K>>>,
    /// Combined statistics
    stats: CacheStats,
    /// Whether values can be serialized to disk
    serializable: bool,
}

impl<K, V> TwoTierCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString,
    V: Clone + Send + Sync,
{
    /// Create a new two-tier cache with only memory layer
    pub fn memory_only(memory_size: usize) -> Self {
        Self {
            memory: LruCache::new(memory_size),
            disk: None,
            stats: CacheStats::new(),
            serializable: false,
        }
    }

    /// Get the current statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get the memory cache statistics
    pub fn memory_stats(&self) -> &CacheStats {
        self.memory.stats()
    }

    /// Get memory cache size
    pub fn memory_len(&self) -> usize {
        self.memory.len()
    }

    /// Get disk cache size (number of entries)
    pub fn disk_len(&self) -> usize {
        self.disk
            .as_ref()
            .map(|d| d.read().entries.len())
            .unwrap_or(0)
    }

    /// Get total disk bytes used
    pub fn disk_bytes(&self) -> u64 {
        self.disk
            .as_ref()
            .map(|d| d.read().total_bytes)
            .unwrap_or(0)
    }

    /// Check if disk layer is enabled
    pub fn has_disk_layer(&self) -> bool {
        self.disk.is_some()
    }

    /// Promote an entry from disk to memory
    fn promote_to_memory(&self, key: &K, value: V) {
        self.memory.set(key.clone(), value);
    }
}

impl<K, V> TwoTierCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString,
    V: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    /// Create a new two-tier cache with memory and disk layers
    pub fn with_disk(memory_size: usize, disk_config: DiskSpillConfig) -> Result<Self, CacheError> {
        let disk_state = DiskState::new(disk_config)?;
        Ok(Self {
            memory: LruCache::new(memory_size),
            disk: Some(RwLock::new(disk_state)),
            stats: CacheStats::new(),
            serializable: true,
        })
    }

    /// Write a value to disk
    fn write_to_disk(&self, key: &K, value: &V, ttl: Option<Duration>) -> Result<(), CacheError> {
        let Some(disk) = &self.disk else {
            return Err(CacheError::DiskSpillover("Disk layer not configured".into()));
        };

        let mut disk = disk.write();

        // Serialize the value
        let data = serde_json::to_vec(value).map_err(|e| CacheError::Serialization(e.to_string()))?;

        // Optionally compress
        let final_data = if disk.config.compress {
            use flate2::write::GzEncoder;
            use flate2::Compression;

            let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
            encoder
                .write_all(&data)
                .map_err(|e| CacheError::DiskSpillover(e.to_string()))?;
            encoder
                .finish()
                .map_err(|e| CacheError::DiskSpillover(e.to_string()))?
        } else {
            data
        };

        let size_bytes = final_data.len() as u64;

        // Check if we need to evict to make room
        while disk.total_bytes + size_bytes > disk.config.max_size_bytes {
            // First try to evict expired
            let expired = disk.evict_expired();
            if expired > 0 {
                for _ in 0..expired {
                    self.stats.record_expiration();
                }
                continue;
            }

            // Then evict oldest
            if disk.evict_oldest().is_some() {
                self.stats.record_eviction();
            } else {
                return Err(CacheError::CacheFull);
            }
        }

        // Generate file path
        let file_name = format!(
            "{}.{}",
            key.to_string().replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_"),
            disk.config.file_extension
        );
        let file_path = disk.config.directory.join(&file_name);

        // Write to file
        std::fs::write(&file_path, &final_data)?;

        // Update metadata
        if let Some(old_entry) = disk.entries.remove(key) {
            disk.total_bytes = disk.total_bytes.saturating_sub(old_entry.size_bytes);
            let _ = std::fs::remove_file(&old_entry.file_path);
        }

        disk.entries.insert(
            key.clone(),
            DiskEntry {
                file_path,
                size_bytes,
                created_at: Instant::now(),
                ttl,
            },
        );
        disk.total_bytes += size_bytes;

        Ok(())
    }

    /// Read a value from disk
    fn read_from_disk(&self, key: &K) -> Result<Option<V>, CacheError> {
        let Some(disk) = &self.disk else {
            return Ok(None);
        };

        let disk = disk.read();
        let Some(entry) = disk.entries.get(key) else {
            return Ok(None);
        };

        if entry.is_expired() {
            return Ok(None);
        }

        // Read the file
        let mut file =
            std::fs::File::open(&entry.file_path).map_err(CacheError::Io)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .map_err(CacheError::Io)?;

        // Optionally decompress
        let final_data = if disk.config.compress {
            use flate2::read::GzDecoder;

            let mut decoder = GzDecoder::new(&data[..]);
            let mut decompressed = Vec::new();
            decoder
                .read_to_end(&mut decompressed)
                .map_err(|e| CacheError::DiskSpillover(e.to_string()))?;
            decompressed
        } else {
            data
        };

        // Deserialize
        let value: V = serde_json::from_slice(&final_data)
            .map_err(|e| CacheError::Deserialization(e.to_string()))?;

        Ok(Some(value))
    }

    /// Delete a value from disk
    fn delete_from_disk(&self, key: &K) -> Option<()> {
        let disk = self.disk.as_ref()?;
        let mut disk = disk.write();

        if let Some(entry) = disk.entries.remove(key) {
            disk.total_bytes = disk.total_bytes.saturating_sub(entry.size_bytes);
            let _ = std::fs::remove_file(&entry.file_path);
            Some(())
        } else {
            None
        }
    }

    /// Spill entries from memory to disk when memory is full
    /// This is called automatically when memory eviction occurs
    pub fn spill_to_disk(&self, key: K, value: V, ttl: Option<Duration>) -> Result<(), CacheError> {
        if self.disk.is_some() {
            self.write_to_disk(&key, &value, ttl)?;
        }
        Ok(())
    }

    /// Evict expired entries from both layers
    pub fn evict_all_expired(&self) -> usize {
        let mut total = self.memory.evict_expired();

        if let Some(disk) = &self.disk {
            let expired = disk.write().evict_expired();
            for _ in 0..expired {
                self.stats.record_expiration();
            }
            total += expired;
        }

        total
    }

    /// Clear the disk cache
    pub fn clear_disk(&self) {
        if let Some(disk) = &self.disk {
            let mut disk = disk.write();
            for entry in disk.entries.values() {
                let _ = std::fs::remove_file(&entry.file_path);
            }
            disk.entries.clear();
            disk.total_bytes = 0;
        }
    }
}

impl<K, V> Cache<K, V> for TwoTierCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString,
    V: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    fn get(&self, key: &K) -> Option<V> {
        let start = Instant::now();

        // Try L1 (memory) first
        if let Some(value) = self.memory.get(key) {
            self.stats.record_hit();
            self.stats.record_operation_time(start.elapsed());
            return Some(value);
        }

        // Try L2 (disk) if available
        if self.disk.is_some() {
            match self.read_from_disk(key) {
                Ok(Some(value)) => {
                    // Promote to L1
                    self.promote_to_memory(key, value.clone());
                    self.stats.record_hit();
                    self.stats.record_operation_time(start.elapsed());
                    return Some(value);
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!("Disk read error for key {}: {}", key.to_string(), e);
                }
            }
        }

        self.stats.record_miss();
        self.stats.record_operation_time(start.elapsed());
        None
    }

    fn get_entry(&self, key: &K) -> Option<CacheEntry<V>> {
        // For simplicity, we only return entries from memory
        self.memory.get_entry(key)
    }

    fn set(&self, key: K, value: V) {
        let start = Instant::now();

        // Always write to memory
        self.memory.set(key.clone(), value.clone());

        // Optionally write to disk as well (write-through)
        if let Some(_disk) = &self.disk {
            // Only spill to disk if memory is getting full
            if self.memory.len() >= self.memory.max_size() / 2 {
                if let Err(e) = self.write_to_disk(&key, &value, None) {
                    tracing::warn!("Disk write error for key {}: {}", key.to_string(), e);
                }
            }
        }

        self.stats.record_insert();
        self.stats.record_operation_time(start.elapsed());
    }

    fn set_with_ttl(&self, key: K, value: V, ttl: Duration) {
        let start = Instant::now();

        // Write to memory with TTL
        self.memory.set_with_ttl(key.clone(), value.clone(), ttl);

        // Optionally write to disk
        if self.disk.is_some() && self.memory.len() >= self.memory.max_size() / 2 {
            if let Err(e) = self.write_to_disk(&key, &value, Some(ttl)) {
                tracing::warn!("Disk write error for key {}: {}", key.to_string(), e);
            }
        }

        self.stats.record_insert();
        self.stats.record_operation_time(start.elapsed());
    }

    fn delete(&self, key: &K) -> Option<V> {
        let start = Instant::now();

        // Delete from memory
        let memory_result = self.memory.delete(key);

        // Delete from disk
        self.delete_from_disk(key);

        if memory_result.is_some() {
            self.stats.record_delete();
        }
        self.stats.record_operation_time(start.elapsed());

        memory_result
    }

    fn clear(&self) {
        let start = Instant::now();

        self.memory.clear();
        self.clear_disk();

        self.stats.record_operation_time(start.elapsed());
    }

    fn len(&self) -> usize {
        self.memory.len() + self.disk_len()
    }

    fn keys(&self) -> Vec<K> {
        let mut keys = self.memory.keys();

        if let Some(disk) = &self.disk {
            let disk_keys: Vec<K> = disk
                .read()
                .entries
                .iter()
                .filter(|(_, e)| !e.is_expired())
                .map(|(k, _)| k.clone())
                .collect();
            keys.extend(disk_keys);
        }

        keys
    }

    fn evict_expired(&self) -> usize {
        self.evict_all_expired()
    }
}
