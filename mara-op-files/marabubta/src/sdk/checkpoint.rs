// Marabunta - Licensed under the MIT License.
//! Checkpointing for fault-tolerant task execution
//!
//! Allows tasks to save and restore state, enabling:
//! - Recovery from crashes
//! - Task migration between workers
//! - Pause and resume functionality

use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::sdk::errors::*;

/// Maximum checkpoint data size (256 MB)
pub const MAX_CHECKPOINT_SIZE: usize = 256 * 1024 * 1024;

/// Checkpoint storage for a task
#[derive(Debug)]
pub struct CheckpointStorage {
    /// Directory where checkpoints are stored
    storage_dir: PathBuf,
    /// Task ID for namespacing
    task_id: u64,
    /// Cached checkpoint data (for fast access)
    cached_data: RwLock<Option<Vec<u8>>>,
    /// Checksum of cached data
    cached_checksum: RwLock<Option<String>>,
}

impl CheckpointStorage {
    /// Create new checkpoint storage for a task
    pub fn new(storage_dir: PathBuf, task_id: u64) -> Self {
        Self {
            storage_dir,
            task_id,
            cached_data: RwLock::new(None),
            cached_checksum: RwLock::new(None),
        }
    }

    /// Get the checkpoint file path
    fn checkpoint_path(&self) -> PathBuf {
        self.storage_dir
            .join(format!("task_{}.checkpoint", self.task_id))
    }

    /// Get the checksum file path
    fn checksum_path(&self) -> PathBuf {
        self.storage_dir
            .join(format!("task_{}.checksum", self.task_id))
    }

    /// Compute SHA256 checksum of data
    fn compute_checksum(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    /// Save checkpoint data
    ///
    /// Returns MARABUNTA_OK on success, or an error code on failure.
    pub fn save(&self, data: &[u8]) -> i32 {
        if data.len() > MAX_CHECKPOINT_SIZE {
            return MARABUNTA_ERR_BUFFER_TOO_SMALL;
        }

        // Ensure directory exists
        if fs::create_dir_all(&self.storage_dir).is_err() {
            return MARABUNTA_ERR_IO;
        }

        let checksum = Self::compute_checksum(data);

        // Write data file
        let path = self.checkpoint_path();
        let mut file = match fs::File::create(&path) {
            Ok(f) => f,
            Err(_) => return MARABUNTA_ERR_IO,
        };
        if file.write_all(data).is_err() {
            return MARABUNTA_ERR_IO;
        }
        if file.sync_all().is_err() {
            return MARABUNTA_ERR_IO;
        }

        // Write checksum file
        let checksum_path = self.checksum_path();
        let mut checksum_file = match fs::File::create(&checksum_path) {
            Ok(f) => f,
            Err(_) => return MARABUNTA_ERR_IO,
        };
        if checksum_file.write_all(checksum.as_bytes()).is_err() {
            return MARABUNTA_ERR_IO;
        }
        if checksum_file.sync_all().is_err() {
            return MARABUNTA_ERR_IO;
        }

        // Update cache
        *self.cached_data.write() = Some(data.to_vec());
        *self.cached_checksum.write() = Some(checksum);

        MARABUNTA_OK
    }

    /// Load checkpoint data into a buffer
    ///
    /// Returns the actual size of the checkpoint data on success,
    /// or a negative error code on failure.
    pub fn load(&self, buffer: &mut [u8]) -> i64 {
        // Check cache first
        {
            let cached = self.cached_data.read();
            if let Some(data) = cached.as_ref() {
                if buffer.len() < data.len() {
                    return MARABUNTA_ERR_BUFFER_TOO_SMALL as i64;
                }
                buffer[..data.len()].copy_from_slice(data);
                return data.len() as i64;
            }
        }

        // Load from disk
        let path = self.checkpoint_path();
        let checksum_path = self.checksum_path();

        if !path.exists() || !checksum_path.exists() {
            return MARABUNTA_ERR_CHECKPOINT_NOT_FOUND as i64;
        }

        // Read checksum
        let expected_checksum = match fs::read_to_string(&checksum_path) {
            Ok(s) => s,
            Err(_) => return MARABUNTA_ERR_IO as i64,
        };

        // Read data
        let mut file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return MARABUNTA_ERR_IO as i64,
        };

        let metadata = match file.metadata() {
            Ok(m) => m,
            Err(_) => return MARABUNTA_ERR_IO as i64,
        };

        let data_len = metadata.len() as usize;
        if buffer.len() < data_len {
            return MARABUNTA_ERR_BUFFER_TOO_SMALL as i64;
        }

        if file.read_exact(&mut buffer[..data_len]).is_err() {
            return MARABUNTA_ERR_IO as i64;
        }

        // Verify checksum
        let actual_checksum = Self::compute_checksum(&buffer[..data_len]);
        if actual_checksum != expected_checksum {
            return MARABUNTA_ERR_IO as i64; // Corruption detected
        }

        // Update cache
        *self.cached_data.write() = Some(buffer[..data_len].to_vec());
        *self.cached_checksum.write() = Some(actual_checksum);

        data_len as i64
    }

    /// Check if a checkpoint exists
    pub fn exists(&self) -> bool {
        // Check cache
        if self.cached_data.read().is_some() {
            return true;
        }

        // Check disk
        self.checkpoint_path().exists() && self.checksum_path().exists()
    }

    /// Get the size of the checkpoint data without loading it
    pub fn size(&self) -> Option<usize> {
        // Check cache
        if let Some(data) = self.cached_data.read().as_ref() {
            return Some(data.len());
        }

        // Check disk
        let path = self.checkpoint_path();
        if path.exists() {
            if let Ok(metadata) = fs::metadata(&path) {
                return Some(metadata.len() as usize);
            }
        }

        None
    }

    /// Delete the checkpoint
    pub fn delete(&self) -> i32 {
        // Clear cache
        *self.cached_data.write() = None;
        *self.cached_checksum.write() = None;

        // Delete files (ignore errors if they don't exist)
        let _ = fs::remove_file(self.checkpoint_path());
        let _ = fs::remove_file(self.checksum_path());

        MARABUNTA_OK
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    fn create_test_storage() -> (TempDir, CheckpointStorage) {
        let temp_dir = TempDir::new().unwrap();
        let storage = CheckpointStorage::new(temp_dir.path().to_path_buf(), 12345);
        (temp_dir, storage)
    }

    #[test]
    fn test_save_and_load() {
        let (_temp_dir, storage) = create_test_storage();

        let data = b"Hello, checkpoint!";
        assert_eq!(storage.save(data), MARABUNTA_OK);

        let mut buffer = vec![0u8; 1024];
        let len = storage.load(&mut buffer);
        assert_eq!(len, data.len() as i64);
        assert_eq!(&buffer[..data.len()], data);
    }

    #[test]
    fn test_exists() {
        let (_temp_dir, storage) = create_test_storage();

        assert!(!storage.exists());

        storage.save(b"test data");
        assert!(storage.exists());
    }

    #[test]
    fn test_load_nonexistent() {
        let (_temp_dir, storage) = create_test_storage();

        let mut buffer = vec![0u8; 1024];
        let result = storage.load(&mut buffer);
        assert_eq!(result, MARABUNTA_ERR_CHECKPOINT_NOT_FOUND as i64);
    }

    #[test]
    fn test_buffer_too_small() {
        let (_temp_dir, storage) = create_test_storage();

        let data = b"This is a longer piece of data";
        storage.save(data);

        let mut small_buffer = vec![0u8; 5];
        let result = storage.load(&mut small_buffer);
        assert_eq!(result, MARABUNTA_ERR_BUFFER_TOO_SMALL as i64);
    }

    #[test]
    fn test_overwrite() {
        let (_temp_dir, storage) = create_test_storage();

        storage.save(b"First data");
        storage.save(b"Second data, different length");

        let mut buffer = vec![0u8; 1024];
        let len = storage.load(&mut buffer);
        assert_eq!(len, 29);
        assert_eq!(&buffer[..29], b"Second data, different length");
    }

    #[test]
    fn test_delete() {
        let (_temp_dir, storage) = create_test_storage();

        storage.save(b"test data");
        assert!(storage.exists());

        assert_eq!(storage.delete(), MARABUNTA_OK);
        assert!(!storage.exists());
    }

    #[test]
    fn test_size() {
        let (_temp_dir, storage) = create_test_storage();

        assert!(storage.size().is_none());

        let data = b"test data 123";
        storage.save(data);
        assert_eq!(storage.size(), Some(data.len()));
    }

    #[test]
    fn test_large_data() {
        let (_temp_dir, storage) = create_test_storage();

        // Test with 1MB of data
        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        assert_eq!(storage.save(&data), MARABUNTA_OK);

        let mut buffer = vec![0u8; data.len()];
        let len = storage.load(&mut buffer);
        assert_eq!(len, data.len() as i64);
        assert_eq!(buffer, data);
    }

    #[test]
    fn test_binary_data() {
        let (_temp_dir, storage) = create_test_storage();

        // Test with binary data including null bytes
        let data: Vec<u8> = vec![0, 1, 2, 0, 255, 254, 0, 128, 127];
        assert_eq!(storage.save(&data), MARABUNTA_OK);

        let mut buffer = vec![0u8; 100];
        let len = storage.load(&mut buffer);
        assert_eq!(len, data.len() as i64);
        assert_eq!(&buffer[..data.len()], &data[..]);
    }

    #[test]
    fn test_concurrent_access() {
        let (_temp_dir, storage) = create_test_storage();
        let storage = Arc::new(storage);

        // Initial save
        storage.save(b"initial data");

        let mut handles = vec![];

        // Multiple readers
        for _ in 0..5 {
            let storage_clone = Arc::clone(&storage);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _ = storage_clone.exists();
                    let mut buffer = vec![0u8; 1024];
                    let _ = storage_clone.load(&mut buffer);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_cache_is_used() {
        let (temp_dir, storage) = create_test_storage();

        let data = b"cached data";
        storage.save(data);

        // Delete the file but keep the cache
        let path = temp_dir.path().join(format!("task_{}.checkpoint", 12345));
        fs::remove_file(&path).unwrap();

        // Should still be able to load from cache
        let mut buffer = vec![0u8; 100];
        let len = storage.load(&mut buffer);
        assert_eq!(len, data.len() as i64);
    }

    #[test]
    fn test_checksum_verification() {
        let (temp_dir, storage) = create_test_storage();

        let data = b"original data";
        storage.save(data);

        // Clear the cache
        *storage.cached_data.write() = None;
        *storage.cached_checksum.write() = None;

        // Corrupt the data file
        let path = temp_dir.path().join(format!("task_{}.checkpoint", 12345));
        fs::write(&path, b"corrupted!").unwrap();

        // Load should fail due to checksum mismatch
        let mut buffer = vec![0u8; 100];
        let result = storage.load(&mut buffer);
        assert_eq!(result, MARABUNTA_ERR_IO as i64);
    }
}
