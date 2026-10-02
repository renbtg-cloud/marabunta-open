// Marabunta - Licensed under the MIT License.
//! Storage abstractions for cluster state, checkpoints, and jobs
//!
//! This module provides various storage backends and abstractions:
//!
//! - [`traits`] - Core storage traits for state, checkpoints, and jobs
//! - [`local`] - Local filesystem and in-memory implementations
//! - [`persistence`] - Generic persistence layer with namespace support
//! - [`sqlite`] - SQLite-backed persistence implementation

pub mod local;
pub mod persistence;
pub mod sqlite;
pub mod traits;

pub use local::*;
pub use persistence::{PersistenceBackend, PersistenceError, TypedStore};
pub use sqlite::SqliteBackend;
pub use traits::*;
