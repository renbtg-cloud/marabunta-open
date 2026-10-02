// Marabunta - Licensed under the MIT License.
//! Protocol definitions for inter-component communication

pub mod codec;
pub mod coordinator_master;
pub mod master_worker;
pub mod messages;

pub use codec::*;
pub use coordinator_master::*;
pub use master_worker::*;
// Re-export messages explicitly to avoid ambiguous glob re-exports
// (messages.rs also defines CoordinatorToMaster and MasterToCoordinator)
pub use messages::{
    LogEntry, MasterToWorker, MessageEnvelope, MessagePayload, RaftCommand, RaftMessage,
    WorkerToMaster,
};
