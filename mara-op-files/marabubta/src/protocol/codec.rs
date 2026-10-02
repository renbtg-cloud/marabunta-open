// Marabunta - Licensed under the MIT License.
//! Message codec for length-prefixed binary framing

use bytes::{Buf, BufMut, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

use super::MessageEnvelope;

/// Length-prefixed codec for message framing
/// Format: [4 bytes length][JSON payload]
pub struct MessageCodec {
    max_message_size: usize,
}

impl MessageCodec {
    pub fn new() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024, // 16MB max
        }
    }

    pub fn with_max_size(max_message_size: usize) -> Self {
        Self { max_message_size }
    }
}

impl Default for MessageCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for MessageCodec {
    type Item = MessageEnvelope;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes for length
        if src.len() < 4 {
            return Ok(None);
        }

        // Read length prefix
        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // Validate length
        if length > self.max_message_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Message too large: {} bytes", length),
            ));
        }

        // Check if we have the full message
        if src.len() < 4 + length {
            // Reserve space for the rest
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // Consume length prefix
        src.advance(4);

        // Extract message bytes
        let data = src.split_to(length);

        // Deserialize
        let envelope: MessageEnvelope = serde_json::from_slice(&data).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("JSON error: {}", e))
        })?;

        Ok(Some(envelope))
    }
}

impl Encoder<MessageEnvelope> for MessageCodec {
    type Error = io::Error;

    fn encode(&mut self, item: MessageEnvelope, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Serialize to JSON
        let data = serde_json::to_vec(&item).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("JSON error: {}", e))
        })?;

        // Check size
        if data.len() > self.max_message_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Message too large: {} bytes", data.len()),
            ));
        }

        // Write length prefix + data
        dst.reserve(4 + data.len());
        dst.put_u32(data.len() as u32);
        dst.extend_from_slice(&data);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::*;
    use crate::protocol::{MessagePayload, WorkerToMaster};

    #[test]
    fn test_roundtrip() {
        let mut codec = MessageCodec::new();
        let mut buf = BytesMut::new();

        let envelope = MessageEnvelope::new(
            "test-source",
            "test-dest",
            MessagePayload::WorkerToMaster(WorkerToMaster::Heartbeat {
                worker_id: WorkerId::new(),
                status: WorkerStatus::Ready,
                load: 0.5,
                running_tasks: vec![],
                available_memory_mb: 8192,
                available_cpu: 4.0,
                thermal_state: "normal".to_string(),
            }),
        );

        // Encode
        codec.encode(envelope.clone(), &mut buf).unwrap();

        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(decoded.source, envelope.source);
        assert_eq!(decoded.destination, envelope.destination);
    }
}
