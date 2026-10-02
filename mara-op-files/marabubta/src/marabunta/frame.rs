// Marabunta - Licensed under the MIT License.
//! Fixed 1024-byte frame codec for the Marabunta protocol.
//!
//! Every frame on the wire is exactly FRAME_SIZE (1024) bytes. The codec handles
//! encoding frames with padding and decoding them from fixed-size wire format.

use crate::marabunta::config::{FRAME_HEADER_SIZE, FRAME_SIZE, MAX_PAYLOAD_SIZE};
use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

/// Frame types on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// Gossip protocol message.
    Gossip = 0x01,
    /// Gossip summary (inter-neighborhood).
    GossipSummary = 0x02,
    /// Job routing message.
    JobRoute = 0x10,
    /// Job data chunk.
    JobChunk = 0x11,
    /// Job result.
    JobResult = 0x12,
    /// Heartbeat / keepalive.
    Heartbeat = 0x20,
    /// Relay forwarding.
    Relay = 0x30,
    /// Padding frame (traffic shaping).
    Pad = 0xFF,
}

impl FrameType {
    /// Convert from wire byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Gossip),
            0x02 => Some(Self::GossipSummary),
            0x10 => Some(Self::JobRoute),
            0x11 => Some(Self::JobChunk),
            0x12 => Some(Self::JobResult),
            0x20 => Some(Self::Heartbeat),
            0x30 => Some(Self::Relay),
            0xFF => Some(Self::Pad),
            _ => None,
        }
    }

    /// Convert to wire byte.
    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

/// A fixed-size protocol frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub sequence: u32,
    pub payload: Vec<u8>,
}

impl Frame {
    /// Create a new frame with the given type, sequence, and payload.
    ///
    /// Payload is truncated to MAX_PAYLOAD_SIZE if too large.
    pub fn new(frame_type: FrameType, sequence: u32, payload: Vec<u8>) -> Self {
        let payload = if payload.len() > MAX_PAYLOAD_SIZE {
            payload[..MAX_PAYLOAD_SIZE].to_vec()
        } else {
            payload
        };
        Self {
            frame_type,
            sequence,
            payload,
        }
    }

    /// Create a PAD frame (traffic shaping filler).
    pub fn pad(sequence: u32) -> Self {
        Self {
            frame_type: FrameType::Pad,
            sequence,
            payload: Vec::new(),
        }
    }

    /// Create a heartbeat frame.
    pub fn heartbeat(sequence: u32) -> Self {
        Self {
            frame_type: FrameType::Heartbeat,
            sequence,
            payload: Vec::new(),
        }
    }
}

/// Frame codec errors.
#[derive(Debug, Error)]
pub enum FrameError {
    #[error("unknown frame type: 0x{0:02x}")]
    UnknownFrameType(u8),

    #[error("payload too large: {0} bytes (max {MAX_PAYLOAD_SIZE})")]
    PayloadTooLarge(usize),

    #[error("incomplete frame: need {FRAME_SIZE} bytes, got {0}")]
    IncompleteFrame(usize),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Codec that encodes/decodes fixed 1024-byte frames.
///
/// Wire format: type(1B) + sequence(4B) + payload_len(2B) + payload + zero-padding to 1024B.
#[derive(Debug, Default)]
pub struct FrameCodec;

impl Encoder<Frame> for FrameCodec {
    type Error = FrameError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if frame.payload.len() > MAX_PAYLOAD_SIZE {
            return Err(FrameError::PayloadTooLarge(frame.payload.len()));
        }

        dst.reserve(FRAME_SIZE);

        // Header: type(1) + sequence(4) + payload_len(2)
        dst.put_u8(frame.frame_type.to_byte());
        dst.put_u32(frame.sequence);
        dst.put_u16(frame.payload.len() as u16);

        // Payload
        dst.put_slice(&frame.payload);

        // Zero-pad to exactly FRAME_SIZE
        let padding_len = FRAME_SIZE - FRAME_HEADER_SIZE - frame.payload.len();
        dst.put_bytes(0, padding_len);

        Ok(())
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < FRAME_SIZE {
            return Ok(None); // Need more data
        }

        let mut frame_bytes = src.split_to(FRAME_SIZE);

        // Parse header
        let type_byte = frame_bytes.get_u8();
        let frame_type =
            FrameType::from_byte(type_byte).ok_or(FrameError::UnknownFrameType(type_byte))?;
        let sequence = frame_bytes.get_u32();
        let payload_len = frame_bytes.get_u16() as usize;

        if payload_len > MAX_PAYLOAD_SIZE {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }

        let payload = frame_bytes[..payload_len].to_vec();
        // Remaining bytes are padding — we just discard them

        Ok(Some(Frame {
            frame_type,
            sequence,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut codec = FrameCodec;
        let frame = Frame::new(FrameType::Gossip, 42, b"hello marabunta".to_vec());

        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();
        assert_eq!(buf.len(), FRAME_SIZE);

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_all_frame_types_roundtrip() {
        let types = [
            FrameType::Gossip,
            FrameType::GossipSummary,
            FrameType::JobRoute,
            FrameType::JobChunk,
            FrameType::JobResult,
            FrameType::Heartbeat,
            FrameType::Relay,
            FrameType::Pad,
        ];

        for (i, ft) in types.iter().enumerate() {
            let mut codec = FrameCodec;
            let frame = Frame::new(*ft, i as u32, vec![i as u8; 10]);
            let mut buf = BytesMut::new();
            codec.encode(frame.clone(), &mut buf).unwrap();
            assert_eq!(buf.len(), FRAME_SIZE);
            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(decoded, frame);
        }
    }

    #[test]
    fn test_frame_always_1024_bytes() {
        let mut codec = FrameCodec;

        // Empty payload
        let mut buf = BytesMut::new();
        codec
            .encode(Frame::new(FrameType::Gossip, 0, vec![]), &mut buf)
            .unwrap();
        assert_eq!(buf.len(), FRAME_SIZE);

        // Max payload
        let mut buf = BytesMut::new();
        codec
            .encode(
                Frame::new(FrameType::Gossip, 0, vec![0xAB; MAX_PAYLOAD_SIZE]),
                &mut buf,
            )
            .unwrap();
        assert_eq!(buf.len(), FRAME_SIZE);
    }

    #[test]
    fn test_pad_frame() {
        let mut codec = FrameCodec;
        let frame = Frame::pad(99);
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();
        assert_eq!(buf.len(), FRAME_SIZE);

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.frame_type, FrameType::Pad);
        assert_eq!(decoded.sequence, 99);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn test_heartbeat_frame() {
        let mut codec = FrameCodec;
        let frame = Frame::heartbeat(7);
        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.frame_type, FrameType::Heartbeat);
        assert_eq!(decoded.sequence, 7);
    }

    #[test]
    fn test_payload_too_large_rejected() {
        let mut codec = FrameCodec;
        let oversized = vec![0u8; MAX_PAYLOAD_SIZE + 1];
        let frame = Frame {
            frame_type: FrameType::Gossip,
            sequence: 0,
            payload: oversized,
        };
        let mut buf = BytesMut::new();
        assert!(codec.encode(frame, &mut buf).is_err());
    }

    #[test]
    fn test_incomplete_frame_returns_none() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::from(&[0u8; FRAME_SIZE - 1][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_unknown_frame_type_rejected() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::from(&[0u8; FRAME_SIZE][..]);
        // First byte 0x00 is not a valid frame type
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_multiple_frames_in_buffer() {
        let mut codec = FrameCodec;
        let f1 = Frame::new(FrameType::Gossip, 1, b"first".to_vec());
        let f2 = Frame::new(FrameType::JobRoute, 2, b"second".to_vec());

        let mut buf = BytesMut::new();
        codec.encode(f1.clone(), &mut buf).unwrap();
        codec.encode(f2.clone(), &mut buf).unwrap();
        assert_eq!(buf.len(), FRAME_SIZE * 2);

        let d1 = codec.decode(&mut buf).unwrap().unwrap();
        let d2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(d1, f1);
        assert_eq!(d2, f2);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_frame_type_byte_roundtrip() {
        for b in 0..=255u8 {
            if let Some(ft) = FrameType::from_byte(b) {
                assert_eq!(ft.to_byte(), b);
            }
        }
    }

    #[test]
    fn test_payload_truncation_in_constructor() {
        let oversized = vec![0xAA; MAX_PAYLOAD_SIZE + 100];
        let frame = Frame::new(FrameType::Gossip, 0, oversized);
        assert_eq!(frame.payload.len(), MAX_PAYLOAD_SIZE);
    }
}
