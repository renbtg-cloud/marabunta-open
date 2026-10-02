// Marabunta - Licensed under the MIT License.
//! DNS-over-HTTPS tunneling transport — fallback 5 (last resort) for the Marabunta protocol.
//!
//! Frames are encoded as base64 in DNS TXT query/response payloads via DoH providers.
//! Very low bandwidth (~1 KB/s effective) — emergency only.

use super::{Transport, TransportError, TransportMethod};
use crate::marabunta::config::FRAME_SIZE;
use crate::marabunta::frame::{Frame, FrameCodec};
use async_trait::async_trait;
use base64::Engine;
use bytes::BytesMut;
use std::collections::VecDeque;
use tokio_util::codec::{Decoder, Encoder};

/// Maximum bytes per DNS TXT record value.
const DNS_TXT_MAX_BYTES: usize = 255;

/// Number of DNS queries needed per frame.
const QUERIES_PER_FRAME: usize = FRAME_SIZE.div_ceil(DNS_TXT_MAX_BYTES);

/// DoH transport implementation.
///
/// In production, this would use DNS-over-HTTPS providers (Cloudflare, Google)
/// to tunnel frame data as DNS TXT record queries and responses.
pub struct DohTransport {
    codec: FrameCodec,
    connected: bool,
    /// Inbound frame buffer.
    inbound: VecDeque<Frame>,
    /// Outbound encoded chunks (for testing).
    outbound: VecDeque<String>,
}

impl Default for DohTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl DohTransport {
    pub fn new() -> Self {
        Self {
            codec: FrameCodec,
            connected: false,
            inbound: VecDeque::new(),
            outbound: VecDeque::new(),
        }
    }

    /// Encode a frame as base64 DNS TXT chunks.
    pub fn encode_frame_to_dns_chunks(frame: &Frame) -> Result<Vec<String>, TransportError> {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();
        codec
            .encode(frame.clone(), &mut buf)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        let b64 = base64::engine::general_purpose::STANDARD.encode(&buf);
        let chunks: Vec<String> = b64
            .as_bytes()
            .chunks(DNS_TXT_MAX_BYTES)
            .map(|c| String::from_utf8_lossy(c).to_string())
            .collect();
        Ok(chunks)
    }

    /// Decode a frame from base64 DNS TXT chunks.
    pub fn decode_frame_from_dns_chunks(chunks: &[String]) -> Result<Frame, TransportError> {
        let b64: String = chunks.iter().map(|c| c.as_str()).collect();
        let data = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .map_err(|e| TransportError::ReceiveFailed(e.to_string()))?;

        let mut codec = FrameCodec;
        let mut buf = BytesMut::from(data.as_slice());
        match codec.decode(&mut buf) {
            Ok(Some(frame)) => Ok(frame),
            Ok(None) => Err(TransportError::ReceiveFailed("incomplete frame".into())),
            Err(e) => Err(TransportError::FrameError(e.to_string())),
        }
    }

    /// Inject a received frame (for testing).
    pub fn inject_inbound(&mut self, frame: Frame) {
        self.inbound.push_back(frame);
    }
}

#[async_trait]
impl Transport for DohTransport {
    async fn connect(&mut self, _endpoint: &str) -> Result<(), TransportError> {
        // In production: verify DoH provider is reachable
        self.connected = true;
        Ok(())
    }

    async fn send_frame(&mut self, frame: Frame) -> Result<(), TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }

        let chunks = Self::encode_frame_to_dns_chunks(&frame)?;
        // In production: send each chunk as a DNS TXT query to the DoH provider
        for chunk in chunks {
            self.outbound.push_back(chunk);
        }
        Ok(())
    }

    async fn recv_frame(&mut self) -> Result<Frame, TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }

        self.inbound
            .pop_front()
            .ok_or(TransportError::ReceiveFailed(
                "no frames available via DoH".into(),
            ))
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        self.connected = false;
        self.inbound.clear();
        self.outbound.clear();
        Ok(())
    }

    fn transport_type(&self) -> TransportMethod {
        TransportMethod::Doh
    }

    fn is_connected(&self) -> bool {
        self.connected
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marabunta::frame::FrameType;

    #[test]
    fn test_doh_initial_state() {
        let t = DohTransport::new();
        assert!(!t.is_connected());
        assert_eq!(t.transport_type(), TransportMethod::Doh);
    }

    #[test]
    fn test_encode_decode_dns_chunks() {
        let frame = Frame::new(FrameType::Gossip, 99, b"doh tunnel test".to_vec());
        let chunks = DohTransport::encode_frame_to_dns_chunks(&frame).unwrap();
        assert!(!chunks.is_empty());

        let decoded = DohTransport::decode_frame_from_dns_chunks(&chunks).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_chunks_cover_full_frame() {
        let frame = Frame::new(FrameType::JobChunk, 1, vec![0xAB; 500]);
        let chunks = DohTransport::encode_frame_to_dns_chunks(&frame).unwrap();
        // Base64 of 1024 bytes = 1368 chars, split into 255-char chunks = ~6 chunks
        assert!(chunks.len() >= 4);
    }

    #[tokio::test]
    async fn test_send_recv_with_inject() {
        let mut t = DohTransport::new();
        t.connect("doh://1.1.1.1").await.unwrap();

        let frame = Frame::new(FrameType::Gossip, 1, b"doh test".to_vec());
        t.inject_inbound(frame.clone());
        let received = t.recv_frame().await.unwrap();
        assert_eq!(received, frame);
    }

    #[tokio::test]
    async fn test_send_produces_outbound_chunks() {
        let mut t = DohTransport::new();
        t.connect("doh://1.1.1.1").await.unwrap();
        t.send_frame(Frame::heartbeat(1)).await.unwrap();
        assert!(!t.outbound.is_empty());
    }

    #[tokio::test]
    async fn test_close_clears() {
        let mut t = DohTransport::new();
        t.connect("doh://1.1.1.1").await.unwrap();
        t.inject_inbound(Frame::heartbeat(1));
        t.close().await.unwrap();
        assert!(!t.is_connected());
        assert!(t.inbound.is_empty());
    }
}
