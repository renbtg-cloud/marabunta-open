// Marabunta - Licensed under the MIT License.
//! WebRTC data channel transport — fallback 3 for the Marabunta protocol.
//!
//! Peer-to-peer data channels for direct node communication. Uses signaling
//! via any available transport for SDP exchange.

use super::{Transport, TransportError, TransportMethod};
use crate::marabunta::frame::{Frame, FrameCodec};
use async_trait::async_trait;
use bytes::BytesMut;
use std::collections::VecDeque;
use tokio::sync::mpsc;
use tokio_util::codec::{Decoder, Encoder};

/// WebRTC transport implementation.
///
/// Uses tokio channels to simulate data channel behavior.
/// In production, this would use a WebRTC library for STUN/TURN/ICE.
pub struct WebRtcTransport {
    codec: FrameCodec,
    connected: bool,
    /// Outbound frames queued for the peer.
    outbound: VecDeque<Vec<u8>>,
    /// Inbound frames from the peer.
    inbound_tx: Option<mpsc::Sender<Vec<u8>>>,
    inbound_rx: Option<mpsc::Receiver<Vec<u8>>>,
}

impl Default for WebRtcTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl WebRtcTransport {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(256);
        Self {
            codec: FrameCodec,
            connected: false,
            outbound: VecDeque::new(),
            inbound_tx: Some(tx),
            inbound_rx: Some(rx),
        }
    }

    /// Inject a frame into the inbound channel (for testing / signaling integration).
    pub fn inject_inbound(&self, data: Vec<u8>) -> Result<(), TransportError> {
        self.inbound_tx
            .as_ref()
            .ok_or(TransportError::NotConnected)?
            .try_send(data)
            .map_err(|e| TransportError::ReceiveFailed(e.to_string()))
    }
}

#[async_trait]
impl Transport for WebRtcTransport {
    async fn connect(&mut self, _endpoint: &str) -> Result<(), TransportError> {
        // In production: perform ICE candidate exchange via signaling channel
        // For now, mark as connected for local testing
        self.connected = true;
        Ok(())
    }

    async fn send_frame(&mut self, frame: Frame) -> Result<(), TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }

        let mut buf = BytesMut::new();
        self.codec
            .encode(frame, &mut buf)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        self.outbound.push_back(buf.to_vec());
        Ok(())
    }

    async fn recv_frame(&mut self) -> Result<Frame, TransportError> {
        let rx = self
            .inbound_rx
            .as_mut()
            .ok_or(TransportError::NotConnected)?;

        let data = rx
            .recv()
            .await
            .ok_or(TransportError::ConnectionClosed)?;

        let mut buf = BytesMut::from(data.as_slice());
        match self.codec.decode(&mut buf) {
            Ok(Some(frame)) => Ok(frame),
            Ok(None) => Err(TransportError::ReceiveFailed("incomplete frame".into())),
            Err(e) => Err(TransportError::FrameError(e.to_string())),
        }
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        self.connected = false;
        self.inbound_tx = None;
        self.inbound_rx = None;
        self.outbound.clear();
        Ok(())
    }

    fn transport_type(&self) -> TransportMethod {
        TransportMethod::WebRtc
    }

    fn is_connected(&self) -> bool {
        self.connected
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marabunta::config::FRAME_SIZE;
    use crate::marabunta::frame::FrameType;

    #[test]
    fn test_webrtc_initial_state() {
        let t = WebRtcTransport::new();
        assert!(!t.is_connected());
        assert_eq!(t.transport_type(), TransportMethod::WebRtc);
    }

    #[tokio::test]
    async fn test_send_recv_via_inject() {
        let mut t = WebRtcTransport::new();
        t.connect("local").await.unwrap();

        // Encode a frame manually and inject
        let frame = Frame::new(FrameType::Gossip, 42, b"webrtc test".to_vec());
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();
        assert_eq!(buf.len(), FRAME_SIZE);

        t.inject_inbound(buf.to_vec()).unwrap();
        let received = t.recv_frame().await.unwrap();
        assert_eq!(received, frame);
    }

    #[tokio::test]
    async fn test_send_queues_outbound() {
        let mut t = WebRtcTransport::new();
        t.connect("local").await.unwrap();
        t.send_frame(Frame::heartbeat(1)).await.unwrap();
        assert_eq!(t.outbound.len(), 1);
    }

    #[tokio::test]
    async fn test_close_clears_state() {
        let mut t = WebRtcTransport::new();
        t.connect("local").await.unwrap();
        t.send_frame(Frame::heartbeat(1)).await.unwrap();
        t.close().await.unwrap();
        assert!(!t.is_connected());
        assert!(t.outbound.is_empty());
    }
}
