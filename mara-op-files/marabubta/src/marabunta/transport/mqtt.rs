// Marabunta - Licensed under the MIT License.
//! MQTT over TLS transport — fallback 4 for the Marabunta protocol.
//!
//! Uses MQTT v5 publish/subscribe with per-node topics for frame delivery.
//! Topic format: marabunta/{node_id_hex}/inbox

use super::{Transport, TransportError, TransportMethod};
use crate::marabunta::frame::{Frame, FrameCodec};
use async_trait::async_trait;
use bytes::BytesMut;
use std::collections::VecDeque;
use tokio_util::codec::Encoder;

/// MQTT transport implementation.
///
/// In production, this would use an MQTT v5 client (e.g., rumqttc) over TLS.
/// Frame payloads are published as binary messages to node-specific topics.
pub struct MqttTransport {
    codec: FrameCodec,
    connected: bool,
    endpoint: Option<String>,
    /// Node topic for subscription.
    topic: Option<String>,
    /// Buffered inbound frames.
    inbound: VecDeque<Frame>,
    /// Buffered outbound frames (for testing).
    outbound: VecDeque<Vec<u8>>,
}

impl Default for MqttTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl MqttTransport {
    pub fn new() -> Self {
        Self {
            codec: FrameCodec,
            connected: false,
            endpoint: None,
            topic: None,
            inbound: VecDeque::new(),
            outbound: VecDeque::new(),
        }
    }

    /// Set the node topic for this transport.
    pub fn set_topic(&mut self, node_id_hex: &str) {
        self.topic = Some(format!("marabunta/{}/inbox", node_id_hex));
    }

    /// Inject a received frame (for testing).
    pub fn inject_inbound(&mut self, frame: Frame) {
        self.inbound.push_back(frame);
    }
}

#[async_trait]
impl Transport for MqttTransport {
    async fn connect(&mut self, endpoint: &str) -> Result<(), TransportError> {
        // In production: connect to MQTT broker over TLS, subscribe to node topic
        self.endpoint = Some(endpoint.to_string());
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

        // In production: publish to target node's topic with QoS 1
        self.outbound.push_back(buf.to_vec());
        Ok(())
    }

    async fn recv_frame(&mut self) -> Result<Frame, TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }

        // Return buffered frame
        self.inbound
            .pop_front()
            .ok_or(TransportError::ReceiveFailed(
                "no frames available (would block in production)".into(),
            ))
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        // In production: unsubscribe and disconnect from broker
        self.connected = false;
        self.endpoint = None;
        self.inbound.clear();
        self.outbound.clear();
        Ok(())
    }

    fn transport_type(&self) -> TransportMethod {
        TransportMethod::Mqtt
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
    fn test_mqtt_initial_state() {
        let t = MqttTransport::new();
        assert!(!t.is_connected());
        assert_eq!(t.transport_type(), TransportMethod::Mqtt);
    }

    #[test]
    fn test_set_topic() {
        let mut t = MqttTransport::new();
        t.set_topic("abcd1234");
        assert_eq!(t.topic.as_deref(), Some("marabunta/abcd1234/inbox"));
    }

    #[tokio::test]
    async fn test_send_recv_with_inject() {
        let mut t = MqttTransport::new();
        t.connect("mqtt://localhost:1883").await.unwrap();

        let frame = Frame::new(FrameType::Gossip, 1, b"mqtt test".to_vec());
        t.inject_inbound(frame.clone());
        let received = t.recv_frame().await.unwrap();
        assert_eq!(received, frame);
    }

    #[tokio::test]
    async fn test_send_queues_outbound() {
        let mut t = MqttTransport::new();
        t.connect("mqtt://localhost").await.unwrap();
        t.send_frame(Frame::heartbeat(1)).await.unwrap();
        assert_eq!(t.outbound.len(), 1);
    }

    #[tokio::test]
    async fn test_recv_empty_returns_error() {
        let mut t = MqttTransport::new();
        t.connect("mqtt://localhost").await.unwrap();
        assert!(t.recv_frame().await.is_err());
    }

    #[tokio::test]
    async fn test_close_clears_state() {
        let mut t = MqttTransport::new();
        t.connect("mqtt://localhost").await.unwrap();
        t.inject_inbound(Frame::heartbeat(1));
        t.close().await.unwrap();
        assert!(!t.is_connected());
        assert!(t.inbound.is_empty());
    }
}
