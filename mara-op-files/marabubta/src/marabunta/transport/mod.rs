// Marabunta - Licensed under the MIT License.
//! Transport layer for the Marabunta protocol.
//!
//! Defines the `Transport` trait and `TransportManager` for managing connections
//! across multiple transport methods with fallback and reconnection.

pub mod doh;
pub mod http2;
pub mod http_poll;
pub mod mqtt;
pub mod webrtc;
pub mod websocket;

use crate::marabunta::config;
use crate::marabunta::frame::Frame;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;
use thiserror::Error;

/// Available transport methods ordered by preference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransportMethod {
    WebSocket,
    Http2,
    HttpPoll,
    WebRtc,
    Mqtt,
    Doh,
}

impl fmt::Display for TransportMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WebSocket => write!(f, "WebSocket"),
            Self::Http2 => write!(f, "HTTP/2"),
            Self::HttpPoll => write!(f, "HTTP/1.1-Poll"),
            Self::WebRtc => write!(f, "WebRTC"),
            Self::Mqtt => write!(f, "MQTT"),
            Self::Doh => write!(f, "DoH"),
        }
    }
}

/// Ordered fallback chain of transport methods.
pub const FALLBACK_CHAIN: &[TransportMethod] = &[
    TransportMethod::WebSocket,
    TransportMethod::Http2,
    TransportMethod::HttpPoll,
    TransportMethod::WebRtc,
    TransportMethod::Mqtt,
    TransportMethod::Doh,
];

/// A relay endpoint description.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayEndpoint {
    pub url: String,
    pub region: String,
    pub transport_method: TransportMethod,
}

/// Transport errors.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("timeout after {0:?}")]
    Timeout(Duration),

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("not connected")]
    NotConnected,

    #[error("send failed: {0}")]
    SendFailed(String),

    #[error("receive failed: {0}")]
    ReceiveFailed(String),

    #[error("frame error: {0}")]
    FrameError(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// The core transport trait. Each transport method implements this.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect to a relay endpoint.
    async fn connect(&mut self, endpoint: &str) -> Result<(), TransportError>;

    /// Send a frame.
    async fn send_frame(&mut self, frame: Frame) -> Result<(), TransportError>;

    /// Receive a frame (blocks until available).
    async fn recv_frame(&mut self) -> Result<Frame, TransportError>;

    /// Close the connection gracefully.
    async fn close(&mut self) -> Result<(), TransportError>;

    /// Which transport method this implements.
    fn transport_type(&self) -> TransportMethod;

    /// Whether the transport is currently connected.
    fn is_connected(&self) -> bool;
}

/// Manages transport connections with exponential backoff reconnection.
pub struct TransportManager {
    current_transport: Option<Box<dyn Transport>>,
    current_method_index: usize,
    backoff_ms: u64,
    endpoint: Option<String>,
}

impl TransportManager {
    pub fn new() -> Self {
        Self {
            current_transport: None,
            current_method_index: 0,
            backoff_ms: config::RECONNECT_BACKOFF_INIT_MS,
            endpoint: None,
        }
    }

    /// Attempt to connect using the preferred transport, falling back through the chain.
    pub async fn connect(&mut self, endpoint: &str) -> Result<TransportMethod, TransportError> {
        self.endpoint = Some(endpoint.to_string());

        // Try WebSocket first (only transport fully implemented so far)
        let mut transport = websocket::WebSocketTransport::new();
        match transport.connect(endpoint).await {
            Ok(()) => {
                let method = transport.transport_type();
                self.current_transport = Some(Box::new(transport));
                self.current_method_index = 0;
                self.reset_backoff();
                return Ok(method);
            }
            Err(e) => {
                tracing::warn!("WebSocket connect failed: {}", e);
            }
        }

        Err(TransportError::ConnectionFailed(
            "all transports failed".into(),
        ))
    }

    /// Send a frame through the current transport.
    pub async fn send_frame(&mut self, frame: Frame) -> Result<(), TransportError> {
        let transport = self
            .current_transport
            .as_mut()
            .ok_or(TransportError::NotConnected)?;
        transport.send_frame(frame).await
    }

    /// Receive a frame from the current transport.
    pub async fn recv_frame(&mut self) -> Result<Frame, TransportError> {
        let transport = self
            .current_transport
            .as_mut()
            .ok_or(TransportError::NotConnected)?;
        transport.recv_frame().await
    }

    /// Close the current transport.
    pub async fn close(&mut self) -> Result<(), TransportError> {
        if let Some(mut transport) = self.current_transport.take() {
            transport.close().await?;
        }
        Ok(())
    }

    /// Whether we currently have an active connection.
    pub fn is_connected(&self) -> bool {
        self.current_transport
            .as_ref()
            .is_some_and(|t| t.is_connected())
    }

    /// Get the current transport method.
    pub fn current_method(&self) -> Option<TransportMethod> {
        self.current_transport.as_ref().map(|t| t.transport_type())
    }

    /// Calculate next backoff delay and advance.
    pub fn next_backoff(&mut self) -> Duration {
        let delay = Duration::from_millis(self.backoff_ms);
        self.backoff_ms = (self.backoff_ms * config::RECONNECT_BACKOFF_MULTIPLIER as u64)
            .min(config::RECONNECT_BACKOFF_MAX_MS);
        delay
    }

    /// Reset backoff to initial value.
    pub fn reset_backoff(&mut self) {
        self.backoff_ms = config::RECONNECT_BACKOFF_INIT_MS;
    }
}

impl Default for TransportManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallback_chain_order() {
        assert_eq!(FALLBACK_CHAIN[0], TransportMethod::WebSocket);
        assert_eq!(FALLBACK_CHAIN[5], TransportMethod::Doh);
        assert_eq!(FALLBACK_CHAIN.len(), 6);
    }

    #[test]
    fn test_transport_method_display() {
        assert_eq!(TransportMethod::WebSocket.to_string(), "WebSocket");
        assert_eq!(TransportMethod::Doh.to_string(), "DoH");
    }

    #[test]
    fn test_backoff_exponential() {
        let mut tm = TransportManager::new();
        let d1 = tm.next_backoff();
        assert_eq!(d1, Duration::from_millis(config::RECONNECT_BACKOFF_INIT_MS));
        let d2 = tm.next_backoff();
        assert_eq!(
            d2,
            Duration::from_millis(
                config::RECONNECT_BACKOFF_INIT_MS * config::RECONNECT_BACKOFF_MULTIPLIER as u64
            )
        );
    }

    #[test]
    fn test_backoff_capped() {
        let mut tm = TransportManager::new();
        // Run enough times to hit cap
        for _ in 0..30 {
            tm.next_backoff();
        }
        let capped = tm.next_backoff();
        assert_eq!(
            capped,
            Duration::from_millis(config::RECONNECT_BACKOFF_MAX_MS)
        );
    }

    #[test]
    fn test_backoff_reset() {
        let mut tm = TransportManager::new();
        tm.next_backoff();
        tm.next_backoff();
        tm.reset_backoff();
        let d = tm.next_backoff();
        assert_eq!(d, Duration::from_millis(config::RECONNECT_BACKOFF_INIT_MS));
    }

    #[test]
    fn test_not_connected_initially() {
        let tm = TransportManager::new();
        assert!(!tm.is_connected());
        assert!(tm.current_method().is_none());
    }
}
