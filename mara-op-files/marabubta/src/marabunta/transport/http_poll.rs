// Marabunta - Licensed under the MIT License.
//! HTTP/1.1 long-polling transport — fallback 2 for the Marabunta protocol.
//!
//! Frames are sent via POST and received via long-polling GET requests.
//! Uses reqwest (already in Cargo.toml).

use super::{Transport, TransportError, TransportMethod};
use crate::marabunta::frame::{Frame, FrameCodec};
use async_trait::async_trait;
use bytes::BytesMut;
use std::time::Duration;
use tokio_util::codec::{Decoder, Encoder};

const DEFAULT_POLL_TIMEOUT_S: u64 = 30;

/// HTTP/1.1 long-polling transport implementation.
pub struct HttpPollTransport {
    client: reqwest::Client,
    endpoint: Option<String>,
    codec: FrameCodec,
    connected: bool,
    poll_timeout: Duration,
}

impl Default for HttpPollTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpPollTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: None,
            codec: FrameCodec,
            connected: false,
            poll_timeout: Duration::from_secs(DEFAULT_POLL_TIMEOUT_S),
        }
    }
}

#[async_trait]
impl Transport for HttpPollTransport {
    async fn connect(&mut self, endpoint: &str) -> Result<(), TransportError> {
        // Verify endpoint is reachable
        let url = format!("{}/marabunta/v1/poll/connect", endpoint);
        let resp = self
            .client
            .post(&url)
            .timeout(Duration::from_secs(10))
            .body(b"ping".to_vec())
            .send()
            .await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        if !resp.status().is_success() && resp.status().as_u16() != 404 {
            return Err(TransportError::ConnectionFailed(format!(
                "HTTP poll connect returned {}",
                resp.status()
            )));
        }

        self.endpoint = Some(endpoint.to_string());
        self.connected = true;
        Ok(())
    }

    async fn send_frame(&mut self, frame: Frame) -> Result<(), TransportError> {
        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or(TransportError::NotConnected)?;

        let mut buf = BytesMut::new();
        self.codec
            .encode(frame, &mut buf)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        let url = format!("{}/marabunta/v1/poll/frame", endpoint);
        self.client
            .post(&url)
            .body(buf.to_vec())
            .send()
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    async fn recv_frame(&mut self) -> Result<Frame, TransportError> {
        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or(TransportError::NotConnected)?;

        let url = format!("{}/marabunta/v1/poll/recv", endpoint);
        let resp = self
            .client
            .get(&url)
            .timeout(self.poll_timeout)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    TransportError::Timeout(self.poll_timeout)
                } else {
                    TransportError::ReceiveFailed(e.to_string())
                }
            })?;

        let data = resp
            .bytes()
            .await
            .map_err(|e| TransportError::ReceiveFailed(e.to_string()))?;

        let mut buf = BytesMut::from(data.as_ref());
        match self.codec.decode(&mut buf) {
            Ok(Some(frame)) => Ok(frame),
            Ok(None) => Err(TransportError::ReceiveFailed("incomplete frame".into())),
            Err(e) => Err(TransportError::FrameError(e.to_string())),
        }
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        self.connected = false;
        self.endpoint = None;
        Ok(())
    }

    fn transport_type(&self) -> TransportMethod {
        TransportMethod::HttpPoll
    }

    fn is_connected(&self) -> bool {
        self.connected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_poll_initial_state() {
        let t = HttpPollTransport::new();
        assert!(!t.is_connected());
        assert_eq!(t.transport_type(), TransportMethod::HttpPoll);
    }

    #[tokio::test]
    async fn test_send_without_connect() {
        let mut t = HttpPollTransport::new();
        assert!(t.send_frame(Frame::heartbeat(1)).await.is_err());
    }

    #[tokio::test]
    async fn test_recv_without_connect() {
        let mut t = HttpPollTransport::new();
        assert!(t.recv_frame().await.is_err());
    }

    #[tokio::test]
    async fn test_close_ok() {
        let mut t = HttpPollTransport::new();
        assert!(t.close().await.is_ok());
    }
}
