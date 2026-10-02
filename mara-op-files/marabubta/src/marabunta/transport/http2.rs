// Marabunta - Licensed under the MIT License.
//! HTTP/2 multiplexed stream transport — fallback 1 for the Marabunta protocol.
//!
//! Uses reqwest (which supports HTTP/2) to send frames as binary POST bodies
//! and receive frames via long-lived responses.

use super::{Transport, TransportError, TransportMethod};
use crate::marabunta::frame::{Frame, FrameCodec};
use async_trait::async_trait;
use bytes::BytesMut;
use std::collections::VecDeque;
use tokio_util::codec::{Decoder, Encoder};

/// HTTP/2 transport implementation using reqwest.
pub struct Http2Transport {
    client: reqwest::Client,
    endpoint: Option<String>,
    codec: FrameCodec,
    connected: bool,
    recv_buffer: VecDeque<Frame>,
}

impl Default for Http2Transport {
    fn default() -> Self {
        Self::new()
    }
}

impl Http2Transport {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap_or_default();

        Self {
            client,
            endpoint: None,
            codec: FrameCodec,
            connected: false,
            recv_buffer: VecDeque::new(),
        }
    }
}

#[async_trait]
impl Transport for Http2Transport {
    async fn connect(&mut self, endpoint: &str) -> Result<(), TransportError> {
        // Verify the endpoint is reachable with an HTTP/2 OPTIONS/HEAD request
        let url = format!("{}/marabunta/v1/connect", endpoint);
        let resp = self
            .client
            .post(&url)
            .body(b"ping".to_vec())
            .send()
            .await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        if !resp.status().is_success() && resp.status().as_u16() != 404 {
            return Err(TransportError::ConnectionFailed(format!(
                "HTTP/2 connect returned {}",
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

        let url = format!("{}/marabunta/v1/frame", endpoint);
        self.client
            .post(&url)
            .body(buf.to_vec())
            .send()
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        Ok(())
    }

    async fn recv_frame(&mut self) -> Result<Frame, TransportError> {
        // Return buffered frame if available
        if let Some(frame) = self.recv_buffer.pop_front() {
            return Ok(frame);
        }

        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or(TransportError::NotConnected)?;

        let url = format!("{}/marabunta/v1/recv", endpoint);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| TransportError::ReceiveFailed(e.to_string()))?;

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
        self.recv_buffer.clear();
        Ok(())
    }

    fn transport_type(&self) -> TransportMethod {
        TransportMethod::Http2
    }

    fn is_connected(&self) -> bool {
        self.connected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http2_initial_state() {
        let t = Http2Transport::new();
        assert!(!t.is_connected());
        assert_eq!(t.transport_type(), TransportMethod::Http2);
    }

    #[tokio::test]
    async fn test_send_without_connect() {
        let mut t = Http2Transport::new();
        assert!(t.send_frame(Frame::heartbeat(1)).await.is_err());
    }

    #[tokio::test]
    async fn test_recv_without_connect() {
        let mut t = Http2Transport::new();
        assert!(t.recv_frame().await.is_err());
    }

    #[tokio::test]
    async fn test_close_ok() {
        let mut t = Http2Transport::new();
        assert!(t.close().await.is_ok());
        assert!(!t.is_connected());
    }
}
