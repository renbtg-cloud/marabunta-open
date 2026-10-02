// Marabunta - Licensed under the MIT License.
//! WebSocket over TLS transport — primary transport method for the Marabunta protocol.

use crate::marabunta::frame::{Frame, FrameCodec};
use super::{Transport, TransportError, TransportMethod};
use async_trait::async_trait;
use bytes::BytesMut;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::codec::{Decoder, Encoder};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// WebSocket transport implementation.
pub struct WebSocketTransport {
    ws_sink: Option<SplitSink<WsStream, Message>>,
    ws_stream: Option<futures::stream::SplitStream<WsStream>>,
    codec: FrameCodec,
    connected: bool,
}

impl Default for WebSocketTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl WebSocketTransport {
    pub fn new() -> Self {
        Self {
            ws_sink: None,
            ws_stream: None,
            codec: FrameCodec,
            connected: false,
        }
    }
}

#[async_trait]
impl Transport for WebSocketTransport {
    async fn connect(&mut self, endpoint: &str) -> Result<(), TransportError> {
        let (ws_stream, _response) = tokio_tungstenite::connect_async(endpoint)
            .await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        let (sink, stream) = ws_stream.split();
        self.ws_sink = Some(sink);
        self.ws_stream = Some(stream);
        self.connected = true;
        Ok(())
    }

    async fn send_frame(&mut self, frame: Frame) -> Result<(), TransportError> {
        let sink = self
            .ws_sink
            .as_mut()
            .ok_or(TransportError::NotConnected)?;

        let mut buf = BytesMut::new();
        self.codec
            .encode(frame, &mut buf)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        sink.send(Message::Binary(buf.to_vec()))
            .await
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;
        Ok(())
    }

    async fn recv_frame(&mut self) -> Result<Frame, TransportError> {
        let stream = self
            .ws_stream
            .as_mut()
            .ok_or(TransportError::NotConnected)?;

        loop {
            match stream.next().await {
                Some(Ok(Message::Binary(data))) => {
                    let mut buf = BytesMut::from(data.as_slice());
                    match self.codec.decode(&mut buf) {
                        Ok(Some(frame)) => return Ok(frame),
                        Ok(None) => continue,
                        Err(e) => {
                            return Err(TransportError::FrameError(e.to_string()));
                        }
                    }
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) => {
                    self.connected = false;
                    return Err(TransportError::ConnectionClosed);
                }
                Some(Ok(_)) => continue, // Text messages, etc.
                Some(Err(e)) => {
                    self.connected = false;
                    return Err(TransportError::ReceiveFailed(e.to_string()));
                }
                None => {
                    self.connected = false;
                    return Err(TransportError::ConnectionClosed);
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if let Some(mut sink) = self.ws_sink.take() {
            let _ = sink.close().await;
        }
        self.ws_stream = None;
        self.connected = false;
        Ok(())
    }

    fn transport_type(&self) -> TransportMethod {
        TransportMethod::WebSocket
    }

    fn is_connected(&self) -> bool {
        self.connected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_websocket_transport_initial_state() {
        let ws = WebSocketTransport::new();
        assert!(!ws.is_connected());
        assert_eq!(ws.transport_type(), TransportMethod::WebSocket);
    }

    #[tokio::test]
    async fn test_send_without_connect_fails() {
        let mut ws = WebSocketTransport::new();
        let frame = Frame::heartbeat(1);
        assert!(ws.send_frame(frame).await.is_err());
    }

    #[tokio::test]
    async fn test_recv_without_connect_fails() {
        let mut ws = WebSocketTransport::new();
        assert!(ws.recv_frame().await.is_err());
    }

    #[tokio::test]
    async fn test_close_without_connect_ok() {
        let mut ws = WebSocketTransport::new();
        assert!(ws.close().await.is_ok());
    }
}
