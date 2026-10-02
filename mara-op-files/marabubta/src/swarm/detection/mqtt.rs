// Marabunta - Licensed under the MIT License.
//! MQTT / Mosquitto detector.
//!
//! Probes port 1883 (default) by sending a minimal MQTT CONNECT packet
//! (protocol version 3.1.1) and expecting a CONNACK response. If accepted,
//! sends DISCONNECT and closes. MQTT has no built-in version query, so
//! version is always None.

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{tcp_connect, Detector, DetectionStatus, SoftwareCapability};

/// MQTT detector using the CONNECT/CONNACK handshake.
pub struct MqttDetector {
    pub host: String,
    pub port: u16,
}

impl Default for MqttDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 1883,
        }
    }
}

/// Build a minimal MQTT v3.1.1 CONNECT packet.
fn build_connect_packet() -> Vec<u8> {
    let client_id = b"marabunta-probe";
    // Variable header:
    //   Protocol Name: length(2) + "MQTT"(4) = 6
    //   Protocol Level: 4 (v3.1.1) = 1
    //   Connect Flags: 0x02 (clean session) = 1
    //   Keep Alive: 10s = 2
    // Payload:
    //   Client ID: length(2) + bytes
    let var_header_len = 6 + 1 + 1 + 2;
    let payload_len = 2 + client_id.len();
    let remaining_len = var_header_len + payload_len;

    let mut packet = Vec::new();
    // Fixed header: packet type 1 (CONNECT) << 4 = 0x10
    packet.push(0x10);
    // Remaining length (single byte if < 128)
    packet.push(remaining_len as u8);

    // Variable header
    // Protocol Name
    packet.extend_from_slice(&[0x00, 0x04]); // length 4
    packet.extend_from_slice(b"MQTT");
    // Protocol Level (4 = v3.1.1)
    packet.push(0x04);
    // Connect Flags: clean session
    packet.push(0x02);
    // Keep Alive: 10 seconds
    packet.extend_from_slice(&10_u16.to_be_bytes());

    // Payload
    // Client ID
    packet.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    packet.extend_from_slice(client_id);

    packet
}

/// Build an MQTT DISCONNECT packet.
fn build_disconnect_packet() -> [u8; 2] {
    [0xE0, 0x00]
}

#[async_trait]
impl Detector for MqttDetector {
    fn name(&self) -> &str {
        "mqtt"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        let mut stream = tcp_connect(&self.host, self.port).await?;

        // Send CONNECT
        let connect = build_connect_packet();
        stream.write_all(&connect).await?;

        // Expect CONNACK: 0x20 (type), remaining_length, flags, return_code
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await?;

        let packet_type = buf[0];
        let _remaining_len = buf[1];
        let _conn_ack_flags = buf[2];
        let return_code = buf[3];

        if packet_type != 0x20 {
            return Ok(None); // Not an MQTT CONNACK
        }

        // Send DISCONNECT and close gracefully
        let _ = stream.write_all(&build_disconnect_packet()).await;

        let status = match return_code {
            0x00 => DetectionStatus::Running,   // Connection Accepted
            0x04 | 0x05 => DetectionStatus::Available, // Bad credentials / Not authorized
            _ => DetectionStatus::Available,     // Some other rejection
        };

        let mut metadata = HashMap::new();
        metadata.insert("return_code".into(), return_code.to_string());
        metadata.insert("protocol".into(), "mqtt_v3.1.1".into());

        Ok(Some(SoftwareCapability {
            software_type: "mqtt".into(),
            version: None, // MQTT protocol has no version query
            port: self.port,
            status,
            detected_at: Utc::now(),
            metadata,
        }))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn mock_mqtt_server(return_code: u8) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                // Read CONNECT packet (discard)
                let mut buf = [0u8; 256];
                let _ = stream.read(&mut buf).await;

                // Send CONNACK
                let connack = [0x20, 0x02, 0x00, return_code];
                stream.write_all(&connack).await.unwrap();
            }
        });
        port
    }

    #[tokio::test]
    async fn test_detect_mqtt_accepted() {
        let port = mock_mqtt_server(0x00).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = MqttDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "mqtt");
        assert_eq!(cap.status, DetectionStatus::Running);
        assert!(cap.version.is_none());
    }

    #[tokio::test]
    async fn test_detect_mqtt_rejected() {
        let port = mock_mqtt_server(0x05).await; // Not authorized
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = MqttDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "mqtt");
        assert_eq!(cap.status, DetectionStatus::Available);
    }

    #[tokio::test]
    async fn test_detect_mqtt_connection_refused() {
        let detector = MqttDetector {
            host: "127.0.0.1".into(),
            port: 19991,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
