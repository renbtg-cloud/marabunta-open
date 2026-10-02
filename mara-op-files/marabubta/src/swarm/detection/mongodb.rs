// Marabunta - Licensed under the MIT License.
//! MongoDB detector.
//!
//! Probes port 27017 (default) by sending an OP_MSG (opcode 2013)
//! containing `{"isMaster": 1, "$db": "admin"}`. Parses the response
//! for the `ismaster` flag and `version` string.
//!
//! We use a simplified BSON encoding/decoding here -- just enough to
//! build the isMaster command and parse the version from the response.

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{tcp_connect, Detector, DetectionStatus, SoftwareCapability};

/// MongoDB detector using OP_MSG wire protocol.
pub struct MongoDbDetector {
    pub host: String,
    pub port: u16,
}

impl Default for MongoDbDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 27017,
        }
    }
}

/// Build a minimal BSON document for `{"isMaster": 1, "$db": "admin"}`.
fn build_ismaster_bson() -> Vec<u8> {
    let mut doc = Vec::new();
    // placeholder for document size (4 bytes, filled later)
    doc.extend_from_slice(&[0u8; 4]);

    // isMaster: 1 (int32, type 0x10)
    doc.push(0x10); // int32 type
    doc.extend_from_slice(b"isMaster\0");
    doc.extend_from_slice(&1_i32.to_le_bytes());

    // $db: "admin" (string, type 0x02)
    doc.push(0x02); // string type
    doc.extend_from_slice(b"$db\0");
    let db_val = b"admin\0";
    doc.extend_from_slice(&(db_val.len() as i32).to_le_bytes());
    doc.extend_from_slice(db_val);

    // Document terminator
    doc.push(0x00);

    // Fill in document size
    let size = doc.len() as i32;
    doc[0..4].copy_from_slice(&size.to_le_bytes());

    doc
}

/// Build an OP_MSG (opcode 2013) containing the isMaster command.
fn build_op_msg(bson: &[u8]) -> Vec<u8> {
    // Message header: messageLength(4) + requestID(4) + responseTo(4) + opCode(4)
    // OP_MSG body: flagBits(4) + section(kind=0(1) + bson)
    let msg_len = 4 + 4 + 4 + 4 + 4 + 1 + bson.len();

    let mut msg = Vec::with_capacity(msg_len);
    msg.extend_from_slice(&(msg_len as i32).to_le_bytes()); // messageLength
    msg.extend_from_slice(&1_i32.to_le_bytes()); // requestID
    msg.extend_from_slice(&0_i32.to_le_bytes()); // responseTo
    msg.extend_from_slice(&2013_i32.to_le_bytes()); // opCode = OP_MSG
    msg.extend_from_slice(&0_u32.to_le_bytes()); // flagBits
    msg.push(0); // section kind = 0 (body)
    msg.extend_from_slice(bson);

    msg
}

/// Extract a string value from a BSON document by key name.
fn bson_extract_string(doc: &[u8], key: &str) -> Option<String> {
    let _key_bytes = format!("{}\0", key);
    let mut pos = 4; // skip document size

    while pos < doc.len() {
        let elem_type = doc[pos];
        if elem_type == 0x00 {
            break; // end of document
        }
        pos += 1;

        // Read element name (null-terminated)
        let name_start = pos;
        while pos < doc.len() && doc[pos] != 0 {
            pos += 1;
        }
        if pos >= doc.len() {
            break;
        }
        let name = &doc[name_start..pos];
        pos += 1; // skip null terminator

        let name_str = String::from_utf8_lossy(name);
        let is_target = name_str == key;

        match elem_type {
            0x02 => {
                // String type
                if pos + 4 > doc.len() {
                    break;
                }
                let str_len =
                    i32::from_le_bytes([doc[pos], doc[pos + 1], doc[pos + 2], doc[pos + 3]])
                        as usize;
                pos += 4;
                if is_target && pos + str_len <= doc.len() && str_len > 1 {
                    let s = String::from_utf8_lossy(&doc[pos..pos + str_len - 1]);
                    return Some(s.to_string());
                }
                pos += str_len;
            }
            0x01 => {
                pos += 8;
            } // double
            0x10 => {
                pos += 4;
            } // int32
            0x12 => {
                pos += 8;
            } // int64
            0x08 => {
                pos += 1;
            } // boolean
            0x03 | 0x04 => {
                // embedded doc or array
                if pos + 4 > doc.len() {
                    break;
                }
                let sub_len =
                    i32::from_le_bytes([doc[pos], doc[pos + 1], doc[pos + 2], doc[pos + 3]])
                        as usize;
                pos += sub_len;
            }
            0x07 => {
                pos += 12;
            } // ObjectId
            0x09 => {
                pos += 8;
            } // datetime
            0x0A => {} // null (0 bytes)
            _ => {
                break;
            } // unknown type, stop
        }
    }
    None
}

/// Extract an int32 value from a BSON document by key name.
fn bson_extract_int32(doc: &[u8], key: &str) -> Option<i32> {
    let mut pos = 4; // skip document size

    while pos < doc.len() {
        let elem_type = doc[pos];
        if elem_type == 0x00 {
            break;
        }
        pos += 1;

        let name_start = pos;
        while pos < doc.len() && doc[pos] != 0 {
            pos += 1;
        }
        if pos >= doc.len() {
            break;
        }
        let name = &doc[name_start..pos];
        pos += 1;

        let name_str = String::from_utf8_lossy(name);
        let is_target = name_str == key;

        match elem_type {
            0x10 => {
                if pos + 4 > doc.len() {
                    break;
                }
                let val =
                    i32::from_le_bytes([doc[pos], doc[pos + 1], doc[pos + 2], doc[pos + 3]]);
                if is_target {
                    return Some(val);
                }
                pos += 4;
            }
            0x02 => {
                if pos + 4 > doc.len() {
                    break;
                }
                let str_len =
                    i32::from_le_bytes([doc[pos], doc[pos + 1], doc[pos + 2], doc[pos + 3]])
                        as usize;
                pos += 4 + str_len;
            }
            0x01 => {
                pos += 8;
            }
            0x12 => {
                pos += 8;
            }
            0x08 => {
                pos += 1;
            }
            0x03 | 0x04 => {
                if pos + 4 > doc.len() {
                    break;
                }
                let sub_len =
                    i32::from_le_bytes([doc[pos], doc[pos + 1], doc[pos + 2], doc[pos + 3]])
                        as usize;
                pos += sub_len;
            }
            0x07 => {
                pos += 12;
            }
            0x09 => {
                pos += 8;
            }
            0x0A => {}
            _ => {
                break;
            }
        }
    }
    None
}

#[async_trait]
impl Detector for MongoDbDetector {
    fn name(&self) -> &str {
        "mongodb"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        let mut stream = tcp_connect(&self.host, self.port).await?;

        // Send OP_MSG with isMaster command
        let bson = build_ismaster_bson();
        let op_msg = build_op_msg(&bson);
        stream.write_all(&op_msg).await?;

        // Read response header (16 bytes)
        let mut header = [0u8; 16];
        stream.read_exact(&mut header).await?;

        let msg_len = i32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let opcode = i32::from_le_bytes([header[12], header[13], header[14], header[15]]);

        if opcode != 2013 || !(21..=65536).contains(&msg_len) {
            return Ok(None);
        }

        // Read remaining body
        let body_len = msg_len - 16;
        let mut body = vec![0u8; body_len.min(8192)];
        let to_read = body_len.min(8192);
        stream.read_exact(&mut body[..to_read]).await?;

        // Skip flagBits (4 bytes) + section kind (1 byte)
        if body.len() < 5 {
            return Ok(None);
        }
        let bson_doc = &body[5..];

        let version = bson_extract_string(bson_doc, "version");
        let max_wire = bson_extract_int32(bson_doc, "maxWireVersion");

        let mut metadata = HashMap::new();
        if let Some(ref v) = version {
            metadata.insert("version".into(), v.clone());
        }
        if let Some(mw) = max_wire {
            metadata.insert("max_wire_version".into(), mw.to_string());
        }

        Ok(Some(SoftwareCapability {
            software_type: "mongodb".into(),
            version,
            port: self.port,
            status: DetectionStatus::Running,
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

    /// Build a mock isMaster response BSON document.
    fn build_mock_response_bson(version: &str, max_wire: i32) -> Vec<u8> {
        let mut doc = Vec::new();
        doc.extend_from_slice(&[0u8; 4]); // placeholder for size

        // ismaster: true (boolean, type 0x08)
        doc.push(0x08);
        doc.extend_from_slice(b"ismaster\0");
        doc.push(1); // true

        // version: string
        doc.push(0x02);
        doc.extend_from_slice(b"version\0");
        let v = format!("{}\0", version);
        doc.extend_from_slice(&(v.len() as i32).to_le_bytes());
        doc.extend_from_slice(v.as_bytes());

        // maxWireVersion: int32
        doc.push(0x10);
        doc.extend_from_slice(b"maxWireVersion\0");
        doc.extend_from_slice(&max_wire.to_le_bytes());

        doc.push(0x00); // terminator

        let size = doc.len() as i32;
        doc[0..4].copy_from_slice(&size.to_le_bytes());
        doc
    }

    fn build_mock_op_msg_response(bson: &[u8]) -> Vec<u8> {
        let msg_len = 16 + 4 + 1 + bson.len();
        let mut msg = Vec::new();
        msg.extend_from_slice(&(msg_len as i32).to_le_bytes()); // messageLength
        msg.extend_from_slice(&1_i32.to_le_bytes()); // requestID
        msg.extend_from_slice(&1_i32.to_le_bytes()); // responseTo
        msg.extend_from_slice(&2013_i32.to_le_bytes()); // opCode = OP_MSG
        msg.extend_from_slice(&0_u32.to_le_bytes()); // flagBits
        msg.push(0); // section kind = 0
        msg.extend_from_slice(bson);
        msg
    }

    async fn mock_mongodb_server(version: &str, max_wire: i32) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let bson = build_mock_response_bson(version, max_wire);
        let response = build_mock_op_msg_response(&bson);
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                // Read the request (discard)
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                // Send the response
                stream.write_all(&response).await.unwrap();
            }
        });
        port
    }

    #[tokio::test]
    async fn test_detect_mongodb() {
        let port = mock_mongodb_server("7.0.5", 21).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = MongoDbDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "mongodb");
        assert_eq!(cap.version.as_deref(), Some("7.0.5"));
        assert_eq!(cap.status, DetectionStatus::Running);
        assert_eq!(cap.metadata.get("max_wire_version").unwrap(), "21");
    }

    #[tokio::test]
    async fn test_detect_mongodb_incomplete_response() {
        // Server that sends a truncated response
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                // Send incomplete header
                stream.write_all(&[0u8; 5]).await.unwrap();
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = MongoDbDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await;
        // Should fail or return None (incomplete response)
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_detect_mongodb_connection_refused() {
        let detector = MongoDbDetector {
            host: "127.0.0.1".into(),
            port: 19997,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
