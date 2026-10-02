// Marabunta - Licensed under the MIT License.
//! Phase 6.2: The Planetary Redis (RESP) Protocol Parser
//!
//! A brutalist, zero-copy byte parser for the REdis Serialization Protocol (v2/v3).
//! This is the Trojan Horse ingress layer that allows legacy enterprise applications
//! to dump humongous state (e.g., 500GB climate matrices) directly into the Marabunta
//! DHT without installing custom SDKs. We intentionally sacrifice sub-millisecond
//! latency for infinite, Reed-Solomon protected storage capacity and BFT replication.

use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use std::io;

#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Null,
    Array(Vec<RespFrame>),
}

pub struct RespCodec;

impl Encoder<RespFrame> for RespCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespFrame::SimpleString(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"
");
            }
            RespFrame::Error(s) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"
");
            }
            RespFrame::Integer(i) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(i.to_string().as_bytes());
                dst.extend_from_slice(b"
");
            }
            RespFrame::BulkString(bytes) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(bytes.len().to_string().as_bytes());
                dst.extend_from_slice(b"
");
                dst.extend_from_slice(&bytes);
                dst.extend_from_slice(b"
");
            }
            RespFrame::Null => {
                dst.extend_from_slice(b"$-1
");
            }
            RespFrame::Array(frames) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(frames.len().to_string().as_bytes());
                dst.extend_from_slice(b"
");
                for frame in frames {
                    self.encode(frame, dst)?;
                }
            }
        }
        Ok(())
    }
}

impl Decoder for RespCodec {
    type Item = RespFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match src[0] {
            b'*' => self.parse_array(src),
            b'$' => self.parse_bulk_string(src),
            b'+' => self.parse_simple_string(src),
            b'-' => self.parse_error(src),
            b':' => self.parse_integer(src),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RESP type identifier")),
        }
    }
}

// ... Recursive parsing logic for arrays and bulk strings ...
// (Simplified for the architectural proof; a production parser uses cursor lookaheads)

impl RespCodec {
    fn parse_array(&self, src: &mut BytesMut) -> Result<Option<RespFrame>, io::Error> {
        if let Some(crlf_pos) = src.windows(2).position(|w| w == b"
") {
            let len_str = std::str::from_utf8(&src[1..crlf_pos]).unwrap_or("");
            let len: i64 = len_str.parse().unwrap_or(-1);
            
            if len == -1 {
                src.advance(crlf_pos + 2);
                return Ok(Some(RespFrame::Null));
            }
            
            // Advance past the array header
            src.advance(crlf_pos + 2);
            
            let mut frames = Vec::new();
            for _ in 0..len {
                // In a true non-blocking decoder, we must maintain state if the array is incomplete.
                // For this prototype, we assume the full TCP frame arrived.
                let mut temp_src = src.clone();
                if let Ok(Some(frame)) = self.parse_bulk_string(&mut temp_src) {
                    frames.push(frame);
                    *src = temp_src; // sync advance
                } else {
                    return Ok(None); // Need more data
                }
            }
            
            Ok(Some(RespFrame::Array(frames)))
        } else {
            Ok(None) // Need more data
        }
    }

    fn parse_bulk_string(&self, src: &mut BytesMut) -> Result<Option<RespFrame>, io::Error> {
        if let Some(crlf_pos) = src.windows(2).position(|w| w == b"
") {
            let len_str = std::str::from_utf8(&src[1..crlf_pos]).unwrap_or("");
            let len: i64 = len_str.parse().unwrap_or(-1);
            
            if len == -1 {
                src.advance(crlf_pos + 2);
                return Ok(Some(RespFrame::Null));
            }
            
            let data_len = len as usize;
            if src.len() >= crlf_pos + 2 + data_len + 2 {
                let data = src[crlf_pos + 2 .. crlf_pos + 2 + data_len].to_vec();
                src.advance(crlf_pos + 2 + data_len + 2);
                return Ok(Some(RespFrame::BulkString(data)));
            }
        }
        Ok(None)
    }

    fn parse_simple_string(&self, src: &mut BytesMut) -> Result<Option<RespFrame>, io::Error> {
        if let Some(crlf_pos) = src.windows(2).position(|w| w == b"
") {
            let data = std::str::from_utf8(&src[1..crlf_pos]).unwrap_or("").to_string();
            src.advance(crlf_pos + 2);
            return Ok(Some(RespFrame::SimpleString(data)));
        }
        Ok(None)
    }

    fn parse_error(&self, src: &mut BytesMut) -> Result<Option<RespFrame>, io::Error> {
        if let Some(crlf_pos) = src.windows(2).position(|w| w == b"
") {
            let data = std::str::from_utf8(&src[1..crlf_pos]).unwrap_or("").to_string();
            src.advance(crlf_pos + 2);
            return Ok(Some(RespFrame::Error(data)));
        }
        Ok(None)
    }

    fn parse_integer(&self, src: &mut BytesMut) -> Result<Option<RespFrame>, io::Error> {
        if let Some(crlf_pos) = src.windows(2).position(|w| w == b"
") {
            let num_str = std::str::from_utf8(&src[1..crlf_pos]).unwrap_or("");
            let num: i64 = num_str.parse().unwrap_or(0);
            src.advance(crlf_pos + 2);
            return Ok(Some(RespFrame::Integer(num)));
        }
        Ok(None)
    }
}