// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Length-prefixed framing and request/response payload codecs.
//!
//! Frame layout: `u8 op | u32 len (big-endian) | payload[len]`.
//! A response reuses the request's `op` byte on success, or [`Op::Error`] with
//! a UTF-8 message payload on failure.

use std::io::{self, Read, Write};

/// secp256k1 message digest length (the node hashes `PREFIX || msg` before the
/// crossing, so the enclave signs a bare digest and needs no domain knowledge).
pub const SECP_DIGEST_LEN: usize = 32;
/// secp256k1 recoverable signature length (64-byte sig + 1 recovery id).
pub const SECP_SIG_LEN: usize = 65;
/// secp256k1 compressed public key length.
pub const SECP_PUBKEY_LEN: usize = 33;
/// ECDH shared secret length.
pub const ECDH_SECRET_LEN: usize = 32;
/// Sanity cap on a `SignBls` payload (`PREFIX || msg`) to bound allocations.
pub const BLS_PREFIXED_MAX: usize = 64 * 1024;
/// Sanity cap on any single frame payload.
pub const MAX_FRAME_LEN: usize = 1024 * 1024;

/// Operation / response codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    SignSecp = 0x01,
    Ecdh = 0x02,
    SignBls = 0x03,
    GetPubkeys = 0x04,
    Attest = 0x05,
    Provision = 0x06,
    Error = 0xEE,
}

impl Op {
    pub fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            0x01 => Op::SignSecp,
            0x02 => Op::Ecdh,
            0x03 => Op::SignBls,
            0x04 => Op::GetPubkeys,
            0x05 => Op::Attest,
            0x06 => Op::Provision,
            0xEE => Op::Error,
            _ => return None,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("unknown op byte: {0:#x}")]
    UnknownOp(u8),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),
    #[error("malformed payload: {0}")]
    Malformed(&'static str),
    /// The remote returned an [`Op::Error`] frame with this message.
    #[error("remote error: {0}")]
    Remote(String),
    /// Response op did not match the request op.
    #[error("unexpected response op: {got:#x} (wanted {want:#x})")]
    UnexpectedOp { got: u8, want: u8 },
}

/// Write one frame.
pub fn write_frame<W: Write>(w: &mut W, op: Op, payload: &[u8]) -> Result<(), ProtoError> {
    if payload.len() > MAX_FRAME_LEN {
        return Err(ProtoError::FrameTooLarge(payload.len()));
    }
    let len = (payload.len() as u32).to_be_bytes();
    w.write_all(&[op as u8])?;
    w.write_all(&len)?;
    w.write_all(payload)?;
    w.flush()?;
    Ok(())
}

/// Read one frame, returning the op byte and its payload.
pub fn read_frame<R: Read>(r: &mut R) -> Result<(u8, Vec<u8>), ProtoError> {
    let mut header = [0u8; 5];
    r.read_exact(&mut header)?;
    let op = header[0];
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if len > MAX_FRAME_LEN {
        return Err(ProtoError::FrameTooLarge(len));
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok((op, payload))
}

/// Read a response frame and validate it against the expected op, translating
/// [`Op::Error`] frames into [`ProtoError::Remote`].
pub fn read_response<R: Read>(r: &mut R, want: Op) -> Result<Vec<u8>, ProtoError> {
    let (op, payload) = read_frame(r)?;
    if op == Op::Error as u8 {
        return Err(ProtoError::Remote(
            String::from_utf8_lossy(&payload).into_owned(),
        ));
    }
    if op != want as u8 {
        return Err(ProtoError::UnexpectedOp {
            got: op,
            want: want as u8,
        });
    }
    Ok(payload)
}

// ---- small length-prefixed segment codec (used by Provision / GetPubkeys) ----

/// Append a `u32`-length-prefixed segment.
pub fn put_segment(buf: &mut Vec<u8>, seg: &[u8]) {
    buf.extend_from_slice(&(seg.len() as u32).to_be_bytes());
    buf.extend_from_slice(seg);
}

/// Read a `u32`-length-prefixed segment, advancing `pos`.
pub fn get_segment<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8], ProtoError> {
    if *pos + 4 > buf.len() {
        return Err(ProtoError::Malformed("truncated segment length"));
    }
    let len = u32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]) as usize;
    *pos += 4;
    if *pos + len > buf.len() {
        return Err(ProtoError::Malformed("truncated segment body"));
    }
    let seg = &buf[*pos..*pos + len];
    *pos += len;
    Ok(seg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, Op::SignSecp, &[1, 2, 3]).unwrap();
        let (op, payload) = read_frame(&mut buf.as_slice()).unwrap();
        assert_eq!(op, Op::SignSecp as u8);
        assert_eq!(payload, vec![1, 2, 3]);
    }

    #[test]
    fn error_response_translated() {
        let mut buf = Vec::new();
        write_frame(&mut buf, Op::Error, b"boom").unwrap();
        let err = read_response(&mut buf.as_slice(), Op::SignSecp).unwrap_err();
        assert!(matches!(err, ProtoError::Remote(m) if m == "boom"));
    }

    #[test]
    fn segment_roundtrip() {
        let mut buf = Vec::new();
        put_segment(&mut buf, b"hello");
        put_segment(&mut buf, b"");
        put_segment(&mut buf, b"world");
        let mut pos = 0;
        assert_eq!(get_segment(&buf, &mut pos).unwrap(), b"hello");
        assert_eq!(get_segment(&buf, &mut pos).unwrap(), b"");
        assert_eq!(get_segment(&buf, &mut pos).unwrap(), b"world");
        assert_eq!(pos, buf.len());
    }
}
