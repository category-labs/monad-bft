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

//! Host-side client for the remote signer.
//!
//! [`RemoteSigner`] is `Send + Sync` and is shared (behind an `Arc`) by the
//! `Remote` variants of the key types. It keeps a small pool of pre-opened
//! connections so the hot RaptorCast signing path is not serialised behind a
//! single mutexed socket, and records per-op round-trip latency for the
//! benchmark.

use std::{
    sync::Mutex,
    time::Instant,
};

use crossbeam_queue::ArrayQueue;
use hdrhistogram::Histogram;

use crate::{
    protocol::{
        get_segment, put_segment, read_response, write_frame, Op, ProtoError, ECDH_SECRET_LEN,
        SECP_SIG_LEN,
    },
    transport::{Transport, TransportConfig},
};

/// secp + bls public keys returned by `GetPubkeys` / `Provision` (raw bytes;
/// the caller's key crate interprets them).
#[derive(Debug, Clone)]
pub struct Pubkeys {
    pub secp: Vec<u8>,
    pub bls: Vec<u8>,
}

impl Pubkeys {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_segment(&mut buf, &self.secp);
        put_segment(&mut buf, &self.bls);
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, ProtoError> {
        let mut pos = 0;
        let secp = get_segment(buf, &mut pos)?.to_vec();
        let bls = get_segment(buf, &mut pos)?.to_vec();
        Ok(Pubkeys { secp, bls })
    }
}

/// One key's encrypted keystore JSON plus the passphrase to decrypt it. The
/// node forwards this verbatim; decryption happens *inside* the enclave so the
/// plaintext secret never exists in host RAM.
#[derive(Clone)]
pub struct ProvisionBundle {
    pub keystore_json: Vec<u8>,
    pub password: String,
}

/// A provisioning request carrying zero, one, or both keys.
#[derive(Clone, Default)]
pub struct ProvisionRequest {
    pub secp: Option<ProvisionBundle>,
    pub bls: Option<ProvisionBundle>,
}

impl ProvisionRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for bundle in [&self.secp, &self.bls] {
            match bundle {
                Some(b) => {
                    buf.push(1);
                    put_segment(&mut buf, &b.keystore_json);
                    put_segment(&mut buf, b.password.as_bytes());
                }
                None => buf.push(0),
            }
        }
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, ProtoError> {
        let mut pos = 0;
        let mut read_one = || -> Result<Option<ProvisionBundle>, ProtoError> {
            if pos >= buf.len() {
                return Err(ProtoError::Malformed("truncated provision presence byte"));
            }
            let present = buf[pos];
            pos += 1;
            if present == 0 {
                return Ok(None);
            }
            let keystore_json = get_segment(buf, &mut pos)?.to_vec();
            let password = String::from_utf8(get_segment(buf, &mut pos)?.to_vec())
                .map_err(|_| ProtoError::Malformed("password not utf-8"))?;
            Ok(Some(ProvisionBundle {
                keystore_json,
                password,
            }))
        };
        let secp = read_one()?;
        let bls = read_one()?;
        Ok(ProvisionRequest { secp, bls })
    }
}

struct Metrics {
    sign_secp: Histogram<u64>,
    ecdh: Histogram<u64>,
    sign_bls: Histogram<u64>,
}

impl Metrics {
    fn new() -> Self {
        // 3 significant figures, auto-resizing.
        let h = || Histogram::<u64>::new(3).expect("valid histogram");
        Metrics {
            sign_secp: h(),
            ecdh: h(),
            sign_bls: h(),
        }
    }
}

pub struct RemoteSigner {
    cfg: TransportConfig,
    pool: ArrayQueue<Box<dyn Transport>>,
    metrics: Mutex<Metrics>,
}

impl RemoteSigner {
    /// Connect and pre-open `pool_size` connections (min 1). Fails fast if the
    /// signer is unreachable, which surfaces a missing/unprovisioned enclave at
    /// node startup.
    pub fn connect(cfg: TransportConfig, pool_size: usize) -> Result<Self, ProtoError> {
        let pool_size = pool_size.max(1);
        let pool = ArrayQueue::new(pool_size);
        for _ in 0..pool_size {
            let conn = cfg.connect()?;
            let _ = pool.push(conn);
        }
        Ok(RemoteSigner {
            cfg,
            pool,
            metrics: Mutex::new(Metrics::new()),
        })
    }

    fn checkout(&self) -> Result<Box<dyn Transport>, ProtoError> {
        match self.pool.pop() {
            Some(c) => Ok(c),
            // Pool drained under load: open an extra connection rather than block.
            None => Ok(self.cfg.connect()?),
        }
    }

    fn checkin(&self, conn: Box<dyn Transport>) {
        // Drops the connection if the pool is already full.
        let _ = self.pool.push(conn);
    }

    /// One request/response round-trip. On any I/O error the connection is
    /// dropped (not returned to the pool) so a broken socket can't poison it.
    fn request(&self, op: Op, payload: &[u8]) -> Result<Vec<u8>, ProtoError> {
        let mut conn = self.checkout()?;
        let resp = (|| {
            write_frame(&mut conn, op, payload)?;
            read_response(&mut conn, op)
        })();
        match resp {
            Ok(payload) => {
                self.checkin(conn);
                Ok(payload)
            }
            Err(e) => Err(e), // conn dropped here
        }
    }

    fn record(&self, op: Op, elapsed_us: u64) {
        if let Ok(mut m) = self.metrics.lock() {
            let h = match op {
                Op::SignSecp => &mut m.sign_secp,
                Op::Ecdh => &mut m.ecdh,
                Op::SignBls => &mut m.sign_bls,
                _ => return,
            };
            let _ = h.record(elapsed_us);
        }
    }

    pub fn sign_secp(&self, digest: &[u8; 32]) -> Result<[u8; SECP_SIG_LEN], ProtoError> {
        let start = Instant::now();
        let resp = self.request(Op::SignSecp, digest)?;
        self.record(Op::SignSecp, start.elapsed().as_micros() as u64);
        resp.as_slice()
            .try_into()
            .map_err(|_| ProtoError::Malformed("secp signature wrong length"))
    }

    pub fn ecdh(&self, pubkey: &[u8; 33]) -> Result<[u8; ECDH_SECRET_LEN], ProtoError> {
        let start = Instant::now();
        let resp = self.request(Op::Ecdh, pubkey)?;
        self.record(Op::Ecdh, start.elapsed().as_micros() as u64);
        resp.as_slice()
            .try_into()
            .map_err(|_| ProtoError::Malformed("ecdh secret wrong length"))
    }

    pub fn sign_bls(&self, prefixed_msg: &[u8]) -> Result<Vec<u8>, ProtoError> {
        let start = Instant::now();
        let resp = self.request(Op::SignBls, prefixed_msg)?;
        self.record(Op::SignBls, start.elapsed().as_micros() as u64);
        Ok(resp)
    }

    pub fn pubkeys(&self) -> Result<Pubkeys, ProtoError> {
        let resp = self.request(Op::GetPubkeys, &[])?;
        Pubkeys::decode(&resp)
    }

    pub fn provision(&self, req: &ProvisionRequest) -> Result<Pubkeys, ProtoError> {
        let resp = self.request(Op::Provision, &req.encode())?;
        Pubkeys::decode(&resp)
    }

    pub fn attest(&self, nonce: &[u8; 32]) -> Result<Vec<u8>, ProtoError> {
        self.request(Op::Attest, nonce)
    }

    /// Human-readable p50/p99/max (microseconds) per op, for the benchmark.
    pub fn latency_report(&self) -> String {
        let m = self.metrics.lock().expect("metrics poisoned");
        let line = |name: &str, h: &Histogram<u64>| {
            format!(
                "{name:>10}: n={:<8} p50={:>6}us p99={:>6}us max={:>6}us",
                h.len(),
                h.value_at_quantile(0.50),
                h.value_at_quantile(0.99),
                h.max(),
            )
        };
        [
            line("sign_secp", &m.sign_secp),
            line("ecdh", &m.ecdh),
            line("sign_bls", &m.sign_bls),
        ]
        .join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provision_request_roundtrip() {
        let req = ProvisionRequest {
            secp: Some(ProvisionBundle {
                keystore_json: b"{\"secp\":true}".to_vec(),
                password: "hunter2".into(),
            }),
            bls: None,
        };
        let decoded = ProvisionRequest::decode(&req.encode()).unwrap();
        let s = decoded.secp.unwrap();
        assert_eq!(s.keystore_json, b"{\"secp\":true}");
        assert_eq!(s.password, "hunter2");
        assert!(decoded.bls.is_none());
    }

    #[test]
    fn pubkeys_roundtrip() {
        let pk = Pubkeys {
            secp: vec![1, 2, 3],
            bls: vec![4, 5, 6, 7],
        };
        let decoded = Pubkeys::decode(&pk.encode()).unwrap();
        assert_eq!(decoded.secp, vec![1, 2, 3]);
        assert_eq!(decoded.bls, vec![4, 5, 6, 7]);
    }
}
