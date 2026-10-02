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

use alloy_rlp::{BufMut, Decodable, Encodable, Error as RlpError, Header};
use bytes::Bytes;

use super::{
    json::{Object, array},
    tx::{Tx, TxError},
};

pub const LEDGER_VERSION: u8 = 1;

pub type LaneRoot = [u8; 20];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FinalizationPath {
    Fast = 0,
    Fallback = 1,
}

impl FinalizationPath {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fast => "fast",
            Self::Fallback => "fallback",
        }
    }
}

impl Encodable for FinalizationPath {
    fn encode(&self, out: &mut dyn BufMut) {
        (*self as u8).encode(out)
    }

    fn length(&self) -> usize {
        (*self as u8).length()
    }
}

impl Decodable for FinalizationPath {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        match u8::decode(buf)? {
            v if v == Self::Fast as u8 => Ok(Self::Fast),
            v if v == Self::Fallback as u8 => Ok(Self::Fallback),
            _ => Err(RlpError::Custom("unknown finalization path")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneMeta {
    pub index: u32,
    pub proposer: Option<u64>,
    // Some iff the lane is positive and has a lane file.
    pub root: Option<LaneRoot>,
    pub payload_len: u32,
    pub tx_count: u32,
    pub decode_error: bool,
}

impl LaneMeta {
    pub fn is_positive(&self) -> bool {
        self.root.is_some()
    }

    fn check(&self) -> Result<(), &'static str> {
        if self.root.is_none() && (self.payload_len != 0 || self.tx_count != 0 || self.decode_error)
        {
            return Err("negative lane with content");
        }
        if self.decode_error && self.tx_count != 0 {
            return Err("undecodable lane with txs");
        }
        Ok(())
    }

    pub fn to_json(&self) -> String {
        Object::new()
            .num("index", self.index)
            .opt_num("proposer", self.proposer)
            .opt_hex("root", self.root.as_ref().map(|r| &r[..]))
            .num("payload_len", self.payload_len)
            .num("tx_count", self.tx_count)
            .bool("decode_error", self.decode_error)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    pub version: u8,
    pub slot: u64,
    // decided slot deadline, when the writer knew it.
    pub deadline_ns: Option<u128>,
    // local wall clock at finalization.
    pub finalized_at_ns: u128,
    pub path: FinalizationPath,
    pub num_lanes: u32,
    pub lanes: Vec<LaneMeta>,
}

impl BlockMeta {
    pub fn tx_count(&self) -> u64 {
        self.lanes.iter().map(|l| u64::from(l.tx_count)).sum()
    }

    pub fn check(&self) -> Result<(), &'static str> {
        if self.version != LEDGER_VERSION {
            return Err("unsupported ledger version");
        }
        if usize::try_from(self.num_lanes).ok() != Some(self.lanes.len()) {
            return Err("num_lanes does not match lanes");
        }
        for (i, lane) in self.lanes.iter().enumerate() {
            if usize::try_from(lane.index).ok() != Some(i) {
                return Err("lane index out of order");
            }
            lane.check()?;
        }
        Ok(())
    }

    pub fn to_rlp(&self) -> Bytes {
        alloy_rlp::encode(self).into()
    }

    pub fn decode_exact(buf: &[u8]) -> alloy_rlp::Result<Self> {
        alloy_rlp::decode_exact(buf)
    }

    pub fn to_json(&self) -> String {
        Object::new()
            .num("version", self.version)
            .num("slot", self.slot)
            // ns values exceed 2^53 and lose precision in js/jq; exact values are in meta.rlp.
            .opt_num("deadline_ns", self.deadline_ns)
            .num("finalized_at_ns", self.finalized_at_ns)
            .str("path", self.path.as_str())
            .num("num_lanes", self.num_lanes)
            .num("tx_count", self.tx_count())
            .raw("lanes", &array(self.lanes.iter().map(LaneMeta::to_json)))
            .finish()
    }
}

// human-readable lane file: decoded txs, or the decode error.
pub fn lane_json(slot: u64, lane: &LaneMeta, decoded: &Result<Vec<Tx>, TxError>) -> String {
    let mut obj = Object::new();
    obj.num("slot", slot)
        .num("index", lane.index)
        .opt_num("proposer", lane.proposer)
        .opt_hex("root", lane.root.as_ref().map(|r| &r[..]))
        .num("payload_len", lane.payload_len);
    match decoded {
        Ok(txs) => obj.raw(
            "txs",
            &array(txs.iter().map(|tx| {
                Object::new()
                    .hex("hash", &tx.hash())
                    .hex("sender", &tx.sender)
                    .num("nonce", tx.nonce)
                    .hex("payload", &tx.payload)
                    .hex("payload_hash", &tx.payload_hash())
                    .finish()
            })),
        ),
        Err(e) => obj.str("decode_error", &e.to_string()),
    };
    obj.finish()
}

// what the node knows about a finalized block; the writer derives BlockMeta from it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewBlock {
    pub slot: u64,
    pub deadline_ns: Option<u128>,
    pub finalized_at_ns: u128,
    pub path: FinalizationPath,
    // local wall clock when the fast block formed; written to timeline.json. demo(tx-timeline)
    pub fast_block_at_ns: Option<u128>,
    // demo(tx-timeline): local wall clock when DA decoded each lane, None = unknown or negative.
    pub lane_decoded_at_ns: Vec<Option<u128>>,
    // one per proposal index 0..K.
    pub lanes: Vec<NewLane>,
    // rlp of the commit certificate message, opaque here.
    pub proof: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewLane {
    pub proposer: Option<u64>,
    // None for a negative lane.
    pub committed: Option<CommittedLane>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedLane {
    pub root: LaneRoot,
    pub payload: Bytes,
}

fn encode_opt<T: Encodable>(value: &Option<T>, out: &mut dyn BufMut) {
    Header {
        list: true,
        payload_length: opt_payload_len(value),
    }
    .encode(out);
    if let Some(v) = value {
        v.encode(out);
    }
}

fn opt_payload_len<T: Encodable>(value: &Option<T>) -> usize {
    value.as_ref().map_or(0, Encodable::length)
}

fn opt_len<T: Encodable>(value: &Option<T>) -> usize {
    Header {
        list: true,
        payload_length: opt_payload_len(value),
    }
    .length_with_payload()
}

// an option is a list of zero or one items.
fn decode_opt<T: Decodable>(buf: &mut &[u8]) -> alloy_rlp::Result<Option<T>> {
    let mut inner = Header::decode_bytes(buf, true)?;
    if inner.is_empty() {
        return Ok(None);
    }
    let value = T::decode(&mut inner)?;
    if !inner.is_empty() {
        return Err(RlpError::Custom("option holds more than one item"));
    }
    Ok(Some(value))
}

fn finish_list(rest: &[u8]) -> alloy_rlp::Result<()> {
    if rest.is_empty() {
        Ok(())
    } else {
        Err(RlpError::Custom("trailing list items"))
    }
}

impl LaneMeta {
    fn payload_length(&self) -> usize {
        self.index.length()
            + opt_len(&self.proposer)
            + opt_len(&self.root)
            + self.payload_len.length()
            + self.tx_count.length()
            + self.decode_error.length()
    }
}

impl Encodable for LaneMeta {
    fn encode(&self, out: &mut dyn BufMut) {
        Header {
            list: true,
            payload_length: self.payload_length(),
        }
        .encode(out);
        self.index.encode(out);
        encode_opt(&self.proposer, out);
        encode_opt(&self.root, out);
        self.payload_len.encode(out);
        self.tx_count.encode(out);
        self.decode_error.encode(out);
    }

    fn length(&self) -> usize {
        Header {
            list: true,
            payload_length: self.payload_length(),
        }
        .length_with_payload()
    }
}

impl Decodable for LaneMeta {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut b = Header::decode_bytes(buf, true)?;
        let lane = Self {
            index: u32::decode(&mut b)?,
            proposer: decode_opt(&mut b)?,
            root: decode_opt(&mut b)?,
            payload_len: u32::decode(&mut b)?,
            tx_count: u32::decode(&mut b)?,
            decode_error: bool::decode(&mut b)?,
        };
        finish_list(b)?;
        Ok(lane)
    }
}

impl BlockMeta {
    fn payload_length(&self) -> usize {
        self.version.length()
            + self.slot.length()
            + opt_len(&self.deadline_ns)
            + self.finalized_at_ns.length()
            + self.path.length()
            + self.num_lanes.length()
            + self.lanes.length()
    }
}

impl Encodable for BlockMeta {
    fn encode(&self, out: &mut dyn BufMut) {
        Header {
            list: true,
            payload_length: self.payload_length(),
        }
        .encode(out);
        self.version.encode(out);
        self.slot.encode(out);
        encode_opt(&self.deadline_ns, out);
        self.finalized_at_ns.encode(out);
        self.path.encode(out);
        self.num_lanes.encode(out);
        self.lanes.encode(out);
    }

    fn length(&self) -> usize {
        Header {
            list: true,
            payload_length: self.payload_length(),
        }
        .length_with_payload()
    }
}

impl Decodable for BlockMeta {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut b = Header::decode_bytes(buf, true)?;
        // the version gates the layout of everything after it.
        let version = u8::decode(&mut b)?;
        if version != LEDGER_VERSION {
            return Err(RlpError::Custom("unsupported ledger version"));
        }
        let meta = Self {
            version,
            slot: u64::decode(&mut b)?,
            deadline_ns: decode_opt(&mut b)?,
            finalized_at_ns: u128::decode(&mut b)?,
            path: FinalizationPath::decode(&mut b)?,
            num_lanes: u32::decode(&mut b)?,
            lanes: Vec::decode(&mut b)?,
        };
        finish_list(b)?;
        meta.check().map_err(RlpError::Custom)?;
        Ok(meta)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::ledger::tx::{decode_batch, encode_batch};

    pub(crate) fn arb_lane(index: u32) -> impl Strategy<Value = LaneMeta> {
        (
            any::<Option<u64>>(),
            any::<Option<LaneRoot>>(),
            any::<u32>(),
            any::<u32>(),
            any::<bool>(),
        )
            .prop_map(
                move |(proposer, root, payload_len, tx_count, decode_error)| match root {
                    None => LaneMeta {
                        index,
                        proposer,
                        root,
                        payload_len: 0,
                        tx_count: 0,
                        decode_error: false,
                    },
                    Some(_) => LaneMeta {
                        index,
                        proposer,
                        root,
                        payload_len,
                        tx_count: if decode_error { 0 } else { tx_count },
                        decode_error,
                    },
                },
            )
    }

    pub(crate) fn arb_meta() -> impl Strategy<Value = BlockMeta> {
        (
            any::<u64>(),
            any::<Option<u128>>(),
            any::<u128>(),
            prop_oneof![
                Just(FinalizationPath::Fast),
                Just(FinalizationPath::Fallback)
            ],
            0u32..8,
        )
            .prop_flat_map(|(slot, deadline_ns, finalized_at_ns, path, k)| {
                (0..k)
                    .map(arb_lane)
                    .collect::<Vec<_>>()
                    .prop_map(move |lanes| BlockMeta {
                        version: LEDGER_VERSION,
                        slot,
                        deadline_ns,
                        finalized_at_ns,
                        path,
                        num_lanes: k,
                        lanes,
                    })
            })
    }

    fn sample() -> BlockMeta {
        BlockMeta {
            version: LEDGER_VERSION,
            slot: 12345,
            deadline_ns: Some(1_700_000_000_000_000_000),
            finalized_at_ns: 1_700_000_000_050_000_000,
            path: FinalizationPath::Fallback,
            num_lanes: 2,
            lanes: vec![
                LaneMeta {
                    index: 0,
                    proposer: Some(3),
                    root: Some([0xaa; 20]),
                    payload_len: 30,
                    tx_count: 1,
                    decode_error: false,
                },
                LaneMeta {
                    index: 1,
                    proposer: None,
                    root: None,
                    payload_len: 0,
                    tx_count: 0,
                    decode_error: false,
                },
            ],
        }
    }

    #[test]
    fn rejects_inconsistent_meta() {
        let mut m = sample();
        m.version = 2;
        assert!(BlockMeta::decode_exact(&m.to_rlp()).is_err());

        let mut m = sample();
        m.num_lanes = 3;
        assert!(BlockMeta::decode_exact(&m.to_rlp()).is_err());

        let mut m = sample();
        m.lanes.swap(0, 1);
        assert!(BlockMeta::decode_exact(&m.to_rlp()).is_err());

        let mut m = sample();
        m.lanes[1].payload_len = 1;
        assert!(BlockMeta::decode_exact(&m.to_rlp()).is_err());

        let mut m = sample();
        m.lanes[0].decode_error = true;
        assert!(BlockMeta::decode_exact(&m.to_rlp()).is_err());

        let mut bytes = sample().to_rlp().to_vec();
        bytes.push(0x80);
        assert!(BlockMeta::decode_exact(&bytes).is_err());
        assert!(BlockMeta::decode_exact(&bytes[..bytes.len() - 2]).is_err());
        assert!(BlockMeta::decode_exact(b"not rlp").is_err());
    }

    #[test]
    fn rejects_bad_path_and_option() {
        let path = alloy_rlp::encode(2u8);
        assert!(FinalizationPath::decode(&mut &path[..]).is_err());

        // an option list with two items.
        let two = alloy_rlp::encode(vec![1u64, 2u64]);
        assert!(decode_opt::<u64>(&mut &two[..]).is_err());
        // an option must be a list.
        let s = alloy_rlp::encode(1u64);
        assert!(decode_opt::<u64>(&mut &s[..]).is_err());
    }

    #[test]
    fn meta_json() {
        let m = sample();
        let v: serde_json::Value = serde_json::from_str(&m.to_json()).unwrap();
        assert_eq!(v["version"], 1);
        assert_eq!(v["slot"], 12345);
        assert_eq!(v["deadline_ns"].as_u64(), Some(1_700_000_000_000_000_000));
        assert_eq!(v["path"], "fallback");
        assert_eq!(v["num_lanes"], 2);
        assert_eq!(v["tx_count"], 1);
        assert_eq!(v["lanes"][0]["proposer"], 3);
        assert_eq!(v["lanes"][0]["root"], format!("0x{}", "aa".repeat(20)));
        assert_eq!(v["lanes"][0]["payload_len"], 30);
        assert_eq!(v["lanes"][0]["decode_error"], false);
        assert!(v["lanes"][1]["proposer"].is_null());
        assert!(v["lanes"][1]["root"].is_null());

        let mut m = m;
        m.deadline_ns = None;
        m.path = FinalizationPath::Fast;
        let v: serde_json::Value = serde_json::from_str(&m.to_json()).unwrap();
        assert!(v["deadline_ns"].is_null());
        assert_eq!(v["path"], "fast");
    }

    #[test]
    fn lane_json_txs_and_error() {
        let tx = Tx {
            sender: [0x22; 20],
            nonce: 9,
            payload: Bytes::from_static(b"\x00hi"),
            sent_at_ns: 0,             // demo(tx-timeline)
            rpc_received_at_ns: 0,     // demo(tx-timeline)
            mempool_admitted_at_ns: 0, // demo(tx-timeline)
        };
        let lane = &sample().lanes[0];
        let decoded = decode_batch(&encode_batch(1, std::slice::from_ref(&tx))); // demo(tx-timeline)
        let v: serde_json::Value = serde_json::from_str(&lane_json(7, lane, &decoded)).unwrap();
        assert_eq!(v["slot"], 7);
        assert_eq!(v["index"], 0);
        assert!(v.get("decode_error").is_none());
        let t = &v["txs"][0];
        assert_eq!(t["hash"], format!("0x{}", hex::encode(tx.hash())));
        assert_eq!(t["sender"], format!("0x{}", "22".repeat(20)));
        assert_eq!(t["nonce"], 9);
        assert_eq!(t["payload"], "0x006869");
        assert_eq!(
            t["payload_hash"],
            format!("0x{}", hex::encode(tx.payload_hash()))
        );

        let decoded = decode_batch(b"\"garbage\"");
        let v: serde_json::Value = serde_json::from_str(&lane_json(7, lane, &decoded)).unwrap();
        assert!(v.get("txs").is_none());
        assert!(v["decode_error"].as_str().unwrap().contains("rlp"));
    }

    proptest! {
        #[test]
        fn meta_round_trip(meta in arb_meta()) {
            prop_assert!(meta.check().is_ok());
            let bytes = meta.to_rlp();
            prop_assert_eq!(bytes.len(), meta.length());
            prop_assert_eq!(&BlockMeta::decode_exact(&bytes).unwrap(), &meta);
            let v: serde_json::Value = serde_json::from_str(&meta.to_json()).unwrap();
            prop_assert_eq!(v["lanes"].as_array().unwrap().len(), meta.lanes.len());
        }
    }
}
