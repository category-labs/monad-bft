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

use alloy_rlp::{Encodable, Header, RlpDecodable, RlpEncodable};
use bytes::Bytes;
use tiny_keccak::{Hasher, Keccak};

pub type Address = [u8; 20];
pub type Hash = [u8; 32];

// enforced by the tx CLI, the rpc and the node; keeps a forwarded tx in one udp frame.
pub const MAX_TX_PAYLOAD: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash, RlpEncodable, RlpDecodable)]
pub struct Tx {
    pub sender: Address,
    pub nonce: u64,
    pub payload: Bytes,
    // demo(tx-timeline): the *_at_ns are unix ns, 0 = unknown, not hashed. When the client sent it.
    pub sent_at_ns: u64,
    // demo(tx-timeline): when the rpc first admitted it, kept across resends.
    pub rpc_received_at_ns: u64,
    // demo(tx-timeline): when the node mempool admitted it, on the admitting node's clock.
    pub mempool_admitted_at_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TxError {
    #[error("tx payload is {len} bytes, max is {MAX_TX_PAYLOAD}")]
    PayloadTooLarge { len: usize },
    #[error("rlp decode failed: {0}")]
    Rlp(#[from] alloy_rlp::Error),
}

pub fn keccak256(data: &[u8]) -> Hash {
    let mut hasher = Keccak::v256();
    hasher.update(data);
    let mut out = [0; 32];
    hasher.finalize(&mut out);
    out
}

pub fn tx_hash(tx: &Tx) -> Hash {
    // demo(tx-timeline): hashes rlp[sender, nonce, payload], leaving out the *_at_ns stamps
    let fields: [&dyn Encodable; 3] = [&tx.sender, &tx.nonce, &tx.payload]; // demo(tx-timeline)
    let mut rlp = Vec::new(); // demo(tx-timeline)
    alloy_rlp::encode_list::<_, dyn Encodable>(&fields, &mut rlp); // demo(tx-timeline)
    keccak256(&rlp) // demo(tx-timeline)
}

pub fn payload_hash(payload: &[u8]) -> Hash {
    keccak256(payload)
}

impl Tx {
    pub fn hash(&self) -> Hash {
        tx_hash(self)
    }

    pub fn payload_hash(&self) -> Hash {
        payload_hash(&self.payload)
    }

    pub fn to_rlp(&self) -> Bytes {
        alloy_rlp::encode(self).into()
    }

    pub fn validate(&self) -> Result<(), TxError> {
        if self.payload.len() > MAX_TX_PAYLOAD {
            return Err(TxError::PayloadTooLarge {
                len: self.payload.len(),
            });
        }
        Ok(())
    }

    // strict: rejects trailing bytes and oversized payloads.
    pub fn decode_exact(buf: &[u8]) -> Result<Self, TxError> {
        let tx: Self = alloy_rlp::decode_exact(buf)?;
        tx.validate()?;
        Ok(tx)
    }
}

// demo(tx-timeline): a lane is rlp[sealed_at_ns, Vec<Tx>], the seal time in the proposer's unix ns.
#[derive(RlpDecodable)]
struct SealedBatch {
    sealed_at_ns: u64,
    txs: Vec<Tx>,
}

pub fn encode_batch(sealed_at_ns: u64, txs: &[Tx]) -> Bytes {
    // demo(tx-timeline): signature gains sealed_at_ns
    let txs_len = alloy_rlp::list_length::<_, Tx>(txs); // demo(tx-timeline)
    // demo(tx-timeline)
    let header = Header {
        list: true,
        payload_length: sealed_at_ns.length() + txs_len,
    };
    let mut out = Vec::with_capacity(header.length_with_payload()); // demo(tx-timeline)
    header.encode(&mut out); // demo(tx-timeline)
    sealed_at_ns.encode(&mut out); // demo(tx-timeline)
    alloy_rlp::encode_list::<_, Tx>(txs, &mut out);
    out.into()
}

// strict: a lane with trailing bytes or an oversized tx is not a valid batch.
pub fn decode_batch(buf: &[u8]) -> Result<Vec<Tx>, TxError> {
    decode_sealed_batch(buf).map(|(_, txs)| txs) // demo(tx-timeline)
}

// demo(tx-timeline): `decode_batch` plus the lane's seal time.
pub fn decode_sealed_batch(buf: &[u8]) -> Result<(u64, Vec<Tx>), TxError> {
    let SealedBatch { sealed_at_ns, txs } = alloy_rlp::decode_exact(buf)?; // demo(tx-timeline)
    txs.iter().try_for_each(Tx::validate)?;
    Ok((sealed_at_ns, txs)) // demo(tx-timeline)
}

// accumulates valid txs into an `rlp[sealed_at_ns, Vec<Tx>]` payload of at most
// `max(proposal_size_limit, BatchBuilder::new(0).encoded_size())` bytes; its output always
// passes `decode_batch`. demo(tx-timeline): sizes reserve a full 8-byte seal time.
#[derive(Debug)]
pub struct BatchBuilder {
    proposal_size_limit: usize,
    content_len: usize,
    txs: Vec<Tx>,
}

impl BatchBuilder {
    pub fn new(proposal_size_limit: usize) -> Self {
        Self {
            proposal_size_limit,
            content_len: 0,
            txs: Vec::new(),
        }
    }

    fn encoded_len(content_len: usize) -> usize {
        // demo(tx-timeline): the tx list nested in the sealed header
        let txs_len = Header {
            list: true,
            payload_length: content_len,
        }
        .length_with_payload();
        // demo(tx-timeline)
        Header {
            list: true,
            payload_length: u64::MAX.length() + txs_len,
        }
        .length_with_payload()
    }

    // false for an invalid tx, which would make `decode_batch` reject the whole lane.
    pub fn fits(&self, tx: &Tx) -> bool {
        tx.validate().is_ok()
            && Self::encoded_len(self.content_len + tx.length()) <= self.proposal_size_limit
    }

    pub fn try_push(&mut self, tx: Tx) -> Result<(), Tx> {
        if !self.fits(&tx) {
            return Err(tx);
        }
        self.content_len += tx.length();
        self.txs.push(tx);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.txs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    // demo(tx-timeline): the most bytes `finish` returns; exact for seal times from 2^56 ns (1972).
    pub fn encoded_size(&self) -> usize {
        Self::encoded_len(self.content_len)
    }

    // demo(tx-timeline): the proposer's seal time goes in the header
    pub fn finish(self, sealed_at_ns: u64) -> Bytes {
        let out = encode_batch(sealed_at_ns, &self.txs); // demo(tx-timeline)
        debug_assert!(out.len() <= self.encoded_size()); // demo(tx-timeline)
        out
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn arb_tx(max_payload: usize) -> impl Strategy<Value = Tx> {
        (
            any::<[u8; 20]>(),
            any::<u64>(),
            proptest::collection::vec(any::<u8>(), 0..=max_payload),
            any::<[u64; 3]>(), // demo(tx-timeline)
        )
            .prop_map(|(sender, nonce, payload, [sent, received, admitted])| Tx {
                sender,
                nonce,
                payload: payload.into(),
                sent_at_ns: sent,                 // demo(tx-timeline)
                rpc_received_at_ns: received,     // demo(tx-timeline)
                mempool_admitted_at_ns: admitted, // demo(tx-timeline)
            })
    }

    fn sample_tx() -> Tx {
        Tx {
            sender: [0x11; 20],
            nonce: 1,
            payload: Bytes::from_static(b"hello"),
            sent_at_ns: 5,             // demo(tx-timeline)
            rpc_received_at_ns: 7,     // demo(tx-timeline)
            mempool_admitted_at_ns: 9, // demo(tx-timeline)
        }
    }

    const SEAL: u64 = 1_700_000_000_000_000_000; // demo(tx-timeline)

    #[test]
    fn keccak_standard_vectors() {
        assert_eq!(
            hex::encode(keccak256(b"")),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
        assert_eq!(
            hex::encode(keccak256(b"abc")),
            "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"
        );
    }

    #[test]
    fn fixed_encoding_and_hash_vector() {
        let tx = sample_tx();
        let mut expected = vec![0xdc, 0x94];
        expected.extend([0x11; 20]);
        expected.extend([0x01, 0x85]);
        expected.extend(b"hello");
        let mut encoded = expected.clone(); // demo(tx-timeline)
        encoded[0] = 0xdf; // demo(tx-timeline)
        encoded.extend([0x05, 0x07, 0x09]); // demo(tx-timeline)
        assert_eq!(tx.to_rlp().as_ref(), encoded.as_slice()); // demo(tx-timeline)
        assert_eq!(tx.hash(), keccak256(&expected));
        assert_eq!(
            hex::encode(tx.hash()),
            "86c71da9f7fda074692c7a7b0886948d526c565019fd35b4f5b0e471cce6c060"
        );
        assert_eq!(
            hex::encode(tx.payload_hash()),
            "1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8"
        );
    }

    // demo(tx-timeline): rlp[sealed_at_ns, []]
    #[test]
    fn empty_batch_vectors() {
        assert_eq!(encode_batch(0, &[]).as_ref(), &[0xc2, 0x80, 0xc0]);
        assert_eq!(
            decode_sealed_batch(&[0xc2, 0x80, 0xc0]).unwrap(),
            (0, vec![])
        );
        let mut sealed = vec![0xca, 0x88];
        sealed.extend(SEAL.to_be_bytes());
        sealed.push(0xc0);
        assert_eq!(encode_batch(SEAL, &[]).as_ref(), sealed.as_slice());
        assert_eq!(decode_sealed_batch(&sealed).unwrap(), (SEAL, vec![]));
        assert_eq!(BatchBuilder::new(1).encoded_size(), sealed.len());
        assert_eq!(
            BatchBuilder::new(1).finish(SEAL).as_ref(),
            sealed.as_slice()
        );
        assert_eq!(BatchBuilder::new(1).finish(0).as_ref(), &[0xc2, 0x80, 0xc0]);
    }

    // demo(tx-timeline): the old `rlp(Vec<Tx>)` lane is not a batch, empty or not
    #[test]
    fn old_lane_format_rejected() {
        assert!(decode_batch(&[0xc0]).is_err());
        let mut old = Vec::new();
        alloy_rlp::encode_list::<_, Tx>(&[sample_tx()], &mut old);
        assert!(matches!(decode_batch(&old), Err(TxError::Rlp(_))));
        assert!(decode_sealed_batch(&old).is_err());
    }

    #[test]
    fn size_limit() {
        let mut tx = sample_tx();
        tx.payload = vec![0; MAX_TX_PAYLOAD].into();
        tx.validate().unwrap();
        Tx::decode_exact(&tx.to_rlp()).unwrap();
        decode_batch(&encode_batch(SEAL, std::slice::from_ref(&tx))).unwrap(); // demo(tx-timeline)

        tx.payload = vec![0; MAX_TX_PAYLOAD + 1].into();
        let err = TxError::PayloadTooLarge {
            len: MAX_TX_PAYLOAD + 1,
        };
        assert_eq!(tx.validate(), Err(err.clone()));
        assert_eq!(Tx::decode_exact(&tx.to_rlp()), Err(err.clone()));
        assert_eq!(
            decode_batch(&encode_batch(SEAL, &[sample_tx(), tx])),
            Err(err)
        ); // demo(tx-timeline)
    }

    #[test]
    fn strict_decoding() {
        let mut buf = sample_tx().to_rlp().to_vec();
        buf.push(0);
        assert!(matches!(Tx::decode_exact(&buf), Err(TxError::Rlp(_))));

        let mut batch = encode_batch(SEAL, &[sample_tx()]).to_vec(); // demo(tx-timeline)
        batch.push(0xc0);
        assert!(matches!(decode_batch(&batch), Err(TxError::Rlp(_))));

        for garbage in [&b""[..], b"\x00", b"\xff\xff", b"hello world", b"\xc1\xc0"] {
            assert!(decode_batch(garbage).is_err(), "{garbage:?}");
        }
        // a single tx is not a batch.
        assert!(decode_batch(&sample_tx().to_rlp()).is_err());
        // short sender.
        let mut short = Vec::new();
        let fields: [&dyn Encodable; 3] = [&[0u8; 19], &1u64, &Bytes::new()];
        alloy_rlp::encode_list::<_, dyn Encodable>(&fields, &mut short);
        assert!(Tx::decode_exact(&short).is_err());
        // demo(tx-timeline): the old 3-field tx is rejected, alone and in a batch
        let mut old = Vec::new(); // demo(tx-timeline)
        let fields: [&dyn Encodable; 3] = [&[0u8; 20], &1u64, &Bytes::new()]; // demo(tx-timeline)
        alloy_rlp::encode_list::<_, dyn Encodable>(&fields, &mut old); // demo(tx-timeline)
        assert!(Tx::decode_exact(&old).is_err()); // demo(tx-timeline)
        let old_batch = [&[0xc0 + old.len() as u8][..], &old].concat(); // demo(tx-timeline)
        assert!(decode_batch(&old_batch).is_err()); // demo(tx-timeline)
    }

    #[test]
    fn builder_rejects_overflow() {
        let tx = sample_tx();
        let one = encode_batch(SEAL, std::slice::from_ref(&tx)).len(); // demo(tx-timeline)
        let mut b = BatchBuilder::new(one);
        b.try_push(tx.clone()).unwrap();
        assert_eq!(b.try_push(tx.clone()), Err(tx.clone()));
        assert_eq!(b.len(), 1);
        assert_eq!(b.finish(SEAL).len(), one); // demo(tx-timeline)

        let mut b = BatchBuilder::new(one - 1);
        assert!(!b.fits(&tx));
        assert!(b.try_push(tx).is_err());
        assert!(b.is_empty());
        assert_eq!(BatchBuilder::new(0).finish(0).as_ref(), &[0xc2, 0x80, 0xc0]); // demo(tx-timeline)
    }

    // demo(tx-timeline): the limit counts the header, sized for any seal time
    #[test]
    fn builder_counts_the_seal_header() {
        let tx = sample_tx();
        let txs_only = alloy_rlp::list_length::<_, Tx>(std::slice::from_ref(&tx));
        let one = encode_batch(u64::MAX, std::slice::from_ref(&tx)).len();
        assert_eq!(one, txs_only + 10);
        assert!(!BatchBuilder::new(txs_only).fits(&tx));
        assert!(!BatchBuilder::new(one - 1).fits(&tx));
        let mut b = BatchBuilder::new(one);
        b.try_push(tx).unwrap();
        assert_eq!(b.encoded_size(), one);
        assert_eq!(b.finish(1).len(), one - 8);
    }

    // demo(tx-timeline): the stamps count toward the limit
    #[test]
    fn builder_counts_the_stamps() {
        let mut tx = sample_tx();
        let one = encode_batch(u64::MAX, std::slice::from_ref(&tx)).len();
        tx.mempool_admitted_at_ns = u64::MAX;
        assert!(!BatchBuilder::new(one).fits(&tx));
        let mut b = BatchBuilder::new(one + 8);
        b.try_push(tx).unwrap();
        assert_eq!(b.encoded_size(), one + 8);
    }

    #[test]
    fn builder_rejects_invalid_tx() {
        let mut big = sample_tx();
        big.payload = vec![0; MAX_TX_PAYLOAD + 1].into();
        let mut b = BatchBuilder::new(usize::MAX);
        b.try_push(sample_tx()).unwrap();
        assert!(!b.fits(&big));
        assert_eq!(b.try_push(big.clone()), Err(big));
        let mut max = sample_tx();
        max.payload = vec![0; MAX_TX_PAYLOAD].into();
        b.try_push(max).unwrap();
        assert_eq!(decode_batch(&b.finish(SEAL)).unwrap().len(), 2); // demo(tx-timeline)
    }

    proptest! {
        #[test]
        fn tx_round_trip(tx in arb_tx(2 * MAX_TX_PAYLOAD)) {
            let decoded: Tx = alloy_rlp::decode_exact(tx.to_rlp()).unwrap();
            prop_assert_eq!(&decoded, &tx);
            prop_assert_eq!(decoded.hash(), tx.hash());
            prop_assert_eq!(Tx::decode_exact(&tx.to_rlp()).is_ok(), tx.validate().is_ok());
            // demo(tx-timeline)
            for stamp in 0..3 {
                let mut restamped = tx.clone();
                let field = match stamp {
                    0 => &mut restamped.sent_at_ns,
                    1 => &mut restamped.rpc_received_at_ns,
                    _ => &mut restamped.mempool_admitted_at_ns,
                };
                *field = !*field;
                prop_assert_eq!(restamped.hash(), tx.hash());
                prop_assert_ne!(restamped.to_rlp(), tx.to_rlp());
            }
        }

        #[test]
        fn batch_round_trip(
            sealed_at_ns in any::<u64>(), // demo(tx-timeline)
            txs in proptest::collection::vec(arb_tx(MAX_TX_PAYLOAD), 0..16),
        ) {
            let encoded = encode_batch(sealed_at_ns, &txs); // demo(tx-timeline)
            prop_assert_eq!(&decode_batch(&encoded).unwrap(), &txs); // demo(tx-timeline)
            prop_assert_eq!(decode_sealed_batch(&encoded).unwrap(), (sealed_at_ns, txs)); // demo(tx-timeline)
        }

        #[test]
        fn builder_respects_proposal_size_limit(
            txs in proptest::collection::vec(arb_tx(MAX_TX_PAYLOAD + 64), 0..32),
            proposal_size_limit in 1usize..4096,
            sealed_at_ns in any::<u64>(), // demo(tx-timeline)
        ) {
            let mut b = BatchBuilder::new(proposal_size_limit);
            let mut taken = Vec::new();
            for tx in txs {
                let fits = b.fits(&tx);
                let pushed = b.try_push(tx.clone()).is_ok();
                prop_assert_eq!(fits, pushed);
                if pushed {
                    taken.push(tx);
                } else if tx.validate().is_ok() {
                    let mut with = taken.clone();
                    with.push(tx);
                    prop_assert!(encode_batch(u64::MAX, &with).len() > proposal_size_limit); // demo(tx-timeline)
                }
            }
            let size = b.encoded_size();
            let out = b.finish(sealed_at_ns); // demo(tx-timeline)
            prop_assert!(out.len() <= size); // demo(tx-timeline)
            if sealed_at_ns >> 56 != 0 { // demo(tx-timeline)
                prop_assert_eq!(out.len(), size); // demo(tx-timeline)
            } // demo(tx-timeline)
            let minimal = BatchBuilder::new(0).encoded_size(); // demo(tx-timeline)
            prop_assert!(out.len() <= proposal_size_limit.max(minimal)); // demo(tx-timeline)
            prop_assert_eq!(decode_sealed_batch(&out).unwrap(), (sealed_at_ns, taken)); // demo(tx-timeline)
        }
    }
}
