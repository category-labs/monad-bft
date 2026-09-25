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
    keccak256(&tx.to_rlp())
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

pub fn encode_batch(txs: &[Tx]) -> Bytes {
    let mut out = Vec::with_capacity(alloy_rlp::list_length::<_, Tx>(txs));
    alloy_rlp::encode_list::<_, Tx>(txs, &mut out);
    out.into()
}

// strict: a lane with trailing bytes or an oversized tx is not a valid batch.
pub fn decode_batch(buf: &[u8]) -> Result<Vec<Tx>, TxError> {
    let txs: Vec<Tx> = alloy_rlp::decode_exact(buf)?;
    txs.iter().try_for_each(Tx::validate)?;
    Ok(txs)
}

// accumulates valid txs into an `rlp(Vec<Tx>)` payload of at most `max(proposal_size_limit, 1)`
// bytes, since the empty batch is 1 byte; its output always passes `decode_batch`.
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
        Header {
            list: true,
            payload_length: content_len,
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

    // the exact byte length `finish` will return.
    pub fn encoded_size(&self) -> usize {
        Self::encoded_len(self.content_len)
    }

    pub fn finish(self) -> Bytes {
        let out = encode_batch(&self.txs);
        debug_assert_eq!(out.len(), self.encoded_size());
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
        )
            .prop_map(|(sender, nonce, payload)| Tx {
                sender,
                nonce,
                payload: payload.into(),
            })
    }

    fn sample_tx() -> Tx {
        Tx {
            sender: [0x11; 20],
            nonce: 1,
            payload: Bytes::from_static(b"hello"),
        }
    }

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
        assert_eq!(tx.to_rlp().as_ref(), expected.as_slice());
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

    #[test]
    fn empty_batch_is_empty_list() {
        assert_eq!(encode_batch(&[]).as_ref(), &[0xc0]);
        assert_eq!(decode_batch(&[0xc0]).unwrap(), vec![]);
        assert_eq!(BatchBuilder::new(1).finish().as_ref(), &[0xc0]);
    }

    #[test]
    fn size_limit() {
        let mut tx = sample_tx();
        tx.payload = vec![0; MAX_TX_PAYLOAD].into();
        tx.validate().unwrap();
        Tx::decode_exact(&tx.to_rlp()).unwrap();
        decode_batch(&encode_batch(std::slice::from_ref(&tx))).unwrap();

        tx.payload = vec![0; MAX_TX_PAYLOAD + 1].into();
        let err = TxError::PayloadTooLarge {
            len: MAX_TX_PAYLOAD + 1,
        };
        assert_eq!(tx.validate(), Err(err.clone()));
        assert_eq!(Tx::decode_exact(&tx.to_rlp()), Err(err.clone()));
        assert_eq!(decode_batch(&encode_batch(&[sample_tx(), tx])), Err(err));
    }

    #[test]
    fn strict_decoding() {
        let mut buf = sample_tx().to_rlp().to_vec();
        buf.push(0);
        assert!(matches!(Tx::decode_exact(&buf), Err(TxError::Rlp(_))));

        let mut batch = encode_batch(&[sample_tx()]).to_vec();
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
    }

    #[test]
    fn builder_rejects_overflow() {
        let tx = sample_tx();
        let one = encode_batch(std::slice::from_ref(&tx)).len();
        let mut b = BatchBuilder::new(one);
        b.try_push(tx.clone()).unwrap();
        assert_eq!(b.try_push(tx.clone()), Err(tx.clone()));
        assert_eq!(b.len(), 1);
        assert_eq!(b.finish().len(), one);

        let mut b = BatchBuilder::new(one - 1);
        assert!(!b.fits(&tx));
        assert!(b.try_push(tx).is_err());
        assert!(b.is_empty());
        assert_eq!(BatchBuilder::new(0).finish().as_ref(), &[0xc0]);
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
        assert_eq!(decode_batch(&b.finish()).unwrap().len(), 2);
    }

    proptest! {
        #[test]
        fn tx_round_trip(tx in arb_tx(2 * MAX_TX_PAYLOAD)) {
            let decoded: Tx = alloy_rlp::decode_exact(tx.to_rlp()).unwrap();
            prop_assert_eq!(&decoded, &tx);
            prop_assert_eq!(decoded.hash(), tx.hash());
            prop_assert_eq!(Tx::decode_exact(&tx.to_rlp()).is_ok(), tx.validate().is_ok());
        }

        #[test]
        fn batch_round_trip(txs in proptest::collection::vec(arb_tx(MAX_TX_PAYLOAD), 0..16)) {
            prop_assert_eq!(decode_batch(&encode_batch(&txs)).unwrap(), txs);
        }

        #[test]
        fn builder_respects_proposal_size_limit(
            txs in proptest::collection::vec(arb_tx(MAX_TX_PAYLOAD + 64), 0..32),
            proposal_size_limit in 1usize..4096,
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
                    prop_assert!(encode_batch(&with).len() > proposal_size_limit);
                }
            }
            let size = b.encoded_size();
            let out = b.finish();
            prop_assert_eq!(out.len(), size);
            prop_assert!(out.len() <= proposal_size_limit);
            prop_assert_eq!(decode_batch(&out).unwrap(), taken);
        }
    }
}
