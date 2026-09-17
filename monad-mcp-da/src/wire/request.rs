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

//! RLP layout of a chunk recovery request:
//! [slot, proposal_index, root, kind, subset], subset = [0] for all
//! chunks or [1, [chunk_id, ...]] for a non-empty selection.

use std::collections::BTreeSet;

use alloy_rlp::{Decodable, Encodable, Header, encode_list, list_length};
use bytes::Bytes;
use monad_mcp_chorus::spec::{Deserializable, Serializable};

use super::super::{
    chunk::{ChunkRequest, ChunksSubset, WireChunkId},
    runtime::ChunkRecoveryRequest,
    types::{ChunkRequestType, MerkleRoot, Slot},
};

const ALL: u8 = 0;
const NARROWED: u8 = 1;

const MY_CHUNKS: u8 = 1;
const YOUR_CHUNKS: u8 = 2;

fn kind_tag(kind: ChunkRequestType) -> u8 {
    match kind {
        ChunkRequestType::MyChunks => MY_CHUNKS,
        ChunkRequestType::YourChunks => YOUR_CHUNKS,
    }
}

fn kind_of(tag: u8) -> alloy_rlp::Result<ChunkRequestType> {
    match tag {
        MY_CHUNKS => Ok(ChunkRequestType::MyChunks),
        YOUR_CHUNKS => Ok(ChunkRequestType::YourChunks),
        _ => Err(alloy_rlp::Error::Custom("unknown ChunkRequestType tag")),
    }
}

impl Encodable for ChunksSubset {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::All => {
                let fields: [&dyn Encodable; 1] = [&ALL];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Narrowed(ids) => {
                let ids: Vec<WireChunkId> = ids.iter().copied().collect();
                let fields: [&dyn Encodable; 2] = [&NARROWED, &ids];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::All => {
                let fields: [&dyn Encodable; 1] = [&ALL];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::Narrowed(ids) => {
                let ids: Vec<WireChunkId> = ids.iter().copied().collect();
                let fields: [&dyn Encodable; 2] = [&NARROWED, &ids];
                list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl Decodable for ChunksSubset {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let subset = match u8::decode(&mut payload)? {
            ALL => Self::All,
            NARROWED => {
                let ids = Vec::<WireChunkId>::decode(&mut payload)?;
                if ids.is_empty() {
                    return Err(alloy_rlp::Error::Custom("empty chunk selection"));
                }
                Self::Narrowed(BTreeSet::from_iter(ids))
            }
            _ => return Err(alloy_rlp::Error::Custom("unknown ChunksSubset tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(subset)
    }
}

impl Encodable for ChunkRecoveryRequest {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let proposal_index = self.proposal_index as u64;
        let kind = kind_tag(self.request.kind);
        let fields: [&dyn Encodable; 5] = [
            &self.slot,
            &proposal_index,
            &self.root,
            &kind,
            &self.request.subset,
        ];
        encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let proposal_index = self.proposal_index as u64;
        let kind = kind_tag(self.request.kind);
        let fields: [&dyn Encodable; 5] = [
            &self.slot,
            &proposal_index,
            &self.root,
            &kind,
            &self.request.subset,
        ];
        list_length::<_, dyn Encodable>(&fields)
    }
}

impl Decodable for ChunkRecoveryRequest {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let slot = Slot::decode(&mut payload)?;
        let proposal_index = u64::decode(&mut payload)?;
        let root = MerkleRoot::decode(&mut payload)?;
        let kind = kind_of(u8::decode(&mut payload)?)?;
        let subset = ChunksSubset::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }

        let proposal_index = usize::try_from(proposal_index)
            .map_err(|_| alloy_rlp::Error::Custom("proposal index out of range"))?;
        Ok(Self {
            slot,
            proposal_index,
            root,
            request: ChunkRequest { kind, subset },
        })
    }
}

impl Serializable<Bytes> for ChunkRecoveryRequest {
    fn serialize(&self) -> Bytes {
        alloy_rlp::encode(self).into()
    }
}

impl Deserializable<Bytes> for ChunkRecoveryRequest {
    type ReadError = alloy_rlp::Error;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        alloy_rlp::decode_exact(message)
    }
}

#[cfg(test)]
mod tests {
    use super::{super::super::types::MerkleHash, *};

    fn request(subset: ChunksSubset) -> ChunkRecoveryRequest {
        ChunkRecoveryRequest {
            slot: Slot(7),
            proposal_index: 3,
            root: MerkleRoot(MerkleHash([9; 20])),
            request: ChunkRequest {
                kind: ChunkRequestType::YourChunks,
                subset,
            },
        }
    }

    #[test]
    fn roundtrips_and_rejects_framing_errors() {
        let all = request(ChunksSubset::All);
        let narrowed = request(ChunksSubset::Narrowed(BTreeSet::from([2, 5, 300])));
        for original in [all, narrowed] {
            let bytes: Bytes = original.serialize();
            assert_eq!(bytes.len(), original.length());
            assert_eq!(ChunkRecoveryRequest::deserialize(&bytes).unwrap(), original);

            for end in 0..bytes.len() {
                assert!(ChunkRecoveryRequest::deserialize(&bytes.slice(..end)).is_err());
            }
            let mut extra = bytes.to_vec();
            extra.push(0);
            assert!(ChunkRecoveryRequest::deserialize(&Bytes::from(extra)).is_err());
        }
    }

    #[test]
    fn rejects_unknown_tags_and_an_empty_selection() {
        // [NARROWED, []]
        let empty = [0xc2, NARROWED, 0xc0];
        assert!(ChunksSubset::decode(&mut &empty[..]).is_err());
        let bad_tag = [0xc1, 7];
        assert!(ChunksSubset::decode(&mut &bad_tag[..]).is_err());

        let bytes: Bytes = request(ChunksSubset::All).serialize();
        let mut bytes = bytes.to_vec();
        let kind_at = bytes.len() - 3;
        assert_eq!(bytes[kind_at], YOUR_CHUNKS);
        bytes[kind_at] = 9;
        assert!(ChunkRecoveryRequest::deserialize(&Bytes::from(bytes)).is_err());
    }
}
