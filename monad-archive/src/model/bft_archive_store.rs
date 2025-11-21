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

use std::path::{Path, PathBuf};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use ist::{block_id_to_hex_prefix, BlockPersist, FileBlockPersist};
use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::QuorumCertificate, voting::Vote,
};
use monad_node_config::{ExecutionProtocolType, SignatureCollectionType, SignatureType};
use monad_types::{BlockId, Epoch, ExecutionProtocol, NodeId, Round, SeqNum};

use crate::{model::block_data_archive::BLOCK_PADDING_WIDTH, prelude::*};

pub type BftBlockBody = ConsensusBlockBody<ExecutionProtocolType>;
pub type BftBlockHeader =
    ConsensusBlockHeader<SignatureType, SignatureCollectionType, ExecutionProtocolType>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BftObjectKind {
    Header,
    Body,
}

#[derive(Clone, Debug)]
pub enum BftObject {
    Header { name: String, path: PathBuf },
    Body { name: String, path: PathBuf },
}

impl BftObject {
    pub fn new(kind: BftObjectKind, name: String, path: PathBuf) -> Self {
        match kind {
            BftObjectKind::Header => BftObject::Header { name, path },
            BftObjectKind::Body => BftObject::Body { name, path },
        }
    }

    pub fn kind(&self) -> BftObjectKind {
        match self {
            BftObject::Header { .. } => BftObjectKind::Header,
            BftObject::Body { .. } => BftObjectKind::Body,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            BftObject::Header { name, .. } | BftObject::Body { name, .. } => name,
        }
    }

    pub fn path(&self) -> &Path {
        match self {
            BftObject::Header { path, .. } | BftObject::Body { path, .. } => path.as_path(),
        }
    }

    /// Compute the remote key for this object.
    pub fn key(&self) -> String {
        match self {
            BftObject::Header { name, .. } => BftArchiveStore::header_key(name),
            BftObject::Body { name, .. } => BftArchiveStore::body_key(name),
        }
    }
}

/// Key prefix and file extensions used by the BFT archive worker.
pub const BFT_BLOCK_ID_INDEX_PREFIX: &str = "bft_block_id_index/";
pub const BFT_BLOCK_PREFIX: &str = "bft_block/";
pub const BFT_BLOCK_HEADER_EXTENSION: &str = ".header";
pub const BFT_BLOCK_BODY_EXTENSION: &str = ".body";

/// Ledger subdirectories scanned by the worker (exposed here for callers that need them).
pub const BFT_BLOCK_HEADER_FILE_PATH: &str = "headers/";
pub const BFT_BLOCK_BODY_FILE_PATH: &str = "bodies/";

/// Thin storage facade over `KVStoreErased` for BFT block headers/bodies.
#[derive(Clone)]
pub struct BftArchiveStore {
    pub kv: KVStoreErased,
}

impl BftArchiveStore {
    pub fn new(kv: KVStoreErased) -> Self {
        Self { kv }
    }

    /// Build the S3 (or KV) key for a header by file name.
    /// Example: "bft_block/<name>.header"
    pub fn header_key(name: &str) -> String {
        format!("{BFT_BLOCK_PREFIX}{name}{BFT_BLOCK_HEADER_EXTENSION}")
    }

    /// Build the S3 (or KV) key for a body by file name.
    /// Example: "bft_block/<name>.body"
    pub fn body_key(name: &str) -> String {
        format!("{BFT_BLOCK_PREFIX}{name}{BFT_BLOCK_BODY_EXTENSION}")
    }

    pub fn block_id_index_key(block_num: u64) -> String {
        format!(
            "{BFT_BLOCK_ID_INDEX_PREFIX}{:0width$}",
            block_num,
            width = BLOCK_PADDING_WIDTH
        )
    }

    /// Poor-man's exists: list with the exact key as prefix and look for an exact match.
    /// Matches `s3_exists_key` semantics in the worker.
    pub async fn exists_exact(&self, key: &str) -> Result<bool> {
        let objs = self.kv.scan_prefix(key).await?;
        Ok(objs.iter().any(|k| k == key))
    }

    pub async fn exists_header(&self, name: &str) -> Result<bool> {
        let key = Self::header_key(name);
        self.exists_exact(&key).await
    }

    pub async fn exists_body(&self, name: &str) -> Result<bool> {
        let key = Self::body_key(name);
        self.exists_exact(&key).await
    }

    /// Put a header blob under the exact key used by the worker.
    pub async fn put_header(&self, name: &str, data: Vec<u8>) -> Result<()> {
        let key = Self::header_key(name);
        self.kv.put(&key, data).await
    }

    pub async fn put_block_id_index(&self, block_id: BlockId, block_num: u64) -> Result<()> {
        let key = Self::block_id_index_key(block_num);
        self.kv
            .put(&key, block_id.0 .0.to_vec())
            .await
            .wrap_err_with(|| format!("Failed to put BFT block ID index: {block_num}"))
    }

    pub async fn get_block_id_index(&self, block_num: u64) -> Result<Option<BlockId>> {
        let key = Self::block_id_index_key(block_num);
        let x = self.kv.get(&key).await?;
        let Some(bytes) = x else {
            return Ok(None);
        };
        let mut hash: [u8; 32] = [0; 32];
        hash.copy_from_slice(&bytes);
        Ok(Some(BlockId(monad_types::Hash(hash))))
    }

    /// Put a body blob under the exact key used by the worker.
    pub async fn put_body(&self, name: &str, data: Vec<u8>) -> Result<()> {
        let key = Self::body_key(name);
        self.kv.put(&key, data).await
    }

    /// Get a header blob by name. Returns None if not present.
    pub async fn get_header_raw(&self, name: &str) -> Result<Option<Bytes>> {
        let key = Self::header_key(name);
        self.kv.get(&key).await
    }

    /// Get a body blob by name. Returns None if not present.
    pub async fn get_body_raw(&self, name: &str) -> Result<Option<Bytes>> {
        let key = Self::body_key(name);
        self.kv.get(&key).await
    }

    async fn get_typed<T: alloy_rlp::Decodable>(&self, key: &str) -> Result<Option<T>> {
        let x = self.kv.get(&key).await?;
        let Some(bytes) = x else {
            return Ok(None);
        };
        let mut bytes: &[u8] = &bytes;
        Ok(Some(T::decode(&mut bytes)?))
    }

    pub async fn get_header(&self, name: BlockId) -> Result<Option<BftBlockHeader>> {
        let key = Self::header_key(&block_id_to_hex_prefix(&name.0));

        let x = self.kv.get(&key).await?;
        let Some(bytes) = x else {
            return Ok(None);
        };
        let header = decode_consensus_block_header_compat::<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
        >(&bytes)
        .wrap_err("Failed to decode BFT block header (compat)")?;
        Ok(Some(header))
    }

    pub async fn get_body(&self, name: ConsensusBlockBodyId) -> Result<Option<BftBlockBody>> {
        let key = Self::body_key(&block_id_to_hex_prefix(&name.0));

        self.get_typed(&key)
            .await
            .wrap_err("Failed to get BFT block body")
    }

    /// Ensure remote object exists. Returns true if newly uploaded, false if already present.
    /// Maintains existing metrics behavior: increments ALREADY_IN_S3 when present,
    /// and UPLOADED when it performs a put.
    pub async fn ensure(&self, obj: &BftObject, metrics: &Metrics) -> Result<bool> {
        let key = obj.key();
        if self.exists_exact(&key).await? {
            metrics.inc_counter(MetricNames::BFT_BLOCK_FILES_ALREADY_IN_S3);
            return Ok(false);
        }
        let bytes = tokio::fs::read(obj.path())
            .await
            .wrap_err("Failed to read local BFT file")?;

        // If it's a header, index by block number
        if let BftObject::Header { .. } = obj {
            let header: BftBlockHeader = decode_consensus_block_header_compat::<
                SignatureType,
                SignatureCollectionType,
                ExecutionProtocolType,
            >(bytes.as_slice())
            .wrap_err("Failed to decode BFT block header (compat)")?;
            let block_num = header.seq_num.0;
            self.put_block_id_index(header.get_id(), block_num).await?;
        }

        self.kv
            .put(&key, bytes)
            .await
            .wrap_err("Failed to upload BFT block")?;
        metrics.inc_counter(MetricNames::BFT_BLOCK_FILES_UPLOADED);
        Ok(true)
    }

    /// Get bytes by an already-formed key.
    pub async fn get_raw(&self, key: &str) -> Result<Option<Bytes>> {
        self.kv.get(&key).await
    }
}

// --- Backward-compatible decode helpers (legacy header formats) ---

#[derive(Clone, PartialEq, Eq, RlpDecodable, RlpEncodable)]
struct LegacyVoteV0 {
    pub id: BlockId,
    pub round: Round,
    pub epoch: Epoch,
    // legacy fields present in ~v0.8 headers
    pub parent_id: BlockId,
    pub parent_round: Round,
}

#[derive(Clone, PartialEq, Eq, RlpDecodable, RlpEncodable)]
struct LegacyQuorumCertificateV0<SCT> {
    pub info: LegacyVoteV0,
    pub signatures: SCT,
}

use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hasher, HasherType},
};
use monad_validator::signature_collection::SignatureCollection;

#[derive(Clone, PartialEq, Eq, RlpDecodable, RlpEncodable)]
struct ConsensusBlockHeaderV0_8<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub block_round: Round,
    pub epoch: Epoch,
    pub qc: LegacyQuorumCertificateV0<SCT>,
    pub author: NodeId<CertificateSignaturePubKey<ST>>,
    pub seq_num: SeqNum,
    pub timestamp_ns: u128,
    pub round_signature: RoundSignature<SCT::SignatureType>,
    pub delayed_execution_results: Vec<EPT::FinalizedHeader>,
    pub execution_inputs: EPT::ProposedHeader,
    pub block_body_id: ConsensusBlockBodyId,
}

impl<ST, SCT, EPT> ConsensusBlockHeaderV0_8<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn into_current(self) -> ConsensusBlockHeader<ST, SCT, EPT> {
        ConsensusBlockHeader {
            block_round: self.block_round,
            epoch: self.epoch,
            qc: QuorumCertificate {
                info: Vote {
                    id: self.qc.info.id,
                    round: self.qc.info.round,
                    epoch: self.qc.info.epoch,
                },
                signatures: self.qc.signatures,
            },
            author: self.author,
            seq_num: self.seq_num,
            timestamp_ns: self.timestamp_ns,
            round_signature: self.round_signature,
            delayed_execution_results: self.delayed_execution_results,
            execution_inputs: self.execution_inputs,
            block_body_id: self.block_body_id,
            base_fee: None,
            base_fee_trend: None,
            base_fee_moment: None,
        }
    }
}

/// Decode a ConsensusBlockHeader from RLP, with lenient handling of pre‑0.10
/// formats (notably ~v0.8 where QC.vote contained parent_id/parent_round).
pub fn decode_consensus_block_header_compat<ST, SCT, EPT>(
    buf: &[u8],
) -> alloy_rlp::Result<ConsensusBlockHeader<ST, SCT, EPT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // First try current decoding path
    if let Ok(mut_bytes_decoded) = (|| {
        let mut s = buf;
        ConsensusBlockHeader::<ST, SCT, EPT>::decode(&mut s)
    })() {
        return Ok(mut_bytes_decoded);
    }

    // Fallback: try legacy v0.8 header (QC with parent_id/parent_round)
    let mut s2 = buf;
    let legacy = ConsensusBlockHeaderV0_8::<ST, SCT, EPT>::decode(&mut s2)?;
    Ok(legacy.into_current())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvstore::memory::MemoryStorage;

    #[tokio::test]
    async fn test_put_get_and_exists() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let bft = BftArchiveStore::new(store.clone());

        // Nothing exists initially
        assert!(!bft.exists_header("block_x").await.unwrap());
        assert!(!bft.exists_body("block_x").await.unwrap());

        // Put header/body
        let hbytes = b"header-data".to_vec();
        let bbytes = b"body-data".to_vec();
        bft.put_header("block_x", hbytes.clone()).await.unwrap();
        bft.put_body("block_x", bbytes.clone()).await.unwrap();

        // Exists checks mirror worker semantics
        assert!(bft.exists_header("block_x").await.unwrap());
        assert!(bft.exists_body("block_x").await.unwrap());

        // Get payloads back
        assert_eq!(
            bft.get_header_raw("block_x")
                .await
                .unwrap()
                .unwrap()
                .to_vec(),
            hbytes
        );
        assert_eq!(
            bft.get_body_raw("block_x").await.unwrap().unwrap().to_vec(),
            bbytes
        );
    }
}
