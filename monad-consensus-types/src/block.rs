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

use std::{collections::BTreeMap, fmt::Debug, ops::Deref};

use alloy_primitives::Address;
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use auto_impl::auto_impl;
use bytes::Bytes;
use monad_chain_config::{
    revision::{ChainRevision, MockChainRevision},
    ChainConfig, MockChainConfig,
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hasher, HasherType},
};
use monad_state_backend::{InMemoryState, StateBackend, StateBackendError};
use monad_types::{
    Balance, BlockId, Epoch, ExecutionProtocol, FinalizedHeader, NodeId, Round, SeqNum,
    GENESIS_SEQ_NUM,
};
use monad_validator::signature_collection::SignatureCollection;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::{
    checkpoint::RootInfo,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::QuorumCertificate,
};

pub const GENESIS_TIMESTAMP: u128 = 0;

/// Represent a range of blocks the last of which is `last_block_id` and includes `num_blocks`.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct BlockRange {
    pub last_block_id: BlockId,
    pub num_blocks: SeqNum,
}

/// structure of the consensus block
///
/// the payload field is used to carry the data of the block which is agnostic
/// to the actual protocol of consensus
///
/// We do not derive RlpEncodable/RlpDecodable with #[rlp(trailing)] because
/// decoding of 0x80 in trailing fields is ambiguous. Both None and Some(0_u64)
/// encode to 0x80. It decodes 0x80 to None only
#[serde_as]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "", deserialize = ""))]
pub struct ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// round this block was first proposed in
    /// note that this will differ from proposal_round for a reproposal
    #[serde_as(as = "DisplayFromStr")]
    pub block_round: Round,
    /// Epoch this block was proposed in
    #[serde_as(as = "DisplayFromStr")]
    pub epoch: Epoch,
    /// Certificate of votes for the parent block
    pub qc: QuorumCertificate<SCT>,
    /// proposer of this block
    pub author: NodeId<CertificateSignaturePubKey<ST>>,

    #[serde_as(as = "DisplayFromStr")]
    pub seq_num: SeqNum,
    #[serde_as(as = "DisplayFromStr")]
    pub timestamp_ns: u128,
    // This is SCT::SignatureType because SCT signatures are guaranteed to be deterministic
    pub round_signature: RoundSignature<SCT::SignatureType>,

    /// data related to the execution side of the protocol
    pub delayed_execution_results: Vec<EPT::FinalizedHeader>,
    pub execution_inputs: EPT::ProposedHeader,
    /// identifier for the transaction payload of this block
    pub block_body_id: ConsensusBlockBodyId,

    // Base fee update rule
    #[serde_as(as = "DisplayFromStr")]
    pub base_fee: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub base_fee_trend: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub base_fee_moment: u64,
}

/// Encodable and Decodable impls follow https://github.com/alloy-rs/alloy/blob/b0e06a09e9b18d13f9a5c0e0951a00a88545bc8f/crates/consensus/src/block/header.rs
impl<ST, SCT, EPT> Encodable for ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn length(&self) -> usize {
        let payload_length = self.rlp_payload_length();
        payload_length + alloy_rlp::length_of_length(payload_length)
    }

    fn encode(&self, out: &mut dyn bytes::BufMut) {
        alloy_rlp::Header {
            list: true,
            payload_length: self.rlp_payload_length(),
        }
        .encode(out);
        self.block_round.encode(out);
        self.epoch.encode(out);
        self.qc.encode(out);
        self.author.encode(out);
        self.seq_num.encode(out);
        self.timestamp_ns.encode(out);
        self.round_signature.encode(out);
        self.delayed_execution_results.encode(out);
        self.execution_inputs.encode(out);
        self.block_body_id.encode(out);
        self.base_fee.encode(out);
        self.base_fee_trend.encode(out);
        self.base_fee_moment.encode(out);
    }
}

impl<ST, SCT, EPT> Decodable for ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let rlp_header = alloy_rlp::Header::decode(buf)?;
        if !rlp_header.list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }
        let starting_len = buf.len();
        let this = Self {
            block_round: Decodable::decode(buf)?,
            epoch: Decodable::decode(buf)?,
            qc: Decodable::decode(buf)?,
            author: Decodable::decode(buf)?,
            seq_num: Decodable::decode(buf)?,
            timestamp_ns: Decodable::decode(buf)?,
            round_signature: Decodable::decode(buf)?,
            delayed_execution_results: Decodable::decode(buf)?,
            execution_inputs: Decodable::decode(buf)?,
            block_body_id: Decodable::decode(buf)?,
            base_fee: Decodable::decode(buf)?,
            base_fee_trend: Decodable::decode(buf)?,
            base_fee_moment: Decodable::decode(buf)?,
        };

        let consumed = starting_len - buf.len();
        if consumed != rlp_header.payload_length {
            return Err(alloy_rlp::Error::ListLengthMismatch {
                expected: rlp_header.payload_length,
                got: consumed,
            });
        }

        Ok(this)
    }
}

impl<ST, SCT, EPT> ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn get_id(&self) -> BlockId {
        let mut hasher = HasherType::new();
        hasher.update(alloy_rlp::encode(self));
        BlockId(hasher.hash())
    }

    pub fn get_parent_id(&self) -> BlockId {
        self.qc.get_block_id()
    }

    fn rlp_payload_length(&self) -> usize {
        let mut length = 0;
        length += self.block_round.length();
        length += self.epoch.length();
        length += self.qc.length();
        length += self.author.length();
        length += self.seq_num.length();
        length += self.timestamp_ns.length();
        length += self.round_signature.length();
        length += self.delayed_execution_results.length();
        length += self.execution_inputs.length();
        length += self.block_body_id.length();
        length += self.base_fee.length();
        length += self.base_fee_trend.length();
        length += self.base_fee_moment.length();

        length
    }
}

impl<ST, SCT, EPT> Debug for ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusBlockHeader")
            .field("author", &self.author)
            .field("epoch", &self.epoch)
            .field("block_round", &self.block_round)
            .field("block_body_id", &self.block_body_id)
            .field("qc", &self.qc)
            .field("seq_num", &self.seq_num)
            .field("timestamp_ns", &self.timestamp_ns)
            .field("id", &self.get_id())
            .field("base_fee", &self.base_fee)
            .field("base_fee_trend", &self.base_fee_trend.cast_signed())
            .field("base_fee_moment", &self.base_fee_moment)
            .finish_non_exhaustive()
    }
}

impl<ST, SCT, EPT> ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // FIXME &QuorumCertificate -> QuorumCertificate
    pub fn new(
        author: NodeId<SCT::NodeIdPubKey>,
        epoch: Epoch,
        block_round: Round,
        delayed_execution_results: Vec<EPT::FinalizedHeader>,
        execution_inputs: EPT::ProposedHeader,
        block_body_id: ConsensusBlockBodyId,
        qc: QuorumCertificate<SCT>,
        seq_num: SeqNum,
        timestamp_ns: u128,
        round_signature: RoundSignature<SCT::SignatureType>,
        base_fee: u64,
        base_fee_trend: u64,
        base_fee_moment: u64,
    ) -> Self {
        Self {
            author,
            epoch,
            block_round,
            delayed_execution_results,
            execution_inputs,
            block_body_id,
            qc,
            seq_num,
            timestamp_ns,
            round_signature,
            base_fee,
            base_fee_trend,
            base_fee_moment,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum BlockPolicyError {
    BlockNotCoherent,
    StateBackendError(StateBackendError),
    TimestampError,
    ExecutionResultMismatch,
    BaseFeeError,
    BlockPolicyBlockValidatorError(BlockPolicyBlockValidatorError),
    Eip7702Error,
    SystemTransactionError,
}

impl From<StateBackendError> for BlockPolicyError {
    fn from(err: StateBackendError) -> Self {
        Self::StateBackendError(err)
    }
}

#[derive(Debug, Clone)]
pub struct AccountBalanceState {
    pub balance: Balance,
    pub remaining_reserve_balance: Balance,
    pub max_reserve_balance: Balance,
    pub block_seqnum_of_latest_txn: SeqNum,
    pub is_delegated: bool,
}

impl AccountBalanceState {
    pub fn new(max_reserve_balance: Balance) -> Self {
        AccountBalanceState {
            balance: Balance::ZERO,
            remaining_reserve_balance: Balance::ZERO,
            max_reserve_balance,
            block_seqnum_of_latest_txn: GENESIS_SEQ_NUM,
            is_delegated: false,
        }
    }
}

pub type AccountBalanceStates = BTreeMap<Address, AccountBalanceState>;

#[derive(Debug, PartialEq)]
pub enum BlockPolicyBlockValidatorError {
    AccountBalanceMissing,
    InsufficientBalance,
    InsufficientReserveBalance,
}

/// Trait that represents how inner contents of a block should be validated
#[auto_impl(Box)]
pub trait BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    type ValidatedBlock: Sized
        + Clone
        + PartialEq
        + Eq
        + Debug
        + Send
        + Deref<Target = ConsensusFullBlock<ST, SCT, EPT>>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        blocktree_root: RootInfo,
        state_backend: &SBT,
        chain_cnfig: &CCT,
    ) -> Result<(), BlockPolicyError>;

    fn get_expected_execution_results(
        &self,
        block_seq_num: SeqNum,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        state_backend: &SBT,
    ) -> Result<Vec<EPT::FinalizedHeader>, StateBackendError>;

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn update_committed_block(&mut self, block: &Self::ValidatedBlock);

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn reset(&mut self, last_delay_committed_blocks: Vec<&Self::ValidatedBlock>);
}

/// A block policy which does not validate the inner contents of the block
#[derive(Copy, Clone, Default)]
pub struct PassthruBlockPolicy;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassthruWrappedBlock<ST, SCT, EPT>(pub ConsensusFullBlock<ST, SCT, EPT>)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol;

impl<ST, SCT, EPT> Deref for PassthruWrappedBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Target = ConsensusFullBlock<ST, SCT, EPT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<ST, SCT, EPT> From<ConsensusFullBlock<ST, SCT, EPT>> for PassthruWrappedBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(block: ConsensusFullBlock<ST, SCT, EPT>) -> Self {
        Self(block)
    }
}

impl<ST, SCT, EPT>
    BlockPolicy<ST, SCT, EPT, InMemoryState<ST, SCT>, MockChainConfig, MockChainRevision>
    for PassthruBlockPolicy
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type ValidatedBlock = PassthruWrappedBlock<ST, SCT, EPT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        blocktree_root: RootInfo,
        state_backend: &InMemoryState<ST, SCT>,
        _chain_config: &MockChainConfig,
    ) -> Result<(), BlockPolicyError> {
        // check coherency against the block being extended or against the root of the blocktree if
        // there is no extending branch
        let (extending_seq_num, extending_timestamp) =
            if let Some(extended_block) = extending_blocks.last() {
                (extended_block.get_seq_num(), extended_block.get_timestamp())
            } else {
                (blocktree_root.seq_num, 0) //TODO: add timestamp to RootInfo
            };

        if block.get_seq_num() != extending_seq_num + SeqNum(1) {
            return Err(BlockPolicyError::BlockNotCoherent);
        }

        if block.get_timestamp() <= extending_timestamp {
            // timestamps must be monotonically increasing
            return Err(BlockPolicyError::TimestampError);
        }

        let expected_execution_results = self.get_expected_execution_results(
            block.get_seq_num(),
            extending_blocks,
            state_backend,
        )?;
        if block.get_execution_results() != &expected_execution_results {
            return Err(BlockPolicyError::ExecutionResultMismatch);
        }

        Ok(())
    }

    fn get_expected_execution_results(
        &self,
        _block_seq_num: SeqNum,
        _extending_blocks: Vec<&Self::ValidatedBlock>,
        _state_backend: &InMemoryState<ST, SCT>,
    ) -> Result<Vec<EPT::FinalizedHeader>, StateBackendError> {
        Ok(Vec::new())
    }

    fn update_committed_block(&mut self, _: &Self::ValidatedBlock) {}
    fn reset(&mut self, _: Vec<&Self::ValidatedBlock>) {}
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable, Serialize)]
pub struct ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    header: ConsensusBlockHeader<ST, SCT, EPT>,
    body: ConsensusBlockBody<EPT>,
}

impl<ST, SCT, EPT> PartialEq for ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn eq(&self, other: &Self) -> bool {
        self.header.get_id() == other.header.get_id()
    }
}
impl<ST, SCT, EPT> Eq for ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
}

#[derive(Debug)]
pub enum ConsensusFullBlockError {
    HeaderPayloadMismatch,
}

impl<ST, SCT, EPT> ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        header: ConsensusBlockHeader<ST, SCT, EPT>,
        body: ConsensusBlockBody<EPT>,
    ) -> Result<Self, ConsensusFullBlockError> {
        if body.get_id() != header.block_body_id {
            return Err(ConsensusFullBlockError::HeaderPayloadMismatch);
        }
        Ok(Self { header, body })
    }

    pub fn header(&self) -> &ConsensusBlockHeader<ST, SCT, EPT> {
        &self.header
    }
    pub fn body(&self) -> &ConsensusBlockBody<EPT> {
        &self.body
    }

    pub fn get_parent_id(&self) -> BlockId {
        self.header.qc.get_block_id()
    }
    pub fn get_id(&self) -> BlockId {
        self.header.get_id()
    }
    pub fn get_body_id(&self) -> ConsensusBlockBodyId {
        self.header.block_body_id
    }
    pub fn get_block_round(&self) -> Round {
        self.header.block_round
    }
    pub fn get_parent_round(&self) -> Round {
        self.header.qc.get_round()
    }
    pub fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.header.qc
    }
    pub fn get_epoch(&self) -> Epoch {
        self.header.epoch
    }
    pub fn get_seq_num(&self) -> SeqNum {
        self.header.seq_num
    }
    pub fn get_timestamp(&self) -> u128 {
        self.header.timestamp_ns
    }
    pub fn get_base_fee(&self) -> u64 {
        self.header.base_fee
    }
    pub fn get_base_fee_trend(&self) -> u64 {
        self.header.base_fee_trend
    }
    pub fn get_base_fee_moment(&self) -> u64 {
        self.header.base_fee_moment
    }
    pub fn get_author(&self) -> &NodeId<CertificateSignaturePubKey<ST>> {
        &self.header.author
    }

    pub fn get_execution_results(&self) -> &Vec<EPT::FinalizedHeader> {
        &self.header.delayed_execution_results
    }

    pub fn split(self) -> (ConsensusBlockHeader<ST, SCT, EPT>, ConsensusBlockBody<EPT>) {
        (self.header, self.body)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpDecodable, RlpEncodable, Serialize)]
pub struct ProposedExecutionInputs<EPT>
where
    EPT: ExecutionProtocol,
{
    pub header: EPT::ProposedHeader,
    pub body: EPT::Body,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct MockExecutionProtocol {}

impl ExecutionProtocol for MockExecutionProtocol {
    type ProposedHeader = MockExecutionProposedHeader;
    type Body = MockExecutionBody;
    type FinalizedHeader = MockExecutionFinalizedHeader;
}

#[derive(
    Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize, Default,
)]
pub struct MockExecutionProposedHeader {}

#[derive(
    Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize, Default,
)]
pub struct MockExecutionBody {
    pub data: Bytes,
}
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
pub struct MockExecutionFinalizedHeader {
    number: SeqNum,
}
impl FinalizedHeader for MockExecutionFinalizedHeader {
    fn seq_num(&self) -> SeqNum {
        self.number
    }
}

// This type is one level "higher" than the OptimisticCommit type below in that this type retains
// the block policy's validated block type. This is useful when passing blocks to executors like the
// txpool which leverage information about the block body itself, which is only available at the
// block policy validated level, rather than using it in a "type abstracted" way like the ledger
// which uses the "Encodable" trait to simply write the bytes to a file without needing to inspect
// the body itself.
#[derive(Debug)]
pub enum OptimisticPolicyCommit<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    Proposed(BPT::ValidatedBlock),
    Finalized(BPT::ValidatedBlock),
}

#[derive(Debug)]
pub enum OptimisticCommit<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Proposed(ConsensusFullBlock<ST, SCT, EPT>),
    Finalized(ConsensusFullBlock<ST, SCT, EPT>),
}
impl<ST, SCT, EPT, BPT, SBT, CCT, CRT>
    From<&OptimisticPolicyCommit<ST, SCT, EPT, BPT, SBT, CCT, CRT>>
    for OptimisticCommit<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn from(value: &OptimisticPolicyCommit<ST, SCT, EPT, BPT, SBT, CCT, CRT>) -> Self {
        match value {
            OptimisticPolicyCommit::Proposed(block) => Self::Proposed(block.deref().to_owned()),
            OptimisticPolicyCommit::Finalized(block) => Self::Finalized(block.deref().to_owned()),
        }
    }
}

#[cfg(test)]
mod test {
    use monad_bls::BlsSignatureCollection;
    use monad_secp::SecpSignature;

    use super::*;

    type SignatureType = SecpSignature;
    type SignatureCollectionType =
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
    type ExecutionProtocolType = MockExecutionProtocol;
    use monad_testutil::signing::{get_certificate_key, get_key};

    #[test]
    fn test_header_rlp_roundtrip_trailing_zero() {
        let key = get_key::<SignatureType>(1246);
        let cert_key = get_certificate_key::<SignatureCollectionType>(22354);

        let header = ConsensusBlockHeader::<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
        >::new(
            NodeId::new(key.pubkey()),
            Epoch(1),
            Round(10),
            Vec::new(),
            MockExecutionProposedHeader {},
            ConsensusBlockBodyId(monad_crypto::hasher::Hash::default()),
            QuorumCertificate::genesis_qc(),
            SeqNum(10),
            12658127,
            RoundSignature::new(Round(10), &cert_key),
            124,
            0,
            100,
        );

        let encoded = alloy_rlp::encode(&header);
        let decoded: ConsensusBlockHeader<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
        > = alloy_rlp::decode_exact(&encoded).unwrap();

        let re_encoded = alloy_rlp::encode(&decoded);
        assert_eq!(re_encoded, encoded);
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_header_toml_roundtrip_u128_max() {
        let key = get_key::<SignatureType>(1246);
        let cert_key = get_certificate_key::<SignatureCollectionType>(22354);

        let header = ConsensusBlockHeader::<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
        >::new(
            NodeId::new(key.pubkey()),
            Epoch::MAX,
            Round::MAX,
            Vec::new(),
            MockExecutionProposedHeader {},
            ConsensusBlockBodyId(monad_crypto::hasher::Hash::default()),
            QuorumCertificate::genesis_qc(),
            SeqNum::MAX,
            u128::MAX,
            RoundSignature::new(Round::MAX, &cert_key),
            u64::MAX,
            u64::MAX,
            u64::MAX,
        );

        let encoded = toml::to_string_pretty(&header).unwrap();
        let decoded: ConsensusBlockHeader<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
        > = toml::from_str(&encoded).unwrap();

        let re_encoded = toml::to_string_pretty(&decoded).unwrap();
        assert_eq!(re_encoded, encoded);
        assert_eq!(decoded, header);
    }
}
