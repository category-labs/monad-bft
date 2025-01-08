use std::{ops::Deref, sync::Arc};

use crate::block::{ExecutionProtocol, FinalizedHeader, MockableFinalizedHeader};
use alloy_consensus::{Header, TxEnvelope};
use alloy_primitives::Address;
use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey,
    },
    hasher::{Hash, Hasher, HasherType},
};
use monad_eth_types::EMPTY_RLP_TX_LIST;
use monad_types::{DontCare, Round, SeqNum};
use serde::{Deserialize, Serialize};
use zerocopy::AsBytes;

const BLOOM_SIZE: usize = 256;

/// Type to represent the Ethereum Logs Bloom Filter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bloom(pub [u8; BLOOM_SIZE]);

impl Bloom {
    pub fn zero() -> Self {
        Bloom([0x00_u8; BLOOM_SIZE])
    }
}

impl AsRef<[u8]> for Bloom {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Ethereum unit of gas
#[repr(transparent)]
#[derive(Debug, Default, Copy, Clone, Eq, Ord, PartialEq, PartialOrd, AsBytes)]
pub struct Gas(pub u64);

impl AsRef<[u8]> for Gas {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

pub const BASE_FEE_PER_GAS: u64 = 50_000_000_000;
pub const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;
/// Max proposal size in bytes (average transactions ~400 bytes)
pub const PROPOSAL_SIZE_LIMIT: u64 = 4_000_000;

/// A subset of Ethereum block header fields that are included in consensus
/// proposals. The values are populated from the results of executing the
/// previous block
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Default)]
pub struct ProposedEthHeader {
    pub ommers_hash: [u8; 32],
    pub beneficiary: Address,
    pub transactions_root: [u8; 32],
    pub difficulty: u64,
    pub number: u64,
    pub gas_limit: u64,
    pub timestamp: u64,
    pub extra_data: [u8; 32],
    pub mix_hash: [u8; 32],
    pub nonce: [u8; 8],
    pub base_fee_per_gas: u64,
    pub withdrawals_root: [u8; 32],
    // cancun
    pub blob_gas_used: u64,
    pub excess_blob_gas: u64,
    pub parent_beacon_block_root: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct EthHeader(pub Header);

impl FinalizedHeader for EthHeader {
    fn seq_num(&self) -> SeqNum {
        SeqNum(self.0.number)
    }
}

impl MockableFinalizedHeader for EthHeader {
    fn from_seq_num(seq_num: SeqNum) -> Self {
        Self(Header {
            number: seq_num.0,
            ..Header::default()
        })
    }
}

#[derive(Clone, PartialEq, Eq, Debug, RlpEncodable, RlpDecodable)]
pub struct EthExecutionProtocol;
impl ExecutionProtocol for EthExecutionProtocol {
    type ProposedHeader = ProposedEthHeader;
    type FinalizedHeader = EthHeader;
    type Body = EthBlockBody;
}

/// RLP encoded list of a set of full RLP encoded Eth transactions
// Do NOT derive or implement Default!
// Empty byte array is not valid RLP
#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)] //FIXME: don't derive rlpencodable
pub struct FullTransactionList(Bytes);

impl FullTransactionList {
    pub fn empty() -> Self {
        Self::new(vec![EMPTY_RLP_TX_LIST].into())
    }

    pub fn new(txs: Bytes) -> Self {
        Self(txs)
    }

    pub fn bytes(&self) -> &Bytes {
        &self.0
    }
}

impl std::fmt::Debug for FullTransactionList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Txns").field(&self.0).finish()
    }
}

impl AsRef<[u8]> for FullTransactionList {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// randao_reveal uses a proposer's public key to contribute randomness
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct RoundSignature<CST: CertificateSignature>(pub CST);

impl<CST: CertificateSignature> RoundSignature<CST> {
    /// TODO should this incorporate parent_block_id to increase "randomness"?
    pub fn new(round: Round, keypair: &CST::KeyPairType) -> Self {
        let encoded_round = alloy_rlp::encode(&round);
        Self(CST::sign(&encoded_round, keypair))
    }

    pub fn verify(
        &self,
        round: Round,
        pubkey: &CertificateSignaturePubKey<CST>,
    ) -> Result<(), CST::Error> {
        let encoded_round = alloy_rlp::encode(&round);
        self.0.verify(&encoded_round, pubkey)
    }

    pub fn get_hash(&self) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(alloy_rlp::encode(self));
        hasher.hash()
    }
}

/// Contents of a proposal that are part of the Monad protocol
/// but not in the core bft consensus protocol
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct ConsensusBlockBody<EPT>(Arc<ConsensusBlockBodyInner<EPT>>)
where
    EPT: ExecutionProtocol;
impl<EPT> ConsensusBlockBody<EPT>
where
    EPT: ExecutionProtocol,
{
    pub fn new(body: ConsensusBlockBodyInner<EPT>) -> Self {
        Self(body.into())
    }
}
impl<EPT> Deref for ConsensusBlockBody<EPT>
where
    EPT: ExecutionProtocol,
{
    type Target = ConsensusBlockBodyInner<EPT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<EPT> DontCare for ConsensusBlockBody<EPT>
where 
    EPT: ExecutionProtocol,
{
    fn dont_care() -> Self {
        Self::new(ConsensusBlockBodyInner::<EPT> {
            execution_body: EPT::Body::default(),
        })
    }
}

#[derive(Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct ConsensusBlockBodyInner<EPT>
where
    EPT: ExecutionProtocol,
{
    pub execution_body: EPT::Body,
}

#[derive(PartialEq, Eq, Default, RlpEncodable, RlpDecodable)]
pub struct EthBlockBody {
    // TODO consider storing recovered txs inline here
    pub transactions: Vec<TxEnvelope>,
    pub ommers: Vec<Ommer>,
    pub withdrawals: Vec<Withdrawal>,
}

#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Ommer {}
#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Withdrawal {}

impl std::fmt::Debug for EthBlockBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthBlockBody")
            .field("num_txns", &format!("{}", self.transactions.len()))
            .finish_non_exhaustive()
    }
}

impl<EPT> ConsensusBlockBody<EPT>
where
    EPT: ExecutionProtocol,
{
    pub fn get_id(&self) -> ConsensusBlockBodyId {
        let mut hasher = HasherType::new();
        hasher.update(alloy_rlp::encode(self));
        ConsensusBlockBodyId(hasher.hash())
    }
}

#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct ConsensusBlockBodyId(pub Hash);

impl std::fmt::Debug for ConsensusBlockBodyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[30], self.0[31]
        )
    }
}
