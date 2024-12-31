use auto_impl::auto_impl;
use bytes::{Bytes, BytesMut};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_state_backend::{InMemoryState, StateBackend, StateBackendError};
use monad_types::SeqNum;

use crate::{
    block::{
        BlockPolicy, ExecutionProtocol, MockExecutionBody,
        MockExecutionProposedHeader, MockExecutionProtocol, PassthruBlockPolicy,
        ProposedExecutionInputs,
    },
    payload::RoundSignature,
    signature_collection::SignatureCollection,
};

#[derive(Debug)]
pub enum TxPoolInsertionError {
    NotWellFormed,
    NonceTooLow,
    FeeTooLow,
    InsufficientBalance,
    PoolFull,
    ExistingHigherPriority,
}

/// This trait represents the storage of transactions that
/// are potentially available for a proposal
#[auto_impl(Box)]
pub trait TxPool<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    /// Handle transactions:
    /// 1. submitted by users via RPC
    /// 2. forwarded by other validators/full-nodes
    ///
    /// Note that right now, tx is not guaranteed to be well-formed. The IPC path will always be
    /// well-formed, but the forwarded tx path is not.
    ///
    /// Because of this, for now, tx well-formedness must be checked again inside the insert_tx
    /// implementation. Ideally, the Bytes type should be replaced with a DecodedTx type which
    /// TxPool is generic over.
    fn insert_tx(
        &mut self,
        txns: Vec<Bytes>,
        block_policy: &BPT,
        state_backend: &SBT,
    ) -> Vec<Bytes>;

    /// Returns an RLP encoded lists of transactions to include in the proposal
    fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        beneficiary: &EthAddress,
        timestamp_ns: u128,
        round_signature: &RoundSignature<SCT::SignatureType>,

        block_policy: &BPT,
        pending_blocks: Vec<&BPT::ValidatedBlock>,
        state_backend: &SBT,
    ) -> Result<ProposedExecutionInputs<EPT>, StateBackendError>;

    /// Optional callback on block commit
    /// Can be used for clearing of stale txs from txpool
    fn update_committed_block(&mut self, committed_block: &BPT::ValidatedBlock);

    /// Reclaims memory used by internal TxPool datastructures
    fn clear(&mut self);

    fn reset(&mut self, last_delay_committed_blocks: Vec<&BPT::ValidatedBlock>);
}

use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng};

const MOCK_DEFAULT_SEED: u64 = 1;
const TXN_SIZE: usize = 32;

#[derive(Clone)]
pub struct MockTxPool {
    rng: ChaCha20Rng,
}

impl Default for MockTxPool {
    fn default() -> Self {
        Self {
            rng: ChaCha20Rng::seed_from_u64(MOCK_DEFAULT_SEED),
        }
    }
}

impl<ST, SCT> TxPool<ST, SCT, MockExecutionProtocol, PassthruBlockPolicy, InMemoryState>
    for MockTxPool
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn insert_tx(
        &mut self,
        _tx: Vec<Bytes>,
        _block_policy: &PassthruBlockPolicy,
        _state_backend: &InMemoryState,
    ) -> Vec<Bytes> {
        vec![]
    }

    fn create_proposal(
        &mut self,
        _proposed_seq_num: SeqNum,
        tx_limit: usize,
        _beneficiary: &EthAddress,
        _timestamp_ns: u128,
        _round_signature: &RoundSignature<SCT::SignatureType>,

        _block_policy: &PassthruBlockPolicy,
        _pending_blocks: Vec<
            &<PassthruBlockPolicy as BlockPolicy<ST, SCT, MockExecutionProtocol, InMemoryState>>::ValidatedBlock,
        >,
        _state_backend: &InMemoryState,
    ) -> Result<ProposedExecutionInputs<MockExecutionProtocol>, StateBackendError> {
        let header = MockExecutionProposedHeader {};
        let body = MockExecutionBody {
            data: {
                // Random non-empty value with size = num_fetch_txs * hash_size
                let mut buf = BytesMut::zeroed(tx_limit * TXN_SIZE);
                self.rng.fill_bytes(&mut buf);
                buf.freeze()
            },
        };
        Ok(ProposedExecutionInputs { header, body })
    }

    fn clear(&mut self) {}

    fn update_committed_block(
        &mut self,
        _committed_block: &<PassthruBlockPolicy as BlockPolicy<
            ST,
            SCT,
            MockExecutionProtocol,
            InMemoryState,
        >>::ValidatedBlock,
    ) {
    }

    fn reset(
        &mut self,
        _last_delay_committed_blocks: Vec<
            &<PassthruBlockPolicy as BlockPolicy<ST, SCT, MockExecutionProtocol, InMemoryState>>::ValidatedBlock,
        >,
    ) {
    }
}
