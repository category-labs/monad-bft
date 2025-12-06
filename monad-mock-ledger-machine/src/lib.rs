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

use alloy_consensus::constants::EMPTY_WITHDRAWALS;
use alloy_consensus::proofs::calculate_transaction_root;
use alloy_consensus::TxEnvelope;
use alloy_consensus::EMPTY_OMMER_ROOT_HASH;
use monad_bls::{BlsKeyPair, BlsSignatureCollection};
use monad_chain_config::revision::ChainRevision;
use monad_chain_config::revision::MonadChainRevision;
use monad_chain_config::ChainConfig;
use monad_chain_config::MonadChainConfig;
use monad_consensus_types::block::{OptimisticCommit, GENESIS_TIMESTAMP};
use monad_consensus_types::block_validator::BlockValidator;
use monad_consensus_types::metrics;
use monad_consensus_types::payload::RoundSignature;
use monad_consensus_types::voting::Vote;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_crypto::signing_domain;
use monad_eth_block_policy::timestamp_ns_to_secs;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::error::EthBlockValidationError;
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_types::EthExecutionProtocol;
use monad_eth_types::ProposedEthHeader;
use monad_executor::Executor;
use monad_secp::{KeyPair, PubKey, SecpSignature};
use monad_state_backend::InMemoryState;
use monad_types::GENESIS_ROUND;
use monad_types::{Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
use monad_validator::signature_collection::SignatureCollection;
use monad_validator::validator_mapping::ValidatorMapping;
use std::collections::VecDeque;
use std::path::PathBuf;

use monad_bls::BlsSignature;
use monad_consensus_types::block::ConsensusBlockHeader;
use monad_consensus_types::block::ConsensusFullBlock;
use monad_consensus_types::payload::ConsensusBlockBody;
use monad_consensus_types::payload::ConsensusBlockBodyInner;
use monad_consensus_types::quorum_certificate::QuorumCertificate;
use monad_eth_types::EthBlockBody;
use monad_executor_glue::LedgerCommand;
use monad_ledger::MonadBlockFileLedger;

pub type SignatureType = SecpSignature;

pub type SignatureCollectionType = BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

pub type BlockType = ConsensusFullBlock<SignatureType, SignatureCollectionType, EthExecutionProtocol>;

type StateBackendType = InMemoryState<SignatureType, SignatureCollectionType>;

type LedgerType = MonadBlockFileLedger<SignatureType, SignatureCollectionType>;

type BlockPolicyType =
    EthBlockPolicy<SignatureType, SignatureCollectionType, MonadChainConfig, MonadChainRevision>;

type ValidatorType = dyn BlockValidator<
    SignatureType,
    SignatureCollectionType,
    EthExecutionProtocol,
    BlockPolicyType,
    StateBackendType,
    MonadChainConfig,
    MonadChainRevision,
    BlockValidationError = EthBlockValidationError,
>;

fn node_id_from_private_key(mut node_private_key: [u8; 32]) -> NodeId<PubKey> {
    let node_keypair = KeyPair::from_bytes(&mut node_private_key).unwrap();
    let node_pubkey_bytes = node_keypair.pubkey().bytes();
    let node_pubkey = PubKey::from_slice(node_pubkey_bytes.as_slice()).unwrap();
    NodeId::new(node_pubkey)
}

pub struct MonadMockLedgerMachine {
    chain_config: MonadChainConfig,
    timestamp: u128,
    epoch: Epoch,
    round: Round,
    seq_num: SeqNum,
    ledger: LedgerType,
    proposer_node_id: NodeId<PubKey>,
    unfinalized_blocks: VecDeque<BlockType>,
    qc: QuorumCertificate<SignatureCollectionType>,
}

impl MonadMockLedgerMachine {
    pub fn new(
        chain_config: MonadChainConfig,
        ledger_path: PathBuf,
        proposer_private_key: [u8; 32],
    ) -> MonadMockLedgerMachine {
        MonadMockLedgerMachine {
            chain_config,
            timestamp: GENESIS_TIMESTAMP,
            epoch: Epoch(0),
            round: GENESIS_ROUND,
            seq_num: GENESIS_SEQ_NUM + SeqNum(1),
            ledger: LedgerType::new(ledger_path),
            proposer_node_id: node_id_from_private_key(proposer_private_key),
            unfinalized_blocks: VecDeque::default(),
            qc: QuorumCertificate::genesis_qc(),
        }
    }

    pub fn unfinalized_blocks(&self) -> &VecDeque<BlockType> {
        &self.unfinalized_blocks
    }

    pub fn get_round(&self) -> Round {
        self.round
    }

    pub fn inc_round(&mut self, n: u64) {
        self.round = self.round + Round(n)
    }

    pub fn inc_epoch(&mut self, n: u64) {
        self.epoch = self.epoch + Epoch(n)
    }

    pub fn inc_timestamp(&mut self, t: u128) {
        self.timestamp += t
    }

    fn round_signature(&self) -> RoundSignature<BlsSignature> {
        let mut keypair_bytes = [0u8; 32];
        let keypair = BlsKeyPair::from_bytes(&mut keypair_bytes).unwrap();
        RoundSignature::new(self.round, &keypair)
    }

    pub fn propose(
        &mut self,
        txs: Vec<TxEnvelope>,
        base_fee: u64,
        base_fee_trend: u64,
        base_fee_moment: u64,
        beneficiary: [u8; 20],
    ) {
        let round_sig = self.round_signature();
        let chain_config = self.chain_config;
        let node_id = self.proposer_node_id;

        let body = EthBlockBody {
            transactions: txs,
            ommers: Vec::new(),
            withdrawals: Vec::new(),
        };

        let maybe_request_hash = if self
            .chain_config
            .get_execution_chain_revision(self.round.0)
            .execution_chain_params()
            .prague_enabled
        {
            Some([0_u8; 32])
        } else {
            None
        };

        let header = ProposedEthHeader {
            transactions_root: *calculate_transaction_root(&body.transactions),
            ommers_hash: *EMPTY_OMMER_ROOT_HASH,
            withdrawals_root: *EMPTY_WITHDRAWALS,
            beneficiary: beneficiary.into(),
            difficulty: 0,
            number: self.seq_num.0,
            gas_limit: self
                .chain_config
                .get_chain_revision(self.round)
                .chain_params()
                .proposal_gas_limit,
            timestamp: timestamp_ns_to_secs(self.timestamp),
            mix_hash: round_sig.get_hash().0,
            nonce: [0_u8; 8],
            extra_data: [0_u8; 32],
            base_fee_per_gas: base_fee,
            blob_gas_used: 0,
            excess_blob_gas: 0,
            parent_beacon_block_root: [0_u8; 32],
            requests_hash: maybe_request_hash,
        };

        let consensus_block_body_inner = ConsensusBlockBodyInner {
            execution_body: body,
        };
        let consensus_block_body = ConsensusBlockBody::new(consensus_block_body_inner);

        let consensus_block_header = ConsensusBlockHeader {
            block_round: self.round,
            epoch: self.epoch,
            qc: self.qc.clone(),
            author: node_id,
            seq_num: self.seq_num,
            timestamp_ns: self.timestamp,
            round_signature: round_sig,
            delayed_execution_results: Vec::default(),
            execution_inputs: header,
            block_body_id: consensus_block_body.get_id(),
            base_fee: Some(base_fee),
            base_fee_trend: Some(base_fee_trend),
            base_fee_moment: Some(base_fee_moment),
        };

        let consensus_full_block =
            ConsensusFullBlock::new(consensus_block_header, consensus_block_body).unwrap();
        let validator: &ValidatorType = &EthBlockValidator::default();
        let mut validator_metrics = metrics::Metrics::default();
        let _ = validator
            .validate(
                consensus_full_block.header().clone(),
                consensus_full_block.body().clone(),
                None,
                &chain_config,
                &mut validator_metrics,
            )
            .expect("valid block");

        let optimistic_commit = OptimisticCommit::Proposed(consensus_full_block.clone());

        let ledger_command = LedgerCommand::LedgerCommit(optimistic_commit);
        self.ledger.exec(vec![ledger_command]);

        self.qc = QuorumCertificate::new(
            Vote {
                id: consensus_full_block.get_id(),
                round: self.round,
                epoch: self.epoch,
            },
            SignatureCollectionType::new::<signing_domain::Vote>(
                Vec::default().into_iter(),
                &ValidatorMapping::new(Vec::default().into_iter()),
                &[],
            )
            .unwrap(),
        );
        self.seq_num += SeqNum(1);
        self.unfinalized_blocks.push_back(consensus_full_block);
    }

    pub fn finalize(&mut self) {
        let consensus_full_block = self
            .unfinalized_blocks
            .pop_front()
            .expect("unfinalized block");
        let optimistic_commit = OptimisticCommit::Finalized(consensus_full_block);
        let ledger_command = LedgerCommand::LedgerCommit(optimistic_commit);
        self.ledger.exec(vec![ledger_command]);
    }
}
