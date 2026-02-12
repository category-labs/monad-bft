use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
use alloy_primitives::B256;
use futures::StreamExt;
use monad_chain_config::MockChainConfig;
use monad_consensus_types::{
    block::GENESIS_TIMESTAMP, payload::RoundSignature, quorum_certificate::QuorumCertificate,
};
use monad_eth_testutil::generate_block_with_txs;
use monad_eth_txpool_executor::EthTxPoolExecutorClient;
use monad_executor::Executor;
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_secp::KeyPair;
use monad_state_backend::StateBackendTest;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum, GENESIS_ROUND, GENESIS_SEQ_NUM};

use crate::router::{CCT, CRT, SBT, SCT, ST};

type Client = EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>;

pub struct BlockCommitter {
    state: SBT,
    seq_num: SeqNum,
    round: Round,
    epoch: Epoch,
    node_id: NodeId<monad_secp::PubKey>,
    keypair: KeyPair,
    parent_block_id: BlockId,
    proposal_tx_limit: usize,
    proposal_gas_limit: u64,
    proposal_byte_limit: u64,
}

pub struct CommitResult {
    pub block_number: u64,
    pub txs_included: usize,
    pub commit_latency_us: u64,
    pub per_peer_included: BTreeMap<NodeId<monad_secp::PubKey>, usize>,
}

impl BlockCommitter {
    pub fn new(
        state: SBT,
        keypair: KeyPair,
        proposal_tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
    ) -> Self {
        let pubkey = keypair.pubkey();
        Self {
            state,
            seq_num: SeqNum(GENESIS_SEQ_NUM.0 + 1),
            round: Round(GENESIS_ROUND.0 + 1),
            epoch: Epoch(1),
            node_id: NodeId::new(pubkey),
            keypair,
            parent_block_id: monad_types::GENESIS_BLOCK_ID,
            proposal_tx_limit,
            proposal_gas_limit,
            proposal_byte_limit,
        }
    }

    pub async fn commit(
        &mut self,
        client: &mut Client,
        tx_peer_map: &HashMap<B256, NodeId<monad_secp::PubKey>>,
    ) -> CommitResult {
        let start = Instant::now();

        let round_sig = RoundSignature::new(self.round, &self.keypair);
        let high_qc = QuorumCertificate::<SCT>::genesis_qc();

        client.exec(vec![TxPoolCommand::CreateProposal {
            node_id: self.node_id,
            epoch: self.epoch,
            round: self.round,
            seq_num: self.seq_num,
            high_qc: high_qc.clone(),
            round_signature: round_sig,
            last_round_tc: None,
            fresh_proposal_certificate: None,
            tx_limit: self.proposal_tx_limit,
            proposal_gas_limit: self.proposal_gas_limit,
            proposal_byte_limit: self.proposal_byte_limit,
            beneficiary: [0u8; 20],
            timestamp_ns: GENESIS_TIMESTAMP + (self.seq_num.0 as u128 * 1_000_000_000),
            extending_blocks: vec![],
            delayed_execution_results: vec![],
        }]);

        let mut txs_included = 0usize;
        let mut recovered_txs: Vec<Recovered<TxEnvelope>> = Vec::new();

        loop {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(5), client.next()).await;

            match event {
                Ok(Some(MonadEvent::MempoolEvent(MempoolEvent::Proposal {
                    proposed_execution_inputs,
                    ..
                }))) => {
                    let body_txs = proposed_execution_inputs.body.transactions;
                    txs_included = body_txs.len();

                    for tx in body_txs {
                        let signer = tx.recover_signer().unwrap_or_default();
                        recovered_txs.push(Recovered::new_unchecked(tx, signer));
                    }
                    break;
                }
                Ok(Some(_)) => continue,
                Ok(None) => {
                    tracing::error!("executor stream ended");
                    break;
                }
                Err(_) => {
                    tracing::warn!("timeout waiting for proposal");
                    break;
                }
            }
        }

        let block = generate_block_with_txs(
            self.round,
            self.seq_num,
            MIN_BASE_FEE,
            &MockChainConfig::DEFAULT,
            recovered_txs.clone(),
        );

        let mut nonce_updates: BTreeMap<alloy_primitives::Address, u64> = BTreeMap::new();
        let mut per_peer_included: BTreeMap<NodeId<monad_secp::PubKey>, usize> = BTreeMap::new();
        for recovered in &recovered_txs {
            let addr = alloy_primitives::Address::from(recovered.signer().0);
            let nonce = recovered.nonce() + 1;
            nonce_updates
                .entry(addr)
                .and_modify(|n| *n = (*n).max(nonce))
                .or_insert(nonce);
            if let Some(peer) = tx_peer_map.get(recovered.tx_hash()) {
                *per_peer_included.entry(*peer).or_default() += 1;
            }
        }

        let block_id = block.get_id();

        self.state.ledger_propose(
            block_id,
            self.seq_num,
            self.round,
            self.parent_block_id,
            nonce_updates,
        );
        self.state.ledger_commit(&block_id, &self.seq_num);

        client.exec(vec![TxPoolCommand::BlockCommit(vec![block])]);

        let latency = start.elapsed();

        let block_number = self.seq_num.0;
        self.parent_block_id = block_id;
        self.seq_num = SeqNum(self.seq_num.0 + 1);
        self.round = Round(self.round.0 + 1);

        CommitResult {
            block_number,
            txs_included,
            commit_latency_us: latency.as_micros() as u64,
            per_peer_included,
        }
    }
}
