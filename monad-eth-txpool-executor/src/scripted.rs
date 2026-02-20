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

use std::{
    collections::VecDeque,
    io,
    path::PathBuf,
    pin::Pin,
    task::Poll,
};

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS,
    TxEnvelope, EMPTY_OMMER_ROOT_HASH,
};
use alloy_rlp::Decodable;
use futures::Stream;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::block::{BlockPolicy, ProposedExecutionInputs};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{timestamp_ns_to_secs, EthBlockPolicy};
use monad_eth_types::{EthBlockBody, EthExecutionProtocol, ExtractEthAddress, ProposedEthHeader};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::StateBackend;
use monad_system_calls::{SystemTransactionGenerator, SYSTEM_SENDER_ETH_ADDRESS};
use monad_validator::signature_collection::SignatureCollection;
use tokio::{
    io::AsyncReadExt,
    net::{UnixListener, UnixStream},
    sync::mpsc,
};
use tracing::{debug, info, warn};

/// Builds `ProposedExecutionInputs` from a list of transactions and CreateProposal fields.
fn build_proposal(
    transactions: Vec<TxEnvelope>,
    beneficiary: [u8; 20],
    timestamp_ns: u128,
    proposal_gas_limit: u64,
    seq_num_value: u64,
    round_signature_hash: [u8; 32],
    base_fee_per_gas: u64,
) -> ProposedExecutionInputs<EthExecutionProtocol> {
    let body = EthBlockBody {
        transactions,
        ommers: Vec::new(),
        withdrawals: Vec::new(),
    };

    let header = ProposedEthHeader {
        transactions_root: *alloy_consensus::proofs::calculate_transaction_root(
            &body.transactions,
        ),
        ommers_hash: *EMPTY_OMMER_ROOT_HASH,
        withdrawals_root: *EMPTY_WITHDRAWALS,
        beneficiary: beneficiary.into(),
        difficulty: 0,
        number: seq_num_value,
        gas_limit: proposal_gas_limit,
        timestamp: timestamp_ns_to_secs(timestamp_ns),
        mix_hash: round_signature_hash,
        nonce: [0_u8; 8],
        extra_data: [0_u8; 32],
        base_fee_per_gas,
        blob_gas_used: 0,
        excess_blob_gas: 0,
        parent_beacon_block_root: [0_u8; 32],
        requests_hash: Some([0_u8; 32]),
    };

    ProposedExecutionInputs { header, body }
}

/// Core executor that runs in a tokio task, reading blocks from the IPC socket
/// and serving them when consensus asks for proposals.
struct ScriptedTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    block_queue: VecDeque<Vec<TxEnvelope>>,
    listener: UnixListener,
    connections: Vec<UnixStream>,

    block_policy: EthBlockPolicy<ST, SCT, CCT, CRT>,
    state_backend: SBT,
    chain_config: CCT,

    events_tx: mpsc::UnboundedSender<MempoolEvent<ST, SCT, EthExecutionProtocol>>,
    #[allow(dead_code)]
    events_rx: mpsc::UnboundedReceiver<MempoolEvent<ST, SCT, EthExecutionProtocol>>,
}

impl<ST, SCT, SBT, CCT, CRT> ScriptedTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn new(
        listener: UnixListener,
        block_policy: EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: SBT,
        chain_config: CCT,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        Self {
            block_queue: VecDeque::new(),
            listener,
            connections: Vec::new(),
            block_policy,
            state_backend,
            chain_config,
            events_tx,
            events_rx,
        }
    }

    async fn run(
        mut self,
        mut command_rx: mpsc::Receiver<
            Vec<
                TxPoolCommand<
                    ST,
                    SCT,
                    EthExecutionProtocol,
                    EthBlockPolicy<ST, SCT, CCT, CRT>,
                    SBT,
                    CCT,
                    CRT,
                >,
            >,
        >,
        event_tx: mpsc::Sender<MonadEvent<ST, SCT, EthExecutionProtocol>>,
    ) {
        loop {
            tokio::select! {
                biased;

                // Handle commands from consensus
                result = command_rx.recv() => {
                    let Some(commands) = result else {
                        warn!("scripted txpool: command channel closed, shutting down");
                        break;
                    };
                    self.handle_commands(commands);
                }

                // Drain events to send back to consensus
                Some(event) = self.events_rx.recv() => {
                    if let Err(err) = event_tx.send(MonadEvent::MempoolEvent(event)).await {
                        warn!(?err, "scripted txpool: failed to send event, shutting down");
                        break;
                    }
                }

                // Accept new IPC connections
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, _)) => {
                            info!("scripted txpool: new IPC connection");
                            self.connections.push(stream);
                        }
                        Err(e) => {
                            warn!(?e, "scripted txpool: IPC accept error");
                        }
                    }
                }

                // Read blocks from IPC connections
                _ = Self::read_blocks_from_connections(&mut self.connections, &mut self.block_queue), if !self.connections.is_empty() => {}
            }
        }
    }

    /// Read length-prefixed RLP messages from all connections, decode as Vec<TxEnvelope>,
    /// and push into the block queue.
    async fn read_blocks_from_connections(
        connections: &mut Vec<UnixStream>,
        block_queue: &mut VecDeque<Vec<TxEnvelope>>,
    ) {
        // We process one connection at a time. Connections that error are removed.
        let mut i = 0;
        while i < connections.len() {
            match Self::try_read_block(&mut connections[i]).await {
                Ok(Some(txs)) => {
                    info!(
                        num_txs = txs.len(),
                        queue_len = block_queue.len() + 1,
                        "scripted txpool: received block from IPC"
                    );
                    block_queue.push_back(txs);
                    i += 1;
                }
                Ok(None) => {
                    // No complete message ready yet
                    i += 1;
                }
                Err(e) => {
                    warn!(?e, "scripted txpool: removing IPC connection due to error");
                    connections.swap_remove(i);
                }
            }
        }
    }

    /// Try to read one length-prefixed RLP message from a connection.
    /// Returns Ok(Some(txs)) if a complete message was read,
    /// Ok(None) if not enough data yet, Err on connection error.
    async fn try_read_block(stream: &mut UnixStream) -> io::Result<Option<Vec<TxEnvelope>>> {
        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        if msg_len == 0 {
            return Ok(Some(Vec::new()));
        }

        // Read the message body
        let mut msg_buf = vec![0u8; msg_len];
        stream.read_exact(&mut msg_buf).await?;

        // Decode RLP as Vec<TxEnvelope>
        let txs = Vec::<TxEnvelope>::decode(&mut msg_buf.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("RLP decode error: {e}")))?;

        Ok(Some(txs))
    }

    fn handle_commands(
        &mut self,
        commands: Vec<
            TxPoolCommand<
                ST,
                SCT,
                EthExecutionProtocol,
                EthBlockPolicy<ST, SCT, CCT, CRT>,
                SBT,
                CCT,
                CRT,
            >,
        >,
    ) {
        for command in commands {
            match command {
                TxPoolCommand::BlockCommit(committed_blocks) => {
                    for committed_block in committed_blocks {
                        BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT, CCT, CRT>::update_committed_block(
                            &mut self.block_policy,
                            &committed_block,
                            &self.chain_config,
                        );
                    }
                }
                TxPoolCommand::CreateProposal {
                    node_id,
                    epoch,
                    round,
                    seq_num,
                    high_qc,
                    round_signature,
                    last_round_tc,
                    fresh_proposal_certificate,
                    proposal_gas_limit,
                    beneficiary,
                    timestamp_ns,
                    delayed_execution_results,
                    extending_blocks,
                    tx_limit: _,
                    proposal_byte_limit: _,
                } => {
                    let user_transactions = self.block_queue.pop_front().unwrap_or_default();

                    // Generate system transactions (Reward, Snapshot, EpochChange)
                    let extending_blocks_refs: Vec<_> = extending_blocks.iter().collect();
                    let next_system_txn_nonce = *self.block_policy
                        .get_account_base_nonces(
                            seq_num,
                            &self.state_backend,
                            &extending_blocks_refs,
                            [SYSTEM_SENDER_ETH_ADDRESS].iter(),
                        )
                        .expect("state backend nonce lookup succeeds")
                        .get(&SYSTEM_SENDER_ETH_ADDRESS)
                        .unwrap();

                    let parent_block_epoch = if let Some(extending_block) = extending_blocks.last() {
                        extending_block.get_epoch()
                    } else {
                        self.block_policy.get_last_commit_epoch()
                    };

                    let block_author = node_id.pubkey().get_eth_address();
                    let system_txns = SystemTransactionGenerator::generate_system_transactions(
                        seq_num,
                        epoch,
                        parent_block_epoch,
                        block_author,
                        next_system_txn_nonce,
                        &self.chain_config,
                    );

                    let transactions: Vec<TxEnvelope> = system_txns
                        .into_iter()
                        .map(|sys_txn| {
                            let recovered: alloy_consensus::transaction::Recovered<TxEnvelope> = sys_txn.into();
                            recovered.into_inner()
                        })
                        .chain(user_transactions)
                        .collect();

                    debug!(
                        ?seq_num,
                        num_txs = transactions.len(),
                        queue_remaining = self.block_queue.len(),
                        "scripted txpool: creating proposal"
                    );

                    // Compute base fee from extending_blocks, matching real executor
                    let maybe_tfm_base_fees = self.block_policy.compute_base_fee(
                        &extending_blocks,
                        &self.chain_config,
                        timestamp_ns,
                    );

                    let (base_fee, base_fee_field, base_fee_trend_field, base_fee_moment_field) =
                        match maybe_tfm_base_fees {
                            Some((base_fee, base_fee_trend, base_fee_moment)) => (
                                base_fee,
                                Some(base_fee),
                                Some(base_fee_trend),
                                Some(base_fee_moment),
                            ),
                            None => (monad_tfm::base_fee::PRE_TFM_BASE_FEE, None, None, None),
                        };

                    let proposed_execution_inputs = build_proposal(
                        transactions,
                        beneficiary,
                        timestamp_ns,
                        proposal_gas_limit,
                        seq_num.0,
                        round_signature.get_hash().0,
                        base_fee,
                    );

                    self.events_tx
                        .send(MempoolEvent::Proposal {
                            epoch,
                            round,
                            seq_num,
                            high_qc,
                            timestamp_ns,
                            round_signature,
                            base_fee: base_fee_field,
                            base_fee_trend: base_fee_trend_field,
                            base_fee_moment: base_fee_moment_field,
                            delayed_execution_results,
                            proposed_execution_inputs,
                            last_round_tc,
                            fresh_proposal_certificate,
                        })
                        .expect("events channel never dropped");
                }
                // All other commands are no-ops for scripted mode
                TxPoolCommand::EnterRound { .. }
                | TxPoolCommand::Reset { .. }
                | TxPoolCommand::InsertForwardedTxs { .. } => {}
            }
        }
    }
}

const DEFAULT_COMMAND_BUFFER_SIZE: usize = 1024;
const DEFAULT_EVENT_BUFFER_SIZE: usize = 1024;

/// Client handle for the ScriptedTxPoolExecutor.
/// Plugs into `ParentExecutor.txpool` as a drop-in replacement for `EthTxPoolExecutorClient`.
pub struct ScriptedTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    handle: tokio::task::JoinHandle<()>,
    command_tx: mpsc::Sender<
        Vec<
            TxPoolCommand<
                ST,
                SCT,
                EthExecutionProtocol,
                EthBlockPolicy<ST, SCT, CCT, CRT>,
                SBT,
                CCT,
                CRT,
            >,
        >,
    >,
    event_rx: mpsc::Receiver<MonadEvent<ST, SCT, EthExecutionProtocol>>,
}

impl<ST, SCT, SBT, CCT, CRT> ScriptedTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable + Send + 'static,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Send + 'static,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT> + Send + 'static,
    CCT: ChainConfig<CRT> + Send + 'static,
    CRT: ChainRevision + Send + 'static,
{
    pub fn start(
        socket_path: PathBuf,
        block_policy: EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: SBT,
        chain_config: CCT,
    ) -> io::Result<Self> {
        // Clean up stale socket file
        let _ = std::fs::remove_file(&socket_path);

        let listener = {
            let std_listener = std::os::unix::net::UnixListener::bind(&socket_path)?;
            std_listener.set_nonblocking(true)?;
            UnixListener::from_std(std_listener)?
        };

        let (command_tx, command_rx) = mpsc::channel(DEFAULT_COMMAND_BUFFER_SIZE);
        let (event_tx, event_rx) = mpsc::channel(DEFAULT_EVENT_BUFFER_SIZE);

        let executor: ScriptedTxPoolExecutor<ST, SCT, SBT, CCT, CRT> =
            ScriptedTxPoolExecutor::new(listener, block_policy, state_backend, chain_config);

        let handle = tokio::spawn(executor.run(command_rx, event_tx));

        info!(?socket_path, "scripted txpool executor started");

        Ok(Self {
            handle,
            command_tx,
            event_rx,
        })
    }

    fn verify_handle_liveness(&self) {
        if self.handle.is_finished() {
            panic!("ScriptedTxPoolExecutorClient handle terminated!");
        }
        if self.command_tx.is_closed() {
            panic!("ScriptedTxPoolExecutorClient command channel closed!");
        }
    }
}

impl<ST, SCT, SBT, CCT, CRT> Executor for ScriptedTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable + Send + 'static,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Send + 'static,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT> + Send + 'static,
    CCT: ChainConfig<CRT> + Send + 'static,
    CRT: ChainRevision + Send + 'static,
{
    type Command = TxPoolCommand<
        ST,
        SCT,
        EthExecutionProtocol,
        EthBlockPolicy<ST, SCT, CCT, CRT>,
        SBT,
        CCT,
        CRT,
    >;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.verify_handle_liveness();

        if !commands.is_empty() {
            self.command_tx
                .try_send(commands)
                .expect("ScriptedTxPoolExecutorClient executor is lagging");
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
    }
}

impl<ST, SCT, SBT, CCT, CRT> Stream for ScriptedTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable + Send + 'static,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Send + 'static,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT> + Send + 'static,
    CCT: ChainConfig<CRT> + Send + 'static,
    CRT: ChainRevision + Send + 'static,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.verify_handle_liveness();
        this.event_rx.poll_recv(cx)
    }
}
