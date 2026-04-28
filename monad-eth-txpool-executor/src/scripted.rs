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

use std::{collections::VecDeque, io, path::PathBuf, pin::Pin, task::Poll};

use alloy_consensus::{constants::EMPTY_WITHDRAWALS, TxEnvelope, EMPTY_OMMER_ROOT_HASH};
use alloy_rlp::Encodable;
use futures::{Stream, StreamExt};
use monad_chain_config::{
    revision::{ChainRevision, CHAIN_PARAMS_LATEST},
    ChainConfig,
};
use monad_consensus_types::block::{BlockPolicy, ProposedExecutionInputs};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{timestamp_ns_to_secs, EthBlockPolicy};
use monad_eth_types::{EthBlockBody, EthExecutionProtocol, ExtractEthAddress, ProposedEthHeader};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, TxPoolCommand};
use monad_peer_score::{ema, StdClock};
use monad_state_backend::StateBackend;
use monad_system_calls::{SystemTransactionGenerator, SYSTEM_SENDER_ETH_ADDRESS};
use monad_types::NodeId;
use monad_validator::signature_collection::SignatureCollection;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::mpsc,
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, info, warn};

use crate::{
    client::ForwardedTxs, EthTxPoolExecutorClient, ForwardedIngressFairQueueConfig,
    TxPoolExecutorCommand, TxPoolExecutorEvent,
};

fn build_length_delimited_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(CHAIN_PARAMS_LATEST.proposal_byte_limit as usize)
        .new_codec()
}

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
        transactions: monad_types::LimitedVec(transactions),
        ommers: Default::default(),
        withdrawals: Default::default(),
    };

    let header = ProposedEthHeader {
        transactions_root: *alloy_consensus::proofs::calculate_transaction_root(&body.transactions),
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
    connection: Option<FramedRead<UnixStream, LengthDelimitedCodec>>,

    block_policy: EthBlockPolicy<ST, SCT, CCT, CRT>,
    state_backend: SBT,
    chain_config: CCT,

    events_tx: mpsc::UnboundedSender<TxPoolExecutorEvent<ST, SCT, EthExecutionProtocol>>,
    events_rx: mpsc::UnboundedReceiver<TxPoolExecutorEvent<ST, SCT, EthExecutionProtocol>>,
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
            connection: None,
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
                TxPoolExecutorCommand<
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
        mut forwarded_rx: mpsc::Receiver<Vec<ForwardedTxs<SCT>>>,
        event_tx: mpsc::Sender<TxPoolExecutorEvent<ST, SCT, EthExecutionProtocol>>,
    ) {
        loop {
            tokio::select! {
                biased;

                result = command_rx.recv() => {
                    let Some(commands) = result else {
                        warn!("scripted txpool: command channel closed, shutting down");
                        break;
                    };
                    self.handle_commands(commands);
                }

                Some(forwarded_txs) = forwarded_rx.recv() => {
                    debug!(
                        batch_items = forwarded_txs.len(),
                        "scripted txpool: dropping forwarded txs"
                    );
                }

                Some(event) = self.events_rx.recv() => {
                    if let Err(err) = event_tx.send(event).await {
                        warn!(?err, "scripted txpool: failed to send event, shutting down");
                        break;
                    }
                }

                result = self.listener.accept() => {
                    match result {
                        Ok((stream, _)) => {
                            info!("scripted txpool: new IPC connection, replacing previous");
                            self.connection = Some(FramedRead::new(stream, build_length_delimited_codec()));
                        }
                        Err(e) => {
                            warn!(?e, "scripted txpool: IPC accept error");
                        }
                    }
                }

                result = Self::read_from_connection(&mut self.connection) => {
                    match result {
                        Ok(txs) => {
                            info!(
                                num_txs = txs.len(),
                                queue_len = self.block_queue.len() + 1,
                                "scripted txpool: received block from IPC"
                            );
                            self.block_queue.push_back(txs);
                        }
                        Err(e) => {
                            warn!(?e, "scripted txpool: IPC connection error, dropping");
                            self.connection = None;
                        }
                    }
                }
            }
        }
    }

    /// Read from the current connection if one exists, otherwise pend forever.
    async fn read_from_connection(
        connection: &mut Option<FramedRead<UnixStream, LengthDelimitedCodec>>,
    ) -> io::Result<Vec<TxEnvelope>> {
        match connection.as_mut() {
            Some(conn) => Self::try_read_block(conn).await,
            None => std::future::pending().await,
        }
    }

    async fn try_read_block(
        stream: &mut FramedRead<UnixStream, LengthDelimitedCodec>,
    ) -> io::Result<Vec<TxEnvelope>> {
        let Some(msg_buf) = stream.next().await.transpose()? else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "IPC connection closed",
            ));
        };

        if msg_buf.is_empty() {
            return Ok(Vec::new());
        }

        alloy_rlp::decode_exact::<Vec<TxEnvelope>>(&msg_buf).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("RLP decode error: {e}"))
        })
    }

    fn handle_commands(
        &mut self,
        commands: Vec<
            TxPoolExecutorCommand<
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
                TxPoolExecutorCommand::BlockCommit(committed_blocks) => {
                    for committed_block in committed_blocks {
                        BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT, CCT, CRT>::update_committed_block(
                            &mut self.block_policy,
                            &committed_block,
                        );
                    }
                }
                TxPoolExecutorCommand::CreateProposal {
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
                    tx_limit,
                    proposal_byte_limit,
                } => {
                    let user_transactions = self.block_queue.pop_front().unwrap_or_default();

                    // Generate system transactions (Reward, Snapshot, EpochChange)
                    let extending_blocks_refs: Vec<_> = extending_blocks.iter().collect();
                    let next_system_txn_nonce = *self
                        .block_policy
                        .get_account_base_nonces(
                            seq_num,
                            &self.state_backend,
                            &extending_blocks_refs,
                            [SYSTEM_SENDER_ETH_ADDRESS].iter(),
                        )
                        .expect("state backend nonce lookup succeeds")
                        .get(&SYSTEM_SENDER_ETH_ADDRESS)
                        .unwrap();

                    let parent_block_epoch = if let Some(extending_block) = extending_blocks.last()
                    {
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

                    // System transactions are mandatory and always included first.
                    let system_tx_envelopes: Vec<TxEnvelope> = system_txns
                        .into_iter()
                        .map(|sys_txn| {
                            let recovered: alloy_consensus::transaction::Recovered<TxEnvelope> =
                                sys_txn.into();
                            recovered.into_inner()
                        })
                        .collect();

                    let mut total_size: u64 = system_tx_envelopes
                        .iter()
                        .map(|tx| tx.length() as u64)
                        .sum();
                    let mut transactions = system_tx_envelopes;

                    // Enforce tx_limit and proposal_byte_limit on user transactions.
                    for tx in user_transactions {
                        if transactions.len() >= tx_limit {
                            break;
                        }
                        let tx_size = tx.length() as u64;
                        if total_size.saturating_add(tx_size) > proposal_byte_limit {
                            break;
                        }
                        total_size += tx_size;
                        transactions.push(tx);
                    }

                    debug!(
                        ?seq_num,
                        num_txs = transactions.len(),
                        queue_remaining = self.block_queue.len(),
                        "scripted txpool: creating proposal"
                    );

                    let (base_fee, base_fee_trend, base_fee_moment) = self
                        .block_policy
                        .compute_base_fee(&extending_blocks, &self.chain_config);

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
                        .send(TxPoolExecutorEvent::Proposal {
                            epoch,
                            round,
                            seq_num,
                            high_qc,
                            timestamp_ns,
                            round_signature,
                            base_fee,
                            base_fee_trend,
                            base_fee_moment,
                            delayed_execution_results,
                            proposed_execution_inputs,
                            last_round_tc,
                            fresh_proposal_certificate,
                        })
                        .expect("events channel never dropped");
                }
                TxPoolExecutorCommand::EnterRound { .. } | TxPoolExecutorCommand::Reset { .. } => {}
            }
        }
    }
}

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
    inner: EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>,
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
        score_provider: ema::ScoreProvider<NodeId<CertificateSignaturePubKey<ST>>, StdClock>,
        score_reader: ema::ScoreReader<NodeId<CertificateSignaturePubKey<ST>>, StdClock>,
    ) -> io::Result<Self> {
        let _ = std::fs::remove_file(&socket_path);

        let listener = {
            let std_listener = std::os::unix::net::UnixListener::bind(&socket_path)?;
            std_listener.set_nonblocking(true)?;
            UnixListener::from_std(std_listener)?
        };

        let inner = EthTxPoolExecutorClient::new(
            move |command_rx, forwarded_rx, event_tx| {
                ScriptedTxPoolExecutor::new(listener, block_policy, state_backend, chain_config)
                    .run(command_rx, forwarded_rx, event_tx)
            },
            Box::new(|_| {}),
            score_provider,
            score_reader,
            ForwardedIngressFairQueueConfig::default(),
        );

        info!(?socket_path, "scripted txpool executor started");

        Ok(Self { inner })
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
        self.inner.exec(commands);
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        self.inner.metrics()
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
        Pin::new(&mut this.inner).poll_next(cx)
    }
}
