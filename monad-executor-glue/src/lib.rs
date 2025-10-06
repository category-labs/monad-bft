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

use std::{fmt::Debug, net::SocketAddrV4};

use alloy_rlp::{encode_list, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::channel::oneshot;
use monad_blocksync::{
    blocksync::BlockSyncSelfRequester,
    messages::message::{BlockSyncRequestMessage, BlockSyncResponseMessage},
};
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus::{
    messages::consensus_message::ConsensusMessage,
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    block::{
        BlockPolicy, BlockRange, ConsensusBlockHeader, ConsensusFullBlock, OptimisticCommit,
        ProposedExecutionInputs,
    },
    checkpoint::Checkpoint,
    metrics::Metrics,
    no_endorsement::FreshProposalCertificate,
    payload::{ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::{QuorumCertificate, TimestampAdjustment},
    timeout::TimeoutCertificate,
    validator_data::ValidatorSetDataWithEpoch,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_state_backend::StateBackend;
use monad_types::{
    deserialize_pubkey, serialize_pubkey, Epoch, ExecutionProtocol, NodeId, Round, RouterTarget,
    SeqNum, Stake,
};
use monad_validator::signature_collection::SignatureCollection;
use serde::{Deserialize, Serialize};

const STATESYNC_NETWORK_MESSAGE_NAME: &str = "StateSyncNetworkMessage";

pub enum RouterCommand<ST: CertificateSignatureRecoverable, OM> {
    // Publish should not be replayed
    Publish {
        target: RouterTarget<CertificateSignaturePubKey<ST>>,
        message: OM,
    },
    PublishToFullNodes {
        epoch: Epoch, // Epoch gets embedded into the raptorcast message
        message: OM,
    },
    AddEpochValidatorSet {
        epoch: Epoch,
        validator_set: Vec<(NodeId<CertificateSignaturePubKey<ST>>, Stake)>,
    },
    UpdateCurrentRound(Epoch, Round),
    GetPeers,
    UpdatePeers {
        peer_entries: Vec<PeerEntry<ST>>,
        pinned_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    },
    GetFullNodes,
    UpdateFullNodes {
        dedicated_full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        prioritized_full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    },
}

impl<ST: CertificateSignatureRecoverable, OM> Debug for RouterCommand<ST, OM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Publish { target, message: _ } => {
                f.debug_struct("Publish").field("target", target).finish()
            }
            Self::PublishToFullNodes { epoch, message: _ } => f
                .debug_struct("PublishToFullNodes")
                .field("epoch", epoch)
                .finish(),
            Self::AddEpochValidatorSet {
                epoch,
                validator_set,
            } => f
                .debug_struct("AddEpochValidatorSet")
                .field("epoch", epoch)
                .field("validator_set", validator_set)
                .finish(),
            Self::UpdateCurrentRound(arg0, arg1) => f
                .debug_tuple("UpdateCurrentRound")
                .field(arg0)
                .field(arg1)
                .finish(),
            Self::GetPeers => write!(f, "GetPeers"),
            Self::UpdatePeers {
                peer_entries,
                pinned_nodes,
            } => f
                .debug_struct("UpdatePeers")
                .field("peer_entries", peer_entries)
                .field("pinned_nodes", pinned_nodes)
                .finish(),
            Self::GetFullNodes => write!(f, "GetFullNodes"),
            Self::UpdateFullNodes {
                dedicated_full_nodes,
                prioritized_full_nodes,
            } => f
                .debug_struct("UpdateFullNodes")
                .field("dedicated_full_nodes", dedicated_full_nodes)
                .field("prioritized_full_nodes", prioritized_full_nodes)
                .finish(),
        }
    }
}

pub trait Message: Clone + Send + Sync {
    type NodeIdPubKey: PubKey;
    type Event: Send + Sync;

    // TODO-3 NodeId -> &NodeId
    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event;
}

/// TimeoutVariant distinguishes the source of the timer scheduled
/// - `Pacemaker`: consensus pacemaker round timeout
/// - `BlockSync`: timeout for a specific blocksync request
#[derive(Hash, Debug, Clone, PartialEq, Eq, Copy)]
pub enum TimeoutVariant {
    Pacemaker,
    BlockSync(BlockSyncRequestMessage),
    SendVote,
}

pub enum TimerCommand<E> {
    /// ScheduleReset should ALMOST ALWAYS be emitted by the state machine after handling E
    /// This is to prevent E from firing twice on replay
    // TODO-2 create test to demonstrate faulty behavior if written improperly
    Schedule {
        duration: std::time::Duration,
        variant: TimeoutVariant,
        on_timeout: E,
    },
    ScheduleReset(TimeoutVariant),
}

impl<E> Debug for TimerCommand<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Schedule {
                duration,
                variant,
                on_timeout: _,
            } => f
                .debug_struct("Schedule")
                .field("duration", duration)
                .field("variant", variant)
                .finish(),
            Self::ScheduleReset(arg0) => f.debug_tuple("ScheduleReset").field(arg0).finish(),
        }
    }
}

pub enum LedgerCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    LedgerCommit(OptimisticCommit<ST, SCT, EPT>),
    LedgerFetchHeaders(BlockRange),
    LedgerFetchPayload(ConsensusBlockBodyId),
}

impl<ST, SCT, EPT> std::fmt::Debug for LedgerCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LedgerCommand::LedgerCommit(x) => f.debug_tuple("LedgerCommit").field(x).finish(),
            LedgerCommand::LedgerFetchHeaders(block_range) => f
                .debug_tuple("LedgerFetchHeaders")
                .field(block_range)
                .finish(),
            LedgerCommand::LedgerFetchPayload(payload_id) => f
                .debug_tuple("LedgerFetchPayload")
                .field(payload_id)
                .finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConfigFileCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Checkpoint {
        root_seq_num: SeqNum,
        checkpoint: Checkpoint<ST, SCT, EPT>,
    },
    ValidatorSetData {
        validator_set_data: ValidatorSetDataWithEpoch<SCT>,
    },
}

#[derive(Debug)]
pub enum ValSetCommand {
    NotifyFinalized(SeqNum),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetMetrics {
    Request,
    Response(Metrics),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(bound = "ST: CertificateSignatureRecoverable")]
pub struct PeerEntry<ST: CertificateSignatureRecoverable> {
    #[serde(serialize_with = "serialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    pub pubkey: CertificateSignaturePubKey<ST>,
    pub addr: SocketAddrV4,

    pub signature: ST,
    pub record_seq_num: u64,
}

impl<ST: CertificateSignatureRecoverable> Encodable for PeerEntry<ST> {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let enc: [&dyn Encodable; 4] = [
            &self.pubkey,
            &self.addr.to_string(),
            &self.signature,
            &self.record_seq_num,
        ];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

impl<ST: CertificateSignatureRecoverable> Decodable for PeerEntry<ST> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;

        let pubkey = CertificateSignaturePubKey::<ST>::decode(&mut payload)?;
        let s = <String as Decodable>::decode(&mut payload)?;
        let addr = s
            .parse::<SocketAddrV4>()
            .map_err(|_| alloy_rlp::Error::Custom("invalid SocketAddrV4"))?;
        let signature = ST::decode(&mut payload)?;
        let record_seq_num = u64::decode(&mut payload)?;

        Ok(Self {
            pubkey,
            addr,
            signature,
            record_seq_num,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GetPeers<ST: CertificateSignatureRecoverable> {
    Request,
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    Response(Vec<PeerEntry<ST>>),
}

impl<ST: CertificateSignatureRecoverable> Encodable for GetPeers<ST> {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Request => {
                let enc: [&dyn Encodable; 1] = [&1u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            // encoding for control panel events only for debugging
            Self::Response(_) => {
                let enc: [&dyn Encodable; 1] = [&2u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST: CertificateSignatureRecoverable> Decodable for GetPeers<ST> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Request),
            2 => Ok(Self::Response(vec![])),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown GetPeers",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GetFullNodes<PT: PubKey> {
    Request,
    #[serde(bound = "PT: PubKey")]
    Response(Vec<NodeId<PT>>),
}

impl<PT: PubKey> Encodable for GetFullNodes<PT> {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Request => {
                let enc: [&dyn Encodable; 1] = [&1u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            // encoding for control panel events only for debugging
            Self::Response(_) => {
                let enc: [&dyn Encodable; 1] = [&2u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<PT: PubKey> Decodable for GetFullNodes<PT> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Request),
            2 => Ok(Self::Response(vec![])),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown GetFullNodes",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReadCommand<ST: CertificateSignatureRecoverable + Clone> {
    GetMetrics(GetMetrics),
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    GetPeers(GetPeers<ST>),
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    GetFullNodes(GetFullNodes<CertificateSignaturePubKey<ST>>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClearMetrics {
    Request,
    Response(Metrics),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReloadConfig {
    Request,
    Response(String),
}

impl Encodable for ReloadConfig {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Request => {
                let enc: [&dyn Encodable; 1] = [&1u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Response(r) => {
                let enc: [&dyn Encodable; 2] = [&2u8, r];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl Decodable for ReloadConfig {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Request),
            2 => Ok(Self::Response(String::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncSelfRequester",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WriteCommand {
    ClearMetrics(ClearMetrics),
    UpdateLogFilter(String),
    ReloadConfig(ReloadConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlPanelCommand<ST: CertificateSignatureRecoverable> {
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    Read(ReadCommand<ST>),
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    Write(WriteCommand),
}

pub enum LoopbackCommand<E> {
    Forward(E),
}

impl<E> Debug for LoopbackCommand<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoopbackCommand::Forward(_e) => f.debug_tuple("Forward").finish(),
        }
    }
}

#[derive(Debug)]
pub enum TimestampCommand {
    AdjustDelta(TimestampAdjustment),
}

#[derive(Debug)]
pub enum StateSyncCommand<ST, EPT>
where
    ST: CertificateSignatureRecoverable,
    EPT: ExecutionProtocol,
{
    /// The *last* RequestSync(n) called is guaranteed to be followed up with DoneSync(n).
    ///
    /// Note that if RequestSync(n') is invoked before receiving DoneSync(n), it is not guaranteed
    /// that DoneSync(n) will be received - so the caller should drop any DoneSync < n'
    RequestSync(EPT::FinalizedHeader),
    Message(
        (
            NodeId<CertificateSignaturePubKey<ST>>,
            StateSyncNetworkMessage,
        ),
    ),
    StartExecution,
}

#[derive(Debug)]
pub enum ConfigReloadCommand {
    ReloadConfig,
}

pub enum TxPoolCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    /// Used to update the nonces of tracked txs
    BlockCommit(Vec<BPT::ValidatedBlock>),

    CreateProposal {
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        epoch: Epoch,
        round: Round,
        seq_num: SeqNum,
        high_qc: QuorumCertificate<SCT>,
        round_signature: RoundSignature<SCT::SignatureType>,
        last_round_tc: Option<TimeoutCertificate<ST, SCT, EPT>>,
        fresh_proposal_certificate: Option<FreshProposalCertificate<SCT>>,

        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        beneficiary: [u8; 20],
        timestamp_ns: u128,

        extending_blocks: Vec<BPT::ValidatedBlock>,
        delayed_execution_results: Vec<EPT::FinalizedHeader>,
    },

    InsertForwardedTxs {
        sender: NodeId<SCT::NodeIdPubKey>,
        txs: Vec<Bytes>,
    },

    EnterRound {
        epoch: Epoch,
        round: Round,
        upcoming_leader_rounds: Vec<Round>,
    },

    // Emitted after statesync is completed
    Reset {
        last_delay_committed_blocks: Vec<BPT::ValidatedBlock>,
    },
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> Debug for TxPoolCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockCommit(arg0) => f.debug_tuple("BlockCommit").field(arg0).finish(),
            Self::CreateProposal {
                node_id,
                epoch,
                round,
                seq_num,
                high_qc,
                round_signature,
                last_round_tc,
                fresh_proposal_certificate,
                tx_limit,
                proposal_gas_limit,
                proposal_byte_limit,
                beneficiary,
                timestamp_ns,
                extending_blocks,
                delayed_execution_results,
            } => f
                .debug_struct("CreateProposal")
                .field("node_id", node_id)
                .field("epoch", epoch)
                .field("round", round)
                .field("seq_num", seq_num)
                .field("high_qc", high_qc)
                .field("round_signature", round_signature)
                .field("last_round_tc", last_round_tc)
                .field("fresh_proposal_certificate", fresh_proposal_certificate)
                .field("tx_limit", tx_limit)
                .field("proposal_gas_limit", proposal_gas_limit)
                .field("proposal_byte_limit", proposal_byte_limit)
                .field("beneficiary", beneficiary)
                .field("timestamp_ns", timestamp_ns)
                .field("extending_blocks", extending_blocks)
                .field("delayed_execution_results", delayed_execution_results)
                .finish(),
            Self::InsertForwardedTxs { sender, txs } => f
                .debug_struct("InsertForwardedTxs")
                .field("sender", sender)
                .field("txs", txs)
                .finish(),
            Self::EnterRound {
                epoch,
                round,
                upcoming_leader_rounds,
            } => f
                .debug_struct("EnterRound")
                .field("epoch", epoch)
                .field("round", round)
                .field("upcoming_leader_rounds", upcoming_leader_rounds)
                .finish(),
            Self::Reset {
                last_delay_committed_blocks,
            } => f
                .debug_struct("Reset")
                .field("last_delay_committed_blocks", last_delay_committed_blocks)
                .finish(),
        }
    }
}

pub enum Command<E, OM, ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    RouterCommand(RouterCommand<ST, OM>),
    TimerCommand(TimerCommand<E>),
    LedgerCommand(LedgerCommand<ST, SCT, EPT>),
    ConfigFileCommand(ConfigFileCommand<ST, SCT, EPT>),
    ValSetCommand(ValSetCommand),
    TimestampCommand(TimestampCommand),

    TxPoolCommand(TxPoolCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>),
    ControlPanelCommand(ControlPanelCommand<ST>),
    LoopbackCommand(LoopbackCommand<E>),
    StateSyncCommand(StateSyncCommand<ST, EPT>),
    ConfigReloadCommand(ConfigReloadCommand),
}

impl<E, OM, ST, SCT, EPT, BPT, SBT, CCT, CRT> Debug
    for Command<E, OM, ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RouterCommand(arg0) => f.debug_tuple("RouterCommand").field(arg0).finish(),
            Self::TimerCommand(arg0) => f.debug_tuple("TimerCommand").field(arg0).finish(),
            Self::LedgerCommand(arg0) => f.debug_tuple("LedgerCommand").field(arg0).finish(),
            Self::ConfigFileCommand(arg0) => {
                f.debug_tuple("ConfigFileCommand").field(arg0).finish()
            }
            Self::ValSetCommand(arg0) => f.debug_tuple("ValSetCommand").field(arg0).finish(),
            Self::TimestampCommand(arg0) => f.debug_tuple("TimestampCommand").field(arg0).finish(),
            Self::TxPoolCommand(arg0) => f.debug_tuple("TxPoolCommand").field(arg0).finish(),
            Self::ControlPanelCommand(arg0) => {
                f.debug_tuple("ControlPanelCommand").field(arg0).finish()
            }
            Self::LoopbackCommand(arg0) => f.debug_tuple("LoopbackCommand").field(arg0).finish(),
            Self::StateSyncCommand(arg0) => f.debug_tuple("StateSyncCommand").field(arg0).finish(),
            Self::ConfigReloadCommand(arg0) => {
                f.debug_tuple("ConfigReloadCommand").field(arg0).finish()
            }
        }
    }
}

impl<E, OM, ST, SCT, EPT, BPT, SBT, CCT, CRT> Command<E, OM, ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (
        Vec<RouterCommand<ST, OM>>,
        Vec<TimerCommand<E>>,
        Vec<LedgerCommand<ST, SCT, EPT>>,
        Vec<ConfigFileCommand<ST, SCT, EPT>>,
        Vec<ValSetCommand>,
        Vec<TimestampCommand>,
        Vec<TxPoolCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>>,
        Vec<ControlPanelCommand<ST>>,
        Vec<LoopbackCommand<E>>,
        Vec<StateSyncCommand<ST, EPT>>,
        Vec<ConfigReloadCommand>,
    ) {
        let mut router_cmds = Vec::new();
        let mut timer_cmds = Vec::new();
        let mut ledger_cmds = Vec::new();
        let mut config_file_cmds = Vec::new();
        let mut val_set_cmds = Vec::new();
        let mut timestamp_cmds = Vec::new();
        let mut txpool_cmds = Vec::new();
        let mut control_panel_cmds = Vec::new();
        let mut loopback_cmds = Vec::new();
        let mut state_sync_cmds = Vec::new();
        let mut config_reload_cmds = Vec::new();

        for command in commands {
            match command {
                Command::RouterCommand(cmd) => router_cmds.push(cmd),
                Command::TimerCommand(cmd) => timer_cmds.push(cmd),
                Command::LedgerCommand(cmd) => ledger_cmds.push(cmd),
                Command::ConfigFileCommand(cmd) => config_file_cmds.push(cmd),
                Command::ValSetCommand(cmd) => val_set_cmds.push(cmd),
                Command::TimestampCommand(cmd) => timestamp_cmds.push(cmd),
                Command::TxPoolCommand(cmd) => txpool_cmds.push(cmd),
                Command::ControlPanelCommand(cmd) => control_panel_cmds.push(cmd),
                Command::LoopbackCommand(cmd) => loopback_cmds.push(cmd),
                Command::StateSyncCommand(cmd) => state_sync_cmds.push(cmd),
                Command::ConfigReloadCommand(cmd) => config_reload_cmds.push(cmd),
            }
        }

        (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            config_file_cmds,
            val_set_cmds,
            timestamp_cmds,
            txpool_cmds,
            control_panel_cmds,
            loopback_cmds,
            state_sync_cmds,
            config_reload_cmds,
        )
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum ConsensusEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Message {
        sender: NodeId<SCT::NodeIdPubKey>,
        unverified_message: Unverified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>>,
    },
    Timeout(Round),
    /// a block that was previously requested
    /// this is an invariant
    BlockSync {
        block_range: BlockRange,
        full_blocks: Vec<ConsensusFullBlock<ST, SCT, EPT>>,
    },
    SendVote(Round),
}

impl<ST, SCT, EPT> Encodable for ConsensusEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Message {
                sender: snd,
                unverified_message: msg,
            } => {
                let enc: [&dyn Encodable; 3] = [&1u8, &snd, &msg];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Timeout(round) => {
                let enc: [&dyn Encodable; 2] = [&2u8, round];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::BlockSync {
                block_range: range,
                full_blocks: blocks,
            } => {
                let enc: [&dyn Encodable; 3] = [&3u8, &range, &blocks];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::SendVote(round) => {
                let enc: [&dyn Encodable; 2] = [&4u8, &round];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for ConsensusEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => {
                let sender = NodeId::<SCT::NodeIdPubKey>::decode(&mut payload)?;
                let msg = Unverified::<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>>::decode(
                    &mut payload,
                )?;
                Ok(Self::Message {
                    sender,
                    unverified_message: msg,
                })
            }
            2 => {
                let round = Round::decode(&mut payload)?;
                Ok(Self::Timeout(round))
            }
            3 => {
                let block_range = BlockRange::decode(&mut payload)?;
                let full_blocks = Vec::<ConsensusFullBlock<ST, SCT, EPT>>::decode(&mut payload)?;
                Ok(Self::BlockSync {
                    block_range,
                    full_blocks,
                })
            }
            4 => Ok(Self::SendVote(Round::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown ConsensusEvent",
            )),
        }
    }
}

impl<ST, SCT, EPT> Debug for ConsensusEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => f
                .debug_struct("Message")
                .field("sender", sender)
                .field("msg", unverified_message)
                .finish(),
            ConsensusEvent::Timeout(round) => {
                f.debug_struct("Timeout").field("round", round).finish()
            }
            ConsensusEvent::BlockSync {
                block_range,
                full_blocks,
            } => f
                .debug_struct("BlockSync")
                .field("block_range", block_range)
                .field("full_blocks", full_blocks)
                .finish(),
            ConsensusEvent::SendVote(round) => {
                f.debug_struct("SendVote").field("round", round).finish()
            }
        }
    }
}

/// BlockSync related events
#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum BlockSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// A peer (not self) requesting for a missing block
    Request {
        sender: NodeId<SCT::NodeIdPubKey>,
        request: BlockSyncRequestMessage,
    },
    /// Outbound request timed out
    Timeout(BlockSyncRequestMessage),
    /// self requesting for a missing block
    /// this request must be retried if necessary
    SelfRequest {
        requester: BlockSyncSelfRequester,
        block_range: BlockRange,
    },
    /// cancel request for block
    SelfCancelRequest {
        requester: BlockSyncSelfRequester,
        block_range: BlockRange,
    },
    /// A peer (not self) sending us a block
    Response {
        sender: NodeId<SCT::NodeIdPubKey>,
        response: BlockSyncResponseMessage<ST, SCT, EPT>,
    },
    /// self sending us missing block (from ledger)
    SelfResponse {
        response: BlockSyncResponseMessage<ST, SCT, EPT>,
    },
}

impl<ST, SCT, EPT> Debug for BlockSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request { sender, request } => f
                .debug_struct("BlockSyncRequest")
                .field("sender", sender)
                .field("request", request)
                .finish(),
            Self::SelfRequest {
                requester,
                block_range,
            } => f
                .debug_struct("BlockSyncSelfRequest")
                .field("requester", requester)
                .field("block_range", block_range)
                .finish(),
            Self::SelfCancelRequest {
                requester,
                block_range,
            } => f
                .debug_struct("BlockSyncSelfCancelRequest")
                .field("requester", requester)
                .field("block_range", block_range)
                .finish(),
            Self::Response { sender, response } => f
                .debug_struct("BlockSyncResponse")
                .field("sender", sender)
                .field("response", response)
                .finish(),
            Self::SelfResponse { response } => f
                .debug_struct("BlockSyncSelfResponse")
                .field("response", response)
                .finish(),
            Self::Timeout(request) => f.debug_struct("Timeout").field("request", request).finish(),
        }
    }
}

impl<ST, SCT, EPT> Encodable for BlockSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Request { sender, request } => {
                let enc: [&dyn Encodable; 3] = [&1u8, &sender, &request];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Timeout(m) => {
                let enc: [&dyn Encodable; 2] = [&2u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::SelfRequest {
                requester,
                block_range,
            } => {
                let enc: [&dyn Encodable; 3] = [&3u8, &requester, &block_range];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::SelfCancelRequest {
                requester,
                block_range,
            } => {
                let enc: [&dyn Encodable; 3] = [&4u8, &requester, &block_range];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Response { sender, response } => {
                let enc: [&dyn Encodable; 3] = [&5u8, &sender, &response];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::SelfResponse { response } => {
                let enc: [&dyn Encodable; 2] = [&6u8, &response];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for BlockSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => {
                let sender = NodeId::<SCT::NodeIdPubKey>::decode(&mut payload)?;
                let request = BlockSyncRequestMessage::decode(&mut payload)?;
                Ok(Self::Request { sender, request })
            }
            2 => Ok(Self::Timeout(BlockSyncRequestMessage::decode(
                &mut payload,
            )?)),
            3 => {
                let requester = BlockSyncSelfRequester::decode(&mut payload)?;
                let block_range = BlockRange::decode(&mut payload)?;
                Ok(Self::SelfRequest {
                    requester,
                    block_range,
                })
            }
            4 => {
                let requester = BlockSyncSelfRequester::decode(&mut payload)?;
                let block_range = BlockRange::decode(&mut payload)?;
                Ok(Self::SelfCancelRequest {
                    requester,
                    block_range,
                })
            }
            5 => {
                let sender = NodeId::<SCT::NodeIdPubKey>::decode(&mut payload)?;
                let response = BlockSyncResponseMessage::<ST, SCT, EPT>::decode(&mut payload)?;
                Ok(Self::Response { sender, response })
            }
            6 => {
                let response = BlockSyncResponseMessage::<ST, SCT, EPT>::decode(&mut payload)?;
                Ok(Self::SelfResponse { response })
            }
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncEvent",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ValidatorEvent<SCT: SignatureCollection> {
    UpdateValidators(ValidatorSetDataWithEpoch<SCT>),
}

impl<SCT: SignatureCollection> Encodable for ValidatorEvent<SCT> {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::UpdateValidators(vset) => {
                let enc: [&dyn Encodable; 2] = [&1u8, vset];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<SCT: SignatureCollection> Decodable for ValidatorEvent<SCT> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => {
                let vset = ValidatorSetDataWithEpoch::<SCT>::decode(&mut payload)?;
                Ok(Self::UpdateValidators(vset))
            }
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown ValidatorEvent",
            )),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum MempoolEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Proposal {
        epoch: Epoch,
        round: Round,
        seq_num: SeqNum,
        high_qc: QuorumCertificate<SCT>,
        timestamp_ns: u128,
        round_signature: RoundSignature<SCT::SignatureType>,
        // base fee fields used to populate consensus block header
        // they are set to None pre tfm activation
        base_fee: Option<u64>,
        base_fee_trend: Option<u64>,
        base_fee_moment: Option<u64>,
        delayed_execution_results: Vec<EPT::FinalizedHeader>,
        proposed_execution_inputs: ProposedExecutionInputs<EPT>,
        last_round_tc: Option<TimeoutCertificate<ST, SCT, EPT>>,
        fresh_proposal_certificate: Option<FreshProposalCertificate<SCT>>,
    },

    /// Txs that are incoming via other nodes
    ForwardedTxs {
        sender: NodeId<SCT::NodeIdPubKey>,
        txs: Vec<Bytes>,
    },

    /// Txs that should be forwarded to upcoming leaders
    ForwardTxs(Vec<Bytes>),
}

impl<ST, SCT, EPT> Encodable for MempoolEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Proposal {
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
            } => {
                let tc_buf: Vec<&dyn Encodable> = match last_round_tc {
                    None => {
                        vec![&1u8]
                    }
                    Some(tc) => {
                        vec![&2u8, tc]
                    }
                };

                let fc_buf: Vec<&dyn Encodable> = match fresh_proposal_certificate {
                    None => {
                        vec![&1u8]
                    }
                    Some(fec) => {
                        vec![&2u8, fec]
                    }
                };

                let base_fee_buf: Vec<&dyn Encodable> = match base_fee {
                    None => {
                        vec![&1u8]
                    }
                    Some(bf) => {
                        vec![&2u8, bf]
                    }
                };

                let base_fee_trend_buf: Vec<&dyn Encodable> = match base_fee_trend {
                    None => {
                        vec![&1u8]
                    }
                    Some(bft) => {
                        vec![&2u8, bft]
                    }
                };

                let base_fee_moment_buf: Vec<&dyn Encodable> = match base_fee_moment {
                    None => {
                        vec![&1u8]
                    }
                    Some(bfm) => {
                        vec![&2u8, bfm]
                    }
                };

                let enc: [&dyn Encodable; 14] = [
                    &1u8,
                    epoch,
                    round,
                    seq_num,
                    high_qc,
                    timestamp_ns,
                    round_signature,
                    &base_fee_buf,
                    &base_fee_trend_buf,
                    &base_fee_moment_buf,
                    delayed_execution_results,
                    proposed_execution_inputs,
                    &tc_buf,
                    &fc_buf,
                ];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ForwardedTxs { sender, txs } => {
                let enc: [&dyn Encodable; 3] = [&2u8, sender, txs];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ForwardTxs(txs) => {
                let enc: [&dyn Encodable; 2] = [&3u8, txs];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for MempoolEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => {
                let epoch = Epoch::decode(&mut payload)?;
                let round = Round::decode(&mut payload)?;
                let seq_num = SeqNum::decode(&mut payload)?;
                let high_qc = QuorumCertificate::<SCT>::decode(&mut payload)?;
                let timestamp_ns = u128::decode(&mut payload)?;
                let round_signature = RoundSignature::<SCT::SignatureType>::decode(&mut payload)?;
                let mut base_fee_payload = Header::decode_bytes(&mut payload, true)?;
                let base_fee = match u8::decode(&mut base_fee_payload)? {
                    1 => None,
                    2 => Some(u64::decode(&mut base_fee_payload)?),
                    _ => {
                        return Err(alloy_rlp::Error::Custom(
                            "failed to decode unknown base_fee in mempool event",
                        ))
                    }
                };
                let mut base_fee_trend_payload = Header::decode_bytes(&mut payload, true)?;
                let base_fee_trend = match u8::decode(&mut base_fee_trend_payload)? {
                    1 => None,
                    2 => Some(u64::decode(&mut base_fee_trend_payload)?),
                    _ => {
                        return Err(alloy_rlp::Error::Custom(
                            "failed to decode unknown base_fee_trend in mempool event",
                        ))
                    }
                };
                let mut base_fee_moment_payload = Header::decode_bytes(&mut payload, true)?;
                let base_fee_moment = match u8::decode(&mut base_fee_moment_payload)? {
                    1 => None,
                    2 => Some(u64::decode(&mut base_fee_moment_payload)?),
                    _ => {
                        return Err(alloy_rlp::Error::Custom(
                            "failed to decode unknown base_fee_moment in mempool event",
                        ))
                    }
                };

                let delayed_execution_results = Vec::<EPT::FinalizedHeader>::decode(&mut payload)?;
                let proposed_execution_inputs =
                    ProposedExecutionInputs::<EPT>::decode(&mut payload)?;
                let mut tc_payload = Header::decode_bytes(&mut payload, true)?;
                let tc = match u8::decode(&mut tc_payload)? {
                    1 => Ok(None),
                    2 => Ok(Some(TimeoutCertificate::<ST, SCT, EPT>::decode(
                        &mut tc_payload,
                    )?)),
                    _ => Err(alloy_rlp::Error::Custom(
                        "failed to decode unknown tc in mempool event",
                    )),
                }?;
                let mut fc_payload = Header::decode_bytes(&mut payload, true)?;
                let fc = match u8::decode(&mut fc_payload)? {
                    1 => Ok(None),
                    2 => Ok(Some(FreshProposalCertificate::<SCT>::decode(
                        &mut fc_payload,
                    )?)),
                    _ => Err(alloy_rlp::Error::Custom(
                        "failed to decode unknown fc in mempool event",
                    )),
                }?;
                Ok(Self::Proposal {
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
                    last_round_tc: tc,
                    fresh_proposal_certificate: fc,
                })
            }
            2 => {
                let sender = NodeId::<SCT::NodeIdPubKey>::decode(&mut payload)?;
                let txs = Vec::<Bytes>::decode(&mut payload)?;
                Ok(Self::ForwardedTxs { sender, txs })
            }
            3 => {
                let txs = Vec::<Bytes>::decode(&mut payload)?;
                Ok(Self::ForwardTxs(txs))
            }
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown mempool event",
            )),
        }
    }
}

impl<ST, SCT, EPT> Debug for MempoolEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Proposal {
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
            } => f
                .debug_struct("Proposal")
                .field("epoch", epoch)
                .field("round", round)
                .field("seq_num", seq_num)
                .field("high_qc", high_qc)
                .field("timestamp_ns", timestamp_ns)
                .field("round_signature", round_signature)
                .field("base_fee", &base_fee)
                .field(
                    "base_fee_trend",
                    &base_fee_trend.map(|trend| trend.cast_signed()),
                )
                .field("base_fee_moment", &base_fee_moment)
                .field("delayed_execution_results", delayed_execution_results)
                .field("proposed_execution_inputs", proposed_execution_inputs)
                .field("last_round_tc", last_round_tc)
                .field("fresh_proposal_certificate", fresh_proposal_certificate)
                .finish(),
            Self::ForwardedTxs { sender, txs } => f
                .debug_struct("ForwardedTxs")
                .field("sender", sender)
                .field("txns_len_bytes", &txs.iter().map(Bytes::len).sum::<usize>())
                .finish(),
            Self::ForwardTxs(txs) => f
                .debug_struct("ForwardTxs")
                .field("txs_len_bytes", &txs.iter().map(Bytes::len).sum::<usize>())
                .finish(),
        }
    }
}

const STATESYNC_VERSION_V0: StateSyncVersion = StateSyncVersion { major: 1, minor: 0 };
const STATESYNC_VERSION_V1: StateSyncVersion = StateSyncVersion { major: 1, minor: 1 };
// Client is required to send completions since this version
pub const STATESYNC_VERSION_V2: StateSyncVersion = StateSyncVersion { major: 1, minor: 2 };
pub const SELF_STATESYNC_VERSION: StateSyncVersion = STATESYNC_VERSION_V2;
pub const STATESYNC_VERSION_MIN: StateSyncVersion = STATESYNC_VERSION_V0;

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, RlpEncodable, RlpDecodable, Serialize,
)]
pub struct StateSyncVersion {
    major: u16,
    minor: u16,
}

impl StateSyncVersion {
    pub fn from_u32(version: u32) -> Self {
        Self {
            major: (version >> 16) as u16,
            minor: (version & 0xFFFF) as u16,
        }
    }

    pub fn to_u32(&self) -> u32 {
        (self.major as u32) << 16 | (self.minor as u32)
    }

    pub fn is_compatible(&self) -> bool {
        *self >= STATESYNC_VERSION_MIN && *self <= SELF_STATESYNC_VERSION
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, RlpEncodable, Serialize)]
pub struct StateSyncRequest {
    pub version: StateSyncVersion,

    pub prefix: u64,
    pub prefix_bytes: u8,
    pub target: u64,
    pub from: u64,
    pub until: u64,
    pub old_target: u64,
}

impl Decodable for StateSyncRequest {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;

        let version = StateSyncVersion::decode(&mut payload)?;

        if version.is_compatible() {
            let prefix = u64::decode(&mut payload)?;
            let prefix_bytes = u8::decode(&mut payload)?;
            let target = u64::decode(&mut payload)?;
            let from = u64::decode(&mut payload)?;
            let until = u64::decode(&mut payload)?;
            let old_target = u64::decode(&mut payload)?;

            Ok(Self {
                version,
                prefix,
                prefix_bytes,
                target,
                from,
                until,
                old_target,
            })
        } else {
            // If the version is not compatible, skip the rest of payload we may not understand
            Ok(Self {
                version,
                prefix: 0,
                prefix_bytes: 0,
                target: 0,
                from: 0,
                until: 0,
                old_target: 0,
            })
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
pub enum StateSyncUpsertType {
    Code,
    Account,
    Storage,
    AccountDelete,
    StorageDelete,
    Header,
}

#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct StateSyncUpsertV0 {
    pub upsert_type: StateSyncUpsertType,
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct StateSyncUpsertV1 {
    pub upsert_type: StateSyncUpsertType,
    pub data: Bytes,
}

impl StateSyncUpsertV0 {
    fn as_v1(&self) -> StateSyncUpsertV1 {
        StateSyncUpsertV1 {
            upsert_type: self.upsert_type,
            data: Bytes::copy_from_slice(&self.data),
        }
    }
}

impl StateSyncUpsertV1 {
    pub fn new(upsert_type: StateSyncUpsertType, data: Bytes) -> Self {
        Self { upsert_type, data }
    }

    fn as_v0(&self) -> StateSyncUpsertV0 {
        StateSyncUpsertV0 {
            upsert_type: self.upsert_type,
            data: self.data.to_vec(),
        }
    }
}

impl Encodable for StateSyncUpsertType {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Code => {
                let enc: [&dyn Encodable; 1] = [&1u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Account => {
                let enc: [&dyn Encodable; 1] = [&2u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Storage => {
                let enc: [&dyn Encodable; 1] = [&3u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::AccountDelete => {
                let enc: [&dyn Encodable; 1] = [&4u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::StorageDelete => {
                let enc: [&dyn Encodable; 1] = [&5u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Header => {
                let enc: [&dyn Encodable; 1] = [&6u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }

    fn length(&self) -> usize {
        // max enum value is << 127
        // the rlp encoding of integers between 0 and 127 is 1 byte.
        // the rlp encoding of a list of 1 byte is always 2 bytes
        2
    }
}

impl Decodable for StateSyncUpsertType {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;

        match u8::decode(&mut payload)? {
            1 => Ok(Self::Code),
            2 => Ok(Self::Account),
            3 => Ok(Self::Storage),
            4 => Ok(Self::AccountDelete),
            5 => Ok(Self::StorageDelete),
            6 => Ok(Self::Header),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown StateSyncUpsertType",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct StateSyncBadVersion {
    pub min_version: StateSyncVersion,
    pub max_version: StateSyncVersion,
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct StateSyncResponse {
    pub version: StateSyncVersion,
    pub nonce: u64,
    pub response_index: u32,

    pub request: StateSyncRequest,
    // consensus state must validate that this sender is "trusted"
    pub response: Vec<StateSyncUpsertV1>,
    pub response_n: u64,
}

impl Encodable for StateSyncResponse {
    fn encode(&self, out: &mut dyn BufMut) {
        // check if client version is past V1: upsert fork
        if self.request.version >= STATESYNC_VERSION_V1 {
            let enc: [&dyn Encodable; 6] = [
                &self.version,
                &self.nonce,
                &self.response_index,
                &self.request,
                &self.response,
                &self.response_n,
            ];
            encode_list::<_, dyn Encodable>(&enc, out);
        } else {
            let v0_response: Vec<StateSyncUpsertV0> =
                self.response.iter().map(StateSyncUpsertV1::as_v0).collect();
            let enc: [&dyn Encodable; 6] = [
                &self.version,
                &self.nonce,
                &self.response_index,
                &self.request,
                &v0_response,
                &self.response_n,
            ];
            encode_list::<_, dyn Encodable>(&enc, out);
        }
    }

    fn length(&self) -> usize {
        // check if client version is past V1: upsert fork
        if self.request.version >= STATESYNC_VERSION_V1 {
            let enc: Vec<&dyn Encodable> = vec![
                &self.version,
                &self.nonce,
                &self.response_index,
                &self.request,
                &self.response,
                &self.response_n,
            ];
            Encodable::length(&enc)
        } else {
            let v0_response: Vec<StateSyncUpsertV0> =
                self.response.iter().map(StateSyncUpsertV1::as_v0).collect();
            let enc: Vec<&dyn Encodable> = vec![
                &self.version,
                &self.nonce,
                &self.response_index,
                &self.request,
                &v0_response,
                &self.response_n,
            ];
            Encodable::length(&enc)
        }
    }
}

impl Decodable for StateSyncResponse {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;

        let version = StateSyncVersion::decode(&mut payload)?;
        let nonce = u64::decode(&mut payload)?;
        let response_index = u32::decode(&mut payload)?;
        let request = StateSyncRequest::decode(&mut payload)?;
        // check if server version is past V1: upsert fork
        let response: Vec<StateSyncUpsertV1> = if version >= STATESYNC_VERSION_V1 {
            Vec::<StateSyncUpsertV1>::decode(&mut payload)?
        } else {
            let v0_response = Vec::<StateSyncUpsertV0>::decode(&mut payload)?;
            v0_response.iter().map(StateSyncUpsertV0::as_v1).collect()
        };
        let response_n = u64::decode(&mut payload)?;

        Ok(Self {
            version,
            nonce,
            response_index,
            request,
            response,
            response_n,
        })
    }
}

impl Debug for StateSyncResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateSyncResponse")
            .field("version", &self.version)
            .field("nonce", &self.nonce)
            .field("response_index", &self.response_index)
            .field("request", &self.request)
            .field("response_len", &self.response.len())
            .field("response_n", &self.response_n)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct SessionId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum StateSyncNetworkMessage {
    Request(StateSyncRequest),
    Response(StateSyncResponse),
    BadVersion(StateSyncBadVersion),
    Completion(SessionId),
}

impl Encodable for StateSyncNetworkMessage {
    fn encode(&self, out: &mut dyn BufMut) {
        let name = STATESYNC_NETWORK_MESSAGE_NAME;
        match self {
            Self::Request(req) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &req];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Response(resp) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &resp];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::BadVersion(bad_version) => {
                let enc: [&dyn Encodable; 3] = [&name, &3u8, &bad_version];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Completion(session_id) => {
                let enc: [&dyn Encodable; 3] = [&name, &4u8, &session_id];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }

    fn length(&self) -> usize {
        let name = STATESYNC_NETWORK_MESSAGE_NAME;
        match self {
            Self::Request(req) => {
                let enc: Vec<&dyn Encodable> = vec![&name, &1u8, &req];
                Encodable::length(&enc)
            }
            Self::Response(resp) => {
                let enc: Vec<&dyn Encodable> = vec![&name, &2u8, &resp];
                Encodable::length(&enc)
            }
            Self::BadVersion(bad_version) => {
                let enc: Vec<&dyn Encodable> = vec![&name, &3u8, &bad_version];
                Encodable::length(&enc)
            }
            Self::Completion(session_id) => {
                let enc: Vec<&dyn Encodable> = vec![&name, &4u8, &session_id];
                Encodable::length(&enc)
            }
        }
    }
}

impl Decodable for StateSyncNetworkMessage {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != STATESYNC_NETWORK_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type StateSyncNetworkMessage",
            ));
        }

        match u8::decode(&mut payload)? {
            1 => Ok(Self::Request(StateSyncRequest::decode(&mut payload)?)),
            2 => Ok(Self::Response(StateSyncResponse::decode(&mut payload)?)),
            3 => Ok(Self::BadVersion(StateSyncBadVersion::decode(&mut payload)?)),
            4 => Ok(Self::Completion(SessionId::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown StateSyncNetworkMessage",
            )),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum StateSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Inbound(NodeId<SCT::NodeIdPubKey>, StateSyncNetworkMessage),
    Outbound(
        NodeId<SCT::NodeIdPubKey>,
        StateSyncNetworkMessage,
        #[serde(skip)] Option<oneshot::Sender<()>>, // completion
    ),

    /// Execution done syncing
    DoneSync(SeqNum),

    // Statesync-requested block
    BlockSync {
        block_range: BlockRange,
        full_blocks: Vec<ConsensusFullBlock<ST, SCT, EPT>>,
    },

    // Statesync re-sync request
    RequestSync {
        root: ConsensusBlockHeader<ST, SCT, EPT>,
        high_qc: QuorumCertificate<SCT>,
    },
}

impl<ST, SCT, EPT> Encodable for StateSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Inbound(nodeid, msg) => {
                let enc: [&dyn Encodable; 3] = [&1u8, &nodeid, &msg];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Outbound(nodeid, msg, _) => {
                // The serialization of this event is only used for local logging
                // so fine to ignore the channel
                let enc: [&dyn Encodable; 3] = [&2u8, &nodeid, &msg];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::DoneSync(seqnum) => {
                let enc: [&dyn Encodable; 2] = [&3u8, &seqnum];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::BlockSync {
                block_range,
                full_blocks,
            } => {
                let enc: [&dyn Encodable; 3] = [&4u8, &block_range, &full_blocks];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::RequestSync { root, high_qc } => {
                let enc: [&dyn Encodable; 3] = [&5u8, &root, &high_qc];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for StateSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => {
                let nodeid = NodeId::<SCT::NodeIdPubKey>::decode(&mut payload)?;
                let msg = StateSyncNetworkMessage::decode(&mut payload)?;
                Ok(Self::Inbound(nodeid, msg))
            }
            2 => {
                let nodeid = NodeId::<SCT::NodeIdPubKey>::decode(&mut payload)?;
                let msg = StateSyncNetworkMessage::decode(&mut payload)?;
                Ok(Self::Outbound(nodeid, msg, None))
            }
            3 => Ok(Self::DoneSync(SeqNum::decode(&mut payload)?)),
            4 => {
                let block_range = BlockRange::decode(&mut payload)?;
                let full_blocks = Vec::<ConsensusFullBlock<ST, SCT, EPT>>::decode(&mut payload)?;
                Ok(Self::BlockSync {
                    block_range,
                    full_blocks,
                })
            }
            5 => {
                let root = ConsensusBlockHeader::<ST, SCT, EPT>::decode(&mut payload)?;
                let high_qc = QuorumCertificate::<SCT>::decode(&mut payload)?;
                Ok(Self::RequestSync { root, high_qc })
            }
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown StateSyncEvent",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ControlPanelEvent<ST>
where
    ST: CertificateSignatureRecoverable,
{
    GetMetricsEvent,
    ClearMetricsEvent,
    UpdateLogFilter(String),
    GetPeers(GetPeers<ST>),
    GetFullNodes(GetFullNodes<CertificateSignaturePubKey<ST>>),
    ReloadConfig(ReloadConfig),
}

impl<ST> Encodable for ControlPanelEvent<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::GetMetricsEvent => {
                let enc: [&dyn Encodable; 1] = [&2u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ClearMetricsEvent => {
                let enc: [&dyn Encodable; 1] = [&3u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::UpdateLogFilter(filter) => {
                let enc: [&dyn Encodable; 2] = [&5u8, &filter];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::GetPeers(peers) => {
                let enc: [&dyn Encodable; 2] = [&6u8, &peers];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::GetFullNodes(nodes) => {
                let enc: [&dyn Encodable; 2] = [&7u8, &nodes];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ReloadConfig(cfg) => {
                let enc: [&dyn Encodable; 2] = [&8u8, &cfg];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST> Decodable for ControlPanelEvent<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            2 => Ok(Self::GetMetricsEvent),
            3 => Ok(Self::ClearMetricsEvent),
            5 => Ok(Self::UpdateLogFilter(String::decode(&mut payload)?)),
            6 => Ok(Self::GetPeers(GetPeers::decode(&mut payload)?)),
            7 => Ok(Self::GetFullNodes(GetFullNodes::decode(&mut payload)?)),
            8 => Ok(Self::ReloadConfig(ReloadConfig::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown ControlPanelEvent",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct ConfigUpdate<SCT>
where
    SCT: SignatureCollection,
{
    pub dedicated_full_nodes: Vec<NodeId<SCT::NodeIdPubKey>>,
    pub prioritized_full_nodes: Vec<NodeId<SCT::NodeIdPubKey>>,
    pub blocksync_override_peers: Vec<NodeId<SCT::NodeIdPubKey>>,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct KnownPeersUpdate<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub known_peers: Vec<PeerEntry<ST>>,
    pub pinned_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct SecondaryRaptorcastEvent<SCT>
where
    SCT: SignatureCollection,
{
    pub confirm_group_peers: Vec<NodeId<SCT::NodeIdPubKey>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ConfigEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    ConfigUpdate(ConfigUpdate<SCT>),
    KnownPeersUpdate(KnownPeersUpdate<ST>),
    LoadError(String),
}

impl<ST, SCT> Encodable for ConfigEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::ConfigUpdate(m) => {
                let enc: [&dyn Encodable; 2] = [&1u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::KnownPeersUpdate(m) => {
                let enc: [&dyn Encodable; 2] = [&2u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::LoadError(m) => {
                let enc: [&dyn Encodable; 2] = [&3u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT> Decodable for ConfigEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => Ok(Self::ConfigUpdate(ConfigUpdate::<SCT>::decode(
                &mut payload,
            )?)),
            2 => Ok(Self::KnownPeersUpdate(KnownPeersUpdate::decode(
                &mut payload,
            )?)),
            3 => Ok(Self::LoadError(String::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown ConfigEvent",
            )),
        }
    }
}

/// MonadEvent are inputs to MonadState
#[derive(Debug, Serialize)]
pub enum MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// Events for consensus state
    ConsensusEvent(ConsensusEvent<ST, SCT, EPT>),
    /// Events for block sync responder
    BlockSyncEvent(BlockSyncEvent<ST, SCT, EPT>),
    /// Events to update validator set
    ValidatorEvent(ValidatorEvent<SCT>),
    /// Events to mempool
    MempoolEvent(MempoolEvent<ST, SCT, EPT>),
    /// Events for the debug control panel
    ControlPanelEvent(ControlPanelEvent<ST>),
    /// Events to update the block timestamper
    TimestampUpdateEvent(u128),
    /// Events to statesync
    StateSyncEvent(StateSyncEvent<ST, SCT, EPT>),
    /// Config updates
    ConfigEvent(ConfigEvent<ST, SCT>),
    /// Events for secondary raptorcast updates
    SecondaryRaptorcastEvent(SecondaryRaptorcastEvent<SCT>),
}

impl<ST, SCT, EPT> MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// We don't implement the normal Clone::clone because it's unnecessary in the general case.
    /// Clone is only used in mock-swarm for added observability.
    ///
    /// Currently, the only inconsistency is that the lossy_clone won't clone the statesync
    /// completion.
    pub fn lossy_clone(&self) -> Self {
        match self {
            MonadEvent::ConsensusEvent(event) => MonadEvent::ConsensusEvent(event.clone()),
            MonadEvent::BlockSyncEvent(event) => MonadEvent::BlockSyncEvent(event.clone()),
            MonadEvent::ValidatorEvent(event) => MonadEvent::ValidatorEvent(event.clone()),
            MonadEvent::MempoolEvent(event) => MonadEvent::MempoolEvent(event.clone()),
            MonadEvent::ControlPanelEvent(event) => MonadEvent::ControlPanelEvent(event.clone()),
            MonadEvent::TimestampUpdateEvent(timestamp) => {
                MonadEvent::TimestampUpdateEvent(*timestamp)
            }
            MonadEvent::StateSyncEvent(event) => {
                let event = match event {
                    StateSyncEvent::Inbound(node_id, state_sync_network_message) => {
                        StateSyncEvent::Inbound(*node_id, state_sync_network_message.clone())
                    }
                    StateSyncEvent::Outbound(
                        node_id,
                        state_sync_network_message,
                        // completion is NOT cloned
                        _completion,
                    ) => {
                        StateSyncEvent::Outbound(*node_id, state_sync_network_message.clone(), None)
                    }
                    StateSyncEvent::DoneSync(seq_num) => StateSyncEvent::DoneSync(*seq_num),
                    StateSyncEvent::BlockSync {
                        block_range,
                        full_blocks,
                    } => StateSyncEvent::BlockSync {
                        block_range: *block_range,
                        full_blocks: full_blocks.clone(),
                    },
                    StateSyncEvent::RequestSync { root, high_qc } => StateSyncEvent::RequestSync {
                        root: root.clone(),
                        high_qc: high_qc.clone(),
                    },
                };
                MonadEvent::StateSyncEvent(event)
            }
            MonadEvent::ConfigEvent(event) => MonadEvent::ConfigEvent(event.clone()),
            MonadEvent::SecondaryRaptorcastEvent(event) => {
                MonadEvent::SecondaryRaptorcastEvent(event.clone())
            }
        }
    }
}

impl<ST, SCT, EPT> Encodable for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::ConsensusEvent(event) => {
                let enc: [&dyn Encodable; 2] = [&1u8, &event];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::BlockSyncEvent(event) => {
                let enc: [&dyn Encodable; 2] = [&2u8, &event];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ValidatorEvent(event) => {
                let enc: [&dyn Encodable; 2] = [&3u8, &event];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::MempoolEvent(event) => {
                let enc: [&dyn Encodable; 2] = [&4u8, &event];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ControlPanelEvent(event) => {
                let enc: [&dyn Encodable; 2] = [&5u8, &event];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::TimestampUpdateEvent(event) => {
                let enc: [&dyn Encodable; 2] = [&6u8, &event];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::StateSyncEvent(event) => {
                let enc: [&dyn Encodable; 2] = [&7u8, &event];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ConfigEvent(event) => {
                let enc: [&dyn Encodable; 2] = [&8u8, &event];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::SecondaryRaptorcastEvent(event) => {
                let enc: [&dyn Encodable; 2] = [&9u8, &event];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => Ok(Self::ConsensusEvent(
                ConsensusEvent::<ST, SCT, EPT>::decode(&mut payload)?,
            )),
            2 => Ok(Self::BlockSyncEvent(
                BlockSyncEvent::<ST, SCT, EPT>::decode(&mut payload)?,
            )),
            3 => Ok(Self::ValidatorEvent(ValidatorEvent::<SCT>::decode(
                &mut payload,
            )?)),
            4 => Ok(Self::MempoolEvent(MempoolEvent::<ST, SCT, EPT>::decode(
                &mut payload,
            )?)),
            5 => Ok(Self::ControlPanelEvent(ControlPanelEvent::<ST>::decode(
                &mut payload,
            )?)),
            6 => Ok(Self::TimestampUpdateEvent(u128::decode(&mut payload)?)),
            7 => Ok(Self::StateSyncEvent(
                StateSyncEvent::<ST, SCT, EPT>::decode(&mut payload)?,
            )),
            8 => Ok(Self::ConfigEvent(ConfigEvent::<ST, SCT>::decode(
                &mut payload,
            )?)),
            9 => Ok(Self::SecondaryRaptorcastEvent(SecondaryRaptorcastEvent::<
                SCT,
            >::decode(
                &mut payload
            )?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown MonadEvent",
            )),
        }
    }
}

impl<ST, SCT, EPT> monad_types::Deserializable<[u8]> for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type ReadError = alloy_rlp::Error;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        MonadEvent::<ST, SCT, EPT>::decode(&mut data.as_ref())
    }
}

impl<ST, SCT, EPT> monad_types::Serializable<Bytes> for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        buf.into()
    }
}

impl<ST, SCT, EPT> std::fmt::Display for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // TODO impl Display for each individual event instead
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = match self {
            MonadEvent::ConsensusEvent(ConsensusEvent::Message {
                sender,
                unverified_message: _,
            }) => {
                format!("ConsensusEvent::Message from {sender}")
            }
            MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(round)) => {
                format!("ConsensusEvent::Timeout Pacemaker local timeout round {round:?}")
            }
            MonadEvent::ConsensusEvent(_) => "CONSENSUS".to_string(),
            MonadEvent::BlockSyncEvent(_) => "BLOCKSYNC".to_string(),
            MonadEvent::ValidatorEvent(_) => "VALIDATOR".to_string(),
            MonadEvent::MempoolEvent(MempoolEvent::Proposal { round, seq_num, .. }) => {
                format!("MempoolEvent::Proposal -- round {round:?}, seq_num {seq_num:?}")
            }
            MonadEvent::MempoolEvent(MempoolEvent::ForwardedTxs { sender, txs: txns }) => {
                format!(
                    "MempoolEvent::ForwardedTxns -- from {sender} number of txns: {}",
                    txns.len()
                )
            }
            MonadEvent::MempoolEvent(MempoolEvent::ForwardTxs(txs)) => {
                format!("MempoolEvent::ForwardTxs -- number of txns: {}", txs.len())
            }
            MonadEvent::ControlPanelEvent(_) => "CONTROLPANELEVENT".to_string(),
            MonadEvent::TimestampUpdateEvent(t) => format!("MempoolEvent::TimestampUpdate: {t}"),
            MonadEvent::StateSyncEvent(_) => "STATESYNC".to_string(),
            MonadEvent::ConfigEvent(_) => "CONFIGEVENT".to_string(),
            MonadEvent::SecondaryRaptorcastEvent(_) => "SECONDARYRAPTORCASTEVENT".to_string(),
        };

        write!(f, "{}", s)
    }
}

/// Wrapper around MonadEvent to capture more information that is useful in logs for
/// retrospection
#[derive(Debug, Serialize)]
pub struct LogFriendlyMonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub timestamp: DateTime<Utc>,
    pub event: MonadEvent<ST, SCT, EPT>,
}

type EventHeaderType = u32;
const EVENT_HEADER_LEN: usize = std::mem::size_of::<EventHeaderType>();

impl<ST, SCT, EPT> monad_types::Deserializable<[u8]> for LogFriendlyMonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type ReadError = alloy_rlp::Error;
    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        let mut offset = 0;
        let header: [u8; 4] = data[0..EVENT_HEADER_LEN].try_into().unwrap();
        let ts_size = EventHeaderType::from_le_bytes(header) as usize;
        offset += EVENT_HEADER_LEN;

        let ts: DateTime<Utc> = bincode::deserialize(&data[offset..offset + ts_size]).unwrap();
        offset += ts_size;

        let event = MonadEvent::<ST, SCT, EPT>::decode(&mut &data[offset..])?;

        Ok(LogFriendlyMonadEvent {
            timestamp: ts,
            event,
        })
    }
}

impl<ST, SCT, EPT> monad_types::Serializable<Bytes> for LogFriendlyMonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();

        let ts = bincode::serialize(&self.timestamp).unwrap();
        let len = (ts.len() as EventHeaderType).to_le_bytes();

        b.put(&len[..]);
        b.put(&ts[..]);

        self.event.encode(&mut b);

        b.into()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddrV4;

    use monad_crypto::{
        certificate_signature::{CertificateSignaturePubKey, PubKey},
        NopSignature,
    };

    use crate::{
        PeerEntry, StateSyncRequest, StateSyncResponse, StateSyncUpsertType, StateSyncUpsertV1,
        StateSyncVersion, SELF_STATESYNC_VERSION, STATESYNC_VERSION_V0, STATESYNC_VERSION_V1,
    };

    #[test]
    fn statesync_version_is_compatible() {
        assert!(STATESYNC_VERSION_V0.is_compatible());
        assert!(STATESYNC_VERSION_V1.is_compatible());
    }

    #[test]
    fn statesync_version_ord() {
        assert!(STATESYNC_VERSION_V0 < STATESYNC_VERSION_V1);
    }

    fn make_response(
        client_version: StateSyncVersion,
        server_version: StateSyncVersion,
    ) -> StateSyncResponse {
        StateSyncResponse {
            version: server_version,
            nonce: 0,
            response_index: 0,
            request: StateSyncRequest {
                version: client_version,
                prefix: 100000,
                prefix_bytes: 20,
                target: 3000,
                from: 10000000000,
                until: 200000000,
                old_target: 30000,
            },
            response: vec![StateSyncUpsertV1 {
                upsert_type: StateSyncUpsertType::Account,
                data: vec![0xFF_u8; 100].into(),
            }],
            response_n: 0,
        }
    }

    #[test]
    fn statesync_version_v0_roundtrip() {
        let response = make_response(STATESYNC_VERSION_V0, STATESYNC_VERSION_V0);
        let serialized_response = alloy_rlp::encode(&response);
        let deserialized_response = alloy_rlp::decode_exact(&serialized_response).unwrap();
        if response != deserialized_response {
            panic!("failed to roundtrip v0 statesync response")
        }
    }

    #[test]
    fn statesync_version_v1_roundtrip() {
        let response = make_response(STATESYNC_VERSION_V1, STATESYNC_VERSION_V1);
        let serialized_response = alloy_rlp::encode(&response);
        let deserialized_response = alloy_rlp::decode_exact(&serialized_response).unwrap();
        if response != deserialized_response {
            panic!("failed to roundtrip v1 statesync response")
        }
    }

    #[test]
    fn statesync_version_v1_to_v0() {
        // v0 client, v1 server
        let response = alloy_rlp::encode(make_response(STATESYNC_VERSION_V0, STATESYNC_VERSION_V1));

        // v0 format
        let v0_response =
            alloy_rlp::encode(make_response(STATESYNC_VERSION_V0, STATESYNC_VERSION_V0));
        // v1 format
        let v1_response =
            alloy_rlp::encode(make_response(STATESYNC_VERSION_V1, STATESYNC_VERSION_V1));
        assert!(
            v0_response.len() > v1_response.len(),
            "v1 serializes smaller messages"
        );

        // use len as a proxy for format
        // can't check pure equality, because the versions won't match in the serialized messages
        assert_eq!(
            response.len(),
            v0_response.len(),
            "v0 client can't understand v1 server"
        );
        assert_ne!(
            response.len(),
            v1_response.len(),
            "v1 server sent v1 response to v0 client"
        );
    }

    #[test]
    fn statesync_request_bad_version() {
        // version too low
        let request = StateSyncRequest {
            version: StateSyncVersion { major: 0, minor: 0 },
            prefix: 10,
            prefix_bytes: 1,
            target: 10,
            from: 9,
            until: 8,
            old_target: 7,
        };
        let serialized_request = alloy_rlp::encode(request);
        let deserialized_request: StateSyncRequest =
            alloy_rlp::decode_exact(&serialized_request).unwrap();
        assert_eq!(
            deserialized_request.version,
            StateSyncVersion { major: 0, minor: 0 }
        );
        assert_eq!(deserialized_request.prefix, 0);
        assert_eq!(deserialized_request.prefix_bytes, 0);
        assert_eq!(deserialized_request.target, 0);
        assert_eq!(deserialized_request.from, 0);
        assert_eq!(deserialized_request.until, 0);
        assert_eq!(deserialized_request.old_target, 0);

        // version too high
        let request = StateSyncRequest {
            version: StateSyncVersion {
                major: SELF_STATESYNC_VERSION.major + 1,
                minor: 2,
            },
            prefix: 10,
            prefix_bytes: 1,
            target: 10,
            from: 9,
            until: 8,
            old_target: 7,
        };
        let serialized_request = alloy_rlp::encode(request);
        let deserialized_request: StateSyncRequest =
            alloy_rlp::decode_exact(&serialized_request).unwrap();
        assert_eq!(
            deserialized_request.version,
            StateSyncVersion {
                major: SELF_STATESYNC_VERSION.major + 1,
                minor: 2
            }
        );
        assert_eq!(deserialized_request.prefix, 0);
        assert_eq!(deserialized_request.prefix_bytes, 0);
        assert_eq!(deserialized_request.target, 0);
        assert_eq!(deserialized_request.from, 0);
        assert_eq!(deserialized_request.until, 0);
        assert_eq!(deserialized_request.old_target, 0);
    }

    #[test]
    fn peer_entry_rlp_encode_decode() {
        let pubkey = CertificateSignaturePubKey::<NopSignature>::from_bytes(&[1u8; 32]).unwrap();
        let addr: SocketAddrV4 = "127.0.0.1:8000".parse().unwrap();
        let signature = NopSignature { pubkey, id: 1234 };
        let record_seq_num = 0;
        let entry = PeerEntry {
            pubkey,
            addr,
            signature,
            record_seq_num,
        };
        let encoded = alloy_rlp::encode(&entry);
        let decoded: PeerEntry<NopSignature> = alloy_rlp::decode_exact(&encoded).unwrap();
        assert_eq!(entry, decoded);
    }
}
