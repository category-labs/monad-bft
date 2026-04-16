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
    fmt::Debug,
    net::{SocketAddr, SocketAddrV4},
    num::NonZeroU16,
    sync::Arc,
};

use alloy_rlp::{encode_list, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use bytes::{BufMut, Bytes, BytesMut};
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
    deserialize_pubkey, serialize_pubkey, Epoch, ExecutionProtocol, LimitedVec, NodeId, Round,
    RouterTarget, SeqNum, Stake, TcpCompletionHandle, UdpPriority,
};
use monad_validator::signature_collection::SignatureCollection;
use monad_wal::wal::WALLog;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

const STATESYNC_NETWORK_MESSAGE_NAME: &str = "StateSyncNetworkMessage";

/// maximum number of upserts we can send in a single response
/// at 75 bytes per upsert, approx 1.5MB
pub const MAX_UPSERTS_PER_RESPONSE: usize = 20_000;

pub enum RouterCommand<ST: CertificateSignatureRecoverable, OM> {
    // Publish should not be replayed
    Publish {
        target: RouterTarget<CertificateSignaturePubKey<ST>>,
        message: OM,
    },
    PublishWithPriority {
        // NOTE(dshulyak) priority for tcp messages is ignored
        target: RouterTarget<CertificateSignaturePubKey<ST>>,
        message: OM,
        priority: UdpPriority,
    },
    // Primary publishing embeds epoch as group_id in chunk header. Secondary
    // publishing embeds round as group_id in chunk header, as rebroadcasting
    // periods are defined in rounds
    PublishToFullNodes {
        epoch: Epoch,
        round: Round,
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
        dedicated_full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        prioritized_full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
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
            Self::PublishWithPriority {
                target,
                message: _,
                priority,
            } => f
                .debug_struct("PublishWithPriority")
                .field("target", target)
                .field("priority", priority)
                .finish(),
            Self::PublishToFullNodes {
                epoch,
                round,
                message: _,
            } => f
                .debug_struct("PublishToFullNodes")
                .field("epoch", epoch)
                .field("round", round)
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
                dedicated_full_nodes,
                prioritized_full_nodes,
            } => f
                .debug_struct("UpdatePeers")
                .field("peer_entries", peer_entries)
                .field("dedicated_full_nodes", dedicated_full_nodes)
                .field("prioritized_full_nodes", prioritized_full_nodes)
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

    fn event_with_source(
        self,
        from: NodeId<Self::NodeIdPubKey>,
        _src_addr: SocketAddr,
    ) -> Self::Event {
        self.event(from)
    }
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

    pub auth_port: NonZeroU16,

    #[serde(
        alias = "direct_udp_auth_port",
        skip_serializing_if = "Option::is_none"
    )]
    pub direct_udp_port: Option<NonZeroU16>,
}

impl<ST: CertificateSignatureRecoverable> Encodable for PeerEntry<ST> {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let addr = self.addr.to_string();
        let auth_port = self.auth_port.get();
        let base = [
            &self.pubkey as &dyn Encodable,
            &addr as &dyn Encodable,
            &self.signature as &dyn Encodable,
            &self.record_seq_num as &dyn Encodable,
            &auth_port as &dyn Encodable,
        ];

        match self.direct_udp_port {
            None => encode_list::<_, dyn Encodable>(&base, out),
            Some(direct_udp_port) => {
                let direct_udp_port = direct_udp_port.get();
                let enc = [
                    base[0],
                    base[1],
                    base[2],
                    base[3],
                    base[4],
                    &direct_udp_port as &dyn Encodable,
                ];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
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

        if payload.is_empty() {
            return Err(alloy_rlp::Error::Custom("missing auth port"));
        }

        let auth_port = NonZeroU16::new(u16::decode(&mut payload)?)
            .ok_or(alloy_rlp::Error::Custom("invalid auth port"))?;

        let direct_udp_port = if !payload.is_empty() {
            NonZeroU16::new(u16::decode(&mut payload)?)
        } else {
            None
        };

        if !payload.is_empty() {
            return Err(alloy_rlp::Error::Custom("extra bytes in peer entry"));
        }

        Ok(Self {
            pubkey,
            addr,
            signature,
            record_seq_num,
            auth_port,
            direct_udp_port,
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
    /// Expand the set of peers the statesync client can sync from
    ExpandUpstreamPeers(Vec<NodeId<CertificateSignaturePubKey<ST>>>),
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

#[derive(Clone, PartialEq, Eq, Serialize, monad_wal::WALLog)]
pub enum ConsensusEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    #[wal(enable)]
    Message {
        sender: NodeId<SCT::NodeIdPubKey>,
        unverified_message: Unverified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>>,
    },
    #[wal(enable)]
    Timeout(Round),
    /// a block that was previously requested
    /// this is an invariant
    #[wal(enable)]
    BlockSync {
        block_range: BlockRange,
        full_blocks: Vec<ConsensusFullBlock<ST, SCT, EPT>>,
    },
    #[wal(enable)]
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
#[derive(Clone, PartialEq, Eq, Serialize, monad_wal::WALLog)]
pub enum BlockSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// A peer (not self) requesting for a missing block
    #[wal(enable)]
    Request {
        sender: NodeId<SCT::NodeIdPubKey>,
        request: BlockSyncRequestMessage,
    },
    /// Outbound request timed out
    #[wal(enable)]
    Timeout(BlockSyncRequestMessage),
    /// self requesting for a missing block
    /// this request must be retried if necessary
    #[wal(enable)]
    SelfRequest {
        requester: BlockSyncSelfRequester,
        block_range: BlockRange,
    },
    /// cancel request for block
    #[wal(enable)]
    SelfCancelRequest {
        requester: BlockSyncSelfRequester,
        block_range: BlockRange,
    },
    /// A peer (not self) sending us a block
    #[wal(enable)]
    Response {
        sender: NodeId<SCT::NodeIdPubKey>,
        response: BlockSyncResponseMessage<ST, SCT, EPT>,
    },
    /// self sending us missing block (from ledger)
    #[wal(enable)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, monad_wal::WALLog)]
pub enum ValidatorEvent<SCT: SignatureCollection> {
    #[wal(enable)]
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

#[serde_as]
#[derive(Clone, PartialEq, Eq, Serialize, monad_wal::WALLog)]
pub enum MempoolEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    #[wal(enable)]
    Proposal {
        epoch: Epoch,
        round: Round,
        seq_num: SeqNum,
        high_qc: QuorumCertificate<SCT>,
        timestamp_ns: u128,
        round_signature: RoundSignature<SCT::SignatureType>,
        // base fee fields used to populate consensus block header
        base_fee: u64,
        base_fee_trend: u64,
        base_fee_moment: u64,
        delayed_execution_results: Vec<EPT::FinalizedHeader>,
        proposed_execution_inputs: ProposedExecutionInputs<EPT>,
        last_round_tc: Option<TimeoutCertificate<ST, SCT, EPT>>,
        fresh_proposal_certificate: Option<FreshProposalCertificate<SCT>>,
    },

    /// Txs that are incoming via other nodes
    ForwardedTxs {
        sender: NodeId<SCT::NodeIdPubKey>,
        #[serde_as(as = "Vec<serde_with::hex::Hex>")]
        txs: Vec<Bytes>,
    },

    /// Txs that should be forwarded to upcoming leaders
    ForwardTxs(#[serde_as(as = "Vec<serde_with::hex::Hex>")] Vec<Bytes>),
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

                let enc: [&dyn Encodable; 14] = [
                    &1u8,
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
                let base_fee = u64::decode(&mut payload)?;
                let base_fee_trend = u64::decode(&mut payload)?;
                let base_fee_moment = u64::decode(&mut payload)?;

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
                .field("base_fee_trend", &base_fee_trend.cast_signed())
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

            if !payload.is_empty() {
                return Err(alloy_rlp::Error::UnexpectedLength);
            }
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

#[serde_as]
#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct StateSyncUpsertV0 {
    pub upsert_type: StateSyncUpsertType,
    #[serde_as(as = "serde_with::hex::Hex")]
    pub data: Vec<u8>,
}

#[serde_as]
#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct StateSyncUpsertV1 {
    pub upsert_type: StateSyncUpsertType,
    #[serde_as(as = "serde_with::hex::Hex")]
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

        let result = match u8::decode(&mut payload)? {
            1 => Self::Code,
            2 => Self::Account,
            3 => Self::Storage,
            4 => Self::AccountDelete,
            5 => Self::StorageDelete,
            6 => Self::Header,
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "failed to decode unknown StateSyncUpsertType",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
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
            LimitedVec::<StateSyncUpsertV1, MAX_UPSERTS_PER_RESPONSE>::decode(&mut payload)?
                .into_inner()
        } else {
            let v0_response =
                LimitedVec::<StateSyncUpsertV0, MAX_UPSERTS_PER_RESPONSE>::decode(&mut payload)?
                    .into_inner();
            v0_response.iter().map(StateSyncUpsertV0::as_v1).collect()
        };
        let response_n = u64::decode(&mut payload)?;

        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
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
    NotWhitelisted,
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
            Self::NotWhitelisted => {
                let enc: [&dyn Encodable; 2] = [&name, &5u8];
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
            Self::NotWhitelisted => {
                let enc: Vec<&dyn Encodable> = vec![&name, &5u8];
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

        let result = match u8::decode(&mut payload)? {
            1 => Self::Request(StateSyncRequest::decode(&mut payload)?),
            2 => Self::Response(StateSyncResponse::decode(&mut payload)?),
            3 => Self::BadVersion(StateSyncBadVersion::decode(&mut payload)?),
            4 => Self::Completion(SessionId::decode(&mut payload)?),
            5 => Self::NotWhitelisted,
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "failed to decode unknown StateSyncNetworkMessage",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[derive(Debug, Clone, Serialize, monad_wal::WALLog)]
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
        #[serde(skip)] Option<TcpCompletionHandle>, // completion
    ),

    /// Execution done syncing
    #[wal(enable)]
    DoneSync(SeqNum),

    // Statesync-requested block
    #[wal(enable)]
    BlockSync {
        block_range: BlockRange,
        full_blocks: Vec<ConsensusFullBlock<ST, SCT, EPT>>,
    },

    // Statesync re-sync request
    #[wal(enable)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, monad_wal::WALLog)]
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
    pub dedicated_full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    pub prioritized_full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, monad_wal::WALLog)]
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
#[derive(Debug, Clone, Serialize, monad_wal::WALLog)]
pub enum InnerMonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// Events for consensus state
    #[wal(enable(nested))]
    ConsensusEvent(ConsensusEvent<ST, SCT, EPT>),
    /// Events for block sync responder
    #[wal(enable(nested))]
    BlockSyncEvent(BlockSyncEvent<ST, SCT, EPT>),
    /// Events to update validator set
    #[wal(enable(nested))]
    ValidatorEvent(ValidatorEvent<SCT>),
    /// Events to mempool
    #[wal(enable(nested))]
    MempoolEvent(MempoolEvent<ST, SCT, EPT>),
    /// Events for the debug control panel
    #[wal(enable(nested))]
    ControlPanelEvent(ControlPanelEvent<ST>),
    /// Events to update the block timestamper
    TimestampUpdateEvent(u128),
    /// Events to statesync
    #[wal(enable(nested))]
    StateSyncEvent(StateSyncEvent<ST, SCT, EPT>),
    /// Config updates
    #[wal(enable(nested))]
    ConfigEvent(ConfigEvent<ST, SCT>),
    /// Secondary raptorcast updates
    SecondaryRaptorcastPeersUpdate {
        expiry_round: Round,
        confirm_group_peers: Vec<NodeId<SCT::NodeIdPubKey>>,
    },
}

#[derive(Clone)]
pub struct MonadEvent<ST, SCT, EPT>(Arc<InnerMonadEvent<ST, SCT, EPT>>)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol;

impl<ST, SCT, EPT> MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(inner: InnerMonadEvent<ST, SCT, EPT>) -> Self {
        Self(Arc::new(inner))
    }

    pub fn consensus_event(event: ConsensusEvent<ST, SCT, EPT>) -> Self {
        Self::new(InnerMonadEvent::ConsensusEvent(event))
    }

    pub fn block_sync_event(event: BlockSyncEvent<ST, SCT, EPT>) -> Self {
        Self::new(InnerMonadEvent::BlockSyncEvent(event))
    }

    pub fn validator_event(event: ValidatorEvent<SCT>) -> Self {
        Self::new(InnerMonadEvent::ValidatorEvent(event))
    }

    pub fn mempool_event(event: MempoolEvent<ST, SCT, EPT>) -> Self {
        Self::new(InnerMonadEvent::MempoolEvent(event))
    }

    pub fn control_panel_event(event: ControlPanelEvent<ST>) -> Self {
        Self::new(InnerMonadEvent::ControlPanelEvent(event))
    }

    pub fn timestamp_update_event(timestamp: u128) -> Self {
        Self::new(InnerMonadEvent::TimestampUpdateEvent(timestamp))
    }

    pub fn state_sync_event(event: StateSyncEvent<ST, SCT, EPT>) -> Self {
        Self::new(InnerMonadEvent::StateSyncEvent(event))
    }

    pub fn config_event(event: ConfigEvent<ST, SCT>) -> Self {
        Self::new(InnerMonadEvent::ConfigEvent(event))
    }

    pub fn secondary_raptorcast_peers_update(
        expiry_round: Round,
        confirm_group_peers: Vec<NodeId<SCT::NodeIdPubKey>>,
    ) -> Self {
        Self::new(InnerMonadEvent::SecondaryRaptorcastPeersUpdate {
            expiry_round,
            confirm_group_peers,
        })
    }

    pub fn is_wal_logged(&self) -> bool {
        self.as_ref().is_wal_logged()
    }

    pub fn lossy_clone(&self) -> Self {
        Self::new(match self.as_ref() {
            InnerMonadEvent::ConsensusEvent(event) => {
                InnerMonadEvent::ConsensusEvent(event.clone())
            }
            InnerMonadEvent::BlockSyncEvent(event) => {
                InnerMonadEvent::BlockSyncEvent(event.clone())
            }
            InnerMonadEvent::ValidatorEvent(event) => {
                InnerMonadEvent::ValidatorEvent(event.clone())
            }
            InnerMonadEvent::MempoolEvent(event) => InnerMonadEvent::MempoolEvent(event.clone()),
            InnerMonadEvent::ControlPanelEvent(event) => {
                InnerMonadEvent::ControlPanelEvent(event.clone())
            }
            InnerMonadEvent::TimestampUpdateEvent(timestamp) => {
                InnerMonadEvent::TimestampUpdateEvent(*timestamp)
            }
            InnerMonadEvent::StateSyncEvent(event) => {
                let event = match event {
                    StateSyncEvent::Inbound(node_id, state_sync_network_message) => {
                        StateSyncEvent::Inbound(*node_id, state_sync_network_message.clone())
                    }
                    StateSyncEvent::Outbound(node_id, state_sync_network_message, _completion) => {
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
                InnerMonadEvent::StateSyncEvent(event)
            }
            InnerMonadEvent::ConfigEvent(event) => InnerMonadEvent::ConfigEvent(event.clone()),
            InnerMonadEvent::SecondaryRaptorcastPeersUpdate {
                expiry_round,
                confirm_group_peers,
            } => InnerMonadEvent::SecondaryRaptorcastPeersUpdate {
                expiry_round: *expiry_round,
                confirm_group_peers: confirm_group_peers.clone(),
            },
        })
    }
}

impl<ST, SCT, EPT> AsRef<InnerMonadEvent<ST, SCT, EPT>> for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn as_ref(&self) -> &InnerMonadEvent<ST, SCT, EPT> {
        self.0.as_ref()
    }
}

impl<ST, SCT, EPT> std::fmt::Debug for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.as_ref(), f)
    }
}

impl<ST, SCT, EPT> Serialize for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_ref().serialize(serializer)
    }
}

impl<ST, SCT, EPT> Encodable for InnerMonadEvent<ST, SCT, EPT>
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
            Self::SecondaryRaptorcastPeersUpdate {
                expiry_round,
                confirm_group_peers,
            } => {
                let enc: [&dyn Encodable; 3] = [&9u8, &expiry_round, &confirm_group_peers];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for InnerMonadEvent<ST, SCT, EPT>
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
            9 => {
                let expiry_round = Round::decode(&mut payload)?;
                let confirm_group_peers = Vec::<NodeId<SCT::NodeIdPubKey>>::decode(&mut payload)?;
                Ok(Self::SecondaryRaptorcastPeersUpdate {
                    expiry_round,
                    confirm_group_peers,
                })
            }
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown MonadEvent",
            )),
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
        self.as_ref().encode(out)
    }
}

impl<ST, SCT, EPT> Decodable for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        InnerMonadEvent::<ST, SCT, EPT>::decode(buf).map(Self::new)
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
        Self::decode(&mut data.as_ref())
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

impl<ST, SCT, EPT> std::fmt::Display for InnerMonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = match self {
            InnerMonadEvent::ConsensusEvent(ConsensusEvent::Message {
                sender,
                unverified_message: _,
            }) => {
                format!("ConsensusEvent::Message from {sender}")
            }
            InnerMonadEvent::ConsensusEvent(ConsensusEvent::Timeout(round)) => {
                format!("ConsensusEvent::Timeout Pacemaker local timeout round {round:?}")
            }
            InnerMonadEvent::ConsensusEvent(_) => "CONSENSUS".to_string(),
            InnerMonadEvent::BlockSyncEvent(_) => "BLOCKSYNC".to_string(),
            InnerMonadEvent::ValidatorEvent(_) => "VALIDATOR".to_string(),
            InnerMonadEvent::MempoolEvent(MempoolEvent::Proposal { round, seq_num, .. }) => {
                format!("MempoolEvent::Proposal -- round {round:?}, seq_num {seq_num:?}")
            }
            InnerMonadEvent::MempoolEvent(MempoolEvent::ForwardedTxs { sender, txs: txns }) => {
                format!(
                    "MempoolEvent::ForwardedTxns -- from {sender} number of txns: {}",
                    txns.len()
                )
            }
            InnerMonadEvent::MempoolEvent(MempoolEvent::ForwardTxs(txs)) => {
                format!("MempoolEvent::ForwardTxs -- number of txns: {}", txs.len())
            }
            InnerMonadEvent::ControlPanelEvent(_) => "CONTROLPANELEVENT".to_string(),
            InnerMonadEvent::TimestampUpdateEvent(t) => {
                format!("MempoolEvent::TimestampUpdate: {t}")
            }
            InnerMonadEvent::StateSyncEvent(_) => "STATESYNC".to_string(),
            InnerMonadEvent::ConfigEvent(_) => "CONFIGEVENT".to_string(),
            InnerMonadEvent::SecondaryRaptorcastPeersUpdate { .. } => {
                "SecondaryRaptorcastPeersUpdate".to_string()
            }
        };

        write!(f, "{}", s)
    }
}

impl<ST, SCT, EPT> std::fmt::Display for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.as_ref(), f)
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
    pub event: MonadEvent<ST, SCT, EPT>,
}

impl<ST, SCT, EPT> monad_types::Deserializable<[u8]> for LogFriendlyMonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type ReadError = alloy_rlp::Error;
    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        let event = MonadEvent::<ST, SCT, EPT>::decode(&mut &data[..])?;
        Ok(LogFriendlyMonadEvent { event })
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
        self.event.encode(&mut b);
        b.into()
    }
}

impl<ST, SCT, EPT> WALLog for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn is_wal_logged(&self) -> bool {
        self.as_ref().is_wal_logged()
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddrV4, num::NonZeroU16};

    use alloy_rlp::{encode_list, Encodable};
    use monad_blocksync::messages::message::BlockSyncRequestMessage;
    use monad_consensus_types::block::BlockRange;
    use monad_crypto::{
        certificate_signature::{CertificateSignaturePubKey, PubKey},
        NopSignature,
    };
    use monad_eth_types::EthExecutionProtocol;
    use monad_multi_sig::MultiSig;
    use monad_types::{NodeId, SeqNum, GENESIS_BLOCK_ID};
    use monad_wal::wal::WALLog;

    use crate::{
        BlockSyncEvent, MempoolEvent, MonadEvent, PeerEntry, StateSyncEvent,
        StateSyncNetworkMessage, StateSyncRequest, StateSyncResponse, StateSyncUpsertType,
        StateSyncUpsertV1, StateSyncVersion, SELF_STATESYNC_VERSION, STATESYNC_VERSION_V0,
        STATESYNC_VERSION_V1,
    };

    type TestSignature = NopSignature;
    type TestSignatureCollection = MultiSig<TestSignature>;
    type TestExecutionProtocol = EthExecutionProtocol;

    #[derive(monad_wal::WALLog)]
    enum VariantLoggedTestEvent {
        #[wal(enable)]
        Logged,
        NotLogged,
    }

    #[derive(monad_wal::WALLog)]
    enum UnannotatedLoggedTestEvent {
        First,
        Second,
    }

    #[derive(monad_wal::WALLog)]
    enum NestedLoggedTestEvent {
        #[wal(enable(nested))]
        Nested(VariantLoggedTestEvent),
        #[wal(enable)]
        Scalar(SeqNum),
        Hidden,
    }

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
            auth_port: NonZeroU16::new(addr.port()).unwrap(),
            direct_udp_port: None,
        };
        let encoded = alloy_rlp::encode(&entry);
        let decoded: PeerEntry<NopSignature> = alloy_rlp::decode_exact(&encoded).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn peer_entry_rlp_encode_decode_with_direct_udp() {
        let pubkey = CertificateSignaturePubKey::<NopSignature>::from_bytes(&[2u8; 32]).unwrap();
        let addr: SocketAddrV4 = "127.0.0.1:8001".parse().unwrap();
        let signature = NopSignature { pubkey, id: 4321 };
        let entry = PeerEntry {
            pubkey,
            addr,
            signature,
            record_seq_num: 7,
            auth_port: NonZeroU16::new(9000).unwrap(),
            direct_udp_port: Some(NonZeroU16::new(9001).unwrap()),
        };

        let encoded = alloy_rlp::encode(&entry);
        let decoded: PeerEntry<NopSignature> = alloy_rlp::decode_exact(&encoded).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn peer_entry_rlp_decode_legacy_auth_only_form() {
        let pubkey = CertificateSignaturePubKey::<NopSignature>::from_bytes(&[3u8; 32]).unwrap();
        let addr: SocketAddrV4 = "127.0.0.1:8002".parse().unwrap();
        let signature = NopSignature { pubkey, id: 99 };
        let record_seq_num = 11u64;
        let auth_port = 9002u16;
        let enc: [&dyn Encodable; 5] = [
            &pubkey,
            &addr.to_string(),
            &signature,
            &record_seq_num,
            &auth_port,
        ];
        let mut encoded = Vec::new();
        encode_list::<_, dyn Encodable>(&enc, &mut encoded);

        let decoded: PeerEntry<NopSignature> = alloy_rlp::decode_exact(&encoded).unwrap();
        assert_eq!(decoded.auth_port, NonZeroU16::new(auth_port).unwrap());
        assert_eq!(decoded.direct_udp_port, None);
    }

    #[test]
    fn peer_entry_rlp_decode_rejects_missing_auth_port() {
        let pubkey = CertificateSignaturePubKey::<NopSignature>::from_bytes(&[5u8; 32]).unwrap();
        let addr: SocketAddrV4 = "127.0.0.1:8004".parse().unwrap();
        let signature = NopSignature { pubkey, id: 8 };
        let record_seq_num = 13u64;
        let enc: [&dyn Encodable; 4] = [&pubkey, &addr.to_string(), &signature, &record_seq_num];
        let mut encoded = Vec::new();
        encode_list::<_, dyn Encodable>(&enc, &mut encoded);

        let decoded: alloy_rlp::Result<PeerEntry<NopSignature>> = alloy_rlp::decode_exact(&encoded);
        assert!(decoded.is_err());
    }

    #[test]
    fn peer_entry_rlp_decode_rejects_zero_auth_port() {
        let pubkey = CertificateSignaturePubKey::<NopSignature>::from_bytes(&[6u8; 32]).unwrap();
        let addr: SocketAddrV4 = "127.0.0.1:8005".parse().unwrap();
        let signature = NopSignature { pubkey, id: 9 };
        let record_seq_num = 14u64;
        let auth_port = 0u16;
        let enc: [&dyn Encodable; 5] = [
            &pubkey,
            &addr.to_string(),
            &signature,
            &record_seq_num,
            &auth_port,
        ];
        let mut encoded = Vec::new();
        encode_list::<_, dyn Encodable>(&enc, &mut encoded);

        let decoded: alloy_rlp::Result<PeerEntry<NopSignature>> = alloy_rlp::decode_exact(&encoded);
        assert!(decoded.is_err());
    }

    #[test]
    fn peer_entry_rlp_decode_rejects_extra_fields() {
        let pubkey = CertificateSignaturePubKey::<NopSignature>::from_bytes(&[4u8; 32]).unwrap();
        let addr: SocketAddrV4 = "127.0.0.1:8003".parse().unwrap();
        let signature = NopSignature { pubkey, id: 7 };
        let record_seq_num = 12u64;
        let auth_port = 9003u16;
        let direct_udp_port = 9004u16;
        let extra_port = 9005u16;
        let enc: [&dyn Encodable; 7] = [
            &pubkey,
            &addr.to_string(),
            &signature,
            &record_seq_num,
            &auth_port,
            &direct_udp_port,
            &extra_port,
        ];
        let mut encoded = Vec::new();
        encode_list::<_, dyn Encodable>(&enc, &mut encoded);

        let decoded = alloy_rlp::decode_exact::<PeerEntry<NopSignature>>(&encoded);
        assert!(decoded.is_err());
    }

    #[test]
    fn wal_logging_only_keeps_traceable_events() {
        assert!(VariantLoggedTestEvent::Logged.is_wal_logged());
        assert!(!VariantLoggedTestEvent::NotLogged.is_wal_logged());
        assert!(!UnannotatedLoggedTestEvent::First.is_wal_logged());
        assert!(!UnannotatedLoggedTestEvent::Second.is_wal_logged());
        assert!(NestedLoggedTestEvent::Nested(VariantLoggedTestEvent::Logged).is_wal_logged());
        assert!(!NestedLoggedTestEvent::Nested(VariantLoggedTestEvent::NotLogged).is_wal_logged());
        assert!(NestedLoggedTestEvent::Scalar(SeqNum(3)).is_wal_logged());
        assert!(!NestedLoggedTestEvent::Hidden.is_wal_logged());

        let logged_event = MonadEvent::<
            TestSignature,
            TestSignatureCollection,
            TestExecutionProtocol,
        >::block_sync_event(BlockSyncEvent::Timeout(
            BlockSyncRequestMessage::Headers(BlockRange {
                last_block_id: GENESIS_BLOCK_ID,
                num_blocks: SeqNum(1),
            }),
        ));
        assert!(logged_event.is_wal_logged());

        let mempool_event = MonadEvent::<
            TestSignature,
            TestSignatureCollection,
            TestExecutionProtocol,
        >::mempool_event(MempoolEvent::ForwardTxs(Vec::new()));
        assert!(!mempool_event.is_wal_logged());

        let state_sync_event = MonadEvent::<
            TestSignature,
            TestSignatureCollection,
            TestExecutionProtocol,
        >::state_sync_event(StateSyncEvent::DoneSync(SeqNum(2)));
        assert!(state_sync_event.is_wal_logged());

        let sender = NodeId::new(
            CertificateSignaturePubKey::<TestSignature>::from_bytes(&[8u8; 32]).unwrap(),
        );
        let inbound_statesync = MonadEvent::<
            TestSignature,
            TestSignatureCollection,
            TestExecutionProtocol,
        >::state_sync_event(StateSyncEvent::Inbound(
            sender,
            StateSyncNetworkMessage::NotWhitelisted,
        ));
        assert!(!inbound_statesync.is_wal_logged());
    }
}
