pub mod convert;

use std::{fmt::Debug, net::SocketAddr};

use alloy_rlp::{encode_list, Decodable, Encodable, RlpDecodable, RlpEncodable};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use monad_blocksync::{
    blocksync::BlockSyncSelfRequester,
    messages::message::{BlockSyncRequestMessage, BlockSyncResponseMessage},
};
use monad_consensus::{
    messages::consensus_message::ConsensusMessage,
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    block::{
        BlockPolicy, BlockRange, ConsensusBlockHeader, ConsensusFullBlock, ExecutionResult,
        OptimisticCommit, ProposedExecutionInputs,
    },
    checkpoint::Checkpoint,
    metrics::Metrics,
    payload::{ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::{QuorumCertificate, TimestampAdjustment},
    signature_collection::SignatureCollection,
    timeout::TimeoutCertificate,
    validator_data::{ValidatorSetData, ValidatorSetDataWithEpoch},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_state_backend::StateBackend;
use monad_types::{BlockId, Epoch, ExecutionProtocol, NodeId, Round, RouterTarget, SeqNum, Stake};
use serde::{Deserialize, Serialize};

const STATESYNC_NETWORK_MESSAGE_NAME: &str = "StateSyncNetworkMessage";

#[derive(Clone, Debug)]
pub enum RouterCommand<PT: PubKey, OM> {
    // Publish should not be replayed
    Publish {
        target: RouterTarget<PT>,
        message: OM,
    },
    AddEpochValidatorSet {
        epoch: Epoch,
        validator_set: Vec<(NodeId<PT>, Stake)>,
    },
    UpdateCurrentRound(Epoch, Round),
    GetPeers,
    UpdatePeers(Vec<(NodeId<PT>, SocketAddr)>),
    GetFullNodes,
    UpdateFullNodes(Vec<NodeId<PT>>),
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

#[derive(Debug)]
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

pub enum LedgerCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    LedgerClearWal,
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
            LedgerCommand::LedgerClearWal => f.debug_tuple("LedgerClearWal").finish(),
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

#[derive(Clone)]
pub struct CheckpointCommand<SCT: SignatureCollection> {
    pub root_seq_num: SeqNum,
    pub high_qc_round: Round,
    pub checkpoint: Checkpoint<SCT>,
}

pub enum StateRootHashCommand<SCT>
where
    SCT: SignatureCollection,
{
    RequestProposed(BlockId, SeqNum, Round),
    RequestFinalized(SeqNum),
    CancelBelow(SeqNum),
    UpdateValidators((ValidatorSetData<SCT>, Epoch)),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetValidatorSet<SCT: SignatureCollection> {
    Request,
    #[serde(bound = "SCT: SignatureCollection")]
    Response(ValidatorSetDataWithEpoch<SCT>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetMetrics {
    Request,
    Response(Metrics),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GetPeers<PT: PubKey> {
    Request,
    #[serde(bound = "PT: PubKey")]
    Response(Vec<(NodeId<PT>, SocketAddr)>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GetFullNodes<PT: PubKey> {
    Request,
    #[serde(bound = "PT: PubKey")]
    Response(Vec<NodeId<PT>>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReadCommand<SCT: SignatureCollection + Clone> {
    #[serde(bound = "SCT: SignatureCollection")]
    GetValidatorSet(GetValidatorSet<SCT>),
    GetMetrics(GetMetrics),
    #[serde(bound = "SCT: SignatureCollection")]
    GetPeers(GetPeers<SCT::NodeIdPubKey>),
    #[serde(bound = "SCT: SignatureCollection")]
    GetFullNodes(GetFullNodes<SCT::NodeIdPubKey>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UpdateValidatorSet<SCT: SignatureCollection> {
    #[serde(bound(
        deserialize = "SCT: SignatureCollection",
        serialize = "SCT: SignatureCollection",
    ))]
    Request(ValidatorSetDataWithEpoch<SCT>),
    Response,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClearMetrics {
    Request,
    Response(Metrics),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum UpdatePeers<PT: PubKey> {
    #[serde(bound = "PT: PubKey")]
    Request(Vec<(NodeId<PT>, SocketAddr)>),
    Response,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum UpdateFullNodes<PT: PubKey> {
    #[serde(bound = "PT: PubKey")]
    Request(Vec<NodeId<PT>>),
    Response,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReloadConfig {
    Request,
    Response(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WriteCommand<SCT: SignatureCollection> {
    #[serde(bound = "SCT: SignatureCollection")]
    UpdateValidatorSet(UpdateValidatorSet<SCT>),
    ClearMetrics(ClearMetrics),
    UpdateLogFilter(String),
    #[serde(bound = "SCT: SignatureCollection")]
    UpdatePeers(UpdatePeers<SCT::NodeIdPubKey>),
    #[serde(bound = "SCT: SignatureCollection")]
    UpdateFullNodes(UpdateFullNodes<SCT::NodeIdPubKey>),
    ReloadConfig(ReloadConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlPanelCommand<SCT: SignatureCollection> {
    #[serde(bound = "SCT: SignatureCollection")]
    Read(ReadCommand<SCT>),
    #[serde(bound = "SCT: SignatureCollection")]
    Write(WriteCommand<SCT>),
}

#[derive(Debug)]
pub enum LoopbackCommand<E> {
    Forward(E),
}

#[derive(Debug)]
pub enum TimestampCommand {
    AdjustDelta(TimestampAdjustment),
}

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

pub enum ConfigReloadCommand {
    ReloadConfig,
}

pub enum TxPoolCommand<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    /// Used to update the nonces of tracked txs
    BlockCommit(Vec<BPT::ValidatedBlock>),

    CreateProposal {
        epoch: Epoch,
        round: Round,
        seq_num: SeqNum,
        high_qc: QuorumCertificate<SCT>,
        round_signature: RoundSignature<SCT::SignatureType>,
        last_round_tc: Option<TimeoutCertificate<SCT>>,

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
    },

    // Emitted after statesync is completed
    Reset {
        last_delay_committed_blocks: Vec<BPT::ValidatedBlock>,
    },
}

pub enum Command<E, OM, ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    RouterCommand(RouterCommand<SCT::NodeIdPubKey, OM>),
    TimerCommand(TimerCommand<E>),
    LedgerCommand(LedgerCommand<ST, SCT, EPT>),
    CheckpointCommand(CheckpointCommand<SCT>),
    StateRootHashCommand(StateRootHashCommand<SCT>),
    TimestampCommand(TimestampCommand),

    TxPoolCommand(TxPoolCommand<ST, SCT, EPT, BPT, SBT>),
    ControlPanelCommand(ControlPanelCommand<SCT>),
    LoopbackCommand(LoopbackCommand<E>),
    StateSyncCommand(StateSyncCommand<ST, EPT>),
    ConfigReloadCommand(ConfigReloadCommand),
}

impl<E, OM, ST, SCT, EPT, BPT, SBT> Command<E, OM, ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (
        Vec<RouterCommand<SCT::NodeIdPubKey, OM>>,
        Vec<TimerCommand<E>>,
        Vec<LedgerCommand<ST, SCT, EPT>>,
        Vec<CheckpointCommand<SCT>>,
        Vec<StateRootHashCommand<SCT>>,
        Vec<TimestampCommand>,
        Vec<TxPoolCommand<ST, SCT, EPT, BPT, SBT>>,
        Vec<ControlPanelCommand<SCT>>,
        Vec<LoopbackCommand<E>>,
        Vec<StateSyncCommand<ST, EPT>>,
        Vec<ConfigReloadCommand>,
    ) {
        let mut router_cmds = Vec::new();
        let mut timer_cmds = Vec::new();
        let mut ledger_cmds = Vec::new();
        let mut checkpoint_cmds = Vec::new();
        let mut state_root_hash_cmds = Vec::new();
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
                Command::CheckpointCommand(cmd) => checkpoint_cmds.push(cmd),
                Command::StateRootHashCommand(cmd) => state_root_hash_cmds.push(cmd),
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
            checkpoint_cmds,
            state_root_hash_cmds,
            timestamp_cmds,
            txpool_cmds,
            control_panel_cmds,
            loopback_cmds,
            state_sync_cmds,
            config_reload_cmds,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
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
    Timeout,
    /// a block that was previously requested
    /// this is an invariant
    BlockSync {
        block_range: BlockRange,
        full_blocks: Vec<ConsensusFullBlock<ST, SCT, EPT>>,
    },
    SendVote(Round),
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
            ConsensusEvent::Timeout => f.debug_struct("Timeout").finish(),
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
#[derive(Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatorEvent<SCT: SignatureCollection> {
    UpdateValidators((ValidatorSetData<SCT>, Epoch)),
}

#[derive(Clone, PartialEq, Eq)]
pub enum MempoolEvent<SCT: SignatureCollection, EPT: ExecutionProtocol> {
    Proposal {
        epoch: Epoch,
        round: Round,
        seq_num: SeqNum,
        high_qc: QuorumCertificate<SCT>,
        timestamp_ns: u128,
        round_signature: RoundSignature<SCT::SignatureType>,
        delayed_execution_results: Vec<EPT::FinalizedHeader>,
        proposed_execution_inputs: ProposedExecutionInputs<EPT>,
        last_round_tc: Option<TimeoutCertificate<SCT>>,
    },

    /// Txs that are incoming via other nodes
    ForwardedTxs {
        sender: NodeId<SCT::NodeIdPubKey>,
        txs: Vec<Bytes>,
    },

    /// Txs that should be forwarded to upcoming leaders
    ForwardTxs(Vec<Bytes>),
}

impl<SCT: SignatureCollection, EPT: ExecutionProtocol> Debug for MempoolEvent<SCT, EPT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Proposal {
                epoch,
                round,
                seq_num,
                high_qc,
                timestamp_ns,
                round_signature,
                delayed_execution_results,
                proposed_execution_inputs,
                last_round_tc,
            } => f
                .debug_struct("Proposal")
                .field("epoch", epoch)
                .field("round", round)
                .field("seq_num", seq_num)
                .field("high_qc", high_qc)
                .field("timestamp_ns", timestamp_ns)
                .field("round_signature", round_signature)
                .field("delayed_execution_results", delayed_execution_results)
                .field("proposed_execution_inputs", proposed_execution_inputs)
                .field("last_round_tc", last_round_tc)
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

pub const SELF_STATESYNC_VERSION: StateSyncVersion = StateSyncVersion { major: 1, minor: 0 };

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, RlpEncodable, RlpDecodable)]
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
        self.major == SELF_STATESYNC_VERSION.major
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, RlpEncodable, RlpDecodable)]
pub struct StateSyncRequest {
    pub version: StateSyncVersion,

    pub prefix: u64,
    pub prefix_bytes: u8,
    pub target: u64,
    pub from: u64,
    pub until: u64,
    pub old_target: u64,
}

#[derive(Clone, PartialEq, Eq)]
pub enum StateSyncUpsertType {
    Code,
    Account,
    Storage,
    AccountDelete,
    StorageDelete,
    Header,
}

#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct StateSyncUpsert {
    pub upsert_type: StateSyncUpsertType,
    pub data: Vec<u8>,
}

impl StateSyncUpsert {
    pub fn new(upsert_type: StateSyncUpsertType, data: Vec<u8>) -> Self {
        Self { upsert_type, data }
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

#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct StateSyncResponse {
    pub version: StateSyncVersion,
    pub nonce: u64,
    pub response_index: u32,

    pub request: StateSyncRequest,
    // consensus state must validate that this sender is "trusted"
    pub response: Vec<StateSyncUpsert>,
    pub response_n: u64,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateSyncNetworkMessage {
    Request(StateSyncRequest),
    Response(StateSyncResponse),
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
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown StateSyncNetworkMessage",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Inbound(NodeId<SCT::NodeIdPubKey>, StateSyncNetworkMessage),
    Outbound(NodeId<SCT::NodeIdPubKey>, StateSyncNetworkMessage),

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPanelEvent<SCT>
where
    SCT: SignatureCollection,
{
    GetValidatorSet,
    GetMetricsEvent,
    ClearMetricsEvent,
    UpdateValidators(ValidatorSetDataWithEpoch<SCT>),
    UpdateLogFilter(String),
    GetPeers(GetPeers<SCT::NodeIdPubKey>),
    UpdatePeers(UpdatePeers<SCT::NodeIdPubKey>),
    GetFullNodes(GetFullNodes<SCT::NodeIdPubKey>),
    UpdateFullNodes(UpdateFullNodes<SCT::NodeIdPubKey>),
    ReloadConfig(ReloadConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigUpdate<SCT>
where
    SCT: SignatureCollection,
{
    pub full_nodes: Vec<NodeId<SCT::NodeIdPubKey>>,
    pub maybe_known_peers: Option<Vec<(NodeId<SCT::NodeIdPubKey>, SocketAddr)>>,
    pub blocksync_override_peers: Vec<NodeId<SCT::NodeIdPubKey>>,
    pub error_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigEvent<SCT>
where
    SCT: SignatureCollection,
{
    ConfigUpdate(ConfigUpdate<SCT>),
    LoadError(String),
}

/// MonadEvent are inputs to MonadState
#[derive(Debug, Clone, PartialEq, Eq)]
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
    MempoolEvent(MempoolEvent<SCT, EPT>),
    /// Execution updates
    ExecutionResultEvent(ExecutionResult<EPT>),
    /// Events for the debug control panel
    ControlPanelEvent(ControlPanelEvent<SCT>),
    /// Events to update the block timestamper
    TimestampUpdateEvent(u128),
    /// Events to statesync
    StateSyncEvent(StateSyncEvent<ST, SCT, EPT>),
    /// Config updates
    ConfigEvent(ConfigEvent<SCT>),
}

impl<ST, SCT, EPT> monad_types::Deserializable<[u8]> for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_event(data)
    }
}

impl<ST, SCT, EPT> monad_types::Serializable<Bytes> for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn serialize(&self) -> Bytes {
        crate::convert::interface::serialize_event(self)
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
            MonadEvent::ConsensusEvent(ConsensusEvent::Timeout) => {
                "ConsensusEvent::Timeout Pacemaker local timeout".to_string()
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
            MonadEvent::ExecutionResultEvent(_) => "EXECUTION_RESULT".to_string(),
            MonadEvent::ControlPanelEvent(_) => "CONTROLPANELEVENT".to_string(),
            MonadEvent::TimestampUpdateEvent(t) => format!("MempoolEvent::TimestampUpdate: {t}"),
            MonadEvent::StateSyncEvent(_) => "STATESYNC".to_string(),
            MonadEvent::ConfigEvent(_) => "CONFIGEVENT".to_string(),
        };

        write!(f, "{}", s)
    }
}

/// Wrapper around MonadEvent to capture more information that is useful in logs for
/// retrospection
#[derive(Debug, Clone, PartialEq, Eq)]
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
    type ReadError = monad_proto::error::ProtoError;
    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        let mut offset = 0;
        let header: [u8; 4] = data[0..EVENT_HEADER_LEN].try_into().unwrap();
        let ts_size = EventHeaderType::from_le_bytes(header) as usize;
        offset += EVENT_HEADER_LEN;

        let ts: DateTime<Utc> = bincode::deserialize(&data[offset..offset + ts_size]).unwrap();
        offset += ts_size;

        let event = crate::convert::interface::deserialize_event(&data[offset..])?;

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

        let ev = crate::convert::interface::serialize_event(&self.event);
        b.put(&ev[..]);

        b.into()
    }
}
