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

// Raptorcast-aware in-process router scheduler for mock-swarm.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
use monad_consensus_types::{
    block::{MockExecutionProtocol, PassthruBlockPolicy},
    block_validator::MockValidator,
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, PubKey},
    NopSignature,
};
use monad_execution_state_read::InMemoryState;
use monad_multi_sig::MultiSig;
use monad_raptorcast::{
    packet::{deterministic::PrimaryEncoding, regular},
    util::{EncodingScheme, PrimaryBroadcastGroup},
};
use monad_router_scheduler::{RouterEvent, RouterScheduler, RouterSchedulerBuilder};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_transformer::GenericTransformerPipeline;
use monad_types::{Deserializable, Epoch, NodeId, Round, RouterTarget, Serializable, Stake};
use monad_updaters::{
    ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{
    proposer_schedule::{ElectedProposerSchedule, ProposerSchedule},
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSet, ValidatorSetFactory, ValidatorSetType},
};

use crate::swarm_relation::SwarmRelation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkMode {
    Raptorcast,
    Unicast,
}

const PROPOSER_SCHEDULE_CACHE_MAX_PAST_ROUNDS: Round = Round(100);
const PROPOSER_SCHEDULE_CACHE_MAX_FUTURE_ROUNDS: Round = Round(100);

#[derive(Default)]
struct DecodingState {
    seen_chunk_ids: BTreeSet<usize>,
    decoded: bool,
}

#[derive(Clone)]
pub struct RaptorcastRouterConfig<
    PT: PubKey,
    IM,
    OM,
    PS = ElectedProposerSchedule<PT, SimpleRoundRobin<PT>>,
> where
    PS: ProposerSchedule<PT>,
{
    pub self_id: NodeId<PT>,

    /// The extra fraction of num_source_symbols(K) required for
    /// successful decode. 0 means decoding at K+1 chunks.
    pub decoding_threshold: f32,

    /// The parameters for unicast/broadcast (v0). Raptorcast uses
    /// parameters specified by deterministic encoding.
    pub unicast_redundancy: f32,
    pub symbol_len: u32,

    pub proposer_schedule: PS,

    pub _phantom: PhantomData<(IM, OM)>,
}

impl<PT: PubKey, IM, OM> RaptorcastRouterConfig<PT, IM, OM> {
    pub fn new(self_id: NodeId<PT>) -> Self {
        Self::new_with_proposer_schedule(
            self_id,
            ElectedProposerSchedule::new(SimpleRoundRobin::default()),
        )
    }
}

impl<PT, IM, OM, PS> RaptorcastRouterConfig<PT, IM, OM, PS>
where
    PT: PubKey,
    PS: ProposerSchedule<PT>,
{
    pub fn new_with_proposer_schedule(self_id: NodeId<PT>, proposer_schedule: PS) -> Self {
        Self {
            self_id,
            decoding_threshold: 0.0,
            unicast_redundancy: 2.5,
            symbol_len: regular::MIN_CHUNK_LENGTH as u32,
            proposer_schedule,
            _phantom: PhantomData,
        }
    }
}

impl<PT, IM, OM, PS> RouterSchedulerBuilder for RaptorcastRouterConfig<PT, IM, OM, PS>
where
    IM: Deserializable<Bytes>,
    OM: Serializable<Bytes>,
    PT: PubKey,
    PS: ProposerSchedule<PT>,
{
    type RouterScheduler = RaptorcastRouterScheduler<PT, IM, OM, PS>;

    fn build(self) -> Self::RouterScheduler {
        RaptorcastRouterScheduler {
            next_msg_id: 0,
            config: self,
            events: BTreeMap::new(),
            decoding_states: HashMap::new(),
            validator_sets: BTreeMap::new(),
            current_round: None,
            _phantom: PhantomData,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum WireMsg<PT: PubKey> {
    Chunk(ChunkMsg<PT>),
    // tcp or direct udp
    Direct(Arc<Bytes>),
}

#[derive(Clone, PartialEq, Eq)]
pub struct ChunkMsg<PT: PubKey> {
    pub msg_id: usize,
    pub round: Round,
    pub author: NodeId<PT>,
    pub chunk_id: usize,
    pub total_chunks: usize,
    pub num_source_symbols: usize,
    pub mode: ChunkMode,
    pub payload: Arc<Bytes>,
}

pub struct RaptorcastRouterScheduler<
    PT: PubKey,
    IM,
    OM,
    PS = ElectedProposerSchedule<PT, SimpleRoundRobin<PT>>,
> where
    PS: ProposerSchedule<PT>,
{
    next_msg_id: usize,
    config: RaptorcastRouterConfig<PT, IM, OM, PS>,
    events: BTreeMap<Duration, VecDeque<RouterEvent<PT, IM, WireMsg<PT>>>>,
    decoding_states: HashMap<(NodeId<PT>, Round, u64), DecodingState>,
    validator_sets: BTreeMap<Epoch, ValidatorSet<PT>>,
    current_round: Option<Round>,
    _phantom: PhantomData<(IM, OM)>,
}

impl<PT, IM, OM, PS> RaptorcastRouterScheduler<PT, IM, OM, PS>
where
    IM: Deserializable<Bytes>,
    OM: Serializable<Bytes>,

    PT: PubKey,
    PS: ProposerSchedule<PT>,
{
    fn push_rx_event(&mut self, time: Duration, from: NodeId<PT>, payload: &Bytes) {
        let msg = IM::deserialize(payload).expect("failed to deserialize");
        self.events
            .entry(time)
            .or_default()
            .push_back(RouterEvent::Rx(from, msg));
    }

    fn push_tx_event(&mut self, time: Duration, to: NodeId<PT>, message: WireMsg<PT>) {
        self.events
            .entry(time)
            .or_default()
            .push_back(RouterEvent::Tx(to, message));
    }

    fn emit_direct(&mut self, time: Duration, payload: &Arc<Bytes>, to: NodeId<PT>) {
        self.push_tx_event(time, to, WireMsg::Direct(payload.clone()));
    }

    fn emit_unicast(
        &mut self,
        time: Duration,
        msg_id: usize,
        payload: &Arc<Bytes>,
        to: NodeId<PT>,
    ) {
        let num_source_symbols = payload.len().div_ceil(self.config.symbol_len as usize);
        let num_scaled_chunks =
            ((num_source_symbols as f32) * self.config.unicast_redundancy).ceil() as usize;
        let round = self.current_round.unwrap_or(Round(0));

        for chunk_id in 0..num_scaled_chunks {
            let chunk_msg = ChunkMsg {
                msg_id,
                round,
                mode: ChunkMode::Unicast,
                author: self.config.self_id,
                payload: payload.clone(),
                total_chunks: num_scaled_chunks,
                num_source_symbols,
                chunk_id,
            };

            self.push_tx_event(time, to, WireMsg::Chunk(chunk_msg));
        }
    }

    fn primary_encoding(
        &self,
        chunk: &ChunkMsg<PT>,
        epoch: Epoch,
        validator_set: &ValidatorSet<PT>,
    ) -> PrimaryEncoding<PT> {
        // use msg_id to introduce some variability.
        let unix_ts_ms = chunk.msg_id as u64;

        let encoding_scheme = EncodingScheme::Deterministic25(chunk.round);
        let group = PrimaryBroadcastGroup::new_unchecked(epoch, &chunk.author, validator_set);
        PrimaryEncoding::new(encoding_scheme, &group, chunk.payload.len(), unix_ts_ms)
            .expect("failed to build raptorcast message")
    }

    fn emit_raptorcast(
        &mut self,
        time: Duration,
        msg_id: usize,
        round: Round,
        epoch: Epoch,
        payload: &Arc<Bytes>,
    ) {
        let Some(validator_set) = self.validator_sets.get(&epoch) else {
            return;
        };

        let self_id = self.config.self_id;
        let mut chunk_msg = ChunkMsg {
            msg_id,
            round,
            mode: ChunkMode::Raptorcast,
            author: self_id,
            payload: payload.clone(),
            // to be overridden later
            num_source_symbols: 0,
            total_chunks: 0,
            chunk_id: 0,
        };
        let encoding = self.primary_encoding(&chunk_msg, epoch, validator_set);
        chunk_msg.num_source_symbols = encoding.layout().num_base_symbols(payload.len());

        let chunks = encoding.make_chunks().expect("failed to assign chunks");
        chunk_msg.total_chunks = chunks.len();

        // raptorcast does not build chunks for the publisher, so we
        // need to emit a direct message to ensure self reception.
        self.emit_direct(time, payload, self_id);

        for chunk in chunks {
            let chunk_msg = ChunkMsg {
                chunk_id: chunk.chunk_id(),
                ..chunk_msg.clone()
            };
            let recipient = *chunk.recipient().node_id();
            self.push_tx_event(time, recipient, WireMsg::Chunk(chunk_msg));
        }
    }

    fn handle_inbound_direct(&mut self, time: Duration, from: NodeId<PT>, message: &Arc<Bytes>) {
        self.push_rx_event(time, from, message.as_ref());
    }

    fn handle_inbound_unicast(&mut self, time: Duration, from: NodeId<PT>, message: ChunkMsg<PT>) {
        if !self.try_decode(&message) {
            return;
        }

        self.push_rx_event(time, from, message.payload.as_ref());
    }

    fn handle_inbound_raptorcast(
        &mut self,
        time: Duration,
        _from: NodeId<PT>,
        message: ChunkMsg<PT>,
    ) {
        if !self.is_round_in_window(message.round) {
            return;
        }

        let Some((epoch, validator_set)) = self.validator_set_for_round(message.round) else {
            return;
        };

        if self
            .config
            .proposer_schedule
            .check_proposer(&message.author, message.round)
            != Some(true)
        {
            return;
        }

        let encoding = self.primary_encoding(&message, epoch, validator_set);
        let assignment = encoding
            .make_assignment()
            .expect("failed to infer chunk assignment");
        let routing = assignment
            .resolve_chunk_id(message.chunk_id)
            .expect("invalid chunk assignment");

        if routing.recipient() == &self.config.self_id {
            // rebroadcasting
            for target in routing.rebroadcast_targets() {
                self.push_tx_event(time, target, WireMsg::Chunk(message.clone()));
            }
        }

        // not the first hop, simply consume the message.
        if !self.try_decode(&message) {
            return;
        }

        self.push_rx_event(time, message.author, message.payload.as_ref());
    }

    // Returns whether the message should be processed as decoded.
    fn try_decode(&mut self, message: &ChunkMsg<PT>) -> bool {
        let key = (message.author, message.round, message.msg_id as u64);
        let state = self.decoding_states.entry(key).or_default();
        if state.decoded {
            return false;
        }

        if state.seen_chunk_ids.contains(&message.chunk_id) {
            return false;
        }

        state.seen_chunk_ids.insert(message.chunk_id);

        let extra_chunks_needed =
            (message.num_source_symbols as f32 * self.config.decoding_threshold).round() as usize;
        // ensure decode when receiving all chunks
        let threshold =
            (message.num_source_symbols + extra_chunks_needed + 1).min(message.total_chunks);
        if state.seen_chunk_ids.len() < threshold {
            return false;
        }

        state.decoded = true;
        true
    }

    fn validator_set_for_round(&self, round: Round) -> Option<(Epoch, &ValidatorSet<PT>)> {
        self.validator_sets
            .iter()
            .find(|(epoch, _)| {
                self.config.proposer_schedule.check_epoch(**epoch, round) == Some(true)
            })
            .map(|(epoch, validator_set)| (*epoch, validator_set))
    }

    fn is_round_in_window(&self, round: Round) -> bool {
        let Some(current_round) = self.current_round else {
            return true;
        };

        let min_round = current_round.saturating_sub(PROPOSER_SCHEDULE_CACHE_MAX_PAST_ROUNDS);
        let max_round = current_round.saturating_add(PROPOSER_SCHEDULE_CACHE_MAX_FUTURE_ROUNDS);

        min_round <= round && round <= max_round
    }
}

impl<PT, IM, OM, PS> RouterScheduler for RaptorcastRouterScheduler<PT, IM, OM, PS>
where
    IM: Deserializable<Bytes>,
    OM: Serializable<Bytes>,
    PT: PubKey,
    PS: ProposerSchedule<PT>,
{
    type NodeIdPublicKey = PT;
    type TransportMessage = WireMsg<PT>;
    type InboundMessage = IM;
    type OutboundMessage = OM;

    fn process_inbound(
        &mut self,
        time: Duration,
        from: NodeId<PT>,
        message: Self::TransportMessage,
    ) {
        match message {
            WireMsg::Direct(payload) => self.handle_inbound_direct(time, from, &payload),
            WireMsg::Chunk(
                chunk @ ChunkMsg {
                    mode: ChunkMode::Raptorcast,
                    ..
                },
            ) => self.handle_inbound_raptorcast(time, from, chunk),
            WireMsg::Chunk(
                chunk @ ChunkMsg {
                    mode: ChunkMode::Unicast,
                    ..
                },
            ) => self.handle_inbound_unicast(time, from, chunk),
        }
    }

    fn send_outbound(
        &mut self,
        time: Duration,
        to: RouterTarget<PT>,
        message: Self::OutboundMessage,
    ) {
        let msg_id = self.next_msg_id;
        self.next_msg_id += 1;

        let message = message.serialize();
        let payload = Arc::new(message);

        match to {
            RouterTarget::Raptorcast { round, epoch } => {
                self.emit_raptorcast(time, msg_id, round, epoch, &payload);
            }
            RouterTarget::Broadcast(epoch) => {
                let Some(validator_set) = self.validator_sets.get(&epoch) else {
                    return;
                };

                // .clone() to get around borrowed self
                for recipient in validator_set.get_members().clone().keys() {
                    self.emit_unicast(time, msg_id, &payload, *recipient);
                }
            }
            RouterTarget::PointToPoint(to) => {
                self.emit_unicast(time, msg_id, &payload, to);
            }
            RouterTarget::DirectPointToPoint(to) => {
                self.emit_direct(time, &payload, to);
            }
            RouterTarget::TcpPointToPoint { to, completion } => {
                if let Some(completion) = completion {
                    let _ = completion.send(());
                }
                self.emit_direct(time, &payload, to);
            }
        }
    }

    fn add_epoch_validator_set(
        &mut self,
        epoch: Epoch,
        epoch_start: Round,
        validator_set: Vec<(NodeId<Self::NodeIdPublicKey>, Stake)>,
    ) {
        let validator_set = ValidatorSet::new_unchecked(validator_set.into_iter().collect());
        self.config
            .proposer_schedule
            .insert_epoch(epoch, epoch_start, validator_set.clone());
        self.validator_sets.insert(epoch, validator_set);
    }

    fn update_current_round(&mut self, _epoch: Epoch, round: Round) {
        self.current_round = Some(round);

        let cutoff = round.saturating_sub(PROPOSER_SCHEDULE_CACHE_MAX_PAST_ROUNDS);
        self.config.proposer_schedule.prune_below(cutoff);
    }

    fn peek_tick(&self) -> Option<Duration> {
        self.events.keys().next().copied()
    }

    fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<RouterEvent<PT, IM, Self::TransportMessage>> {
        let next_tick = self.peek_tick()?;
        if next_tick > until {
            return None;
        }
        let mut entry = self.events.first_entry().expect("checked non-empty above");
        let queue = entry.get_mut();
        let event = queue.pop_front().expect("non-empty bucket");
        if queue.is_empty() {
            entry.remove_entry();
        }
        Some(event)
    }
}

pub struct RaptorcastSwarm;
impl SwarmRelation for RaptorcastSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;
    type BlockPolicyType = PassthruBlockPolicy;
    type ExecutionStateReadType = InMemoryState<Self::SignatureType, Self::SignatureCollectionType>;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;

    type TransportMessage = WireMsg<CertificateSignaturePubKey<Self::SignatureType>>;
    type BlockValidator = MockValidator;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type Ledger =
        MockLedger<Self::SignatureType, Self::SignatureCollectionType, Self::ExecutionProtocolType>;

    type RouterScheduler = RaptorcastRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
        VerifiedMonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type ValSetUpdater = MockValSetUpdaterNop<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
    type TxPoolExecutor = MockTxPoolExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
        Self::BlockPolicyType,
        Self::ExecutionStateReadType,
        Self::ChainConfigType,
        Self::ChainRevisionType,
    >;
    type StateSyncExecutor = MockStateSyncExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
}
