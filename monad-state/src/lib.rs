use std::fmt::Debug;
use std::time::Duration;

use monad_blocktree::blocktree::BlockTree;
use monad_consensus::types::consensus_message::ConsensusMessage;
use monad_consensus::validation::signing::Unverified;
use monad_consensus::{
    pacemaker::{Pacemaker, PacemakerCommand, PacemakerTimerExpire},
    types::{
        block::{Block, TransactionList},
        ledger::{InMemoryLedger, Ledger},
        mempool::{Mempool, SimulationMempool},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
        quorum_certificate::{QuorumCertificate, Rank},
        signature::SignatureCollection,
        timeout::TimeoutCertificate,
        voting::VoteInfo,
    },
    validation::{
        hashing::{Hasher, Sha256Hash},
        safety::Safety,
        signing::Verified,
    },
    vote_state::VoteState,
};
use monad_crypto::{
    secp256k1::{KeyPair, PubKey},
    Signature,
};
use monad_executor::{Command, Message, PeerId, RouterCommand, State, TimerCommand};
use monad_types::{NodeId, Round};
use monad_validator::{
    leader_election::LeaderElection, validator::Validator, validator_set::ValidatorSet,
    weighted_round_robin::WeightedRoundRobin,
};

use message::MessageState;
use ref_cast::RefCast;

use tracing::{span, Level};

#[cfg(feature = "proto")]
pub mod convert;

mod message;

type LeaderElectionType = WeightedRoundRobin;
type HasherType = Sha256Hash;

pub struct MonadState<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    message_state: MessageState<MonadMessage<ST, SCT>, VerifiedMonadMessage<ST, SCT>>,

    consensus_state: ConsensusState<ST, SCT, InMemoryLedger<SCT>, SimulationMempool>,
    validator_set: ValidatorSet<LeaderElectionType>,
}

impl<ST, SCT> MonadState<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    pub fn pubkey(&self) -> PubKey {
        self.consensus_state.nodeid.0
    }

    pub fn ledger(&self) -> &Vec<Block<SCT>> {
        self.consensus_state.ledger.get_blocks()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonadEvent<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    Ack {
        peer: PeerId,
        id: <MonadMessage<ST, SCT> as Message>::Id,
        round: Round,
    },
    ConsensusEvent(ConsensusEvent<ST, SCT>),
}

#[cfg(feature = "proto")]
impl monad_types::Deserializable
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus::signatures::aggregate_signature::AggregateSignatures<
            monad_crypto::secp256k1::SecpSignature,
        >,
    >
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_event(data)
    }
}

#[cfg(feature = "proto")]
impl monad_types::Serializable
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus::signatures::aggregate_signature::AggregateSignatures<
            monad_crypto::secp256k1::SecpSignature,
        >,
    >
{
    fn serialize(&self) -> Vec<u8> {
        crate::convert::interface::serialize_event(self)
    }
}

#[derive(Debug, Clone)]
pub struct VerifiedMonadMessage<ST, SCT>(Verified<ST, ConsensusMessage<ST, SCT>>);

#[derive(RefCast)]
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct MonadMessage<ST, SCT>(Unverified<ST, ConsensusMessage<ST, SCT>>);

#[cfg(feature = "proto")]
impl monad_types::Serializable
    for VerifiedMonadMessage<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus::signatures::aggregate_signature::AggregateSignatures<
            monad_crypto::secp256k1::SecpSignature,
        >,
    >
{
    fn serialize(&self) -> Vec<u8> {
        monad_consensus::convert::interface::serialize_verified_consensus_message(&self.0)
    }
}

#[cfg(feature = "proto")]
impl monad_types::Deserializable
    for MonadMessage<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus::signatures::aggregate_signature::AggregateSignatures<
            monad_crypto::secp256k1::SecpSignature,
        >,
    >
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
        Ok(MonadMessage(
            monad_consensus::convert::interface::deserialize_unverified_consensus_message(message)?,
        ))
    }
}

impl<ST, SCT> From<VerifiedMonadMessage<ST, SCT>> for MonadMessage<ST, SCT> {
    fn from(value: VerifiedMonadMessage<ST, SCT>) -> Self {
        MonadMessage(value.0.into())
    }
}

impl<ST, SCT> AsRef<MonadMessage<ST, SCT>> for VerifiedMonadMessage<ST, SCT> {
    fn as_ref(&self) -> &MonadMessage<ST, SCT> {
        MonadMessage::ref_cast(self.0.as_ref())
    }
}

impl<ST, SCT> Message for MonadMessage<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    type Event = MonadEvent<ST, SCT>;
    type Id = ST;

    fn id(&self) -> Self::Id {
        *self.0.author_signature()
    }

    fn event(self, from: PeerId) -> Self::Event {
        // MUST assert that output is valid and came from the `from` PeerId
        // `from` must somehow be guaranteed to be staked at this point so that subsequent
        // malformed stuff (that gets added to event log) can be slashed? TODO

        Self::Event::ConsensusEvent(ConsensusEvent::Message {
            sender: from.0,
            unverified_message: self.0,
        })
    }
}

pub struct MonadConfig<SCT> {
    pub validators: Vec<PubKey>,
    pub key: KeyPair,

    pub delta: Duration,
    pub genesis_block: Block<SCT>,
    pub genesis_vote_info: VoteInfo,
    pub genesis_signatures: SCT,
}

impl<ST, SCT> State for MonadState<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    type Config = MonadConfig<SCT>;
    type Event = MonadEvent<ST, SCT>;
    type Message = MonadMessage<ST, SCT>;
    type OutboundMessage = VerifiedMonadMessage<ST, SCT>;

    fn init(config: Self::Config) -> (Self, Vec<Command<Self::Message, Self::OutboundMessage>>) {
        // create my keys and validator structs
        let validator_list = config
            .validators
            .into_iter()
            .map(|pubkey| Validator { pubkey, stake: 1 })
            .collect::<Vec<_>>();

        // create the initial validator set
        let val_set =
            ValidatorSet::new(validator_list.clone()).expect("initial validator set init failed");

        let genesis_qc = QuorumCertificate::genesis_qc::<HasherType>(
            config.genesis_vote_info,
            config.genesis_signatures,
        );

        let mut monad_state = Self {
            message_state: MessageState::new(
                10,
                validator_list
                    .into_iter()
                    .map(|v| PeerId(v.pubkey))
                    .collect(),
            ),
            validator_set: val_set,
            consensus_state: ConsensusState::new(
                config.key.pubkey(),
                config.genesis_block,
                genesis_qc,
                config.delta,
                config.key,
            ),
        };

        let init_cmds = monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
            PacemakerTimerExpire,
        )));

        (monad_state, init_cmds)
    }

    fn update(&mut self, event: Self::Event) -> Vec<Command<Self::Message, Self::OutboundMessage>> {
        match event {
            MonadEvent::Ack { peer, id, round } => self
                .message_state
                .handle_ack(round, peer, id)
                .into_iter()
                .map(|cmd| {
                    Command::RouterCommand(RouterCommand::Unpublish {
                        to: cmd.to,
                        id: cmd.id,
                    })
                })
                .collect(),

            MonadEvent::ConsensusEvent(consensus_event) => {
                let span = span!(Level::TRACE, "consensus_event", ?consensus_event);

                let _guard = span.enter();
                let consensus_commands: Vec<ConsensusCommand<ST, SCT>> = match consensus_event {
                    ConsensusEvent::Timeout(pacemaker_expire) => self
                        .consensus_state
                        .pacemaker
                        .handle_event(
                            &mut self.consensus_state.safety,
                            &self.consensus_state.high_qc,
                            pacemaker_expire,
                        )
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    ConsensusEvent::Message {
                        sender,
                        unverified_message,
                    } => {
                        let verified_message = match unverified_message
                            .verify::<HasherType>(self.validator_set.get_members(), &sender)
                        {
                            Ok(m) => m,
                            Err(_) => todo!(),
                        };
                        let (author, signature, verified_message) = verified_message.destructure();
                        match verified_message {
                            ConsensusMessage::Proposal(msg) => self
                                .consensus_state
                                .handle_proposal_message::<HasherType, _>(
                                    author,
                                    msg,
                                    &mut self.validator_set,
                                ),
                            ConsensusMessage::Vote(msg) => self
                                .consensus_state
                                .handle_vote_message::<HasherType, LeaderElectionType>(
                                    author,
                                    signature,
                                    msg,
                                    &mut self.validator_set,
                                ),
                            ConsensusMessage::Timeout(msg) => self
                                .consensus_state
                                .handle_timeout_message::<HasherType, _>(
                                    author,
                                    signature,
                                    msg,
                                    &mut self.validator_set,
                                ),
                        }
                    }
                };
                let mut cmds = Vec::new();
                for consensus_command in consensus_commands {
                    match consensus_command {
                        ConsensusCommand::Send { to, message } => {
                            let message = VerifiedMonadMessage(
                                message.sign::<HasherType>(&self.consensus_state.keypair),
                            );
                            let publish_action = self.message_state.send(to, message);
                            let id = Self::Message::from(publish_action.message.clone()).id();
                            cmds.push(Command::RouterCommand(RouterCommand::Publish {
                                to: publish_action.to,
                                message: publish_action.message,
                                on_ack: MonadEvent::Ack {
                                    peer: publish_action.to,
                                    id,

                                    // TODO verify that this is the correct round()?
                                    // should we be extracting this from `message` instead?
                                    round: self.message_state.round(),
                                },
                            }))
                        }
                        ConsensusCommand::Broadcast { message } => {
                            let message = VerifiedMonadMessage(
                                message.sign::<HasherType>(&self.consensus_state.keypair),
                            );
                            cmds.extend(self.message_state.broadcast(message).into_iter().map(
                                |publish_action| {
                                    let id =
                                        Self::Message::from(publish_action.message.clone()).id();
                                    Command::RouterCommand(RouterCommand::Publish {
                                        to: publish_action.to,
                                        message: publish_action.message,
                                        on_ack: MonadEvent::Ack {
                                            peer: publish_action.to,
                                            id,

                                            // TODO verify that this is the correct round()?
                                            // should we be extracting this from `message` instead?
                                            round: self.message_state.round(),
                                        },
                                    })
                                },
                            ));
                        }
                        ConsensusCommand::Schedule {
                            duration,
                            on_timeout,
                        } => cmds.push(Command::TimerCommand(TimerCommand::Schedule {
                            duration,
                            on_timeout: Self::Event::ConsensusEvent(ConsensusEvent::Timeout(
                                on_timeout,
                            )),
                        })),
                        ConsensusCommand::Unschedule => {
                            cmds.push(Command::TimerCommand(TimerCommand::Unschedule))
                        }
                    }
                }
                cmds
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusEvent<S, T> {
    Message {
        sender: PubKey,
        unverified_message: Unverified<S, ConsensusMessage<S, T>>,
    },
    Timeout(PacemakerTimerExpire),
}

impl<S: Debug, T: Debug> Debug for ConsensusEvent<S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => f
                .debug_struct("Message")
                .field("sender", &sender)
                .field("msg", &unverified_message)
                .finish(),
            ConsensusEvent::Timeout(p) => p.fmt(f),
        }
    }
}

#[derive(Debug)]
pub enum ConsensusCommand<S, T: SignatureCollection> {
    Send {
        to: PeerId,
        message: ConsensusMessage<S, T>,
    },
    Broadcast {
        message: ConsensusMessage<S, T>,
    },
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    Unschedule,
    // TODO add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
}

struct ConsensusState<S, T, L, M> {
    pending_block_tree: BlockTree<T>,
    vote_state: VoteState<T>,
    high_qc: QuorumCertificate<T>,

    ledger: L,
    mempool: M,

    pacemaker: Pacemaker<S, T>,
    safety: Safety,

    nodeid: NodeId,

    // TODO deprecate
    keypair: KeyPair,
}

impl<S, T, L, M> ConsensusState<S, T, L, M>
where
    S: Signature,
    T: SignatureCollection + Debug,
    L: Ledger<Signatures = T>,
    M: Mempool,
{
    pub fn new(
        my_pubkey: PubKey,
        genesis_block: Block<T>,
        genesis_qc: QuorumCertificate<T>,
        delta: Duration,

        // TODO deprecate
        keypair: KeyPair,
    ) -> Self {
        ConsensusState {
            pending_block_tree: BlockTree::new(genesis_block),
            vote_state: VoteState::default(),
            high_qc: genesis_qc,
            ledger: L::new(),
            mempool: M::new(),
            pacemaker: Pacemaker::new(delta, Round(1), None),
            safety: Safety::default(),
            nodeid: NodeId(my_pubkey),

            keypair,
        }
    }

    fn handle_proposal_message<H: Hasher, V: LeaderElection>(
        &mut self,
        author: NodeId,
        p: ProposalMessage<S, T>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, T>> {
        let mut cmds = Vec::new();
        if p.block.round < self.pacemaker.get_current_round() {
            return cmds;
        }

        let process_certificate_cmds = self.process_certificate_qc(&p.block.qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.last_round_tc.as_ref() {
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc)
                .map(Into::into)
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        let round = self.pacemaker.get_current_round();
        let leader = *validators.get_leader(round);

        if p.block.round != round || author != leader || p.block.author != leader {
            return cmds;
        }

        self.pending_block_tree
            .add(p.block.clone())
            .expect("Failed to add block to blocktree");

        let vote_msg = self.safety.make_vote::<S, T, H>(&p.block, &p.last_round_tc);

        if let Some(v) = vote_msg {
            let next_leader = validators.get_leader(round + Round(1));
            let send_cmd = ConsensusCommand::Send {
                to: PeerId(next_leader.0),
                message: ConsensusMessage::Vote(v),
            };
            cmds.push(send_cmd);
        }

        cmds
    }

    fn handle_vote_message<H: Hasher, V: LeaderElection>(
        &mut self,
        author: NodeId,
        signature: T::SignatureType,
        v: VoteMessage,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, T>> {
        if v.vote_info.round < self.pacemaker.get_current_round() {
            return Default::default();
        }

        let qc = self
            .vote_state
            .process_vote::<V, H>(&author, &signature, &v, validators);

        let mut cmds = Vec::new();
        if let Some(qc) = qc {
            cmds.extend(self.process_certificate_qc(&qc));

            if self.nodeid == *validators.get_leader(self.pacemaker.get_current_round()) {
                cmds.extend(self.process_new_round_event::<H>(None));
            }
        }
        cmds
    }

    fn handle_timeout_message<H: Hasher, V: LeaderElection>(
        &mut self,
        author: NodeId,
        signature: S,
        tm: TimeoutMessage<S, T>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, T>> {
        let mut cmds = Vec::new();
        if tm.tminfo.round < self.pacemaker.get_current_round() {
            return cmds;
        }

        let process_certificate_cmds = self.process_certificate_qc(&tm.tminfo.high_qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = tm.last_round_tc.as_ref() {
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc)
                .map(Into::into)
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        let (tc, remote_timeout_cmds) = self.pacemaker.process_remote_timeout(
            validators,
            &mut self.safety,
            &self.high_qc,
            author,
            signature,
            tm,
        );
        cmds.extend(remote_timeout_cmds.into_iter().map(Into::into));
        if let Some(tc) = tc {
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(&tc)
                .into_iter()
                .map(Into::into);
            cmds.extend(advance_round_cmds);

            if self.nodeid == *validators.get_leader(self.pacemaker.get_current_round()) {
                cmds.extend(self.process_new_round_event::<H>(Some(tc)));
            }
        }

        cmds
    }

    // If the qc has a commit_state_hash, commit the parent block and prune the
    // block tree
    // Update our highest seen qc (high_qc) if the incoming qc is of higher rank
    fn process_qc(&mut self, qc: &QuorumCertificate<T>) {
        if Rank(qc.info) <= Rank(self.high_qc.info) {
            return;
        }

        if qc.info.ledger_commit.commit_state_hash.is_some() {
            let blocks_to_commit = self
                .pending_block_tree
                .prune(&qc.info.vote.parent_id)
                .unwrap();
            if !blocks_to_commit.is_empty() {
                self.ledger.add_blocks(blocks_to_commit);
            }
        }
        self.high_qc = qc.clone();
    }

    // TODO consider changing return type to Option<T>
    #[must_use]
    fn process_certificate_qc(&mut self, qc: &QuorumCertificate<T>) -> Vec<ConsensusCommand<S, T>> {
        self.process_qc(qc);

        self.pacemaker
            .advance_round_qc(qc)
            .map(Into::into)
            .into_iter()
            .collect()
    }

    // TODO consider changing return type to Option<T>
    #[must_use]
    fn process_new_round_event<H: Hasher>(
        &mut self,
        last_round_tc: Option<TimeoutCertificate<S>>,
    ) -> Vec<ConsensusCommand<S, T>> {
        self.vote_state
            .start_new_round(self.pacemaker.get_current_round());

        let txns: TransactionList = self.mempool.get_transactions(10000);
        let b = Block::new::<H>(
            self.nodeid,
            self.pacemaker.get_current_round(),
            &txns,
            &self.high_qc,
        );

        let p = ProposalMessage {
            block: b,
            last_round_tc,
        };

        vec![ConsensusCommand::Broadcast {
            message: ConsensusMessage::Proposal(p),
        }]
    }
}

impl<S: Signature, T: SignatureCollection> From<PacemakerCommand<S, T>> for ConsensusCommand<S, T> {
    fn from(cmd: PacemakerCommand<S, T>) -> Self {
        match cmd {
            PacemakerCommand::Broadcast(message) => ConsensusCommand::Broadcast {
                message: ConsensusMessage::Timeout(message),
            },
            PacemakerCommand::Schedule {
                duration,
                on_timeout,
            } => ConsensusCommand::Schedule {
                duration,
                on_timeout,
            },
            PacemakerCommand::Unschedule => ConsensusCommand::Unschedule,
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use monad_consensus::pacemaker::PacemakerTimerExpire;
    use monad_consensus::signatures::aggregate_signature::AggregateSignatures;
    use monad_consensus::types::ledger::LedgerCommitInfo;
    use monad_consensus::types::message::{TimeoutMessage, VoteMessage};
    use monad_consensus::types::quorum_certificate::{genesis_vote_info, QuorumCertificate};
    use monad_consensus::types::signature::SignatureCollection;
    use monad_consensus::types::timeout::TimeoutInfo;
    use monad_consensus::types::voting::VoteInfo;
    use monad_consensus::validation::hashing::Sha256Hash;
    use monad_consensus::validation::signing::Verified;
    use monad_crypto::secp256k1::KeyPair;
    use monad_crypto::{NopSignature, Signature};
    use monad_testutil::proposal::ProposalGen;
    use monad_testutil::signing::{create_keys, get_genesis_config};
    use monad_types::{BlockId, Hash, Round};
    use monad_validator::{
        validator::Validator, validator_set::ValidatorSet, weighted_round_robin::WeightedRoundRobin,
    };

    use crate::{
        ConsensusCommand, ConsensusMessage, ConsensusState, InMemoryLedger, SimulationMempool,
    };

    fn setup<ST: Signature, SCT: SignatureCollection<SignatureType = ST>>() -> (
        Vec<KeyPair>,
        ValidatorSet<WeightedRoundRobin>,
        ConsensusState<ST, SCT, InMemoryLedger<SCT>, SimulationMempool>,
    ) {
        let keys = create_keys(4);
        let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) = get_genesis_config::<Sha256Hash, SCT>(keys.iter());

        let validator_list = pubkeys
            .into_iter()
            .map(|pubkey| Validator { pubkey, stake: 1 })
            .collect::<Vec<_>>();

        let val_set: ValidatorSet<WeightedRoundRobin> =
            ValidatorSet::new(validator_list.clone()).expect("initial validator set init failed");

        let genesis_qc = QuorumCertificate::genesis_qc::<Sha256Hash>(
            genesis_vote_info(genesis_block.get_id()),
            genesis_sigs,
        );

        let mut key = create_keys(1);
        let consensus_state =
            ConsensusState::<ST, SCT, InMemoryLedger<SCT>, SimulationMempool>::new(
                key[0].pubkey(),
                genesis_block,
                genesis_qc,
                Duration::from_secs(1),
                key.remove(0),
            );

        (keys, val_set, consensus_state)
    }

    // 2f+1 votes for a VoteInfo leads to a QC locking -- ie, high_qc is set to that QC.
    #[test]
    fn lock_qc_high() {
        let (keys, mut valset, mut state) =
            setup::<NopSignature, AggregateSignatures<NopSignature>>();

        assert_eq!(state.high_qc.info.vote.round, Round(0));

        let expected_qc_high_round = Round(5);

        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: expected_qc_high_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: expected_qc_high_round - Round(1),
        };
        let vm = VoteMessage {
            vote_info: vi,
            ledger_commit_info: LedgerCommitInfo::new::<Sha256Hash>(None, &vi),
        };

        let v1 = Verified::<_, VoteMessage>::new::<Sha256Hash>(vm, &keys[1]);
        let v2 = Verified::<_, VoteMessage>::new::<Sha256Hash>(vm, &keys[2]);
        let v3 = Verified::<_, VoteMessage>::new::<Sha256Hash>(vm, &keys[3]);

        state.handle_vote_message::<Sha256Hash, _>(
            *v1.author(),
            *v1.author_signature(),
            *v1,
            &mut valset,
        );
        state.handle_vote_message::<Sha256Hash, _>(
            *v2.author(),
            *v2.author_signature(),
            *v2,
            &mut valset,
        );

        // less than 2f+1, so expect not locked
        assert_eq!(state.high_qc.info.vote.round, Round(0));

        state.handle_vote_message::<Sha256Hash, _>(
            *v3.author(),
            *v3.author_signature(),
            *v3,
            &mut valset,
        );
        assert_eq!(state.high_qc.info.vote.round, expected_qc_high_round);
    }

    // When a node locally timesout on a round, it no longer produces votes in that round
    #[test]
    fn timeout_stops_voting() {
        let (keys, mut valset, mut state) =
            setup::<NopSignature, AggregateSignatures<NopSignature>>();
        let mut propgen = ProposalGen::new(state.high_qc.clone());
        let p1 = propgen.next_proposal(&keys, &mut valset);

        // local timeout for state in Round 1
        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        let _ =
            state
                .pacemaker
                .handle_event(&mut state.safety, &state.high_qc, PacemakerTimerExpire);

        // check no vote commands result from receiving the proposal for round 1

        let (author, _, verified_message) = p1.destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| match c {
            ConsensusCommand::Send {
                to: _,
                message: ConsensusMessage::Vote(_),
            } => true,
            _ => false,
        });

        assert!(result.is_none());
    }

    // TODO: remove the should_panic once we handle block requests for skipped blocks
    #[test]
    #[should_panic]
    fn enter_proposalmsg_round() {
        let (keys, mut valset, mut state) =
            setup::<NopSignature, AggregateSignatures<NopSignature>>();
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let p1 = propgen.next_proposal(&keys, &mut valset);
        let (author, _, verified_message) = p1.destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| match c {
            ConsensusCommand::Send {
                to: _,
                message: ConsensusMessage::Vote(_),
            } => true,
            _ => false,
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(1));
        assert!(result.is_some());

        let p2 = propgen.next_proposal(&keys, &mut valset);
        let (author, _, verified_message) = p2.destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| match c {
            ConsensusCommand::Send {
                to: _,
                message: ConsensusMessage::Vote(_),
            } => true,
            _ => false,
        });

        assert_eq!(state.pacemaker.get_current_round(), Round(2));
        assert!(result.is_some());

        for _ in 0..4 {
            propgen.next_proposal(&keys, &mut valset);
        }
        let p7 = propgen.next_proposal(&keys, &mut valset);
        let (author, _, verified_message) = p7.destructure();
        state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);

        assert_eq!(state.pacemaker.get_current_round(), Round(7));
    }

    #[test]
    fn old_qc_in_timeout_message() {
        let (keys, mut valset, mut state) =
            setup::<NopSignature, AggregateSignatures<NopSignature>>();
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let mut qc2 = state.high_qc.clone();

        for i in 1..5 {
            let p = propgen.next_proposal(&keys, &mut valset);
            let (author, _, verified_message) = p.clone().destructure();
            let cmds = state.handle_proposal_message::<Sha256Hash, _>(
                author,
                verified_message,
                &mut valset,
            );
            let result = cmds.iter().find(|&c| match c {
                ConsensusCommand::Send {
                    to: _,
                    message: ConsensusMessage::Vote(_),
                } => true,
                _ => false,
            });

            if i == 3 {
                qc2 = p.block.qc.clone();
                assert_eq!(qc2.info.vote.round, Round(2));
            }

            assert_eq!(state.pacemaker.get_current_round(), Round(i));
            assert!(result.is_some());
        }

        let byzantine_tm = TimeoutMessage {
            tminfo: TimeoutInfo {
                round: state.pacemaker.get_current_round(),
                high_qc: qc2,
            },
            last_round_tc: None,
        };
        let signed_byzantine_tm = Verified::new::<Sha256Hash>(byzantine_tm, &keys[1]);
        let (author, signature, tm) = signed_byzantine_tm.destructure();
        state.handle_timeout_message::<Sha256Hash, _>(author, signature, tm, &mut valset);
    }

    #[test]
    fn duplicate_proposals() {
        let (keys, mut valset, mut state) =
            setup::<NopSignature, AggregateSignatures<NopSignature>>();
        let mut propgen = ProposalGen::new(state.high_qc.clone());

        let p1 = propgen.next_proposal(&keys, &mut valset);
        let (author, _, verified_message) = p1.clone().destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        let result = cmds.iter().find(|&c| match c {
            ConsensusCommand::Send {
                to: _,
                message: ConsensusMessage::Vote(_),
            } => true,
            _ => false,
        });
        assert!(result.is_some());

        // send duplicate of p1, expect it to be ignored and no output commands
        let (author, _, verified_message) = p1.clone().destructure();
        let cmds =
            state.handle_proposal_message::<Sha256Hash, _>(author, verified_message, &mut valset);
        assert!(cmds.is_empty());
    }
}
