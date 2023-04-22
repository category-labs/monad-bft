use std::time::Duration;
use std::{collections::HashMap, fmt::Debug};

use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    pacemaker::{Pacemaker, PacemakerCommand, PacemakerTimerExpire},
    types::{
        block::{Block, TransactionList},
        consensus_message::{SignedConsensusMessage, VerifiedConsensusMessage},
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct VerifiedMonadMessage<ST, SCT>(VerifiedConsensusMessage<ST, SCT>)
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>;

#[derive(Debug, Clone)]
pub struct MonadMessage<ST, SCT>(SignedConsensusMessage<ST, SCT>)
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>;

#[cfg(feature = "proto")]
impl monad_executor::Serializable
    for VerifiedMonadMessage<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus::signatures::aggregate_signature::AggregateSignatures<
            monad_crypto::secp256k1::SecpSignature,
        >,
    >
{
    fn serialize(&self) -> Vec<u8> {
        monad_proto::types::message::serialize_verified_consensus_message(&self.0)
    }
}

#[cfg(feature = "proto")]
impl monad_executor::Deserializable
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
            monad_proto::types::message::deserialize_unverified_consensus_message(message)?,
        ))
    }
}

impl<ST, SCT> From<VerifiedMonadMessage<ST, SCT>> for MonadMessage<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    fn from(value: VerifiedMonadMessage<ST, SCT>) -> Self {
        MonadMessage(value.0.into())
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
        *match &self.0 {
            SignedConsensusMessage::Proposal(msg) => msg.author_signature(),
            SignedConsensusMessage::Vote(msg) => msg.author_signature(),
            SignedConsensusMessage::Timeout(msg) => msg.author_signature(),
        }
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

    fn init(config: Self::Config) -> (Self, Vec<Command<Self>>) {
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

    fn update(&mut self, event: Self::Event) -> Vec<Command<Self>> {
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
                    } => match unverified_message {
                        SignedConsensusMessage::Proposal(msg) => {
                            let proposal =
                                msg.verify::<HasherType>(self.validator_set.get_members(), &sender);

                            match proposal {
                                Ok(p) => self
                                    .consensus_state
                                    .handle_proposal_message::<Sha256Hash, _>(
                                        &p,
                                        &mut self.validator_set,
                                    ),
                                Err(e) => todo!(),
                            }
                        }
                        SignedConsensusMessage::Vote(msg) => {
                            let vote =
                                msg.verify::<HasherType>(self.validator_set.get_members(), &sender);

                            match vote {
                                Ok(p) => self
                                    .consensus_state
                                    .handle_vote_message::<HasherType, LeaderElectionType>(
                                        &p,
                                        &mut self.validator_set,
                                    ),
                                Err(_) => todo!(),
                            }
                        }
                        SignedConsensusMessage::Timeout(msg) => {
                            let timeout =
                                msg.verify::<HasherType>(self.validator_set.get_members(), &sender);

                            match timeout {
                                Ok(p) => self
                                    .consensus_state
                                    .handle_timeout_message::<HasherType, _>(
                                        p,
                                        &mut self.validator_set,
                                    ),
                                Err(e) => todo!("{:?}", e),
                            }
                        }
                    },
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

#[derive(Debug, Clone)]
pub enum ConsensusEvent<S, T> {
    Message {
        sender: PubKey,
        unverified_message: SignedConsensusMessage<S, T>,
    },
    Timeout(PacemakerTimerExpire),
}

#[derive(Debug, Clone)]
pub enum ConsensusMessage<S, T> {
    Proposal(ProposalMessage<S, T>),
    Vote(VoteMessage),
    Timeout(TimeoutMessage<S, T>),
}

impl<ST, SCT> ConsensusMessage<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    fn sign<H: Hasher>(self, keypair: &KeyPair) -> VerifiedConsensusMessage<ST, SCT> {
        match self {
            ConsensusMessage::Proposal(msg) => {
                VerifiedConsensusMessage::Proposal(Verified::new::<H>(msg, keypair))
            }
            ConsensusMessage::Vote(msg) => {
                // FIXME:
                // implemented Hashable for VoteMsg, which only hash the ledger commit info
                // it can be confusing as we are hashing only part of the message
                // in the signature refactoring, we might want a clean split between:
                //      integrity sig: sign over the entire serialized struct
                //      protocol sig: signatures outlined in the protocol
                // TimeoutMsg doesn't have a protocol sig
                VerifiedConsensusMessage::Vote(Verified::new::<H>(msg, keypair))
            }
            ConsensusMessage::Timeout(msg) => {
                VerifiedConsensusMessage::Timeout(Verified::new::<H>(msg, keypair))
            }
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
            pacemaker: Pacemaker::new(delta, Round(1), None, HashMap::new()),
            safety: Safety::default(),
            nodeid: NodeId(my_pubkey),

            keypair,
        }
    }

    fn handle_proposal_message<H: Hasher, V: LeaderElection>(
        &mut self,
        p: &Verified<S, ProposalMessage<S, T>>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, T>> {
        let mut cmds = Vec::new();

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

        if p.block.round != round || p.author() != &leader || p.block.author != leader {
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
        v: &Verified<T::SignatureType, VoteMessage>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, T>> {
        if v.vote_info.round < self.pacemaker.get_current_round() {
            return Default::default();
        }

        let qc = self.vote_state.process_vote::<V, H>(v, validators);

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
        p: Verified<S, TimeoutMessage<S, T>>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<S, T>> {
        let mut cmds = Vec::new();

        let process_certificate_cmds = self.process_certificate_qc(&p.tminfo.high_qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.last_round_tc.as_ref() {
            let advance_round_cmds = self
                .pacemaker
                .advance_round_tc(last_round_tc)
                .map(Into::into)
                .into_iter();
            cmds.extend(advance_round_cmds);
        }

        let (tc, remote_timeout_cmds) =
            self.pacemaker
                .process_remote_timeout(validators, &mut self.safety, &self.high_qc, p);
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
        if qc.info.ledger_commit.commit_state_hash.is_some() {
            let blocks_to_commit = self
                .pending_block_tree
                .prune(&qc.info.vote.parent_id)
                .unwrap();
            self.ledger.add_blocks(blocks_to_commit);
        }

        if Rank(qc.info) > Rank(self.high_qc.info) {
            self.high_qc = qc.clone();
        }
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
