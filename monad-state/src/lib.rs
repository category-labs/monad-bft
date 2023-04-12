use std::time::Duration;
use std::{collections::HashMap, fmt::Debug};

use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    pacemaker::{Pacemaker, PacemakerCommand, PacemakerTimerExpire},
    signatures::aggregate_signature::AggregateSignatures,
    types::{
        block::{Block, TransactionList},
        ledger::{InMemoryLedger, Ledger},
        mempool::{Mempool, SimulationMempool},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
        quorum_certificate::{QuorumCertificate, Rank},
        signature::{ConsensusSignature, SignatureCollection},
        timeout::TimeoutCertificate,
    },
    validation::{
        hashing::{Hashable, Hasher, Sha256Hash},
        protocol::{verify_proposal, verify_timeout_message, verify_vote_message},
        safety::Safety,
        signing::{Unverified, Verified},
    },
    vote_state::VoteState,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_executor::{Command, Message, PeerId, RouterCommand, State, TimerCommand};
use monad_types::{NodeId, Round};
use monad_validator::{
    leader_election::LeaderElection, validator::Validator, validator_set::ValidatorSet,
    weighted_round_robin::WeightedRoundRobin,
};

use message::MessageState;

mod message;

type SignatureType = AggregateSignatures;
type LeaderElectionType = WeightedRoundRobin;
type HasherType = Sha256Hash;

pub struct MonadState {
    message_state: MessageState<MonadMessage>,

    consensus_state:
        ConsensusState<SignatureType, InMemoryLedger<SignatureType>, SimulationMempool>,
    validator_set: ValidatorSet<LeaderElectionType>,
}

impl MonadState {
    pub fn pubkey(&self) -> PubKey {
        self.consensus_state.nodeid.0
    }

    pub fn ledger(&self) -> Vec<Block<SignatureType>> {
        self.consensus_state.ledger.get_blocks()
    }
}

#[derive(Debug, Clone)]
pub enum MonadEvent {
    Ack {
        peer: PeerId,
        id: <MonadMessage as Message>::Id,
        round: Round,
    },
    ConsensusEvent(ConsensusEvent<SignatureType>),
}

#[derive(Debug, Clone)]
pub struct MonadMessage(Unverified<ConsensusMessage<SignatureType>>);

impl Message for MonadMessage {
    type Event = MonadEvent;
    type ReadError = ();

    type Id = ConsensusSignature;

    fn deserialize(from: PeerId, message: &[u8]) -> Result<Self, Self::ReadError> {
        // MUST assert that output is valid and came from the `from` PeerId
        // `from` must somehow be guaranteed to be staked at this point so that subsequent
        // malformed stuff (that gets added to event log) can be slashed? TODO
        todo!("proto deserialize")
    }

    fn serialize(&self) -> Vec<u8> {
        todo!("proto serialize")
    }

    fn id(&self) -> Self::Id {
        self.0.author_signature
    }

    fn event(self, from: PeerId) -> Self::Event {
        Self::Event::ConsensusEvent(ConsensusEvent::Message {
            sender: from.0,
            unverified_message: self.0,
        })
    }
}

pub struct MonadConfig {
    pub validators: Vec<PubKey>,
    pub key: KeyPair,
}

impl State for MonadState {
    type Config = MonadConfig;
    type Event = MonadEvent;
    type Message = MonadMessage;

    fn init(config: Self::Config) -> (Self, Vec<Command<Self::Event, Self::Message>>) {
        // create my keys and validator structs
        let validator_list = config
            .validators
            .into_iter()
            .map(|pubkey| Validator { pubkey, stake: 1 })
            .collect::<Vec<_>>();

        // create the genesis block
        // FIXME init from genesis config, don't use random key
        let genesis_txn = TransactionList::default();

        let genesis_qc = QuorumCertificate::default();
        let genesis_block = Block::<SignatureType>::new::<HasherType>(
            NodeId(KeyPair::from_slice(&[0xBE as u8; 32]).unwrap().pubkey()),
            Round(0),
            &genesis_txn,
            &genesis_qc,
        );

        // create the initial validator set
        let val_set =
            ValidatorSet::new(validator_list.clone()).expect("initial validator set init failed");

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
                genesis_block,
                genesis_qc,
                config.key,
            ),
        };

        let init_cmds = monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
            PacemakerTimerExpire,
        )));

        (monad_state, init_cmds)
    }

    fn update(&mut self, event: Self::Event) -> Vec<Command<Self::Event, Self::Message>> {
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
                let consensus_commands: Vec<ConsensusCommand<AggregateSignatures>> =
                    match consensus_event {
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
                        } => match UnverifiedConsensusMessage::from(unverified_message) {
                            UnverifiedConsensusMessage::Proposal(msg) => {
                                let proposal = verify_proposal::<HasherType, SignatureType>(
                                    self.validator_set.get_members(),
                                    &sender,
                                    msg,
                                );

                                match proposal {
                                    Ok(p) => self
                                        .consensus_state
                                        .handle_proposal_message::<Sha256Hash, _>(
                                            &p.author,
                                            &p.obj,
                                            &mut self.validator_set,
                                        ),
                                    Err(e) => todo!(),
                                }
                            }
                            UnverifiedConsensusMessage::Vote(msg) => {
                                let vote = verify_vote_message::<HasherType>(
                                    self.validator_set.get_members(),
                                    &sender,
                                    msg,
                                );

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
                            UnverifiedConsensusMessage::Timeout(msg) => {
                                let timeout = verify_timeout_message::<HasherType, SignatureType>(
                                    self.validator_set.get_members(),
                                    &sender,
                                    msg,
                                );

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
                            let signature = ConsensusSignature(
                                self.consensus_state
                                    .keypair
                                    .sign(&HasherType::hash_object(message.clone())),
                            );
                            let message = MonadMessage(Unverified {
                                obj: message,
                                author_signature: signature,
                            });
                            let publish_action = self.message_state.send(to, message);
                            let id = publish_action.message.id();
                            cmds.push(Command::RouterCommand(RouterCommand::Publish {
                                to: publish_action.to.clone(),
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
                            let signature = ConsensusSignature(
                                self.consensus_state
                                    .keypair
                                    .sign(&HasherType::hash_object(message.clone())),
                            );
                            let message = MonadMessage(Unverified {
                                obj: message,
                                author_signature: signature,
                            });
                            cmds.extend(self.message_state.broadcast(message).into_iter().map(
                                |publish_action| {
                                    let id = publish_action.message.id();
                                    Command::RouterCommand(RouterCommand::Publish {
                                        to: publish_action.to.clone(),
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
pub enum ConsensusEvent<T: SignatureCollection> {
    Message {
        sender: PubKey,
        unverified_message: Unverified<ConsensusMessage<T>>,
    },
    Timeout(PacemakerTimerExpire),
}

#[derive(Debug, Clone)]
pub enum ConsensusMessage<T: SignatureCollection> {
    Proposal(ProposalMessage<T>),
    Vote(VoteMessage),
    Timeout(TimeoutMessage<T>),
}

impl<T: SignatureCollection> Hashable for ConsensusMessage<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ConsensusMessage::Proposal(msg) => msg.hash(state),
            ConsensusMessage::Vote(msg) => (&msg.ledger_commit_info).hash(state),
            ConsensusMessage::Timeout(msg) => msg.hash(state),
        }
    }
}

#[derive(Debug, Clone)]
pub enum UnverifiedConsensusMessage<T: SignatureCollection> {
    Proposal(Unverified<ProposalMessage<T>>),
    Vote(Unverified<VoteMessage>),
    Timeout(Unverified<TimeoutMessage<T>>),
}

impl<T: SignatureCollection> From<Unverified<ConsensusMessage<T>>>
    for UnverifiedConsensusMessage<T>
{
    fn from(unverified: Unverified<ConsensusMessage<T>>) -> Self {
        match unverified.obj {
            ConsensusMessage::Proposal(proposal) => {
                UnverifiedConsensusMessage::Proposal(Unverified {
                    obj: proposal,
                    author_signature: unverified.author_signature,
                })
            }
            ConsensusMessage::Vote(vote) => UnverifiedConsensusMessage::Vote(Unverified {
                obj: vote,
                author_signature: unverified.author_signature,
            }),
            ConsensusMessage::Timeout(timeout) => UnverifiedConsensusMessage::Timeout(Unverified {
                obj: timeout,
                author_signature: unverified.author_signature,
            }),
        }
    }
}

#[derive(Debug)]
pub enum ConsensusCommand<T: SignatureCollection> {
    Send {
        to: PeerId,
        message: ConsensusMessage<T>,
    },
    Broadcast {
        message: ConsensusMessage<T>,
    },
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    Unschedule,
    // TODO add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
}

struct ConsensusState<T, L, M>
where
    T: SignatureCollection,
    L: Ledger<Signatures = T>,
    M: Mempool,
{
    pending_block_tree: BlockTree<T>,
    vote_state: VoteState<T>,
    high_qc: QuorumCertificate<T>,

    ledger: L,
    mempool: M,

    pacemaker: Pacemaker<T>,
    safety: Safety,

    nodeid: NodeId,

    // TODO deprecate
    keypair: KeyPair,
}

impl<T, L, M> ConsensusState<T, L, M>
where
    T: SignatureCollection + Debug,
    L: Ledger<Signatures = T>,
    M: Mempool,
{
    pub fn new(
        my_pubkey: PubKey,
        genesis_block: Block<T>,
        genesis_qc: QuorumCertificate<T>,

        // TODO deprecate
        keypair: KeyPair,
    ) -> Self {
        ConsensusState {
            pending_block_tree: BlockTree::new(genesis_block),
            vote_state: VoteState::new(),
            high_qc: genesis_qc,
            ledger: L::new(),
            mempool: M::new(),
            pacemaker: Pacemaker::new(Duration::new(1, 0), Round(1), None, HashMap::new()),
            safety: Safety::new(),
            nodeid: NodeId(my_pubkey),

            keypair,
        }
    }

    fn handle_proposal_message<H: Hasher, V: LeaderElection>(
        &mut self,
        author: &NodeId,
        p: &ProposalMessage<T>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<T>> {
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

        if p.block.round != round || author != &leader || p.block.author != leader {
            return cmds;
        }

        self.pending_block_tree
            .add(p.block.clone())
            .expect("Failed to add block to blocktree");

        let vote_msg = self.safety.make_vote::<T, H>(&p.block, &p.last_round_tc);

        match vote_msg {
            Some(v) => {
                let next_leader = validators.get_leader(round + Round(1));
                let send_cmd = ConsensusCommand::Send {
                    to: PeerId(next_leader.0),
                    message: ConsensusMessage::Vote(v),
                };
                cmds.push(send_cmd);
            }
            None => (),
        }

        cmds
    }

    fn handle_vote_message<H: Hasher, V: LeaderElection>(
        &mut self,
        v: &Verified<VoteMessage>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<T>> {
        if self.pacemaker.get_current_round() != v.obj.vote_info.round {
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
        p: Verified<TimeoutMessage<T>>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<T>> {
        let mut cmds = Vec::new();

        let process_certificate_cmds = self.process_certificate_qc(&p.obj.tminfo.high_qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.obj.last_round_tc.as_ref() {
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
    fn process_certificate_qc(&mut self, qc: &QuorumCertificate<T>) -> Vec<ConsensusCommand<T>> {
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
        last_round_tc: Option<TimeoutCertificate>,
    ) -> Vec<ConsensusCommand<T>> {
        self.vote_state.start_new_round();

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

impl<T: SignatureCollection> From<PacemakerCommand<T>> for ConsensusCommand<T> {
    fn from(cmd: PacemakerCommand<T>) -> Self {
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
