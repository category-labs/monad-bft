use std::time::Duration;

use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    pacemaker::{Pacemaker, PacemakerCommand, PacemakerTimerExpire},
    signatures::aggregate_signature::AggregateSignatures,
    types::{
        ledger::{InMemoryLedger, Ledger},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
        quorum_certificate::{QuorumCertificate, Rank},
        signature::{ConsensusSignature, SignatureCollection},
        timeout::TimeoutCertificate,
    },
    validation::{
        hashing::{Hasher, Sha256Hash},
        protocol::{verify_proposal, verify_timeout_message, verify_vote_message},
        safety::Safety,
        signing::{Signable, Signed, Unverified, Verified},
    },
    vote_state::VoteState,
};
use monad_executor::{Command, Message, PeerId, RouterCommand, State, TimerCommand};
use monad_types::{NodeId, Round};
use monad_validator::leader_election::LeaderElection;
use monad_validator::{validator_set::ValidatorSet, weighted_round_robin::WeightedRoundRobin};

use message::MessageState;

mod message;

type SignatureType = AggregateSignatures;
type LeaderElectionType = WeightedRoundRobin;

pub struct MonadState {
    message_state: MessageState<MonadMessage>,

    consensus_state: ConsensusState<SignatureType, InMemoryLedger<SignatureType>>,
    validator_set: ValidatorSet<LeaderElectionType>,
}

#[derive(Clone)]
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

    type Id = Vec<u8>;

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
        self.serialize()
    }

    fn event(self, _from: PeerId) -> Self::Event {
        Self::Event::ConsensusEvent(ConsensusEvent::UnverifiedMessage(self.0))
    }
}

impl State for MonadState {
    type Event = MonadEvent;
    type Message = MonadMessage;

    fn init() -> (Self, Vec<Command<Self::Event, Self::Message>>) {
        todo!()
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
                        ConsensusEvent::Timeout(pacemaker_expire) => {
                            todo!()
                        }
                        ConsensusEvent::UnverifiedMessage(msg) => {
                            match UnverifiedConsensusMessage::from(msg) {
                                UnverifiedConsensusMessage::Proposal(msg) => {
                                    let proposal = verify_proposal::<Sha256Hash, SignatureType>(
                                        self.validator_set.get_members(),
                                        msg,
                                    );

                                    match proposal {
                                        Ok(p) => self
                                            .consensus_state
                                            .handle_proposal_message(&p, &self.validator_set),
                                        Err(_) => todo!(),
                                    }
                                }
                                UnverifiedConsensusMessage::Vote(msg) => {
                                    let vote = verify_vote_message::<Sha256Hash>(
                                        self.validator_set.get_members(),
                                        msg,
                                    );

                                    match vote {
                                        Ok(p) => self
                                            .consensus_state
                                            .handle_vote_message::<Sha256Hash, LeaderElectionType>(
                                                &p,
                                                &mut self.validator_set,
                                            ),
                                        Err(_) => todo!(),
                                    }
                                }
                                UnverifiedConsensusMessage::Timeout(msg) => {
                                    let timeout = verify_timeout_message::<Sha256Hash, SignatureType>(
                                        self.validator_set.get_members(),
                                        msg,
                                    );

                                    match timeout {
                                        Ok(p) => self
                                            .consensus_state
                                            .handle_timeout_message(p, &self.validator_set),
                                        Err(_) => todo!(),
                                    }
                                }
                            }
                        }
                    };
                let mut cmds = Vec::new();
                for consensus_command in consensus_commands {
                    match consensus_command {
                        ConsensusCommand::Send { to, message } => {
                            let message = MonadMessage(
                                message.signed_object(todo!("author"), todo!("signature")),
                            );
                            let publish_action = self.message_state.send(to, message);
                            let id = publish_action.message.id();
                            cmds.push(Command::RouterCommand(RouterCommand::Publish {
                                to: publish_action.to.clone(),
                                message: publish_action.message,
                                on_ack: MonadEvent::Ack {
                                    peer: publish_action.to,
                                    id,
                                    round: self.message_state.round(),
                                },
                            }))
                        }
                        ConsensusCommand::Broadcast { message } => {
                            let message = MonadMessage(
                                message.signed_object(todo!("author"), todo!("signature")),
                            );
                            cmds.extend(self.message_state.broadcast(message).into_iter().map(
                                |publish_action| {
                                    let id = publish_action.message.id();
                                    Command::RouterCommand(RouterCommand::Publish {
                                        to: publish_action.to.clone(),
                                        message: publish_action.message,
                                        on_ack: MonadEvent::Ack {
                                            peer: publish_action.to,
                                            id,
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
    UnverifiedMessage(Unverified<ConsensusMessage<T>>),
    Timeout(PacemakerTimerExpire),
}

#[derive(Debug, Clone)]
pub enum ConsensusMessage<T: SignatureCollection> {
    Proposal(ProposalMessage<T>),
    Vote(VoteMessage),
    Timeout(TimeoutMessage<T>),
}

impl<T: SignatureCollection> Signable for ConsensusMessage<T> {
    type Output = Unverified<ConsensusMessage<T>>;
    fn signed_object(self, author: NodeId, signature: ConsensusSignature) -> Self::Output {
        Unverified(Signed {
            obj: self,
            author,
            author_signature: signature,
        })
    }
}

impl<T: SignatureCollection> ConsensusMessage<T> {
    fn round(&self) -> Round {
        todo!()
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
        match unverified.0.obj {
            ConsensusMessage::Proposal(proposal) => {
                UnverifiedConsensusMessage::Proposal(Unverified(Signed {
                    obj: proposal,
                    author: unverified.0.author,
                    author_signature: unverified.0.author_signature,
                }))
            }
            ConsensusMessage::Vote(vote) => UnverifiedConsensusMessage::Vote(Unverified(Signed {
                obj: vote,
                author: unverified.0.author,
                author_signature: unverified.0.author_signature,
            })),
            ConsensusMessage::Timeout(timeout) => {
                UnverifiedConsensusMessage::Timeout(Unverified(Signed {
                    obj: timeout,
                    author: unverified.0.author,
                    author_signature: unverified.0.author_signature,
                }))
            }
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

struct ConsensusState<T, L>
where
    T: SignatureCollection,
    L: Ledger<Signatures = T>,
{
    pending_block_tree: BlockTree<T>,
    vote_state: VoteState<T>,
    high_qc: QuorumCertificate<T>,

    ledger: L,

    pacemaker: Pacemaker<T>,
    safety: Safety,

    // TODO this might be in synchronizer only
    round: Round,
}

impl<T, L> ConsensusState<T, L>
where
    T: SignatureCollection,
    L: Ledger<Signatures = T>,
{
    fn handle_proposal_message<V: LeaderElection>(
        &mut self,
        p: &Verified<ProposalMessage<T>>,
        validators: &ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<T>> {
        todo!();
    }

    fn handle_vote_message<H: Hasher, V: LeaderElection>(
        &mut self,
        v: &Verified<VoteMessage>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<T>> {
        let mut cmds = Vec::new();
        if self.round != v.0.obj.vote_info.round {
            return cmds;
        }

        let qc = self.vote_state.process_vote::<V, H>(v, validators);

        let mut cmds = Vec::new();
        match qc {
            Some(qc) => {
                cmds.extend(self.process_certificate_qc(&qc));
                cmds.extend(self.process_new_round_event(&None));
            }
            None => (),
        }
        cmds
    }

    fn handle_timeout_message<V: LeaderElection>(
        &mut self,
        p: Verified<TimeoutMessage<T>>,
        validators: &ValidatorSet<V>,
    ) -> Vec<ConsensusCommand<T>> {
        let mut cmds = Vec::new();

        let process_certificate_cmds = self.process_certificate_qc(&p.0.obj.tminfo.high_qc);
        cmds.extend(process_certificate_cmds);

        if let Some(last_round_tc) = p.0.obj.last_round_tc.as_ref() {
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

            self.process_new_round_event(&Some(tc));
        }

        cmds
    }

    // If the qc has a commit_state_hash, commit the parent block and prune the
    // block tree
    // Update our highest seen qc (high_qc) if the incoming qc is of higher rank
    fn process_qc(&mut self, qc: &QuorumCertificate<T>) {
        match qc.info.ledger_commit.commit_state_hash {
            Some(_) => {
                let blocks_to_commit = self.pending_block_tree.prune(&qc.info.vote.parent_id);
                match blocks_to_commit {
                    Ok(blocks) => self.ledger.add_blocks(blocks),
                    Err(e) => panic!("{}", e),
                }
            }
            None => (),
        }

        if Rank(qc.info) > Rank(self.high_qc.info) {
            self.high_qc = qc.clone();
        }
    }

    #[must_use]
    fn process_certificate_qc(&mut self, qc: &QuorumCertificate<T>) -> Vec<ConsensusCommand<T>> {
        let cmds = Vec::new();
        self.process_qc(qc);

        // TODO: Pacemaker.advance_round(qc.info.vote.round)
        cmds
    }

    #[must_use]
    fn process_new_round_event(
        &self,
        last_round_tc: &Option<TimeoutCertificate>,
    ) -> Vec<ConsensusCommand<T>> {
        todo!();
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
