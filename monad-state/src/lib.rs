use std::time::Duration;

use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    pacemaker::PacemakerTimerExpire,
    signatures::aggregate_signature::AggregateSignatures,
    types::{
        ledger::{InMemoryLedger, Ledger},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
        quorum_certificate::{QuorumCertificate, Rank},
        signature::SignatureCollection,
        timeout::TimeoutCertificate,
    },
    validation::{
        hashing::{Hasher, Sha256Hash},
        protocol::{verify_proposal, verify_timeout_message, verify_vote_message},
        signing::{Unverified, Verified},
    },
    vote_state::VoteState,
};
use monad_executor::{Command, Message, PeerId, RouterCommand, State, TimerCommand};
use monad_types::Round;
use monad_validator::leader_election::LeaderElection;
use monad_validator::{validator_set::ValidatorSet, weighted_round_robin::WeightedRoundRobin};

use message::{MessageActionPublish, MessageState};

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
        round: u64,
    },
    ConsensusEvent(ConsensusEvent),
}

type MonadMessage = ConsensusMessage;

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
        Self::Event::ConsensusEvent(ConsensusEvent::UnverifiedMessage(self))
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
                let consensus_commands: Vec<ConsensusCommand> = match consensus_event {
                    ConsensusEvent::Timeout(pacemaker_expire) => {
                        todo!()
                    }
                    ConsensusEvent::UnverifiedMessage(ConsensusMessage::Proposal(msg)) => {
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
                    ConsensusEvent::UnverifiedMessage(ConsensusMessage::Vote(msg)) => {
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
                    ConsensusEvent::UnverifiedMessage(ConsensusMessage::Timeout(msg)) => {
                        let timeout = verify_timeout_message::<Sha256Hash, SignatureType>(
                            self.validator_set.get_members(),
                            msg,
                        );

                        match timeout {
                            Ok(p) => self
                                .consensus_state
                                .handle_timeout_message(&p, &self.validator_set),
                            Err(_) => todo!(),
                        }
                    }
                };
                let mut cmds = Vec::new();
                for consensus_command in consensus_commands {
                    match consensus_command {
                        ConsensusCommand::Send { to, message } => {
                            cmds.push(self.message_state.send(to, message).into())
                        }
                        ConsensusCommand::Broadcast { message } => {
                            cmds.extend(
                                self.message_state
                                    .broadcast(message)
                                    .into_iter()
                                    .map(Into::into),
                            );
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
pub enum ConsensusEvent {
    UnverifiedMessage(ConsensusMessage),
    Timeout(PacemakerTimerExpire),
}

#[derive(Debug, Clone)]
pub enum ConsensusMessage {
    Proposal(Unverified<ProposalMessage<SignatureType>>),
    Vote(Unverified<VoteMessage>),
    Timeout(Unverified<TimeoutMessage<SignatureType>>),
}

impl ConsensusMessage {
    fn round(&self) -> u64 {
        todo!()
    }
}

#[derive(Debug)]
pub enum ConsensusCommand {
    Send {
        to: PeerId,
        message: ConsensusMessage,
    },
    Broadcast {
        message: ConsensusMessage,
    },
    Schedule {
        duration: Duration,
        on_timeout: PacemakerTimerExpire,
    },
    Unschedule,
    // TODO add command for updating validator_set/round
    // - to handle this command, we need to call message_state.set_round()
}

impl From<MessageActionPublish<ConsensusMessage>> for Command<MonadEvent, MonadMessage> {
    fn from(publish: MessageActionPublish<ConsensusMessage>) -> Self {
        let id = publish.message.id();
        let round = publish.message.round();
        Command::RouterCommand(RouterCommand::Publish {
            to: publish.to.clone(),
            message: publish.message,
            on_ack: MonadEvent::Ack {
                peer: publish.to,
                id,
                round,
            },
        })
    }
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
    ) -> Vec<ConsensusCommand> {
        todo!();
    }

    fn handle_vote_message<H: Hasher, V: LeaderElection>(
        &mut self,
        v: &Verified<VoteMessage>,
        validators: &mut ValidatorSet<V>,
    ) -> Vec<ConsensusCommand> {
        let mut retval = Vec::new();
        if self.round != v.0.obj.vote_info.round {
            return retval;
        }

        let qc = self.vote_state.process_vote::<V, H>(v, validators);

        let mut retval = Vec::new();
        match qc {
            Some(qc) => {
                retval.extend(self.process_certificate_qc(&qc));
                retval.extend(self.process_new_round_event(&None));
            }
            None => (),
        }
        retval
    }

    fn handle_timeout_message<V: LeaderElection>(
        &mut self,
        p: &Verified<TimeoutMessage<T>>,
        validators: &ValidatorSet<V>,
    ) -> Vec<ConsensusCommand> {
        todo!();
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
    fn process_certificate_qc(&mut self, qc: &QuorumCertificate<T>) -> Vec<ConsensusCommand> {
        let retval = Vec::new();
        self.process_qc(qc);

        // TODO: Pacemaker.advance_round(qc.info.vote.round)
        retval
    }

    #[must_use]
    fn process_new_round_event(
        &self,
        last_round_tc: &Option<TimeoutCertificate>,
    ) -> Vec<ConsensusCommand> {
        todo!();
    }
}
