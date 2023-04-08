use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    signatures::aggregate_signature::AggregateSignatures,
    types::{
        block::Block,
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
use monad_executor::{Command, Message, PeerId, RouterCommand, State};
use monad_types::Round;
use monad_validator::leader_election::LeaderElection;
use monad_validator::{validator_set::ValidatorSet, weighted_round_robin::WeightedRoundRobin};

use message::MessageState;

mod message;

type SerializedConsensusMessage = Vec<u8>;

type SignatureType = AggregateSignatures;
type LeaderElectionType = WeightedRoundRobin;

pub struct MonadState {
    message_state: MessageState<MonadMessage>,

    consensus_state: ConsensusState<SignatureType>,
    validator_set: ValidatorSet<LeaderElectionType>,
}

#[derive(Clone)]
pub enum MonadEvent {
    Ack {
        peer: PeerId,
        id: MonadMessage,
        round: u64,
    },
    Proposal {
        msg: Unverified<ProposalMessage<SignatureType>>,
    },
    Vote {
        msg: Unverified<VoteMessage>,
    },
    Timeout {
        msg: Unverified<TimeoutMessage<SignatureType>>,
    },
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub enum MonadMessage {
    Proposal(SerializedConsensusMessage),
    Vote(SerializedConsensusMessage),
    Timeout(SerializedConsensusMessage),
}

//TODO: use the protobuf functions
fn deserialize_proposal(
    _s: SerializedConsensusMessage,
) -> Result<Unverified<ProposalMessage<SignatureType>>, ()> {
    todo!();
}

fn deserialize_vote(_s: SerializedConsensusMessage) -> Result<Unverified<VoteMessage>, ()> {
    todo!();
}

fn deserialize_timeout(
    _s: SerializedConsensusMessage,
) -> Result<Unverified<TimeoutMessage<SignatureType>>, ()> {
    todo!();
}

impl Message for MonadMessage {
    type Event = MonadEvent;
    type ReadError = ();

    type Id = Self;

    fn deserialize(from: PeerId, message: &[u8]) -> Result<Self, Self::ReadError> {
        todo!()
    }

    fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    fn id(&self) -> Self::Id {
        self.clone()
    }

    fn event(self, _from: PeerId) -> Self::Event {
        match self {
            Self::Proposal(a) => match deserialize_proposal(a) {
                Ok(m) => Self::Event::Proposal { msg: m },
                Err(()) => todo!(),
            },
            Self::Vote(a) => match deserialize_vote(a) {
                Ok(m) => Self::Event::Vote { msg: m },
                Err(()) => todo!(),
            },
            Self::Timeout(a) => match deserialize_timeout(a) {
                Ok(m) => Self::Event::Timeout { msg: m },
                Err(()) => todo!(),
            },
        }
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

            MonadEvent::Proposal { msg } => {
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

            MonadEvent::Vote { msg } => {
                let vote = verify_vote_message::<Sha256Hash>(self.validator_set.get_members(), msg);

                match vote {
                    Ok(p) => self
                        .consensus_state
                        .handle_vote_message::<Sha256Hash, LeaderElectionType>(
                            &p,
                            &self.validator_set,
                        ),
                    Err(_) => todo!(),
                }
            }

            MonadEvent::Timeout { msg } => {
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
        }
    }
}

struct ConsensusState<T>
where
    T: SignatureCollection,
{
    pending_block_tree: BlockTree<T>,
    vote_state: VoteState<T>,
    high_qc: QuorumCertificate<T>,

    //TODO use ledger interface from monad-consensus
    ledger: Vec<Block<T>>,

    // TODO this might be in synchronizer only
    round: Round,
}

impl<T> ConsensusState<T>
where
    T: SignatureCollection,
{
    fn handle_proposal_message<V: LeaderElection>(
        &mut self,
        p: &Verified<ProposalMessage<T>>,
        validators: &ValidatorSet<V>,
    ) -> Vec<Command<MonadEvent, MonadMessage>> {
        todo!();
    }

    fn handle_vote_message<H: Hasher, V: LeaderElection>(
        &mut self,
        v: &Verified<VoteMessage>,
        validators: &ValidatorSet<V>,
    ) -> Vec<Command<MonadEvent, MonadMessage>> {
        if self.round != v.0.obj.vote_info.round {
            todo!();
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
    ) -> Vec<Command<MonadEvent, MonadMessage>> {
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
                    Ok(blocks) => self.ledger.extend(blocks),
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
    fn process_certificate_qc(
        &self,
        qc: &QuorumCertificate<T>,
    ) -> Vec<Command<MonadEvent, MonadMessage>> {
        todo!();
    }

    #[must_use]
    fn process_new_round_event(
        &self,
        last_round_tc: &Option<TimeoutCertificate>,
    ) -> Vec<Command<MonadEvent, MonadMessage>> {
        todo!();
    }
}
