use message::MessageState;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::types::message::{TimeoutMessage, VoteMessage};
use monad_consensus::types::quorum_certificate::QuorumCertificate;
use monad_consensus::types::signature::SignatureCollection;
use monad_consensus::validation::hashing::Sha256Hash;
use monad_consensus::validation::protocol::{
    verify_proposal, verify_timeout_message, verify_vote_message,
};
use monad_consensus::validation::signing::{Unverified, Verified};
use monad_consensus::{
    signatures::aggregate_signature::AggregateSignatures, types::message::ProposalMessage,
};
use monad_executor::{Command, Message, PeerId, RouterCommand, State};
use monad_validator::leader_election::LeaderElection;
use monad_validator::{validator_set::ValidatorSet, weighted_round_robin::WeightedRoundRobin};

use std::marker::PhantomData;

mod message;

type SerializedConsensusMessage = Vec<u8>;

type SignatureType = AggregateSignatures;
type LeaderElectionType = WeightedRoundRobin;

pub struct MonadState {
    message_state: MessageState<MonadMessage>,

    consensus_state: ConsensusState<SignatureType, LeaderElectionType>,
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
                let proposal =
                    verify_proposal::<Sha256Hash, _>(self.validator_set.get_members(), msg);

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
                        .handle_vote_message(&p, &self.validator_set),
                    Err(_) => todo!(),
                }
            }

            MonadEvent::Timeout { msg } => {
                let timeout =
                    verify_timeout_message::<Sha256Hash, _>(self.validator_set.get_members(), msg);

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

struct ConsensusState<T, V>
where
    T: SignatureCollection,
    V: LeaderElection,
{
    pending_block_tree: BlockTree<T>,
    high_qc: QuorumCertificate<T>,

    _phantom: PhantomData<V>,
}

impl<T, V> ConsensusState<T, V>
where
    T: SignatureCollection,
    V: LeaderElection,
{
    fn handle_proposal_message(
        &mut self,
        p: &Verified<ProposalMessage<T>>,
        validators: &ValidatorSet<V>,
    ) -> Vec<Command<MonadEvent, MonadMessage>> {
        todo!();
    }

    fn handle_vote_message(
        &mut self,
        v: &Verified<VoteMessage>,
        validators: &ValidatorSet<V>,
    ) -> Vec<Command<MonadEvent, MonadMessage>> {
        todo!()
    }

    fn handle_timeout_message(
        &mut self,
        p: &Verified<TimeoutMessage<T>>,
        validators: &ValidatorSet<V>,
    ) -> Vec<Command<MonadEvent, MonadMessage>> {
        todo!();
    }
}
