use std::{fmt::Debug, marker::PhantomData};

use bytes::Bytes;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncResponseMessage, RequestBlockSyncMessage},
    },
    validation::signing::{Unvalidated, Unverified, Validated, Verified},
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand},
    ConsensusConfig, ConsensusProcess,
};
use monad_consensus_types::{
    block::Block,
    signature_collection::{
        SignatureCollection, SignatureCollectionKeyPairType, SignatureCollectionPubKeyType,
    },
    txpool::TxPool,
    validation,
    validator_data::ValidatorData,
    voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_executor::State;
use monad_executor_glue::{
    BlockSyncEvent, CheckpointCommand, Command, ConsensusEvent, ExecutionLedgerCommand,
    LedgerCommand, Message, MonadEvent, RouterCommand, StateRootHashCommand, TimerCommand,
    ValidatorEvent,
};
use monad_tracing_counter::inc_count;
use monad_types::{Epoch, NodeId, Round, RouterTarget, SeqNum, Stake, TimeoutVariant};
use monad_validator::{
    epoch_manager::EpochManager, leader_election::LeaderElection, validator_set::ValidatorSetType,
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::blocksync::BlockSyncResponder;

pub mod blocksync;
pub mod convert;

pub struct MonadState<CT, ST, SCT, VT, LT, TT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    /// Core consensus algorithm state machine
    consensus: CT,
    /// Handle responses to block sync requests from other nodes
    block_sync_respond: BlockSyncResponder,

    /// Algorithm for choosing leaders for the consensus algorithm
    leader_election: LT,
    /// Track the information for epochs
    epoch_manager: EpochManager,
    /// Maps the epoch number to validator stakes and certificate pubkeys
    val_epoch_map: ValidatorsEpochMapping<VT, SCT>,
    /// Transaction pool is the source of Proposals
    txpool: TT,

    _pd: PhantomData<ST>,
}

impl<CT, ST, SCT, VT, LT, TT> MonadState<CT, ST, SCT, VT, LT, TT>
where
    CT: ConsensusProcess<ST, SCT>,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    TT: TxPool,
{
    pub fn consensus(&self) -> &CT {
        &self.consensus
    }

    pub fn epoch_manager(&self) -> &EpochManager {
        &self.epoch_manager
    }

    pub fn pubkey(&self) -> SCT::NodeIdPubKey {
        self.consensus.get_pubkey()
    }

    pub fn blocktree(&self) -> &BlockTree<SCT> {
        self.consensus.blocktree()
    }

    fn update_next_val_set(&mut self, val_data: ValidatorData<SCT>, epoch: Epoch) {
        self.val_epoch_map.insert(
            epoch,
            VT::new(val_data.get_stakes())
                .expect("ValidatorData should not have duplicates or invalid entries"),
            ValidatorMapping::new(val_data.get_cert_pubkeys()),
        );
    }

    fn handle_validation_error(e: validation::Error) {
        match e {
            validation::Error::InvalidAuthor => {
                inc_count!(invalid_author)
            }
            validation::Error::NotWellFormed => {
                inc_count!(not_well_formed_sig)
            }
            validation::Error::InvalidSignature => {
                inc_count!(invalid_signature)
            }
            validation::Error::AuthorNotSender => {
                inc_count!(author_not_sender)
            }
            validation::Error::InvalidTcRound => {
                inc_count!(invalid_tc_round)
            }
            validation::Error::InsufficientStake => {
                inc_count!(insufficient_stake)
            }
            validation::Error::InvalidSeqNum => {
                inc_count!(invalid_seq_num)
            }
            validation::Error::ValidatorDataUnavailable => {
                inc_count!(val_data_unavailable)
            }
            validation::Error::InvalidVoteMessage => {
                inc_count!(invalid_vote_message)
            }
        };
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    Consensus(Verified<ST, Validated<ConsensusMessage<SCT>>>),
    BlockSyncRequest(Validated<RequestBlockSyncMessage>),
    BlockSyncResponse(Validated<BlockSyncResponseMessage<SCT>>),
}

impl<ST, SCT> From<Verified<ST, Validated<ConsensusMessage<SCT>>>> for VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(value: Verified<ST, Validated<ConsensusMessage<SCT>>>) -> Self {
        Self::Consensus(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    /// Consensus protocol message
    Consensus(Unverified<ST, Unvalidated<ConsensusMessage<SCT>>>),

    /// Request a missing block given BlockId
    BlockSyncRequest(Unvalidated<RequestBlockSyncMessage>),

    /// Block sync response
    BlockSyncResponse(Unvalidated<BlockSyncResponseMessage<SCT>>),
}

impl<ST, SCT> monad_types::Serializable<Bytes> for VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn serialize(&self) -> Bytes {
        crate::convert::interface::serialize_verified_monad_message(self)
    }
}

impl<ST, SCT> monad_types::Serializable<MonadMessage<ST, SCT>> for VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn serialize(&self) -> MonadMessage<ST, SCT> {
        match self.clone() {
            VerifiedMonadMessage::Consensus(msg) => MonadMessage::Consensus(msg.into()),
            VerifiedMonadMessage::BlockSyncRequest(msg) => {
                MonadMessage::BlockSyncRequest(msg.into())
            }
            VerifiedMonadMessage::BlockSyncResponse(msg) => {
                MonadMessage::BlockSyncResponse(msg.into())
            }
        }
    }
}

impl<ST, SCT> monad_types::Deserializable<Bytes> for MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_monad_message(message.clone())
    }
}

impl<ST, SCT> From<VerifiedMonadMessage<ST, SCT>> for MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(value: VerifiedMonadMessage<ST, SCT>) -> Self {
        match value {
            VerifiedMonadMessage::Consensus(msg) => MonadMessage::Consensus(msg.into()),
            VerifiedMonadMessage::BlockSyncRequest(msg) => {
                MonadMessage::BlockSyncRequest(msg.into())
            }
            VerifiedMonadMessage::BlockSyncResponse(msg) => {
                MonadMessage::BlockSyncResponse(msg.into())
            }
        }
    }
}

impl<ST, SCT> Message for MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type Event = MonadEvent<ST, SCT>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        // MUST assert that output is valid and came from the `from` NodeId
        // `from` must somehow be guaranteed to be staked at this point so that subsequent
        // malformed stuff (that gets added to event log) can be slashed? TODO
        match self {
            MonadMessage::Consensus(msg) => MonadEvent::ConsensusEvent(ConsensusEvent::Message {
                sender: from.pubkey(),
                unverified_message: msg,
            }),

            MonadMessage::BlockSyncRequest(msg) => {
                MonadEvent::BlockSyncEvent(BlockSyncEvent::BlockSyncRequest {
                    sender: from.pubkey(),
                    unvalidated_request: msg,
                })
            }
            MonadMessage::BlockSyncResponse(msg) => {
                MonadEvent::ConsensusEvent(ConsensusEvent::BlockSyncResponse {
                    sender: from.pubkey(),
                    unvalidated_response: msg,
                })
            }
        }
    }
}

pub struct MonadConfig<ST, SCT, TV>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub transaction_validator: TV,
    pub validators: Vec<(SCT::NodeIdPubKey, Stake, SignatureCollectionPubKeyType<SCT>)>,
    pub key: ST::KeyPairType,
    pub certkey: SignatureCollectionKeyPairType<SCT>,
    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,
    pub beneficiary: EthAddress,

    pub consensus_config: ConsensusConfig,
}

impl<CT, ST, SCT, VT, LT, TT> State for MonadState<CT, ST, SCT, VT, LT, TT>
where
    CT: ConsensusProcess<ST, SCT>,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    LT: LeaderElection,
    TT: TxPool,
{
    type Config = MonadConfig<ST, SCT, CT::BlockValidatorType>;
    type Event = MonadEvent<ST, SCT>;
    type Message = MonadMessage<ST, SCT>;
    type OutboundMessage = VerifiedMonadMessage<ST, SCT>;
    type Block = Block<SCT>;
    type Checkpoint = Checkpoint<SCT>;
    type NodeIdSignature = ST;
    type SignatureCollection = SCT;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<
            Command<
                Self::Event,
                Self::OutboundMessage,
                Self::Block,
                Self::Checkpoint,
                Self::SignatureCollection,
            >,
        >,
    ) {
        let staking_list = config
            .validators
            .iter()
            .map(|(pubkey, stake, _)| (NodeId::new(*pubkey), *stake))
            .collect::<Vec<_>>();

        let voting_identities = config
            .validators
            .into_iter()
            .map(|(pubkey, _, certpubkey)| (NodeId::new(pubkey), certpubkey))
            .collect::<Vec<_>>();

        // create the initial validator set
        let val_set_1 = VT::new(staking_list).expect("failed to create first validator set");
        let val_cert_pubkeys_1 = ValidatorMapping::new(voting_identities);

        let mut val_epoch_map = ValidatorsEpochMapping::default();
        val_epoch_map.insert(Epoch(1), val_set_1, val_cert_pubkeys_1);

        let election = LT::new();

        let mut monad_state: MonadState<CT, ST, SCT, VT, LT, TT> = Self {
            epoch_manager: EpochManager::new(
                config.val_set_update_interval,
                config.epoch_start_delay,
            ),
            val_epoch_map,
            leader_election: election,
            consensus: CT::new(
                config.transaction_validator,
                config.key.pubkey(),
                config.consensus_config,
                config.beneficiary,
                config.key,
                config.certkey,
            ),
            block_sync_respond: BlockSyncResponder {},
            txpool: TT::new(),

            _pd: PhantomData,
        };

        let init_cmds = monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
            TimeoutVariant::Pacemaker,
        )));

        (monad_state, init_cmds)
    }

    fn update(
        &mut self,
        event: Self::Event,
    ) -> Vec<
        Command<
            Self::Event,
            Self::OutboundMessage,
            Self::Block,
            Self::Checkpoint,
            Self::SignatureCollection,
        >,
    > {
        let _event_span = tracing::info_span!("event_span", ?event).entered();

        match event {
            MonadEvent::ConsensusEvent(consensus_event) => {
                let consensus_commands: Vec<ConsensusCommand<SCT>> = match consensus_event {
                    ConsensusEvent::Timeout(tmo_event) => match tmo_event {
                        TimeoutVariant::Pacemaker => self
                            .consensus
                            .handle_timeout_expiry()
                            .into_iter()
                            .map(|cmd| {
                                ConsensusCommand::from_pacemaker_command(
                                    self.consensus.get_cert_keypair(),
                                    cmd,
                                )
                            })
                            .collect(),
                        TimeoutVariant::BlockSync(bid) => {
                            let current_epoch = self
                                .epoch_manager
                                .get_epoch(self.consensus.get_current_round());
                            let val_set = self
                                .val_epoch_map
                                .get_val_set(&current_epoch)
                                .expect("current validator set should be in the map");
                            self.consensus.handle_block_sync_tmo(bid, val_set)
                        }
                    },
                    ConsensusEvent::Message {
                        sender,
                        unverified_message,
                    } => {
                        // Verify the author signature on the message and sender is author
                        let verified_message = match unverified_message.verify(
                            &self.epoch_manager,
                            &self.val_epoch_map,
                            &sender,
                        ) {
                            Ok(m) => m,
                            Err(e) => {
                                Self::handle_validation_error(e);
                                // TODO-2: collect evidence
                                let evidence_cmds = vec![];
                                return evidence_cmds;
                            }
                        };

                        let (author, _, verified_message) = verified_message.destructure();
                        // Validated message according to consensus protocol spec
                        let validated_mesage = match verified_message
                            .validate(&self.epoch_manager, &self.val_epoch_map)
                        {
                            Ok(m) => m.into_inner(),
                            Err(e) => {
                                Self::handle_validation_error(e);
                                // TODO-2: collect evidence
                                let evidence_cmds = vec![];
                                return evidence_cmds;
                            }
                        };

                        match validated_mesage {
                            ConsensusMessage::Proposal(msg) => {
                                self.consensus.handle_proposal_message(
                                    author,
                                    msg,
                                    &mut self.epoch_manager,
                                    &self.val_epoch_map,
                                    &self.leader_election,
                                )
                            }
                            ConsensusMessage::Vote(msg) => self.consensus.handle_vote_message(
                                author,
                                msg,
                                &mut self.txpool,
                                &mut self.epoch_manager,
                                &self.val_epoch_map,
                                &self.leader_election,
                            ),
                            ConsensusMessage::Timeout(msg) => {
                                self.consensus.handle_timeout_message(
                                    author,
                                    msg,
                                    &mut self.txpool,
                                    &mut self.epoch_manager,
                                    &self.val_epoch_map,
                                    &self.leader_election,
                                )
                            }
                        }
                    }
                    ConsensusEvent::StateUpdate((seq_num, root_hash)) => {
                        self.consensus.handle_state_root_update(seq_num, root_hash);
                        Vec::new()
                    }

                    ConsensusEvent::BlockSyncResponse {
                        sender,
                        unvalidated_response,
                    } => {
                        let validated_response = match unvalidated_response
                            .validate(&self.epoch_manager, &self.val_epoch_map)
                        {
                            Ok(req) => req,
                            Err(e) => {
                                Self::handle_validation_error(e);
                                // TODO-2: collect evidence
                                let evidence_cmds = vec![];
                                return evidence_cmds;
                            }
                        }
                        .into_inner();

                        let current_epoch = self
                            .epoch_manager
                            .get_epoch(self.consensus.get_current_round());
                        let val_set = self
                            .val_epoch_map
                            .get_val_set(&current_epoch)
                            .expect("current validator set should be in the map");
                        self.consensus.handle_block_sync(
                            NodeId::new(sender),
                            validated_response,
                            val_set,
                        )
                    }
                };

                let prepare_router_message =
                    |target: RouterTarget<_>, message: ConsensusMessage<SCT>| {
                        let signed_message = VerifiedMonadMessage::Consensus(
                            message.sign::<ST>(self.consensus.get_keypair()),
                        );
                        Command::RouterCommand(RouterCommand::Publish {
                            target,
                            message: signed_message,
                        })
                    };

                let mut cmds = Vec::new();
                for consensus_command in consensus_commands {
                    match consensus_command {
                        ConsensusCommand::Publish { target, message } => {
                            cmds.push(prepare_router_message(target, message));
                        }
                        ConsensusCommand::Schedule {
                            duration,
                            on_timeout,
                        } => cmds.push(Command::TimerCommand(TimerCommand::Schedule {
                            duration,
                            variant: on_timeout,
                            on_timeout: Self::Event::ConsensusEvent(ConsensusEvent::Timeout(
                                on_timeout,
                            )),
                        })),
                        ConsensusCommand::ScheduleReset(on_timeout) => cmds.push(
                            Command::TimerCommand(TimerCommand::ScheduleReset(on_timeout)),
                        ),
                        ConsensusCommand::RequestSync { peer, block_id } => {
                            cmds.push(Command::RouterCommand(RouterCommand::Publish {
                                target: RouterTarget::PointToPoint(peer),
                                message: VerifiedMonadMessage::BlockSyncRequest(Validated::new(
                                    RequestBlockSyncMessage { block_id },
                                )),
                            }));
                        }
                        ConsensusCommand::LedgerCommit(block) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                                block.clone(),
                            )));
                            cmds.push(Command::ExecutionLedgerCommand(
                                ExecutionLedgerCommand::LedgerCommit(block),
                            ))
                        }
                        ConsensusCommand::CheckpointSave(checkpoint) => cmds.push(
                            Command::CheckpointCommand(CheckpointCommand::Save(checkpoint)),
                        ),
                        ConsensusCommand::StateRootHash(full_block) => {
                            cmds.push(Command::StateRootHashCommand(
                                StateRootHashCommand::LedgerCommit(full_block),
                            ))
                        }
                    }
                }

                cmds
            }

            MonadEvent::BlockSyncEvent(block_sync_event) => match block_sync_event {
                BlockSyncEvent::BlockSyncRequest {
                    sender,
                    unvalidated_request,
                } => {
                    let validated_request = match unvalidated_request.validate() {
                        Ok(req) => req,
                        Err(e) => {
                            Self::handle_validation_error(e);
                            // TODO-2: collect evidence
                            let evidence_cmds = vec![];
                            return evidence_cmds;
                        }
                    }
                    .into_inner();
                    let block_id = validated_request.block_id;
                    self.block_sync_respond.handle_request_block_sync_message(
                        NodeId::new(sender),
                        validated_request,
                        self.consensus.fetch_uncommitted_block(&block_id),
                    )
                }
                BlockSyncEvent::FetchedBlock(fetched_block) => {
                    vec![Command::RouterCommand(RouterCommand::Publish {
                        target: RouterTarget::PointToPoint(fetched_block.requester),
                        message: VerifiedMonadMessage::BlockSyncResponse(Validated::new(
                            match fetched_block.unverified_block {
                                Some(b) => BlockSyncResponseMessage::BlockFound(b),
                                None => {
                                    BlockSyncResponseMessage::NotAvailable(fetched_block.block_id)
                                }
                            },
                        )),
                    })]
                }
            },

            MonadEvent::ValidatorEvent(validator_event) => match validator_event {
                ValidatorEvent::UpdateValidators((val_data, epoch)) => {
                    self.update_next_val_set(val_data, epoch);
                    Vec::new()
                }
            },
        }
    }
}
