use std::{fmt::Debug, marker::PhantomData};

use bytes::Bytes;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncResponseMessage, ProposalMessage, RequestBlockSyncMessage},
    },
    validation::signing::{Unvalidated, Unverified, Validated, Verified},
};
use monad_consensus_state::{
    command::{Checkpoint, ConsensusCommand},
    ConsensusConfig, ConsensusProcess,
};
use monad_consensus_types::{
    block::{Block, FullBlock},
    message_signature::MessageSignature,
    payload::{ExecutionArtifacts, Payload, RandaoReveal},
    signature_collection::{
        SignatureCollection, SignatureCollectionKeyPairType, SignatureCollectionPubKeyType,
    },
    validation,
    validator_data::ValidatorData,
    voting::ValidatorMapping,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_eth_types::EthAddress;
use monad_executor::State;
use monad_executor_glue::{
    BlockSyncEvent, CheckpointCommand, Command, ConsensusEvent, ExecutionLedgerCommand,
    LedgerCommand, MempoolCommand, Message, MonadEvent, RouterCommand, StateRootHashCommand,
    TimerCommand,
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

pub struct MonadState<CT, ST, SCT, VT, LT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
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

    _pd: PhantomData<ST>,
}

impl<CT, ST, SCT, VT, LT> MonadState<CT, ST, SCT, VT, LT>
where
    CT: ConsensusProcess<SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    pub fn consensus(&self) -> &CT {
        &self.consensus
    }

    pub fn epoch_manager(&self) -> &EpochManager {
        &self.epoch_manager
    }

    pub fn pubkey(&self) -> PubKey {
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
pub enum VerifiedMonadMessage<ST, SCT: SignatureCollection> {
    Consensus(Verified<ST, Validated<ConsensusMessage<SCT>>>),
    BlockSyncRequest(Validated<RequestBlockSyncMessage>),
    BlockSyncResponse(Validated<BlockSyncResponseMessage<SCT>>),
}

impl<ST, SCT: SignatureCollection> From<Verified<ST, Validated<ConsensusMessage<SCT>>>>
    for VerifiedMonadMessage<ST, SCT>
{
    fn from(value: Verified<ST, Validated<ConsensusMessage<SCT>>>) -> Self {
        Self::Consensus(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonadMessage<ST, SCT: SignatureCollection> {
    /// Consensus protocol message
    Consensus(Unverified<ST, Unvalidated<ConsensusMessage<SCT>>>),

    /// Request a missing block given BlockId
    BlockSyncRequest(Unvalidated<RequestBlockSyncMessage>),

    /// Block sync response
    BlockSyncResponse(Unvalidated<BlockSyncResponseMessage<SCT>>),
}

impl<MS: MessageSignature, SCT: SignatureCollection> monad_types::Serializable<Bytes>
    for VerifiedMonadMessage<MS, SCT>
{
    fn serialize(&self) -> Bytes {
        crate::convert::interface::serialize_verified_monad_message(self)
    }
}

impl<MS: MessageSignature, SCT: SignatureCollection>
    monad_types::Serializable<MonadMessage<MS, SCT>> for VerifiedMonadMessage<MS, SCT>
{
    fn serialize(&self) -> MonadMessage<MS, SCT> {
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

impl<MS: MessageSignature, SCT: SignatureCollection> monad_types::Deserializable<Bytes>
    for MonadMessage<MS, SCT>
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_monad_message(message.clone())
    }
}

impl<ST, SCT: SignatureCollection> From<VerifiedMonadMessage<ST, SCT>> for MonadMessage<ST, SCT> {
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
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Event = MonadEvent<ST, SCT>;

    fn event(self, from: NodeId) -> Self::Event {
        // MUST assert that output is valid and came from the `from` NodeId
        // `from` must somehow be guaranteed to be staked at this point so that subsequent
        // malformed stuff (that gets added to event log) can be slashed? TODO
        match self {
            MonadMessage::Consensus(msg) => MonadEvent::ConsensusEvent(ConsensusEvent::Message {
                sender: from.0,
                unverified_message: msg,
            }),

            MonadMessage::BlockSyncRequest(msg) => {
                MonadEvent::BlockSyncEvent(BlockSyncEvent::BlockSyncRequest {
                    sender: from.0,
                    unvalidated_request: msg,
                })
            }
            MonadMessage::BlockSyncResponse(msg) => {
                MonadEvent::ConsensusEvent(ConsensusEvent::BlockSyncResponse {
                    sender: from.0,
                    unvalidated_response: msg,
                })
            }
        }
    }
}

pub struct MonadConfig<SCT: SignatureCollection, TV> {
    pub transaction_validator: TV,
    pub validators: Vec<(PubKey, Stake, SignatureCollectionPubKeyType<SCT>)>,
    pub key: KeyPair,
    pub certkey: SignatureCollectionKeyPairType<SCT>,
    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,
    pub beneficiary: EthAddress,

    pub consensus_config: ConsensusConfig,
}

impl<CT, ST, SCT, VT, LT> State for MonadState<CT, ST, SCT, VT, LT>
where
    CT: ConsensusProcess<SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
    LT: LeaderElection,
{
    type Config = MonadConfig<SCT, CT::BlockValidatorType>;
    type Event = MonadEvent<ST, SCT>;
    type Message = MonadMessage<ST, SCT>;
    type OutboundMessage = VerifiedMonadMessage<ST, SCT>;
    type Block = FullBlock<SCT>;
    type Checkpoint = Checkpoint<SCT>;
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
            .map(|(pubkey, stake, _)| (NodeId(*pubkey), *stake))
            .collect::<Vec<_>>();

        let voting_identities = config
            .validators
            .into_iter()
            .map(|(pubkey, _, certpubkey)| (NodeId(pubkey), certpubkey))
            .collect::<Vec<_>>();

        // create the initial validator set
        let val_set_1 = VT::new(staking_list).expect("failed to create first validator set");
        let val_cert_pubkeys_1 = ValidatorMapping::new(voting_identities);

        let mut val_epoch_map = ValidatorsEpochMapping::default();
        val_epoch_map.insert(Epoch(1), val_set_1, val_cert_pubkeys_1);

        let election = LT::new();

        let mut monad_state: MonadState<CT, ST, SCT, VT, LT> = Self {
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

                    ConsensusEvent::FetchedTxs(fetched, txns) => {
                        assert_eq!(fetched.node_id, self.consensus.get_nodeid());

                        let mut cmds = vec![ConsensusCommand::FetchTxsReset];

                        // make sure we're still in same round
                        if fetched.round == self.consensus.get_current_round() {
                            let mut header = ExecutionArtifacts::zero();
                            header.state_root = fetched.state_root_hash;
                            let b = Block::new(
                                fetched.node_id,
                                fetched.round,
                                &Payload {
                                    txns,
                                    header,
                                    seq_num: fetched.seq_num,
                                    beneficiary: self.consensus.get_beneficiary(),
                                    randao_reveal: RandaoReveal::new::<SCT::SignatureType>(
                                        fetched.round,
                                        self.consensus.get_cert_keypair(),
                                    ),
                                },
                                &fetched.high_qc,
                            );

                            let p = ProposalMessage {
                                block: b,
                                last_round_tc: fetched.last_round_tc,
                            };

                            cmds.push(ConsensusCommand::Publish {
                                target: RouterTarget::Broadcast,
                                message: ConsensusMessage::Proposal(p),
                            });
                        }

                        cmds
                    }
                    ConsensusEvent::FetchedFullTxs(fetched_txs, txns) => {
                        let mut cmds = vec![ConsensusCommand::FetchFullTxsReset];

                        if let Some(txns) = txns {
                            let proposal_msg = ProposalMessage {
                                block: fetched_txs.p_block,
                                last_round_tc: fetched_txs.p_last_round_tc,
                            };
                            cmds.extend(self.consensus.handle_proposal_message_full(
                                fetched_txs.author,
                                proposal_msg,
                                txns,
                                &mut self.epoch_manager,
                                &self.val_epoch_map,
                                &self.leader_election,
                            ));
                        }

                        cmds
                    }

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
                                self.consensus.handle_proposal_message(author, msg)
                            }
                            ConsensusMessage::Vote(msg) => self.consensus.handle_vote_message(
                                author,
                                msg,
                                &mut self.epoch_manager,
                                &self.val_epoch_map,
                                &self.leader_election,
                            ),
                            ConsensusMessage::Timeout(msg) => {
                                self.consensus.handle_timeout_message(
                                    author,
                                    msg,
                                    &mut self.epoch_manager,
                                    &self.val_epoch_map,
                                    &self.leader_election,
                                )
                            }
                        }
                    }
                    ConsensusEvent::UpdateValidators((val_data, epoch)) => {
                        self.update_next_val_set(val_data, epoch);
                        Vec::new()
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
                            Err(e) => todo!("{e:?}"),
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
                            NodeId(sender),
                            validated_response,
                            val_set,
                        )
                    }
                };

                let prepare_router_message =
                    |target: RouterTarget, message: ConsensusMessage<SCT>| {
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
                        ConsensusCommand::FetchTxs(fetch_criteria) => cmds.push(
                            Command::MempoolCommand(MempoolCommand::FetchTxs(fetch_criteria)),
                        ),
                        ConsensusCommand::FetchTxsReset => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchReset))
                        }
                        ConsensusCommand::FetchFullTxs(txs, fetch_params) => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchFullTxs(
                                txs,
                                fetch_params,
                            )))
                        }
                        ConsensusCommand::FetchFullTxsReset => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::FetchFullReset))
                        }
                        ConsensusCommand::DrainTxs(txs) => {
                            cmds.push(Command::MempoolCommand(MempoolCommand::DrainTxs(txs)))
                        }

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

                        ConsensusCommand::LedgerFetchReset(node_id, block_id) => {
                            cmds.push(Command::LedgerCommand(LedgerCommand::LedgerFetchReset(
                                node_id, block_id,
                            )))
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
                        NodeId(sender),
                        validated_request,
                        self.consensus.fetch_uncommitted_block(&block_id),
                    )
                }
                BlockSyncEvent::FetchedBlock(fetched_block) => {
                    vec![
                        Command::LedgerCommand(LedgerCommand::LedgerFetchReset(
                            fetched_block.requester,
                            fetched_block.block_id,
                        )),
                        Command::RouterCommand(RouterCommand::Publish {
                            target: RouterTarget::PointToPoint(NodeId(fetched_block.requester.0)),
                            message: VerifiedMonadMessage::BlockSyncResponse(Validated::new(
                                match fetched_block.unverified_full_block {
                                    Some(b) => BlockSyncResponseMessage::BlockFound(b),
                                    None => BlockSyncResponseMessage::NotAvailable(
                                        fetched_block.block_id,
                                    ),
                                },
                            )),
                        }),
                    ]
                }
            },
        }
    }
}
