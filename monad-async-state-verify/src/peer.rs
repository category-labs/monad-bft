use std::{
    collections::{BTreeMap, HashMap, HashSet},
    marker::PhantomData,
};

use monad_consensus_types::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    state_root_hash::StateRootHashInfo,
    voting::ValidatorMapping,
};
use monad_crypto::{
    certificate_signature::CertificateSignature,
    hasher::{Hash, Hasher, HasherType},
};
use monad_types::{NodeId, SeqNum, Stake};
use monad_validator::validator_set::ValidatorSetType;
use tracing::{info, warn};

use crate::{
    AsyncStateVerifyCommand, AsyncStateVerifyProcess, AsyncStateVerifyUpdateConfig,
    StateRootCertificate,
};

pub fn majority_threshold(total_stake: Stake) -> Stake {
    Stake(total_stake.0 / 2 + 1)
}

/// Collects votes on execution state root for a block
#[derive(Debug, Clone)]
struct ExecutionStateRootRecord<SCT: SignatureCollection> {
    /// Execution state root computed by the local execution client
    local_root: Option<StateRootHashInfo>,
    /// Votes on state root collected from peers, indexed by the StateRootInfo
    /// they voted on
    pending_roots: HashMap<Hash, BTreeMap<NodeId<SCT::NodeIdPubKey>, SCT::SignatureType>>,
    /// Tracks which state root each node has voted on, to detect double voting
    node_vote_tracker: HashMap<NodeId<SCT::NodeIdPubKey>, HashSet<SCT::SignatureType>>,
    /// Populated if a stake threshold of validators has voted on the same state
    /// root
    quorum: Option<StateRootCertificate<SCT>>,
    /// Whether we have sent a state root update to consensus state
    update_sent: bool,
}

impl<SCT: SignatureCollection> Default for ExecutionStateRootRecord<SCT> {
    fn default() -> Self {
        Self {
            local_root: Option::None,
            pending_roots: HashMap::new(),
            node_vote_tracker: HashMap::new(),
            quorum: None,
            update_sent: false,
        }
    }
}

// TODO: garbage clean old state roots
#[derive(Debug, Clone)]
pub struct PeerAsyncStateVerify<SCT, VT>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    /// Collection of StateRoots after executing each block
    state_roots: BTreeMap<SeqNum, ExecutionStateRootRecord<SCT>>,
    /// The highest sequence number where a quorum of validators agree on the
    /// execution state root
    latest_certified: SeqNum,
    /// StateRoot quorum threshold, takes in the total stake and computes the
    /// threshold stake
    quorum_threshold: fn(Stake) -> Stake,
    /// Control when consensus update is emitted
    consensus_update_config: AsyncStateVerifyUpdateConfig,
    _phantom: PhantomData<(SCT, VT)>,
}

impl<SCT, VT> Default for PeerAsyncStateVerify<SCT, VT>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    fn default() -> Self {
        Self {
            state_roots: Default::default(),
            latest_certified: SeqNum(0),
            quorum_threshold: majority_threshold,
            consensus_update_config: AsyncStateVerifyUpdateConfig::Local,
            _phantom: Default::default(),
        }
    }
}

impl<SCT, VT> PeerAsyncStateVerify<SCT, VT>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    pub fn new(quorum_threshold: fn(Stake) -> Stake) -> Self {
        Self {
            quorum_threshold,
            ..Default::default()
        }
    }
}

fn handle_invalid_signature_collection_creation<SCT: SignatureCollection>(
    err: SignatureCollectionError<SCT::NodeIdPubKey, SCT::SignatureType>,
    pending_roots: &mut BTreeMap<NodeId<SCT::NodeIdPubKey>, SCT::SignatureType>,
) -> Vec<AsyncStateVerifyCommand<SCT>> {
    match err {
        SignatureCollectionError::InvalidSignaturesCreate(invalid_sigs) => {
            // remove the invalid signatures from pending votes
            let invalid_nodes = invalid_sigs
                .into_iter()
                .map(|(node_id, _)| node_id)
                .collect::<HashSet<_>>();

            pending_roots.retain(|node_id, _| !invalid_nodes.contains(node_id));
            // TODO: evidence
            vec![]
        }

        SignatureCollectionError::NodeIdNotInMapping(_)
        | SignatureCollectionError::ConflictingSignatures(_)
        | SignatureCollectionError::InvalidSignaturesVerify
        | SignatureCollectionError::DeserializeError(_) => {
            unreachable!("InvalidSignaturesCreate is only expected error from creating SC");
        }
    }
}

impl<SCT, VT> AsyncStateVerifyProcess for PeerAsyncStateVerify<SCT, VT>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    type SignatureCollectionType = SCT;
    type ValidatorSetType = VT;

    fn handle_local_state_root(
        &mut self,
        self_id: NodeId<<Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey>,
        cert_keypair: &SignatureCollectionKeyPairType<Self::SignatureCollectionType>,
        info: StateRootHashInfo,
    ) -> Vec<AsyncStateVerifyCommand<Self::SignatureCollectionType>> {
        let mut cmds = Vec::new();
        let entry = self.state_roots.entry(info.seq_num).or_default();

        // Intentional match if more update configs are added
        match self.consensus_update_config {
            AsyncStateVerifyUpdateConfig::Local => {
                if !entry.update_sent {
                    cmds.push(AsyncStateVerifyCommand::StateRootUpdate(info));
                    entry.update_sent = true;
                }
            }
            AsyncStateVerifyUpdateConfig::Quorum(_) => {}
        }

        // record the local execution result
        if let Some(local_root) = &entry.local_root {
            if local_root != &info {
                warn!(
                    "conflicting local state root old={:?} new={:?}",
                    local_root, info
                );
            } else {
                warn!("duplicate local state root info={:?}", local_root);
            }
        }
        entry.local_root = Some(info);

        if let Some(cert) = &entry.quorum {
            if cert.info != info {
                warn!(
                    "local execution result differ from quorum seq_num={:?} local={:?} quorum={:?}",
                    info.seq_num, info, cert.info
                );
                // TODO-2: handle the state root mismatch error
            }
        }

        // we don't insert to pending roots because consensus broadcast sends to
        // self
        let msg = HasherType::hash_object(&info);
        let sig = <SCT::SignatureType as CertificateSignature>::sign(msg.as_ref(), cert_keypair);
        cmds.push(AsyncStateVerifyCommand::BroadcastStateRoot {
            peer: self_id,
            info,
            sig,
        });
        cmds
    }

    fn handle_peer_state_root(
        &mut self,
        peer: NodeId<<Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey>,
        info: StateRootHashInfo,
        sig: <Self::SignatureCollectionType as SignatureCollection>::SignatureType,
        validators: &Self::ValidatorSetType,
        validator_mapping: &ValidatorMapping<
            <Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self::SignatureCollectionType>,
        >,
    ) -> Vec<AsyncStateVerifyCommand<Self::SignatureCollectionType>> {
        // TODO: anti-spam: don't accept state root hash much higher than
        // current consensus value
        let mut cmds = Vec::new();

        // check double voting
        let block_state = self.state_roots.entry(info.seq_num).or_default();
        let node_votes = block_state.node_vote_tracker.entry(peer).or_default();
        if !node_votes.is_empty() {
            info!("Node sends multiple state roots node={:?}", peer);
        }
        node_votes.insert(sig);
        if node_votes.len() > 1 {
            // TODO: double voting evidence collection, might be acceptable if
            // it's correcting state root based on msgs received. Can bump the
            // threshold slightly higher
            warn!(
                "Conflicting state roots peer={:?} state_roots={:?}",
                peer, node_votes
            );
        }

        // add to pending votes
        let info_hash = HasherType::hash_object(&info);
        let pending_roots = block_state.pending_roots.entry(info_hash).or_default();
        pending_roots.insert(peer, sig);

        match self.consensus_update_config {
            AsyncStateVerifyUpdateConfig::Local => {}
            AsyncStateVerifyUpdateConfig::Quorum(compute_update_threshold) => {
                if !block_state.update_sent {
                    let update_threshold = compute_update_threshold(validators.get_total_stake());
                    while validators.has_threshold_votes(
                        &pending_roots.keys().copied().collect::<Vec<_>>(),
                        update_threshold,
                    ) {
                        match SCT::new(
                            pending_roots.iter().map(|(node, sig)| (*node, *sig)),
                            validator_mapping,
                            info_hash.as_ref(),
                        ) {
                            Ok(_) => {
                                cmds.push(AsyncStateVerifyCommand::StateRootUpdate(info));
                                block_state.update_sent = true;
                                break;
                            }

                            Err(err) => cmds.extend(handle_invalid_signature_collection_creation(
                                err,
                                pending_roots,
                            )),
                        }
                    }
                }
            }
        }

        // Do not try to recreate a quorum if a quorum has been formed, because creating a
        // signature collection is expensive
        if block_state.quorum.is_none() {
            let quorum_threshold = (self.quorum_threshold)(validators.get_total_stake());
            while validators.has_threshold_votes(
                &pending_roots.keys().copied().collect::<Vec<_>>(),
                quorum_threshold,
            ) {
                assert!(block_state.quorum.is_none());
                match SCT::new(
                    pending_roots.iter().map(|(node, sig)| (*node, *sig)),
                    validator_mapping,
                    info_hash.as_ref(),
                ) {
                    Ok(sigcol) => {
                        self.latest_certified = info.seq_num;
                        if let Some(local_root) = block_state.local_root {
                            if local_root != info {
                                warn!(
                                    "local execution result differ from quorum local={:?} quorum={:?}",
                                    local_root, info
                                );
                            }
                        } else {
                            info!("local execution delayed");
                        }

                        block_state.quorum = Some(StateRootCertificate { info, sigs: sigcol });
                        break;
                    }

                    Err(err) => cmds.extend(handle_invalid_signature_collection_creation(
                        err,
                        pending_roots,
                    )),
                }
            }
        }
        cmds
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        state_root_hash::StateRootHash,
        voting::ValidatorMapping,
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        hasher::{Hash, Hasher, HasherType},
        NopSignature,
    };
    use monad_multi_sig::MultiSig;
    use monad_testutil::{signing::*, validators::create_keys_w_validators};
    use monad_types::{NodeId, Round, SeqNum, Stake};
    use monad_validator::validator_set::{
        ValidatorSet, ValidatorSetFactory, ValidatorSetTypeFactory,
    };

    use super::*;
    use crate::PeerAsyncStateVerify;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<NopSignature>;

    fn sign_state_root_info(
        cert_key: &SignatureCollectionKeyPairType<SignatureCollectionType>,
        info: StateRootHashInfo,
    ) -> <SignatureCollectionType as SignatureCollection>::SignatureType {
        let msg = HasherType::hash_object(&info);
        <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(
            msg.as_ref(),
            cert_key,
        )
    }

    /// test AsyncStateVerify with local consensus update policy: sends state
    /// root update to consensus if local execution sends the update
    #[test]
    fn local_update() {
        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        let node0 = NodeId::new(keys[0].pubkey());
        let mut asv = PeerAsyncStateVerify::<
            SignatureCollectionType,
            ValidatorSet<CertificateSignaturePubKey<SignatureType>>,
        >::default();

        let true_info = StateRootHashInfo {
            state_root_hash: StateRootHash(Hash([0xab_u8; 32])),
            seq_num: SeqNum(1),
            round: Round(1),
        };

        let false_info = StateRootHashInfo {
            state_root_hash: StateRootHash(Hash([0xff_u8; 32])),
            seq_num: SeqNum(1),
            round: Round(1),
        };

        let cmds = asv.handle_local_state_root(node0, &certkeys[0], true_info);
        assert_eq!(cmds.len(), 2);

        if let AsyncStateVerifyCommand::StateRootUpdate(info) = cmds[0] {
            assert_eq!(info, true_info);
        } else {
            panic!("Command type mismatch");
        }

        if let AsyncStateVerifyCommand::BroadcastStateRoot { peer, info, sig } = cmds[1] {
            assert_eq!(peer, node0);
            assert_eq!(info, true_info);
            assert_eq!(sig, sign_state_root_info(&certkeys[0], true_info));
        } else {
            panic!("Command type mismatch");
        }
        assert_eq!(
            asv.state_roots.get(&SeqNum(1)).unwrap().local_root,
            Some(true_info)
        );

        let cmds = asv.handle_peer_state_root(
            node0,
            true_info,
            sign_state_root_info(&certkeys[0], true_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());

        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[1].pubkey()),
            true_info,
            sign_state_root_info(&certkeys[1], true_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());

        // node2 submits a different state root, no quorum is formed
        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[2].pubkey()),
            false_info,
            sign_state_root_info(&certkeys[2], false_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());

        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[3].pubkey()),
            true_info,
            sign_state_root_info(&certkeys[3], true_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());

        assert_eq!(asv.latest_certified, SeqNum(1));
        let block_state = asv.state_roots.get(&SeqNum(1)).unwrap();
        assert_eq!(block_state.local_root, Some(true_info));
        assert_eq!(block_state.pending_roots.len(), 2);
        assert_eq!(block_state.node_vote_tracker.len(), 4);
        assert!(block_state.quorum.is_some());
    }

    /// test AsyncStateVerify with threshold consensus update policy: sends
    /// consensus update if we have collected a threshold of votes on the same
    /// state root hash
    #[test]
    fn threshold_update() {
        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        let node0 = NodeId::new(keys[0].pubkey());
        let mut asv = PeerAsyncStateVerify::<
            SignatureCollectionType,
            ValidatorSet<CertificateSignaturePubKey<SignatureType>>,
        >::default();
        let quarter_stake = |total_stake: Stake| Stake(total_stake.0 / 4);
        asv.consensus_update_config = AsyncStateVerifyUpdateConfig::Quorum(quarter_stake);

        let true_info = StateRootHashInfo {
            state_root_hash: StateRootHash(Hash([0xab_u8; 32])),
            seq_num: SeqNum(1),
            round: Round(1),
        };

        let cmds = asv.handle_local_state_root(node0, &certkeys[0], true_info);
        assert_eq!(cmds.len(), 1);

        if let AsyncStateVerifyCommand::BroadcastStateRoot { peer, info, sig } = cmds[0] {
            assert_eq!(peer, node0);
            assert_eq!(info, true_info);
            assert_eq!(sig, sign_state_root_info(&certkeys[0], true_info));
        } else {
            panic!("Command type mismatch");
        }
        assert_eq!(
            asv.state_roots.get(&SeqNum(1)).unwrap().local_root,
            Some(true_info)
        );

        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[0].pubkey()),
            true_info,
            sign_state_root_info(&certkeys[0], true_info),
            &valset,
            &vmap,
        );
        // reached consensus update threshold (quarter stake). Send update to consensus
        assert_eq!(cmds.len(), 1);
        if let AsyncStateVerifyCommand::StateRootUpdate(info) = cmds[0] {
            assert_eq!(info, true_info);
        } else {
            panic!("Command type mismatch");
        }

        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[1].pubkey()),
            true_info,
            sign_state_root_info(&certkeys[1], true_info),
            &valset,
            &vmap,
        );
        // no ConsensusUpdate is created because we have created one
        assert!(cmds.is_empty());

        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[2].pubkey()),
            true_info,
            sign_state_root_info(&certkeys[2], true_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());
        // reached majority quorum threshold. Quorum is set
        assert_eq!(
            asv.state_roots
                .get(&SeqNum(1))
                .unwrap()
                .quorum
                .as_ref()
                .unwrap()
                .info,
            true_info
        );
        assert_eq!(asv.latest_certified, SeqNum(1));

        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[3].pubkey()),
            true_info,
            sign_state_root_info(&certkeys[3], true_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());
    }

    #[test]
    fn invalid_sigs_quorum() {
        let keys = create_keys::<SignatureType>(4);
        let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

        let mut staking_list = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(std::iter::repeat(Stake(1)))
            .collect::<Vec<_>>();

        // node1 has majority stake by itself
        staking_list[1].1 = Stake(10);

        let voting_identity = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(certkeys.iter().map(|k| k.pubkey()))
            .collect::<Vec<_>>();

        let valset = ValidatorSetFactory::default()
            .create(staking_list)
            .expect("create validator set");
        let vmap = ValidatorMapping::new(voting_identity);

        let node0 = NodeId::new(keys[0].pubkey());
        let mut asv = PeerAsyncStateVerify::<
            SignatureCollectionType,
            ValidatorSet<CertificateSignaturePubKey<SignatureType>>,
        >::default();

        let true_info = StateRootHashInfo {
            state_root_hash: StateRootHash(Hash([0xab_u8; 32])),
            seq_num: SeqNum(1),
            round: Round(1),
        };

        let false_info = StateRootHashInfo {
            state_root_hash: StateRootHash(Hash([0xff_u8; 32])),
            seq_num: SeqNum(1),
            round: Round(1),
        };

        // malformed sig
        let cmds = asv.handle_peer_state_root(
            node0,
            true_info,
            sign_state_root_info(&certkeys[0], false_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());

        // majority-staked validator sends the state root
        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[1].pubkey()),
            true_info,
            sign_state_root_info(&certkeys[1], true_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());

        let info_hash = HasherType::hash_object(&true_info);
        let block_state = asv.state_roots.get(&SeqNum(1)).unwrap();
        let root_state = block_state.pending_roots.get(&info_hash).unwrap();
        // the invalid signature is removed from the map
        assert_eq!(root_state.len(), 1);
    }

    #[test]
    fn invalid_sigs_no_quorum() {
        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        let node0 = NodeId::new(keys[0].pubkey());
        let mut asv = PeerAsyncStateVerify::<
            SignatureCollectionType,
            ValidatorSet<CertificateSignaturePubKey<SignatureType>>,
        >::default();

        let true_info = StateRootHashInfo {
            state_root_hash: StateRootHash(Hash([0xab_u8; 32])),
            seq_num: SeqNum(1),
            round: Round(1),
        };

        let false_info = StateRootHashInfo {
            state_root_hash: StateRootHash(Hash([0xff_u8; 32])),
            seq_num: SeqNum(1),
            round: Round(1),
        };

        let cmds = asv.handle_local_state_root(node0, &certkeys[0], true_info);
        assert_eq!(cmds.len(), 2);

        if let AsyncStateVerifyCommand::StateRootUpdate(info) = cmds[0] {
            assert_eq!(info, true_info);
        } else {
            panic!("Command type mismatch");
        }

        if let AsyncStateVerifyCommand::BroadcastStateRoot { peer, info, sig } = cmds[1] {
            assert_eq!(peer, node0);
            assert_eq!(info, true_info);
            assert_eq!(sig, sign_state_root_info(&certkeys[0], true_info));
        } else {
            panic!("Command type mismatch");
        }
        assert_eq!(
            asv.state_roots.get(&SeqNum(1)).unwrap().local_root,
            Some(true_info)
        );

        let cmds = asv.handle_peer_state_root(
            node0,
            true_info,
            sign_state_root_info(&certkeys[0], true_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());

        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[1].pubkey()),
            true_info,
            sign_state_root_info(&certkeys[1], true_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());

        // node2 submits the same state root with an invalid signature, no
        // quorum is formed
        let cmds = asv.handle_peer_state_root(
            NodeId::new(keys[2].pubkey()),
            true_info,
            sign_state_root_info(&certkeys[2], false_info),
            &valset,
            &vmap,
        );
        assert!(cmds.is_empty());
        let info_hash = HasherType::hash_object(&true_info);
        let block_state = asv.state_roots.get(&SeqNum(1)).unwrap();
        let root_state = block_state.pending_roots.get(&info_hash).unwrap();
        // the invalid signature is removed from the map
        assert_eq!(root_state.len(), 2);
    }
}
