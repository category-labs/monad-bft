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
use monad_types::{NodeId, SeqNum};
use monad_validator::validator_set::ValidatorSetType;
use tracing::{info, warn};

use crate::{AsyncStateVerifyCommand, AsyncStateVerifyProcess, StateRootCertificate};

#[derive(Debug, Clone)]
struct BlockStateRootState<SCT: SignatureCollection> {
    local_root: Option<StateRootHashInfo>,
    pending_roots: HashMap<Hash, BTreeMap<NodeId<SCT::NodeIdPubKey>, SCT::SignatureType>>,
    node_roots: HashMap<NodeId<SCT::NodeIdPubKey>, HashSet<SCT::SignatureType>>,
    quorum: Option<StateRootCertificate<SCT>>,
}

impl<SCT: SignatureCollection> Default for BlockStateRootState<SCT> {
    fn default() -> Self {
        Self {
            local_root: Option::None,
            pending_roots: HashMap::new(),
            node_roots: HashMap::new(),
            quorum: None,
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
    state_roots: BTreeMap<SeqNum, BlockStateRootState<SCT>>,
    /// The highest sequence number where a quorum of validators agree on the
    /// execution state root
    latest_certified: SeqNum,

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
            _phantom: Default::default(),
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
        let entry = self.state_roots.entry(info.seq_num).or_default();

        // note the local execution result
        if let Some(local_root) = &entry.local_root {
            if local_root != &info {
                warn!("conflicting local state root");
            } else {
                info!("duplicate local state root");
            }
        }
        entry.local_root = Some(info);

        if let Some(cert) = &entry.quorum {
            // TODO-2: handle the state root mismatch error
            if cert.info != info {
                warn!(
                    "local execution result differ from quorum seq_num={:?}",
                    info.seq_num
                );
            }
        }

        // we don't insert to pending roots because consensus broadcast sends to
        // self

        let msg = HasherType::hash_object(&info);
        let sig = <SCT::SignatureType as CertificateSignature>::sign(msg.as_ref(), cert_keypair);
        vec![AsyncStateVerifyCommand::BroadcastStateRoot {
            peer: self_id,
            info,
            sig,
        }]
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
        if info.seq_num <= self.latest_certified {
            return cmds;
        }

        // check double voting
        let block_state = self.state_roots.entry(info.seq_num).or_default();
        let node_votes = block_state.node_roots.entry(peer).or_default();
        node_votes.insert(sig);
        if node_votes.len() > 1 {
            // TODO: double voting evidence collection, might be acceptable if
            // it's correcting state root based on msgs received. Can bump the
            // threshold slightly higher
        }

        // add to pending votes
        let info_hash = HasherType::hash_object(&info);
        let pending_roots = block_state.pending_roots.entry(info_hash).or_default();
        pending_roots.insert(peer, sig);

        while validators.has_majority_votes(&pending_roots.keys().copied().collect::<Vec<_>>()) {
            assert!(info.seq_num > self.latest_certified);
            assert!(block_state.quorum.is_none());
            match SCT::new(
                pending_roots.iter().map(|(node, sig)| (*node, *sig)),
                validator_mapping,
                info_hash.as_ref(),
            ) {
                Ok(sigcol) => {
                    // forward certified state root to consensus
                    self.latest_certified = info.seq_num;
                    if let Some(local_root) = block_state.local_root {
                        if local_root != info {
                            warn!("conflicting local state root");
                        }
                    } else {
                        info!("local execution delayed");
                    }
                    block_state.quorum = Some(StateRootCertificate { info, sigs: sigcol });
                    // TODO: log the quorum to a database
                    // forward quorum to consensus?
                    cmds.push(AsyncStateVerifyCommand::StateRootUpdate(info));
                    return cmds;
                }

                Err(SignatureCollectionError::InvalidSignaturesCreate(invalid_sigs)) => {
                    // remove the invalid signatures from pending votes
                    let invalid_nodes = invalid_sigs
                        .into_iter()
                        .map(|(node_id, _)| node_id)
                        .collect::<HashSet<_>>();

                    pending_roots.retain(|node_id, _| !invalid_nodes.contains(node_id));
                    // TODO: evidence
                }

                Err(
                    SignatureCollectionError::NodeIdNotInMapping(_)
                    | SignatureCollectionError::ConflictingSignatures(_)
                    | SignatureCollectionError::InvalidSignaturesVerify
                    | SignatureCollectionError::DeserializeError(_),
                ) => {
                    unreachable!("InvalidSignaturesCreate is only expected error from creating SC");
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

    #[test]
    fn nominal() {
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
        assert_eq!(cmds.len(), 1);

        if let AsyncStateVerifyCommand::StateRootUpdate(info) = cmds[0] {
            assert_eq!(info, true_info);
        } else {
            panic!("Command type mismatch");
        }

        assert_eq!(asv.latest_certified, SeqNum(1));
        let block_state = asv.state_roots.get(&SeqNum(1)).unwrap();
        assert_eq!(block_state.local_root, Some(true_info));
        assert_eq!(block_state.pending_roots.len(), 2);
        assert_eq!(block_state.node_roots.len(), 4);
        assert!(block_state.quorum.is_some());
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
        assert_eq!(cmds.len(), 1);
        assert!(matches!(
            cmds[0],
            AsyncStateVerifyCommand::StateRootUpdate(_)
        ));

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
