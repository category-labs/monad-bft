use std::marker::PhantomData;

use monad_consensus_types::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::StateRootHashInfo,
    voting::ValidatorMapping,
};
use monad_types::NodeId;
use monad_validator::validator_set::ValidatorSetType;

use crate::{AsyncStateVerifyCommand, AsyncStateVerifyProcess};

/// A mock version that doesn't verify/compare anything. It uses the state root
/// calculated by its own execution, and forward that to consensus state
#[derive(Clone)]
pub struct LocalAsyncStateVerify<SCT, VT>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    _phantom: PhantomData<(SCT, VT)>,
}

impl<SCT, VT> AsyncStateVerifyProcess for LocalAsyncStateVerify<SCT, VT>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    type SignatureCollectionType = SCT;
    type ValidatorSetType = VT;

    fn handle_local_state_root(
        &mut self,
        _self_id: NodeId<<Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey>,
        _cert_keypair: &SignatureCollectionKeyPairType<Self::SignatureCollectionType>,
        info: StateRootHashInfo,
    ) -> Vec<AsyncStateVerifyCommand<Self::SignatureCollectionType>> {
        vec![AsyncStateVerifyCommand::StateRootUpdate(info)]
    }

    fn handle_peer_state_root(
        &mut self,
        _peer: NodeId<<Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey>,
        _info: StateRootHashInfo,
        _sig: <Self::SignatureCollectionType as SignatureCollection>::SignatureType,
        _validators: &Self::ValidatorSetType,
        _validator_mapping: &ValidatorMapping<
            <Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self::SignatureCollectionType>,
        >,
    ) -> Vec<AsyncStateVerifyCommand<Self::SignatureCollectionType>> {
        // ignore peer state root values
        vec![]
    }
}

impl<SCT, VT> Default for LocalAsyncStateVerify<SCT, VT>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}
