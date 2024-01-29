use std::any::Any;

use monad_consensus_types::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::StateRootHashInfo,
    voting::ValidatorMapping,
};
use monad_types::NodeId;
use monad_validator::validator_set::ValidatorSetType;

pub mod local;

pub use local::LocalAsyncStateVerify;

pub enum AsyncStateVerifyCommand<SCT: SignatureCollection> {
    /// Broadcast local execution result to other validators
    BroadcastStateRoot {
        peer: NodeId<SCT::NodeIdPubKey>,
        info: StateRootHashInfo,
        sig: SCT::SignatureType,
    },
    /// Send state root update to consensus state (we can remove this by having
    /// consensus taking a reference to async state verify)
    StateRootUpdate(StateRootHashInfo),
}

pub trait AsyncStateVerifyProcess {
    type SignatureCollectionType: SignatureCollection;
    type ValidatorSetType: ValidatorSetType<
        NodeIdPubKey = <Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey,
    >;

    fn handle_local_state_root(
        &mut self,
        self_id: NodeId<<Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey>,
        cert_keypair: &SignatureCollectionKeyPairType<Self::SignatureCollectionType>,
        info: StateRootHashInfo,
    ) -> Vec<AsyncStateVerifyCommand<Self::SignatureCollectionType>>;

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
    ) -> Vec<AsyncStateVerifyCommand<Self::SignatureCollectionType>>;
}

impl<T> AsyncStateVerifyProcess for Box<T>
where
    T: AsyncStateVerifyProcess + ?Sized,
{
    type SignatureCollectionType = T::SignatureCollectionType;
    type ValidatorSetType = T::ValidatorSetType;

    fn handle_local_state_root(
        &mut self,
        self_id: NodeId<<Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey>,
        cert_keypair: &SignatureCollectionKeyPairType<Self::SignatureCollectionType>,
        info: StateRootHashInfo,
    ) -> Vec<AsyncStateVerifyCommand<Self::SignatureCollectionType>> {
        (**self).handle_local_state_root(self_id, cert_keypair, info)
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
        (**self).handle_peer_state_root(peer, info, sig, validators, validator_mapping)
    }
}

/// Helper trait that's only used for dynamic dispatch boxing
/// This trait is necessary so that the ValidatorSetType associated type can be erased
trait AsyncStateVerifyHelper {
    type SCT: SignatureCollection;

    fn handle_local_state_root(
        &mut self,
        self_id: NodeId<<Self::SCT as SignatureCollection>::NodeIdPubKey>,
        cert_keypair: &SignatureCollectionKeyPairType<Self::SCT>,
        info: StateRootHashInfo,
    ) -> Vec<AsyncStateVerifyCommand<Self::SCT>>;

    fn handle_peer_state_root(
        &mut self,
        peer: NodeId<<Self::SCT as SignatureCollection>::NodeIdPubKey>,
        info: StateRootHashInfo,
        sig: <Self::SCT as SignatureCollection>::SignatureType,
        validators: &Box<
            dyn ValidatorSetType<NodeIdPubKey = <Self::SCT as SignatureCollection>::NodeIdPubKey>,
        >,
        validator_mapping: &ValidatorMapping<
            <Self::SCT as SignatureCollection>::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self::SCT>,
        >,
    ) -> Vec<AsyncStateVerifyCommand<Self::SCT>>;
}

impl<T> AsyncStateVerifyHelper for T
where
    T: AsyncStateVerifyProcess + ?Sized,
    T::ValidatorSetType: Send + Sync + 'static,
{
    type SCT = T::SignatureCollectionType;

    fn handle_local_state_root(
        &mut self,
        self_id: NodeId<<Self::SCT as SignatureCollection>::NodeIdPubKey>,
        cert_keypair: &SignatureCollectionKeyPairType<Self::SCT>,
        info: StateRootHashInfo,
    ) -> Vec<AsyncStateVerifyCommand<Self::SCT>> {
        self.handle_local_state_root(self_id, cert_keypair, info)
    }

    fn handle_peer_state_root(
        &mut self,
        peer: NodeId<<Self::SCT as SignatureCollection>::NodeIdPubKey>,
        info: StateRootHashInfo,
        sig: <Self::SCT as SignatureCollection>::SignatureType,
        validators: &Box<
            dyn ValidatorSetType<NodeIdPubKey = <Self::SCT as SignatureCollection>::NodeIdPubKey>,
        >,
        validator_mapping: &ValidatorMapping<
            <Self::SCT as SignatureCollection>::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self::SCT>,
        >,
    ) -> Vec<AsyncStateVerifyCommand<Self::SCT>> {
        self.handle_peer_state_root(
            peer,
            info,
            sig,
            (validators as &dyn Any)
                .downcast_ref::<T::ValidatorSetType>()
                .unwrap(),
            validator_mapping,
        )
    }
}

pub struct BoxedAsyncStateVerifyProcess<SCT: SignatureCollection>(
    Box<dyn AsyncStateVerifyHelper<SCT = SCT> + Send + Sync>,
);

impl<SCT: SignatureCollection> AsyncStateVerifyProcess for BoxedAsyncStateVerifyProcess<SCT> {
    type SignatureCollectionType = SCT;
    type ValidatorSetType = Box<dyn ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>>;

    fn handle_local_state_root(
        &mut self,
        self_id: NodeId<<Self::SignatureCollectionType as SignatureCollection>::NodeIdPubKey>,
        cert_keypair: &SignatureCollectionKeyPairType<Self::SignatureCollectionType>,
        info: StateRootHashInfo,
    ) -> Vec<AsyncStateVerifyCommand<Self::SignatureCollectionType>> {
        self.0.handle_local_state_root(self_id, cert_keypair, info)
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
        self.0
            .handle_peer_state_root(peer, info, sig, validators, validator_mapping)
    }
}

impl<SCT: SignatureCollection> BoxedAsyncStateVerifyProcess<SCT> {
    pub fn new<T>(asv: T) -> Self
    where
        T: AsyncStateVerifyProcess<SignatureCollectionType = SCT> + Send + Sync + 'static,
    {
        Self(Box::new(asv))
    }
}

#[derive(Debug, Clone)]
pub struct StateRootCertificate<SCT: SignatureCollection> {
    info: StateRootHashInfo,
    sigs: SCT,
}
