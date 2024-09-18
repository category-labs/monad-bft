use std::{
    collections::{BTreeMap, HashSet},
    error, fmt,
    marker::PhantomData,
};

use as_any::AsAny;
use auto_impl::auto_impl;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Stake};

pub type Result<T, PT> = std::result::Result<T, ValidatorSetError<PT>>;

#[derive(Debug)]
pub enum ValidatorSetError<PT: PubKey> {
    DuplicateValidator(NodeId<PT>),
}

impl<PT: PubKey> fmt::Display for ValidatorSetError<PT> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateValidator(node_id) => write!(f, "Duplicate NodeId: {:?}", node_id),
        }
    }
}

impl<PT: PubKey> error::Error for ValidatorSetError<PT> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

pub trait ValidatorSetTypeFactory {
    type NodeIdPubKey: PubKey;
    type ValidatorSetType: ValidatorSetType<NodeIdPubKey = Self::NodeIdPubKey>;
    fn create(
        &self,
        validators: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>,
    ) -> Result<Self::ValidatorSetType, Self::NodeIdPubKey>;
}

/// Helper trait that's only used for dynamic dispatch boxing
/// This trait is necessary so that the ValidatorSetType associated type can be erased
trait ValidatorSetTypeFactoryHelper {
    type NodeIdPubKey: PubKey;

    fn create(
        &self,
        validators: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>,
    ) -> Result<Box<dyn ValidatorSetType<NodeIdPubKey = Self::NodeIdPubKey>>, Self::NodeIdPubKey>;
}

impl<T> ValidatorSetTypeFactoryHelper for T
where
    T: ValidatorSetTypeFactory + ?Sized,
    T::ValidatorSetType: Send + Sync + 'static,
{
    type NodeIdPubKey = T::NodeIdPubKey;

    fn create(
        &self,
        validators: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>,
    ) -> Result<Box<dyn ValidatorSetType<NodeIdPubKey = Self::NodeIdPubKey>>, Self::NodeIdPubKey>
    {
        let validator_set = self.create(validators)?;
        Ok(Box::new(validator_set))
    }
}

pub struct BoxedValidatorSetTypeFactory<PT: PubKey>(
    Box<dyn ValidatorSetTypeFactoryHelper<NodeIdPubKey = PT> + Send + Sync>,
);

impl<PT: PubKey> BoxedValidatorSetTypeFactory<PT> {
    pub fn new<T>(factory: T) -> Self
    where
        T: ValidatorSetTypeFactory<NodeIdPubKey = PT> + Send + Sync + 'static,
    {
        Self(Box::new(factory))
    }
}

impl<PT: PubKey> ValidatorSetTypeFactory for BoxedValidatorSetTypeFactory<PT> {
    type NodeIdPubKey = PT;
    type ValidatorSetType = Box<dyn ValidatorSetType<NodeIdPubKey = Self::NodeIdPubKey>>;

    fn create(
        &self,
        validators: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>,
    ) -> Result<Self::ValidatorSetType, Self::NodeIdPubKey> {
        self.0.create(validators)
    }
}

#[auto_impl(Box)]
pub trait ValidatorSetType: Send + Sync + AsAny {
    type NodeIdPubKey: PubKey;

    fn get_members(&self) -> &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>;
    fn get_total_stake(&self) -> Stake;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn is_member(&self, addr: &NodeId<Self::NodeIdPubKey>) -> bool;
    fn has_super_majority_votes(&self, addrs: &[NodeId<Self::NodeIdPubKey>]) -> bool;
    fn has_honest_vote(&self, addrs: &[NodeId<Self::NodeIdPubKey>]) -> bool;
    fn has_threshold_votes(&self, addrs: &[NodeId<Self::NodeIdPubKey>], threshold: Stake) -> bool;
    fn calculate_current_stake(&self, addrs: &[NodeId<Self::NodeIdPubKey>]) -> Stake;
}

#[derive(Clone, Copy)]
pub struct ValidatorSetFactory<PT: PubKey>(PhantomData<PT>);
impl<PT: PubKey> Default for ValidatorSetFactory<PT> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<PT: PubKey> ValidatorSetTypeFactory for ValidatorSetFactory<PT> {
    type NodeIdPubKey = PT;
    type ValidatorSetType = ValidatorSet<PT>;

    fn create(
        &self,
        validators: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>,
    ) -> Result<Self::ValidatorSetType, Self::NodeIdPubKey> {
        let mut vmap = BTreeMap::new();
        let mut total_stake = Stake(0);
        for (node_id, stake) in validators.into_iter() {
            // TODO disallow unstaked?
            let duplicate = vmap.insert(node_id, stake);
            if duplicate.is_some() {
                return Err(ValidatorSetError::DuplicateValidator(node_id));
            }
            total_stake += stake;
        }

        Ok(ValidatorSet {
            validators: vmap,
            total_stake,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ValidatorSet<PT: PubKey> {
    validators: BTreeMap<NodeId<PT>, Stake>,
    total_stake: Stake,
}

impl<PT: PubKey> ValidatorSetType for ValidatorSet<PT> {
    type NodeIdPubKey = PT;

    fn get_members(&self) -> &BTreeMap<NodeId<PT>, Stake> {
        &self.validators
    }

    fn get_total_stake(&self) -> Stake {
        self.total_stake
    }

    fn len(&self) -> usize {
        self.validators.len()
    }

    fn is_empty(&self) -> bool {
        self.validators.is_empty()
    }

    fn is_member(&self, addr: &NodeId<PT>) -> bool {
        self.validators.contains_key(addr)
    }

    fn has_super_majority_votes(&self, addrs: &[NodeId<Self::NodeIdPubKey>]) -> bool {
        let mut duplicates = HashSet::new();

        let mut voter_stake: Stake = Stake(0);
        for addr in addrs {
            if let Some(v) = self.validators.get(addr) {
                voter_stake += *v;
                debug_assert!(duplicates.insert(addr));
            }
        }
        voter_stake >= Stake(self.total_stake.0 * 2 / 3 + 1)
    }

    fn has_honest_vote(&self, addrs: &[NodeId<PT>]) -> bool {
        assert_eq!(addrs.iter().collect::<HashSet<_>>().len(), addrs.len());
        addrs
            .iter()
            .filter_map(|addr| self.validators.get(addr).copied())
            .sum::<Stake>()
            >= Stake(self.total_stake.0 / 3 + 1)
    }

    fn has_threshold_votes(&self, addrs: &[NodeId<Self::NodeIdPubKey>], threshold: Stake) -> bool {
        let mut duplicates = HashSet::new();

        let mut voter_stake: Stake = Stake(0);
        for addr in addrs {
            if let Some(v) = self.validators.get(addr) {
                voter_stake += *v;
                debug_assert!(duplicates.insert(addr));
            }
        }
        voter_stake >= threshold
    }

    fn calculate_current_stake(&self, addrs: &[NodeId<Self::NodeIdPubKey>]) -> Stake {
        let mut voter_stake: Stake = Stake(0);
        for addr in addrs {
            if let Some(v) = self.validators.get(addr) {
                voter_stake += *v;
            }
        }
        voter_stake
    }
}

#[cfg(test)]
mod test {
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        NopSignature,
    };
    use monad_testutil::signing::{create_keys, get_key};
    use monad_types::{NodeId, Stake};

    use crate::validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory};

    type SignatureType = NopSignature;
    type KeyPairType = <SignatureType as CertificateSignature>::KeyPairType;

    #[test]
    fn test_membership() {
        let seed1 = 7_u64;
        let seed2 = 8_u64;
        let keypair1 = get_key::<SignatureType>(seed1);

        let v1 = (NodeId::new(keypair1.pubkey()), Stake(1));
        let v1_ = (NodeId::new(keypair1.pubkey()), Stake(2));

        let keypair2 = get_key::<SignatureType>(seed2);

        let v2 = (NodeId::new(keypair2.pubkey()), Stake(2));

        let validators_duplicate = vec![v1, v1_];
        let _vs_err = ValidatorSetFactory::default()
            .create(validators_duplicate)
            .unwrap_err();

        let validators = vec![v1, v2];
        let vs = ValidatorSetFactory::default().create(validators).unwrap();
        assert!(vs.is_member(&NodeId::new(keypair1.pubkey())));

        let mut pkey3: [u8; 32] = [102; 32];
        let pubkey3 = KeyPairType::from_bytes(&mut pkey3).unwrap().pubkey();
        assert!(!vs.is_member(&NodeId::new(pubkey3)));
    }

    #[test]
    fn test_super_maj() {
        let keypairs = create_keys::<SignatureType>(3);

        let v1 = (NodeId::new(keypairs[0].pubkey()), Stake(1));
        let v2 = (NodeId::new(keypairs[1].pubkey()), Stake(3));

        let pubkey3 = keypairs[2].pubkey();

        let validators = vec![v1, v2];
        let vs = ValidatorSetFactory::default().create(validators).unwrap();
        assert!(vs.has_super_majority_votes(&[v2.0]));
        assert!(!vs.has_super_majority_votes(&[v1.0]));
        assert!(vs.has_super_majority_votes(&[v2.0, NodeId::new(pubkey3)]));
        assert!(!vs.has_super_majority_votes(&[v1.0, NodeId::new(pubkey3)]));
        // Address(3) is a non-member
    }

    #[test]
    fn test_honest_vote() {
        let keypairs = create_keys::<SignatureType>(2);

        let v1 = (NodeId::new(keypairs[0].pubkey()), Stake(1));
        let v2 = (NodeId::new(keypairs[1].pubkey()), Stake(2));

        let validators = vec![v1, v2];
        let vs = ValidatorSetFactory::default().create(validators).unwrap();
        assert!(!vs.has_honest_vote(&[v1.0]));
        assert!(vs.has_honest_vote(&[v2.0]));
    }

    #[test]
    fn test_threshold() {
        let keypairs = create_keys::<SignatureType>(3);

        let v1 = (NodeId::new(keypairs[0].pubkey()), Stake(2));
        let v2 = (NodeId::new(keypairs[1].pubkey()), Stake(3));

        let n3 = NodeId::new(keypairs[2].pubkey());

        let validators = vec![v1, v2];
        let vs = ValidatorSetFactory::default().create(validators).unwrap();
        let majority_threshold = Stake(vs.get_total_stake().0 / 2 + 1);
        assert!(vs.has_threshold_votes(&[v2.0], majority_threshold));
        assert!(!vs.has_threshold_votes(&[v1.0], majority_threshold));
        assert!(vs.has_threshold_votes(&[v2.0, n3], majority_threshold));
        assert!(!vs.has_threshold_votes(&[v1.0, n3], majority_threshold));
    }
}
