// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{collections::BTreeMap, error, fmt, marker::PhantomData};

use alloy_primitives::U256;
use as_any::AsAny;
use auto_impl::auto_impl;
use itertools::Itertools;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Stake};

pub type Result<T, PT> = std::result::Result<T, ValidatorSetError<PT>>;

#[derive(Debug, PartialEq, Eq)]
pub enum ValidatorSetError<PT>
where
    PT: PubKey,
{
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

    fn has_super_majority_votes(
        &self,
        addrs: &[NodeId<Self::NodeIdPubKey>],
    ) -> Result<bool, Self::NodeIdPubKey>;

    fn has_honest_vote(
        &self,
        addrs: &[NodeId<Self::NodeIdPubKey>],
    ) -> Result<bool, Self::NodeIdPubKey>;

    fn has_threshold_votes(
        &self,
        addrs: &[NodeId<Self::NodeIdPubKey>],
        threshold: Stake,
    ) -> Result<bool, Self::NodeIdPubKey>;

    fn calculate_current_stake(
        &self,
        addrs: &[NodeId<Self::NodeIdPubKey>],
    ) -> Result<Stake, Self::NodeIdPubKey>;
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
        let mut total_stake = Stake::ZERO;
        for (node_id, stake) in validators.into_iter() {
            assert!(stake > Stake::ZERO, "validator should have non-zero stake");

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

    fn has_super_majority_votes(
        &self,
        addrs: &[NodeId<Self::NodeIdPubKey>],
    ) -> Result<bool, Self::NodeIdPubKey> {
        self.has_threshold_votes(
            addrs,
            Stake(self.total_stake.0 * U256::from(2) / U256::from(3) + U256::ONE),
        )
    }

    fn has_honest_vote(&self, addrs: &[NodeId<PT>]) -> Result<bool, Self::NodeIdPubKey> {
        self.has_threshold_votes(addrs, Stake(self.total_stake.0 / U256::from(3) + U256::ONE))
    }

    fn has_threshold_votes(
        &self,
        addrs: &[NodeId<Self::NodeIdPubKey>],
        threshold: Stake,
    ) -> Result<bool, Self::NodeIdPubKey> {
        let voter_stake = self.calculate_current_stake(addrs)?;

        Ok(voter_stake >= threshold)
    }

    fn calculate_current_stake(
        &self,
        addrs: &[NodeId<Self::NodeIdPubKey>],
    ) -> Result<Stake, Self::NodeIdPubKey> {
        if let Some(node) = addrs.iter().duplicates().next() {
            return Err(ValidatorSetError::DuplicateValidator(*node));
        }

        Ok(addrs
            .iter()
            .filter_map(|addr| self.validators.get(addr))
            .cloned()
            .sum::<Stake>())
    }
}

#[cfg(test)]
mod test {
    use alloy_primitives::U256;
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        NopSignature,
    };
    use monad_testutil::signing::{create_keys, get_key};
    use monad_types::{NodeId, Stake};

    use crate::validator_set::{
        ValidatorSetError, ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory,
    };

    type SignatureType = NopSignature;
    type KeyPairType = <SignatureType as CertificateSignature>::KeyPairType;

    #[test]
    fn test_membership() {
        let seed1 = 7_u64;
        let seed2 = 8_u64;
        let keypair1 = get_key::<SignatureType>(seed1);

        let v1 = (NodeId::new(keypair1.pubkey()), Stake::ONE);
        let v1_ = (NodeId::new(keypair1.pubkey()), Stake::from(2));

        let keypair2 = get_key::<SignatureType>(seed2);

        let v2 = (NodeId::new(keypair2.pubkey()), Stake::from(2));

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

        let v1 = (NodeId::new(keypairs[0].pubkey()), Stake::ONE);
        let v2 = (NodeId::new(keypairs[1].pubkey()), Stake::from(3));

        let pubkey3 = keypairs[2].pubkey();

        let validators = vec![v1, v2];
        let vs = ValidatorSetFactory::default().create(validators).unwrap();
        assert!(vs.has_super_majority_votes(&[v2.0]).unwrap());
        assert!(!vs.has_super_majority_votes(&[v1.0]).unwrap());
        assert!(vs
            .has_super_majority_votes(&[v2.0, NodeId::new(pubkey3)])
            .unwrap());
        assert!(!vs
            .has_super_majority_votes(&[v1.0, NodeId::new(pubkey3)])
            .unwrap());
        // Address(3) is a non-member
    }

    #[test]
    fn test_honest_vote() {
        let keypairs = create_keys::<SignatureType>(2);

        let v1 = (NodeId::new(keypairs[0].pubkey()), Stake::ONE);
        let v2 = (NodeId::new(keypairs[1].pubkey()), Stake::from(2));

        let validators = vec![v1, v2];
        let vs = ValidatorSetFactory::default().create(validators).unwrap();
        assert!(!vs.has_honest_vote(&[v1.0]).unwrap());
        assert!(vs.has_honest_vote(&[v2.0]).unwrap());
    }

    #[test]
    fn test_threshold() {
        let keypairs = create_keys::<SignatureType>(3);

        let v1 = (NodeId::new(keypairs[0].pubkey()), Stake::from(2));
        let v2 = (NodeId::new(keypairs[1].pubkey()), Stake::from(3));

        let n3 = NodeId::new(keypairs[2].pubkey());

        let validators = vec![v1, v2];
        let vs = ValidatorSetFactory::default().create(validators).unwrap();
        let majority_threshold = Stake(vs.get_total_stake().0 / U256::from(2) + U256::ONE);
        assert!(vs.has_threshold_votes(&[v2.0], majority_threshold).unwrap());
        assert!(!vs.has_threshold_votes(&[v1.0], majority_threshold).unwrap());
        assert!(vs
            .has_threshold_votes(&[v2.0, n3], majority_threshold)
            .unwrap());
        assert!(!vs
            .has_threshold_votes(&[v1.0, n3], majority_threshold)
            .unwrap());
    }

    #[test]
    fn test_duplicates() {
        let keypairs = create_keys::<SignatureType>(3);

        let v1 = (NodeId::new(keypairs[0].pubkey()), Stake::from(2));
        let v2 = (NodeId::new(keypairs[1].pubkey()), Stake::from(3));

        let validators = vec![v1, v2];
        let vs = ValidatorSetFactory::default().create(validators).unwrap();
        assert_eq!(
            vs.calculate_current_stake(&[v2.0, v1.0]),
            Ok(Stake::from(5))
        );
        assert_eq!(
            vs.calculate_current_stake(&[v2.0, v2.0]),
            Err(ValidatorSetError::DuplicateValidator(v2.0))
        );
        assert_eq!(
            vs.calculate_current_stake(&[v2.0, v1.0, v2.0]),
            Err(ValidatorSetError::DuplicateValidator(v2.0))
        );
    }
}
