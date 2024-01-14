use std::{
    collections::{HashMap, HashSet},
    error, fmt,
};

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

pub trait ValidatorSetType {
    type NodeIdPubKey: PubKey;

    fn new(
        validators: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>,
    ) -> Result<Self, Self::NodeIdPubKey>
    where
        Self: Sized;
    fn get_members(&self) -> &HashMap<NodeId<Self::NodeIdPubKey>, Stake>;
    fn get_list(&self) -> &Vec<NodeId<Self::NodeIdPubKey>>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn is_member(&self, addr: &NodeId<Self::NodeIdPubKey>) -> bool;
    fn has_super_majority_votes<'a, I>(&self, addrs: I) -> bool
    where
        I: IntoIterator<Item = &'a NodeId<Self::NodeIdPubKey>>;
    fn has_honest_vote(&self, addrs: &[NodeId<Self::NodeIdPubKey>]) -> bool;
}

#[derive(Debug)]
pub struct ValidatorSet<PT: PubKey> {
    validators: HashMap<NodeId<PT>, Stake>,
    validator_list: Vec<NodeId<PT>>,
    total_stake: Stake,
}

impl<PT: PubKey> ValidatorSetType for ValidatorSet<PT> {
    type NodeIdPubKey = PT;

    fn new(validators: Vec<(NodeId<PT>, Stake)>) -> Result<Self, PT> {
        let mut vmap = HashMap::new();
        let mut vlist = Vec::new();
        for (node_id, stake) in validators.into_iter() {
            let entry = vmap.entry(node_id).or_insert(stake);
            if *entry != stake {
                return Err(ValidatorSetError::DuplicateValidator(node_id));
            }

            if stake != Stake(0) {
                vlist.push(node_id);
            }
        }

        vlist.sort();

        let total_stake: Stake = vmap.values().copied().sum();

        Ok(ValidatorSet {
            validators: vmap,
            validator_list: vlist,
            total_stake,
        })
    }

    fn get_members(&self) -> &HashMap<NodeId<PT>, Stake> {
        &self.validators
    }

    fn get_list(&self) -> &Vec<NodeId<PT>> {
        &self.validator_list
    }

    fn len(&self) -> usize {
        self.validator_list.len()
    }

    fn is_empty(&self) -> bool {
        self.validator_list.is_empty()
    }

    fn is_member(&self, addr: &NodeId<PT>) -> bool {
        self.validators.contains_key(addr)
    }

    fn has_super_majority_votes<'a, I>(&self, addrs: I) -> bool
    where
        I: IntoIterator<Item = &'a NodeId<PT>>,
    {
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
}

#[cfg(test)]
mod test {
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        NopSignature,
    };
    use monad_testutil::signing::{create_keys, get_key};
    use monad_types::{NodeId, Stake};

    use super::ValidatorSet;
    use crate::validator_set::ValidatorSetType;

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
        let _vs_err = ValidatorSet::new(validators_duplicate).unwrap_err();

        let validators = vec![v1, v2];
        let vs = ValidatorSet::new(validators).unwrap();
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
        let vs = ValidatorSet::new(validators).unwrap();
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
        let vs = ValidatorSet::new(validators).unwrap();
        assert!(!vs.has_honest_vote(&[v1.0]));
        assert!(vs.has_honest_vote(&[v2.0]));
    }
}
