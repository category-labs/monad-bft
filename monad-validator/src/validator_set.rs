use std::{
    collections::{HashMap, HashSet},
    error, fmt,
};

use monad_proto::{
    error::ProtoError,
    proto::validator_set::{ProtoValidatorSetData, ValidatorMapEntry},
};
use monad_types::{NodeId, Stake};

pub type Result<T> = std::result::Result<T, ValidatorSetError>;

#[derive(Debug)]
pub enum ValidatorSetError {
    DuplicateValidator(NodeId),
}

impl fmt::Display for ValidatorSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateValidator(node_id) => write!(f, "Duplicate NodeId: {:?}", node_id),
        }
    }
}

impl error::Error for ValidatorSetError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

pub trait ValidatorSetType {
    fn new(validators: Vec<(NodeId, Stake)>) -> Result<Self>
    where
        Self: Sized;
    fn get_members(&self) -> &HashMap<NodeId, Stake>;
    fn get_list(&self) -> &Vec<NodeId>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn is_member(&self, addr: &NodeId) -> bool;
    fn has_super_majority_votes<'a, I>(&self, addrs: I) -> bool
    where
        I: IntoIterator<Item = &'a NodeId>;
    fn has_honest_vote(&self, addrs: &[NodeId]) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorData(pub Vec<(NodeId, Stake)>);

#[derive(Debug)]
pub struct ValidatorSet {
    validators: HashMap<NodeId, Stake>,
    validator_list: Vec<NodeId>,
    total_stake: Stake,
}

impl From<&ValidatorData> for ProtoValidatorSetData {
    fn from(value: &ValidatorData) -> Self {
        let vlist = value
            .0
            .iter()
            .map(|(node, stake)| ValidatorMapEntry {
                key: Some(node.into()),
                value: Some(stake.into()),
            })
            .collect::<Vec<ValidatorMapEntry>>();
        Self { validators: vlist }
    }
}

impl TryFrom<ProtoValidatorSetData> for ValidatorData {
    type Error = ProtoError;
    fn try_from(value: ProtoValidatorSetData) -> std::result::Result<Self, Self::Error> {
        let mut vlist = ValidatorData(Vec::new());
        for v in value.validators {
            let a = v
                .key
                .ok_or(Self::Error::MissingRequiredField(
                    "ValildatorMapEntry.key".to_owned(),
                ))?
                .try_into()?;
            let b = v
                .value
                .ok_or(Self::Error::MissingRequiredField(
                    "ValildatorMapEntry.value".to_owned(),
                ))?
                .try_into()?;

            vlist.0.push((a, b));
        }

        Ok(vlist)
    }
}

impl ValidatorSetType for ValidatorSet {
    fn new(validators: Vec<(NodeId, Stake)>) -> Result<Self> {
        let mut vmap = HashMap::new();
        let mut vlist = Vec::new();
        for (node_id, stake) in validators.into_iter() {
            let entry = vmap.entry(node_id).or_insert(stake);
            if *entry != stake {
                return Err(ValidatorSetError::DuplicateValidator(node_id));
            }
            vlist.push(node_id);
        }

        vlist.sort();

        let total_stake: Stake = vmap.values().copied().sum();

        Ok(ValidatorSet {
            validators: vmap,
            validator_list: vlist,
            total_stake,
        })
    }

    fn get_members(&self) -> &HashMap<NodeId, Stake> {
        &self.validators
    }

    fn get_list(&self) -> &Vec<NodeId> {
        &self.validator_list
    }

    fn len(&self) -> usize {
        self.validator_list.len()
    }

    fn is_empty(&self) -> bool {
        self.validator_list.is_empty()
    }

    fn is_member(&self, addr: &NodeId) -> bool {
        self.validators.contains_key(addr)
    }

    fn has_super_majority_votes<'a, I>(&self, addrs: I) -> bool
    where
        I: IntoIterator<Item = &'a NodeId>,
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

    fn has_honest_vote(&self, addrs: &[NodeId]) -> bool {
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
    use monad_crypto::secp256k1::KeyPair;
    use monad_testutil::signing::{create_keys, get_key};
    use monad_types::{NodeId, Stake};

    use super::ValidatorSet;
    use crate::validator_set::ValidatorSetType;

    #[test]
    fn test_membership() {
        let seed1 = 7_u64;
        let seed2 = 8_u64;
        let keypair1 = get_key(seed1);

        let v1 = (NodeId(keypair1.pubkey()), Stake(1));
        let v1_ = (NodeId(keypair1.pubkey()), Stake(2));

        let keypair2 = get_key(seed2);

        let v2 = (NodeId(keypair2.pubkey()), Stake(2));

        let validators_duplicate = vec![v1, v1_];
        let _vs_err = ValidatorSet::new(validators_duplicate).unwrap_err();

        let validators = vec![v1, v2];
        let vs = ValidatorSet::new(validators).unwrap();
        assert!(vs.is_member(&NodeId(keypair1.pubkey())));

        let mut pkey3: [u8; 32] = [102; 32];
        let pubkey3 = KeyPair::from_bytes(&mut pkey3).unwrap().pubkey();
        assert!(!vs.is_member(&NodeId(pubkey3)));
    }

    #[test]
    fn test_super_maj() {
        let keypairs = create_keys(3);

        let v1 = (NodeId(keypairs[0].pubkey()), Stake(1));
        let v2 = (NodeId(keypairs[1].pubkey()), Stake(3));

        let pubkey3 = keypairs[2].pubkey();

        let validators = vec![v1, v2];
        let vs = ValidatorSet::new(validators).unwrap();
        assert!(vs.has_super_majority_votes(&[v2.0]));
        assert!(!vs.has_super_majority_votes(&[v1.0]));
        assert!(vs.has_super_majority_votes(&[v2.0, NodeId(pubkey3)]));
        assert!(!vs.has_super_majority_votes(&[v1.0, NodeId(pubkey3)]));
        // Address(3) is a non-member
    }

    #[test]
    fn test_honest_vote() {
        let keypairs = create_keys(2);

        let v1 = (NodeId(keypairs[0].pubkey()), Stake(1));
        let v2 = (NodeId(keypairs[1].pubkey()), Stake(2));

        let validators = vec![v1, v2];
        let vs = ValidatorSet::new(validators).unwrap();
        assert!(!vs.has_honest_vote(&[v1.0]));
        assert!(vs.has_honest_vote(&[v2.0]));
    }
}
