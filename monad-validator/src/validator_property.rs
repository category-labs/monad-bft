use std::collections::{HashMap, HashSet};

use monad_crypto::bls12_381::BlsPubKey;
use monad_types::NodeId;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ValidatorProperty {
    pub blspubkey: BlsPubKey,
    pub index: usize,
}

pub trait ValidatorSetPropertyType: Sized {
    fn new(validators: Vec<(NodeId, BlsPubKey)>) -> Result<Self, ValidatorSetPropertyError>;

    fn get_property(&self, node_id: &NodeId) -> Option<&ValidatorProperty>;
    fn get_voting_list(&self) -> &[(NodeId, BlsPubKey)];
    fn get_blspubkey(&self, node_id: &NodeId) -> Option<&BlsPubKey>;
    fn get_index(&self, node_id: &NodeId) -> Option<usize>;
}

#[derive(Debug)]
pub enum ValidatorSetPropertyError {
    DuplicateNodeId(NodeId),
    DuplicateBlsPubKey(BlsPubKey),
}

#[derive(Debug)]
pub struct ValidatorSetProperty {
    map: HashMap<NodeId, ValidatorProperty>,
    list: Vec<(NodeId, BlsPubKey)>,
}

impl ValidatorSetPropertyType for ValidatorSetProperty {
    fn new(mut validators: Vec<(NodeId, BlsPubKey)>) -> Result<Self, ValidatorSetPropertyError> {
        validators.sort_by(|a, b| a.0.cmp(&b.0));

        let mut blskey_set = HashSet::new();

        let mut map = HashMap::new();
        for (index, (node_id, blspubkey)) in validators.iter().enumerate() {
            let property = ValidatorProperty {
                blspubkey: *blspubkey,
                index,
            };
            let old_property = map.insert(*node_id, property);
            if old_property.is_some() {
                return Err(ValidatorSetPropertyError::DuplicateNodeId(*node_id));
            }

            let bls_exist = !blskey_set.insert(*blspubkey);
            if bls_exist {
                return Err(ValidatorSetPropertyError::DuplicateBlsPubKey(*blspubkey));
            }
        }

        Ok(Self {
            map,
            list: validators,
        })
    }

    fn get_voting_list(&self) -> &[(NodeId, BlsPubKey)] {
        &self.list
    }

    fn get_property(&self, node_id: &NodeId) -> Option<&ValidatorProperty> {
        self.map.get(node_id)
    }

    fn get_blspubkey(&self, node_id: &NodeId) -> Option<&BlsPubKey> {
        self.get_property(node_id).map(|p| &p.blspubkey)
    }

    fn get_index(&self, node_id: &NodeId) -> Option<usize> {
        self.get_property(node_id).map(|p| p.index)
    }
}

#[cfg(test)]
mod test {
    use monad_crypto::GenericKeyPair;
    use monad_testutil::signing::{create_keys, create_keys_bls, get_key, get_key_bls};
    use monad_types::NodeId;

    use super::{ValidatorSetProperty, ValidatorSetPropertyType};
    use crate::validator_property::ValidatorSetPropertyError;

    #[test]
    fn test_duplicate_nodeid() {
        let keypair = get_key(5);
        let node_id = NodeId(keypair.pubkey());

        let blskeypairs = create_keys_bls(2);

        let property_list = std::iter::repeat(node_id)
            .zip(blskeypairs.iter().map(|kp| kp.pubkey()))
            .collect::<Vec<_>>();

        let result = ValidatorSetProperty::new(property_list);
        assert!(matches!(
            result,
            Err(ValidatorSetPropertyError::DuplicateNodeId(_))
        ))
    }

    #[test]
    fn test_duplicate_blskey() {
        let keypairs = create_keys(2);
        let blskeypair = get_key_bls(6);
        let blspubkey = blskeypair.pubkey();

        let property_list = keypairs
            .iter()
            .map(|kp| NodeId(kp.pubkey()))
            .zip(std::iter::repeat(blspubkey))
            .collect::<Vec<_>>();

        let result = ValidatorSetProperty::new(property_list);
        assert!(matches!(
            result,
            Err(ValidatorSetPropertyError::DuplicateBlsPubKey(_))
        ));
    }
}
