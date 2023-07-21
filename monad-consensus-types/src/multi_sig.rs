use std::collections::{HashMap, HashSet};

use monad_crypto::{bls12_381::BlsPubKey, secp256k1::PubKey, GenericSignature, Signature};
use monad_types::{Hash, NodeId};

use crate::{
    signature::{SignatureBuilder, SignatureCollection},
    validation::{Hashable, Hasher, Sha256Hash},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiSig<S> {
    pub sigs: Vec<S>,
}

impl<S: Signature + GenericSignature + Hashable> Default for MultiSig<S> {
    fn default() -> Self {
        Self { sigs: Vec::new() }
    }
}

#[derive(Debug)]
pub enum MultiSigError<S> {
    NodeIdIdxOutOfRange((usize, S)),
    ConflictingSignatures((NodeId, S, S)),
    NodeIdPubKeyMismatch((NodeId, PubKey, S)),
    // verifications error
    NodeIdNotExist((NodeId, S)),
    FailedToRecoverPubKey(S),
    InvalidSignature((NodeId, S)),
}

impl<S: Signature + GenericSignature + Hashable> std::fmt::Display for MultiSigError<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            MultiSigError::NodeIdIdxOutOfRange((idx, sig)) => {
                write!(f, "NodeIdx {idx:?} out of range Sig ({sig:?})")
            }
            MultiSigError::ConflictingSignatures((node_id, s1, s2)) => {
                write!(
                    f,
                    "Conflicting signatures from {node_id:?}\ns1: {s1:?}\ns2: {s2:?}"
                )
            }
            MultiSigError::NodeIdPubKeyMismatch((node_id, pubkey, sig)) => {
                write!(f, "NodeId pubkey mismatch: node_id ({node_id:?}) pubkey ({pubkey:?}) sig ({sig:?})")
            }
            MultiSigError::NodeIdNotExist((nodeid, sig)) => {
                write!(f, "NodeId {nodeid:?} doesn't exist sig ({sig:?})")
            }
            MultiSigError::FailedToRecoverPubKey(sig) => {
                write!(f, "Failed to recover pubkey sig ({sig:?})")
            }
            MultiSigError::InvalidSignature((node_id, sig)) => {
                write!(f, "Invalid signature nodeId ({node_id:?}) sig ({sig:?})")
            }
        }
    }
}

impl<S: Signature + GenericSignature + Hashable> std::error::Error for MultiSigError<S> {}

// TODO: consider removing the Signature trait bound - use the pubkey from the validator set to verify
// TODO: test duplicate votes
impl<S: Signature + GenericSignature + Hashable> SignatureCollection for MultiSig<S> {
    type SignatureError = MultiSigError<S>;
    type SignatureType = S;

    fn new(
        sigs: SignatureBuilder<Self>,
        validator_list: &[(NodeId, BlsPubKey)],
        msg: &[u8],
    ) -> Result<Self, Self::SignatureError> {
        let mut sig_map = HashMap::new();
        let mut multi_sig = Vec::new();

        for (idx, sig) in sigs {
            if idx >= validator_list.len() {
                return Err(MultiSigError::NodeIdIdxOutOfRange((idx, sig)));
            }
            let sig_entry = sig_map.entry(idx).or_insert(sig);
            if *sig_entry != sig {
                return Err(MultiSigError::ConflictingSignatures((
                    validator_list[idx].0,
                    *sig_entry,
                    sig,
                )));
            }
            let pubkey = sig
                .recover_pubkey(msg)
                .map_err(|_| MultiSigError::FailedToRecoverPubKey(sig))?;
            if NodeId(pubkey) != validator_list[idx].0 {
                return Err(MultiSigError::NodeIdPubKeyMismatch((
                    validator_list[idx].0,
                    pubkey,
                    sig,
                )));
            }
            <S as Signature>::verify(&sig, msg, &pubkey)
                .map_err(|_| MultiSigError::InvalidSignature((NodeId(pubkey), sig)))?;
            multi_sig.push(sig);
        }
        Ok(MultiSig { sigs: multi_sig })
    }

    fn get_hash(&self) -> Hash {
        let mut hasher = Sha256Hash::new();

        for s in self.sigs.iter() {
            <S as Hashable>::hash(s, &mut hasher);
        }

        hasher.hash()
    }

    fn num_signatures(&self) -> usize {
        self.sigs.len()
    }

    fn verify(
        &self,
        validator_list: &[(NodeId, BlsPubKey)],
        msg: &[u8],
    ) -> Result<Vec<NodeId>, Self::SignatureError> {
        let mut node_ids = Vec::new();
        let validator_set: HashSet<_> = validator_list.iter().map(|(node_id, _)| node_id).collect();

        for sig in self.sigs.iter() {
            let pubkey = sig
                .recover_pubkey(msg)
                .map_err(|_| MultiSigError::FailedToRecoverPubKey(*sig))?;
            let node_id = NodeId(pubkey);
            if !validator_set.contains(&node_id) {
                return Err(MultiSigError::NodeIdNotExist((node_id, *sig)));
            }
            <S as Signature>::verify(sig, msg, &pubkey)
                .map_err(|_| MultiSigError::InvalidSignature((node_id, *sig)))?;

            node_ids.push(NodeId(pubkey));
        }
        Ok(node_ids)
    }
}
