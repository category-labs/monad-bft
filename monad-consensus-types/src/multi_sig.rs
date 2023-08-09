use std::collections::{HashMap, HashSet};

use monad_types::{Hash, NodeId};

use crate::{
    certificate_signature::{CertificateSignature, CertificateSignatureRecoverable},
    signature_collection::SignatureCollection,
    validation::{Hashable, Hasher},
    voting::ValidatorMapping,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiSig<S> {
    pub sigs: Vec<S>,
}

impl<S: CertificateSignatureRecoverable> Default for MultiSig<S> {
    fn default() -> Self {
        Self { sigs: Vec::new() }
    }
}

#[derive(Debug)]
pub enum MultiSigError<S> {
    NodeIdNotInMapping((NodeId, S)),
    ConflictingSignatures((NodeId, S, S)),
    PubKeyMismatch((NodeId, S)),
    // verifications error
    FailedToRecoverPubKey(S),
    InvalidSignature(S),
}

impl<S: CertificateSignatureRecoverable> std::fmt::Display for MultiSigError<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            MultiSigError::NodeIdNotInMapping((node_id, sig)) => {
                write!(f, "NodeId {node_id:?} not in validator mapping sig {sig:?}")
            }
            MultiSigError::ConflictingSignatures((node_id, s1, s2)) => {
                write!(
                    f,
                    "Conflicting signatures from {node_id:?}\ns1: {s1:?}\ns2: {s2:?}"
                )
            }
            MultiSigError::PubKeyMismatch((node_id, sig)) => {
                write!(f, "PubKey mismatch NodeId {node_id:?} Sig {sig:?}")
            }
            MultiSigError::InvalidSignature(sig) => {
                write!(f, "Invalid signature ({sig:?})")
            }
            MultiSigError::FailedToRecoverPubKey(sig) => {
                write!(f, "Failed to recover pubkey sig ({sig:?})")
            }
        }
    }
}

impl<S: CertificateSignatureRecoverable> std::error::Error for MultiSigError<S> {}

impl<S: CertificateSignatureRecoverable> Hashable for MultiSig<S> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for s in self.sigs.iter() {
            <S as Hashable>::hash::<_>(s, state);
        }
    }
}

// TODO: test duplicate votes
impl<S: CertificateSignatureRecoverable> SignatureCollection for MultiSig<S> {
    type SignatureError = MultiSigError<S>;
    type SignatureType = S;

    fn new(
        sigs: Vec<(NodeId, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<
            <Self::SignatureType as CertificateSignature>::KeyPairType,
        >,
        msg: &[u8],
    ) -> Result<Self, Self::SignatureError> {
        let mut sig_map = HashMap::new();
        let mut multi_sig = Vec::new();

        for (node_id, sig) in sigs.into_iter() {
            if let Some(pubkey) = validator_mapping.map.get(&node_id) {
                let pubkey_recovered = sig
                    .recover_pubkey(msg)
                    .map_err(|_| MultiSigError::FailedToRecoverPubKey(sig))?;

                if pubkey_recovered != *pubkey {
                    return Err(MultiSigError::PubKeyMismatch((node_id, sig)));
                }

                let sig_entry = sig_map.entry(node_id).or_insert(sig);
                if *sig_entry != sig {
                    return Err(MultiSigError::ConflictingSignatures((
                        node_id, *sig_entry, sig,
                    )));
                }

                <S as CertificateSignature>::verify(&sig, msg, pubkey)
                    .map_err(|_| MultiSigError::InvalidSignature(sig))?;

                multi_sig.push(sig);
            } else {
                return Err(MultiSigError::NodeIdNotInMapping((node_id, sig)));
            }
        }

        Ok(MultiSig { sigs: multi_sig })
    }

    fn get_hash<H: Hasher>(&self) -> Hash {
        let mut hasher = H::new();
        self.hash(&mut hasher);
        hasher.hash()
    }

    fn num_signatures(&self) -> usize {
        self.sigs.len()
    }

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<
            <Self::SignatureType as CertificateSignature>::KeyPairType,
        >,
        msg: &[u8],
    ) -> Result<Vec<NodeId>, Self::SignatureError> {
        let mut node_ids = Vec::new();
        let mut pubkeys = HashSet::new();

        for sig in self.sigs.iter() {
            let pubkey = sig
                .recover_pubkey(msg)
                .map_err(|_| MultiSigError::FailedToRecoverPubKey(*sig))?;

            sig.verify(msg, &pubkey)
                .map_err(|_| MultiSigError::InvalidSignature(*sig))?;

            pubkeys.insert(pubkey);
        }

        for (node_id, pk) in validator_mapping.map.iter() {
            if pubkeys.contains(pk) {
                node_ids.push(*node_id);
                pubkeys.remove(pk);
            }
        }

        if !pubkeys.is_empty() {
            // TODO: collect evidence: author included signatures from nodes not in the validator set
        }
        Ok(node_ids)
    }
}
