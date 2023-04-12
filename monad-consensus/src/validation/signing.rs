use std::collections::HashMap;
use std::ops::Deref;

use monad_crypto::secp256k1::PubKey;
use monad_types::Hash;
use monad_types::NodeId;
use monad_types::Round;
use monad_validator::validator::Validator;

use crate::types::message::ProposalMessage;
use crate::types::message::TimeoutMessage;
use crate::types::message::VoteMessage;
use crate::types::quorum_certificate::QuorumCertificate;
use crate::types::signature::ConsensusSignature;
use crate::types::signature::SignatureCollection;
use crate::types::timeout::TimeoutCertificate;
use crate::validation::error::Error;
use crate::validation::hashing::Hasher;

use crate::validation::message::well_formed;

#[derive(Clone, Debug)]
pub struct Verified<M> {
    obj: M,
    author: NodeId,
    author_signature: ConsensusSignature,
}

impl<M> Verified<M> {
    pub fn author(&self) -> &NodeId {
        &self.author
    }
    pub fn author_signature(&self) -> &ConsensusSignature {
        &self.author_signature
    }
}

impl<M> Deref for Verified<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.obj
    }
}

#[derive(Clone, Debug)]
pub struct Unverified<M> {
    obj: M,
    author_signature: ConsensusSignature,
}

impl<M> Unverified<M> {
    pub fn new(obj: M, signature: ConsensusSignature) -> Self {
        Self {
            obj,
            author_signature: signature,
        }
    }

    pub fn author_signature(&self) -> &ConsensusSignature {
        &self.author_signature
    }
}

impl<T: SignatureCollection> Unverified<ProposalMessage<T>> {
    // A verified proposal is one which is well-formed and has valid
    // signatures for the present TC or QC
    pub fn verify<H: Hasher>(
        self,
        validators: &ValidatorMember,
        sender: &PubKey,
    ) -> Result<Verified<ProposalMessage<T>>, Error> {
        self.well_formed_proposal()?;
        let msg = H::hash_object(&self.obj);
        let author = verify_author(validators, sender, &msg, &self.author_signature)?;
        verify_certificates::<H, _>(validators, &self.obj.last_round_tc, &self.obj.block.qc)?;

        let result = Verified {
            obj: self.obj,
            author: NodeId(author),
            author_signature: self.author_signature,
        };

        Ok(result)
    }

    fn well_formed_proposal(&self) -> Result<(), Error> {
        well_formed(
            self.obj.block.round,
            self.obj.block.qc.info.vote.round,
            &self.obj.last_round_tc,
        )
    }
}

impl Unverified<VoteMessage> {
    // A verified vote message has a valid signature
    // Return type must keep the signature with the message as it is used later by the protocol
    pub fn verify<H: Hasher>(
        self,
        validators: &ValidatorMember,
        sender: &PubKey,
    ) -> Result<Verified<VoteMessage>, Error> {
        let msg = H::hash_object(&self.obj.ledger_commit_info);

        let author = verify_author(validators, sender, &msg, &self.author_signature)?;

        let result = Verified {
            obj: self.obj,
            author: NodeId(author),
            author_signature: self.author_signature,
        };

        Ok(result)
    }
}

impl<T: SignatureCollection> Unverified<TimeoutMessage<T>> {
    pub fn verify<H: Hasher>(
        self,
        validators: &ValidatorMember,
        sender: &PubKey,
    ) -> Result<Verified<TimeoutMessage<T>>, Error> {
        self.well_formed_timeout()?;
        let msg = H::hash_object(&self.obj);
        let author = verify_author(validators, sender, &msg, &self.author_signature)?;
        verify_certificates::<H, _>(
            validators,
            &self.obj.last_round_tc,
            &self.obj.tminfo.high_qc,
        )?;

        let result = Verified {
            obj: self.obj,
            author: NodeId(author),
            author_signature: self.author_signature,
        };
        Ok(result)
    }

    fn well_formed_timeout(&self) -> Result<(), Error> {
        well_formed(
            self.obj.tminfo.round,
            self.obj.tminfo.high_qc.info.vote.round,
            &self.obj.last_round_tc,
        )
    }
}

fn verify_certificates<H, V>(
    validators: &ValidatorMember,
    tc: &Option<TimeoutCertificate>,
    qc: &QuorumCertificate<V>,
) -> Result<(), Error>
where
    H: Hasher,
    V: SignatureCollection,
{
    if qc.info.vote.round == Round(0) {
        // FIXME lol
        return Ok(());
    }

    let msg_sig = if let Some(tc) = tc {
        tc.high_qc_rounds
            .iter()
            .map(|a| {
                // TODO fix this..
                let mut h = H::new();
                h.update(tc.round);
                h.update(a.0.qc_round);

                (h.hash(), &a.1)
            })
            .collect::<Vec<(Hash, &ConsensusSignature)>>()
    } else {
        qc.signatures
            .get_signatures()
            .into_iter()
            .map(|s| (H::hash_object(&qc.info.ledger_commit), s))
            .collect::<Vec<(Hash, &ConsensusSignature)>>()
    };

    for (hash, sig) in msg_sig {
        get_pubkey(&hash, sig)?
            .valid_pubkey(validators)?
            .verify(&hash, &sig.0)
            .map_err(|_| Error::InvalidSignature)?;
    }
    Ok(())
}

fn verify_author(
    validators: &ValidatorMember,
    sender: &PubKey,
    msg: &Hash,
    sig: &ConsensusSignature,
) -> Result<PubKey, Error> {
    let pubkey = get_pubkey(msg, sig)?.valid_pubkey(validators)?;
    pubkey
        .verify(msg, &sig.0)
        .map_err(|_| Error::InvalidSignature)?;
    if sender != &pubkey {
        Err(Error::AuthorNotSender)
    } else {
        Ok(pubkey)
    }
}

// Extract the PubKey from the Signature if possible
fn get_pubkey(msg: &[u8], sig: &ConsensusSignature) -> Result<PubKey, Error> {
    sig.0
        .recover_pubkey(msg)
        .map_err(|_| Error::InvalidSignature)
}

pub type ValidatorMember = HashMap<NodeId, Validator>;

trait ValidatorPubKey {
    // PubKey is valid if it is in the validator set
    fn valid_pubkey(self, validators: &ValidatorMember) -> Result<Self, Error>
    where
        Self: Sized;
}

impl ValidatorPubKey for PubKey {
    fn valid_pubkey(self, validators: &ValidatorMember) -> Result<Self, Error> {
        if validators.contains_key(&NodeId(self)) {
            Ok(self)
        } else {
            Err(Error::InvalidAuthor)
        }
    }
}
