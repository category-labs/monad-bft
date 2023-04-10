use std::collections::HashMap;

use monad_crypto::secp256k1::PubKey;
use monad_types::Round;
use monad_types::{Hash, NodeId};
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
use crate::validation::message::{well_formed_proposal, well_formed_timeout};
use crate::validation::signing::{Signed, Unverified, Verified};

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

// Extract the PubKey from the Signature if possible
fn get_pubkey(msg: &[u8], sig: &ConsensusSignature) -> Result<PubKey, Error> {
    sig.0
        .recover_pubkey(msg)
        .map_err(|_| Error::InvalidSignature)
}

// A verified proposal is one which is well-formed and has valid
// signatures for the present TC or QC
pub fn verify_proposal<H, T>(
    validators: &ValidatorMember,
    p: Unverified<ProposalMessage<T>>,
) -> Result<Verified<ProposalMessage<T>>, Error>
where
    T: SignatureCollection,
    H: Hasher,
{
    well_formed_proposal(&p)?;
    let msg = H::hash_object(&p.0.obj);
    verify_author(validators, &msg, &p.0.author_signature)?;
    verify_certificates::<H, _>(validators, &p.0.obj.last_round_tc, &p.0.obj.block.qc)?;

    let result = Verified(Signed {
        obj: p.0.obj,
        author: p.0.author,
        author_signature: p.0.author_signature,
    });

    Ok(result)
}

// A verified vote message has a valid signature
// Return type must keep the signature with the message as it is used later by the protocol
pub fn verify_vote_message<H>(
    validators: &ValidatorMember,
    v: Unverified<VoteMessage>,
) -> Result<Verified<VoteMessage>, Error>
where
    H: Hasher,
{
    let msg = H::hash_object(&v.0.obj.ledger_commit_info);

    get_pubkey(&msg, &v.0.author_signature)?
        .valid_pubkey(validators)?
        .verify(&msg, &v.0.author_signature.0)
        .map_err(|_| Error::InvalidSignature)?;

    let result = Verified(Signed {
        obj: v.0.obj,
        author: v.0.author,
        author_signature: v.0.author_signature,
    });

    Ok(result)
}

pub fn verify_timeout_message<H, T>(
    validators: &ValidatorMember,
    t: Unverified<TimeoutMessage<T>>,
) -> Result<Verified<TimeoutMessage<T>>, Error>
where
    H: Hasher,
    T: SignatureCollection,
{
    well_formed_timeout(&t)?;
    let msg = H::hash_object(&t.0.obj);
    verify_author(validators, &msg, &t.0.author_signature)?;
    verify_certificates::<H, _>(validators, &t.0.obj.last_round_tc, &t.0.obj.tminfo.high_qc)?;

    let result = Verified(Signed {
        obj: t.0.obj,
        author: t.0.author,
        author_signature: t.0.author_signature,
    });
    Ok(result)
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
                h.update(a.0.obj.qc_round);

                (h.hash(), &a.0.author_signature)
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
    msg: &Hash,
    sig: &ConsensusSignature,
) -> Result<(), Error> {
    get_pubkey(msg, sig)?
        .valid_pubkey(validators)?
        .verify(msg, &sig.0)
        .map_err(|_| Error::InvalidSignature)?;
    Ok(())
}
