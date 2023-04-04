use std::collections::HashMap;

use crate::types::message::ProposalMessage;
use crate::types::message::TimeoutMessage;
use crate::types::message::VoteMessage;
use crate::types::quorum_certificate::QuorumCertificate;
use crate::types::signature::ConsensusSignature;
use crate::types::timeout::TimeoutCertificate;
use crate::types::voting::VotingQuorum;
use crate::validation::error::Error;
use crate::validation::hashing::Hasher;
use crate::validation::message::{well_formed_proposal, well_formed_timeout};
use crate::validation::signing::Signed;
use crate::validation::signing::Verified;
use crate::Hash;
use monad_crypto::secp256k1::PubKey;
use monad_validator::validator::Validator;

type ValidatorSet = HashMap<PubKey, Validator>;

trait ValidatorPubKey {
    // PubKey is valid if it is in the validator set
    fn valid_pubkey(self, validators: &ValidatorSet) -> Result<Self, Error>
    where
        Self: Sized;
}

impl ValidatorPubKey for PubKey {
    fn valid_pubkey(self, validators: &ValidatorSet) -> Result<Self, Error> {
        // TODO: fix the Address type from monad-validators
        if validators.contains_key(&self) {
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
    h: H,
    validators: &ValidatorSet,
    p: Signed<ProposalMessage<T>>,
) -> Result<Verified<Signed<ProposalMessage<T>>>, Error>
where
    T: VotingQuorum,
    H: Hasher,
{
    well_formed_proposal(&p)?;
    let msg = h.hash_object(&p.obj);
    verify_author(validators, &msg, &p.author_signature)?;
    verify_certificates(&h, validators, &p.obj.last_round_tc, &p.obj.block.qc)?;

    Ok(Verified { obj: p })
}

// A verified vote message has a valid signature
// Return type must keep the signature with the message as it is used later by the protocol
pub fn verify_vote_message<H>(
    h: H,
    validators: &ValidatorSet,
    v: Signed<VoteMessage>,
) -> Result<Verified<Signed<VoteMessage>>, Error>
where
    H: Hasher,
{
    let msg = h.hash_object(&v.obj);

    get_pubkey(&msg, &v.author_signature)?
        .valid_pubkey(validators)?
        .verify(&msg, &v.author_signature.0)
        .map_err(|_| Error::InvalidSignature)?;

    Ok(Verified { obj: v })
}

pub fn verify_timeout_message<H, T>(
    h: H,
    validators: &ValidatorSet,
    t: Signed<TimeoutMessage<T>>,
) -> Result<Verified<Signed<TimeoutMessage<T>>>, Error>
where
    H: Hasher,
    T: VotingQuorum,
{
    well_formed_timeout(&t)?;
    let msg = h.hash_object(&t.obj);
    verify_author(validators, &msg, &t.author_signature)?;
    verify_certificates(&h, validators, &t.obj.last_round_tc, &t.obj.tminfo.high_qc)?;

    Ok(Verified { obj: t })
}

fn verify_certificates<H, V>(
    h: &H,
    validators: &ValidatorSet,
    tc: &Option<TimeoutCertificate>,
    qc: &QuorumCertificate<V>,
) -> Result<(), Error>
where
    H: Hasher,
    V: VotingQuorum,
{
    let msg_sig = if let Some(tc) = tc {
        tc.high_qc_rounds
            .iter()
            .map(|a| (h.hash_object(&a.obj), &a.author_signature))
            .collect::<Vec<(Hash, &ConsensusSignature)>>()
    } else {
        qc.signatures
            .get_signatures()
            .into_iter()
            .map(|s| (qc.signature_hash, s))
            .collect::<Vec<(Hash, &ConsensusSignature)>>()
    };

    for i in msg_sig {
        get_pubkey(&i.0, i.1)?
            .valid_pubkey(validators)?
            .verify(&i.0, &i.1 .0)
            .map_err(|_| Error::InvalidSignature)?;
    }
    Ok(())
}

fn verify_author(
    validators: &ValidatorSet,
    msg: &Hash,
    sig: &ConsensusSignature,
) -> Result<(), Error> {
    get_pubkey(msg, sig)?
        .valid_pubkey(validators)?
        .verify(msg, &sig.0)
        .map_err(|_| Error::InvalidSignature)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::validation::hashing::*;
    use monad_testutil::signing::get_key;
    use monad_testutil::signing::MockSignatures;
    use monad_testutil::signing::Signer;

    use crate::types::block::{Block, TransactionList};
    use crate::types::message::ProposalMessage;
    use crate::types::quorum_certificate::QuorumCertificate;
    use crate::*;

    use monad_validator::validator::{Address, Validator};

    use super::{verify_proposal, ValidatorSet};

    fn setup_block(author: NodeId, block_round: u64, qc_round: u64) -> Block<MockSignatures> {
        let txns = TransactionList(vec![1, 2, 3, 4]);
        let round = Round(block_round);
        let mut qc = QuorumCertificate::<MockSignatures>::new(Default::default(), MockSignatures());
        qc.info.vote.round = Round(qc_round);

        Block::<MockSignatures>::new(author, round, &txns, &qc)
    }

    #[test]
    fn test_proposal_hash() {
        let mut vset = ValidatorSet::new();

        let author = NodeId(12);
        let proposal = ProposalMessage {
            block: setup_block(author, 234, 233),
            last_round_tc: None,
        };

        let keypair = get_key("a");

        vset.insert(
            keypair.pubkey(),
            Validator {
                address: Address(author.0 as i64),
                pubkey: keypair.pubkey(),
                stake: 0,
            },
        );

        let hasher = Sha256Hash;
        let msg = hasher.hash_object(&proposal);
        let sp = Signer::sign_object(proposal, &msg, keypair);

        assert!(verify_proposal(Sha256Hash, &vset, sp).is_ok());
    }

    #[test]
    fn test_proposal_missing_tc() {
        let mut vset = ValidatorSet::new();

        let author = NodeId(12);
        let proposal = ProposalMessage {
            block: setup_block(author, 234, 232),
            last_round_tc: None,
        };

        let keypair = get_key("6");

        vset.insert(
            keypair.pubkey(),
            Validator {
                address: Address(author.0 as i64),
                pubkey: keypair.pubkey(),
                stake: 0,
            },
        );

        let hasher = Sha256Hash;
        let msg = hasher.hash_object(&proposal);
        let sp = Signer::sign_object(proposal, &msg, keypair);

        assert!(verify_proposal(Sha256Hash, &vset, sp).is_err());
    }

    #[test]
    fn test_proposal_invalid_qc() {
        let mut vset = ValidatorSet::new();

        let author = NodeId(12);
        let proposal = ProposalMessage {
            block: setup_block(author, 234, 233),
            last_round_tc: None,
        };

        let keypair = get_key("6");

        vset.insert(
            keypair.pubkey(),
            Validator {
                address: Address(author.0 as i64),
                pubkey: keypair.pubkey(),
                stake: 0,
            },
        );

        let hasher = Sha256Hash;
        let msg = hasher.hash_object(&proposal);
        let sp = Signer::sign_object(proposal, &msg, get_key("7"));

        assert!(verify_proposal(Sha256Hash, &vset, sp).is_err());
    }
}
