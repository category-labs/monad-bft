use monad_consensus::{
    types::message::VoteMessage,
    validation::signing::{Unverified, Verified},
};
use monad_crypto::secp256k1::SecpSignature;

pub mod basic;
pub mod ledger;
pub mod message;
pub mod quorum_certificate;
pub mod signing;
pub mod voting;

type VerifiedVoteMessage = Verified<SecpSignature, VoteMessage>;
type UnverifiedVoteMessage = Unverified<SecpSignature, VoteMessage>;
