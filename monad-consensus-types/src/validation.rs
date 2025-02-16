#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Message is signed by an author not in the validator set
    InvalidAuthor,
    /// Message does not contain the proper QC or TC values
    NotWellFormed,
    /// Bad signature
    InvalidSignature,
    /// There are high qc rounds larger than the TC round
    InvalidTcRound,
    /// The SignatureCollection doesn't have supermajority of the stake signed
    InsufficientStake,
    /// Required validator set (or cert pubkeys) not in validators epoch mapping
    ValidatorSetDataUnavailable,
    /// Vote does not contain a valid commit condition
    InvalidVote,
    /// Consensus Message version must match
    InvalidVersion,
    /// Epoch number in message doesn't match local records
    InvalidEpoch,
}
