#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Message is signed by an author not in the validator set
    InvalidAuthor,
    /// Message does not contain the proper QC or TC values
    NotWellFormed,
    /// Bad signature
    InvalidSignature,
    /// Signature author doesn't match sender
    AuthorNotSender,
    /// There are high qc rounds larger than the TC round
    InvalidTcRound,
    /// The SignatureCollection doesn't have supermajority of the stake signed
    InsufficientStake,
    /// Seq num in block proposal must be 1 higher than in the QC
    InvalidSeqNum,
}
