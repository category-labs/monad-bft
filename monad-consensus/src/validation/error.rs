#[derive(Clone, Copy, Debug)]
pub enum Error {
    /// Message is signed by an author not in the validator set
    InvalidAuthor,
    /// Message does not contain the proper QC or TC values
    NotWellFormed,
    /// Bad signature
    InvalidSignature,
}
