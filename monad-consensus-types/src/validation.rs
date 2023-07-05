use monad_types::Hash;
use sha2::Digest;

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
}

pub trait Hashable {
    fn hash<H: Hasher>(&self, state: &mut H);
}

pub trait Hasher: Sized {
    fn new() -> Self;
    fn update(&mut self, data: impl AsRef<[u8]>);
    fn hash(self) -> Hash;

    fn hash_object<T: Hashable>(obj: &T) -> Hash {
        let mut hasher = Self::new();
        obj.hash(&mut hasher);
        hasher.hash()
    }
}

pub struct Sha256Hash(sha2::Sha256);
impl Hasher for Sha256Hash {
    fn new() -> Self {
        Self(sha2::Sha256::new())
    }
    fn update(&mut self, data: impl AsRef<[u8]>) {
        self.0.update(data);
    }
    fn hash(self) -> Hash {
        Hash(self.0.finalize().into())
    }
}
