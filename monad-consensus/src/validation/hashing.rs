use crate::Hash;
use sha2::Digest;

pub trait Hashable<'a> {
    type DataIter: Iterator<Item = &'a [u8]>;

    fn msg_parts(&self) -> Self::DataIter;
}

pub trait Hasher {
    fn hash_object<'a, T: Hashable<'a>>(&self, o: T) -> Hash;
}

pub struct Sha256Hash;
impl Hasher for Sha256Hash {
    fn hash_object<'a, T: Hashable<'a>>(&self, o: T) -> Hash {
        let mut hasher = sha2::Sha256::new();

        for f in o.msg_parts() {
            hasher.update(f);
        }
        hasher.finalize().into()
    }
}
