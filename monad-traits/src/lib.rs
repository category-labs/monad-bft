use std::error::Error;

pub trait Serializable {
    fn serialize(&self) -> Vec<u8>;
}

pub trait Deserializable: Sized {
    type ReadError: Error + Send + Sync;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError>;
}
