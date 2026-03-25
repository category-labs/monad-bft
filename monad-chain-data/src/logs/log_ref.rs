use bytes::Bytes;

use crate::error::Result;

/// Thin shell for the later zero-copy log view type.
///
/// Commit 3 exposes the real public type name and constructor without yet
/// committing to encoded field-access semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRef {
    buf: Bytes,
}

impl LogRef {
    pub fn new(buf: Bytes) -> Result<Self> {
        Ok(Self { buf })
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buf.as_ref()
    }

    pub fn into_bytes(self) -> Bytes {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::LogRef;

    #[test]
    fn log_ref_preserves_owned_bytes() {
        let log_ref = LogRef::new(Bytes::from(vec![1, 2, 3])).expect("construct log ref");

        assert_eq!(log_ref.as_bytes(), &[1, 2, 3]);
        assert_eq!(log_ref.into_bytes(), Bytes::from(vec![1, 2, 3]));
    }
}
