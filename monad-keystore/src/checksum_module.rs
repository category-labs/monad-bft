use monad_crypto::hasher::{Hasher, Sha256Hash};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub enum ChecksumHash {
    SHA256,
}

#[derive(Debug)]
pub enum ChecksumError {
    FailedChecksumVerification,
}

impl ChecksumHash {
    pub fn generate_checksum(&self, key: &[u8], cipher_message: &[u8]) -> Vec<u8> {
        assert!(key.len() == 32);

        match self {
            ChecksumHash::SHA256 => {
                let mut to_hash = key[16..].to_vec();
                to_hash.extend(cipher_message.to_owned());

                let mut sha256 = Sha256Hash::new();
                sha256.update(to_hash);
                sha256.hash().0.to_vec()
            }
        }
    }

    pub fn verify_checksum(
        &self,
        key: &[u8],
        cipher_message: &[u8],
        checksum: &[u8],
    ) -> Result<(), ChecksumError> {
        let expected_checksum = self.generate_checksum(key, cipher_message);

        (expected_checksum == checksum)
            .then_some(())
            .ok_or(ChecksumError::FailedChecksumVerification)
    }
}

#[cfg(test)]
mod test {
    use crate::checksum_module::ChecksumHash;

    #[test]
    fn test_generate_checksum() {
        let checksum_hash = ChecksumHash::SHA256;
        let key = [0u8; 32];
        let ciphertext = hex::decode("1234").unwrap();

        let checksum = checksum_hash.generate_checksum(&key, &ciphertext);
        let expected_checksum = "a0fec0e194fac478497c7e4b2279bb19379a15357e90f251ef78658bf592fabd";

        assert!(hex::encode(checksum) == expected_checksum);
    }
}
