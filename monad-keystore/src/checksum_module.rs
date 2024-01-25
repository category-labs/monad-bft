use monad_crypto::hasher::{Hasher, Sha256Hash};
use serde::Deserialize;

use super::hex_string::deserialize_bytes_from_hex_string;

#[derive(Debug, Deserialize, PartialEq)]
pub enum ChecksumHash {
    SHA256,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ChecksumModule {
    checksum_hash: ChecksumHash,
    #[serde(deserialize_with = "deserialize_bytes_from_hex_string")]
    checksum: Vec<u8>,
}

#[derive(Debug)]
pub enum ChecksumError {
    FailedChecksumVerification,
}

impl ChecksumModule {
    pub fn verify_checksum(
        &self,
        decryption_key: &Vec<u8>,
        cipher_message: &Vec<u8>,
    ) -> Result<(), ChecksumError> {
        assert!(decryption_key.len() == 32);

        match self.checksum_hash {
            ChecksumHash::SHA256 => {
                let mut to_hash = decryption_key[16..].to_vec();
                to_hash.extend(cipher_message.to_owned());

                let mut sha256 = Sha256Hash::new();
                sha256.update(to_hash);
                let hash = sha256.hash().0.to_vec();

                (hash == self.checksum)
                    .then_some(())
                    .ok_or(ChecksumError::FailedChecksumVerification)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::ChecksumModule;
    use crate::checksum_module::ChecksumHash;

    #[test]
    fn test_parse_json() {
        let checksum = "7e1163a838ecae42f0ddf93021d71d31073b010e61ba304920195f68fcb83b36";
        let checksum_json = json!({
            "checksum_hash": "SHA256",
            "checksum": "7e1163a838ecae42f0ddf93021d71d31073b010e61ba304920195f68fcb83b36"
        });
        let serialized_json = checksum_json.to_string();

        let checksum_module: ChecksumModule = serde_json::from_str(&serialized_json).unwrap();

        let expected_module = ChecksumModule {
            checksum_hash: ChecksumHash::SHA256,
            checksum: hex::decode(checksum).unwrap(),
        };

        assert!(checksum_module == expected_module);
    }
}
