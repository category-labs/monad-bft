use serde::Deserialize;

use crate::{
    checksum_module::{ChecksumError, ChecksumModule},
    cipher_module::CipherModule,
    kdf_module::{KDFError, KDFModule},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Keystore {
    pub kdf: KDFModule,
    pub checksum: ChecksumModule,
    pub cipher: CipherModule,
}

#[derive(Debug)]
pub enum KeystoreError {
    InvalidJSONFormat,
    KDFError(KDFError),
    ChecksumError(ChecksumError),
}

impl Keystore {
    pub fn from_json(str: &str) -> Result<Self, KeystoreError> {
        serde_json::from_str(str).map_err(|_| KeystoreError::InvalidJSONFormat)
    }

    pub fn decrypt(&self, password: &String) -> Result<Vec<u8>, KeystoreError> {
        let decryption_key = self
            .kdf
            .derive_key(password.as_bytes())
            .map_err(KeystoreError::KDFError)?;

        self.checksum
            .verify_checksum(&decryption_key, &self.cipher.cipher_message)
            .map_err(KeystoreError::ChecksumError)?;

        Ok(self.cipher.decrypt(&decryption_key))
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::Keystore;

    #[test]
    fn test_keystore_decrypt() {
        let keystore_json = json!({
            "kdf": {
                "kdf_name": "scrypt",
                "params": {
                    "salt": "563198eb58ea328782ae76a9127b3e58e7af73f93387d77a31d4322b45ba91be",
                    "key_len": 32,
                    "N": 262144,
                    "r": 8,
                    "p": 1
                }
            },
            "checksum": {
                "checksum_hash": "SHA256",
                "checksum": "fc52d979838891dbb18fb42e116403582b20dc4c40169d5ceec99419079eb6c4"
            },
            "cipher": {
                "cipher_function": "AES_128_CTR",
                "cipher_message": "c2755caf2c080f939f2b23ce55e6dfbf7430ac36593d778206a60f968baa7055",
                "params": {
                    "iv": "af29e87a9fd4699e335f718d03e30ed9"
                }
            }
        });
        let sk_hex = "6dd19c802af753f5b89d11becba0aeafe91493e14ade082c3af4e5797cae29b5";
        let password = "".to_owned();

        let serialized_json = keystore_json.to_string();
        let keystore = Keystore::from_json(&serialized_json).expect("valid json format");

        let sk = keystore.decrypt(&password).expect("correct password");

        assert!(hex::encode(sk) == sk_hex);
    }
}
