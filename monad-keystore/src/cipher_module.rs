use aes::cipher::{generic_array::GenericArray, KeyIvInit, StreamCipher};
use serde::{Deserialize, Serialize};

use crate::hex_string::{deserialize_bytes_from_hex_string, serialize_bytes_to_hex_string};

type Aes128Ctr = ctr::Ctr128BE<aes::Aes128>;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Aes128Params {
    #[serde(
        serialize_with = "serialize_bytes_to_hex_string",
        deserialize_with = "deserialize_bytes_from_hex_string"
    )]
    pub iv: Vec<u8>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum CipherParams {
    Aes128Params(Aes128Params),
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CipherModule {
    pub cipher_function: String,
    pub params: CipherParams,
}

impl CipherModule {
    pub fn encrypt(&self, private_key: &[u8], encryption_key: &[u8]) -> Vec<u8> {
        assert!(encryption_key.len() == 32);

        match &self.params {
            CipherParams::Aes128Params(aes_128_params) => {
                let key = GenericArray::clone_from_slice(&encryption_key[..16]);
                let iv = GenericArray::clone_from_slice(&aes_128_params.iv);
                let mut cipher = Aes128Ctr::new(&key, &iv);

                let mut ciphertext = private_key.to_owned();
                cipher.apply_keystream(&mut ciphertext);

                ciphertext
            }
        }
    }

    pub fn decrypt(&self, ciphertext: &[u8], decryption_key: &[u8]) -> Vec<u8> {
        assert!(decryption_key.len() == 32);

        match &self.params {
            CipherParams::Aes128Params(aes_128_params) => {
                let key = GenericArray::clone_from_slice(&decryption_key[..16]);
                let iv = GenericArray::clone_from_slice(&aes_128_params.iv);
                let mut cipher = Aes128Ctr::new(&key, &iv);

                let mut private_key = ciphertext.to_owned();
                cipher.apply_keystream(&mut private_key);

                private_key
            }
        }
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::{Aes128Params, CipherModule, CipherParams};

    #[test]
    fn test_parse_json() {
        let iv = "af29e87a9fd4699e335f718d03e30ed9";
        let cipher_json = json!({
            "cipher_function": "AES_128_CTR",
            "params": {
                "iv": iv
            }
        });
        let serialized_json = cipher_json.to_string();

        let cipher_module: CipherModule = serde_json::from_str(&serialized_json).unwrap();

        let expected_module = CipherModule {
            cipher_function: "AES_128_CTR".to_owned(),
            params: CipherParams::Aes128Params(Aes128Params {
                iv: hex::decode(iv).unwrap(),
            }),
        };

        assert!(cipher_module == expected_module);
    }
}
