use pbkdf2::{hmac::Hmac, pbkdf2};
use scrypt::{scrypt, Params};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::hex_string::{deserialize_bytes_from_hex_string, serialize_bytes_to_hex_string};

#[derive(Deserialize, Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScryptParams {
    #[serde(
        serialize_with = "serialize_bytes_to_hex_string",
        deserialize_with = "deserialize_bytes_from_hex_string"
    )]
    pub salt: Vec<u8>,
    pub key_len: u32,
    #[serde(alias = "N")]
    pub n: u32,
    pub r: u32,
    pub p: u32,
}

#[derive(Deserialize, Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Pbkdf2Params {
    #[serde(
        serialize_with = "serialize_bytes_to_hex_string",
        deserialize_with = "deserialize_bytes_from_hex_string"
    )]
    pub salt: Vec<u8>,
    #[serde(alias = "dKlen")]
    pub dk_len: u32,
    pub count: u32,
}

#[derive(Deserialize, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum KDFParams {
    Scrypt(ScryptParams),
    Pbkdf2(Pbkdf2Params),
}

#[derive(Deserialize, Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KDFModule {
    pub kdf_name: String,
    pub params: KDFParams,
}

#[derive(Debug)]
pub enum KDFError {
    InvalidParams,
    InvalidOutputLen,
}

impl KDFModule {
    pub fn derive_key(&self, password: &[u8]) -> Result<Vec<u8>, KDFError> {
        let mut decryption_key = [0_u8; 32];

        match &self.params {
            KDFParams::Scrypt(scrypt_params) => {
                let salt = scrypt_params.salt.clone();
                let key_len = scrypt_params.key_len as usize;
                let log_n = scrypt_params.n.ilog2() as u8;
                let r = scrypt_params.r;
                let p = scrypt_params.p;

                let params =
                    Params::new(log_n, r, p, key_len).map_err(|_| KDFError::InvalidParams)?;

                scrypt(password, &salt, &params, &mut decryption_key)
                    .map_err(|_| KDFError::InvalidOutputLen)?;
            }
            KDFParams::Pbkdf2(pbkdf2_params) => {
                let salt = pbkdf2_params.salt.clone();
                let rounds = pbkdf2_params.count;
                pbkdf2::<Hmac<Sha256>>(password, &salt, rounds, &mut decryption_key)
                    .map_err(|_| KDFError::InvalidOutputLen)?;
            }
        }

        Ok(decryption_key.to_vec())
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::{KDFModule, KDFParams, ScryptParams};

    #[test]
    fn test_parse_json() {
        let salt = "563198eb58ea328782ae76a9127b3e58e7af73f93387d77a31d4322b45ba91be";
        let kdf_json = json!({
            "kdf_name": "scrypt",
            "params": {
                "salt": salt,
                "key_len": 32,
                "N": 262144,
                "r": 8,
                "p": 1
            }
        });
        let serialized_json = kdf_json.to_string();

        let kdf_module: KDFModule = serde_json::from_str(&serialized_json).unwrap();

        let expected_module = KDFModule {
            kdf_name: "scrypt".to_owned(),
            params: KDFParams::Scrypt(ScryptParams {
                salt: hex::decode(salt).unwrap(),
                key_len: 32,
                n: 262144,
                r: 8,
                p: 1,
            }),
        };

        assert!(kdf_module == expected_module);
    }
}
