/// A placeholder CLI tool to generate the keystore json file
/// The key generation tool is unaudited
/// DO NOT USE IN PRODUCTION YET
/// `cargo run -- --mode create --key-type [bls|secp] --keystore-path <path_for_file_to_be_created>`
use std::path::PathBuf;
use std::{fs::File, io::Write};

use bip39::{Language, Mnemonic, MnemonicType, Seed};
use clap::Parser;
use dialoguer::{theme::ColorfulTheme, Input};
use hdwallet::{
    traits::{Deserialize, Serialize},
    ExtendedPrivKey,
};
use monad_bls::BlsKeyPair;
use monad_secp::KeyPair;

use crate::keystore::Keystore;

pub mod checksum_module;
pub mod cipher_module;
pub mod hex_string;
pub mod kdf_module;
pub mod keystore;

#[derive(Parser)]
#[command(name = "monad-keystore", about, long_about = None)]
struct Args {
    /// Mode
    #[arg(long)]
    mode: String,

    /// Key type
    #[arg(long)]
    key_type: String,

    /// Path to read/write keystore file
    #[arg(long)]
    keystore_path: PathBuf,

    /// Password to encrypt private key
    #[arg(long)]
    password: Option<String>,

    /// Path to read/write private key as JSON object
    #[arg(
        long,
        help = "Path to read/write private key as JSON object. WARNING: Using this argument is potentially unsafe. It is ill-advised to write keys to disk."
    )]
    private_key_path: Option<PathBuf>,
}

fn main() {
    let args = Args::parse();
    let mode = args.mode;
    let key_type = args.key_type;
    let keystore_path = args.keystore_path;
    let private_key_path = args.private_key_path;

    match mode.as_str() {
        "create" => {
            println!("It is recommended to generate key in air-gapped machine to be secure.");
            println!("This tool is currently not fit for production use.");
            let password: String = if args.password.is_some() {
                args.password.unwrap()
            } else {
                Input::with_theme(&ColorfulTheme::default())
                    .with_prompt("Provide a password to encrypt generated key.")
                    .allow_empty(true)
                    .interact_text()
                    .unwrap()
            };

            // create a new randomly generated mnemonic phrase
            let mnemonic = Mnemonic::new(MnemonicType::Words12, Language::English);

            // get the HD wallet seed and the correspondin private key
            let seed = Seed::new(&mnemonic, "");
            let master_private_key = ExtendedPrivKey::with_seed(seed.as_bytes())
                .expect("Failed to create master private key");
            if let Some(private_key_path) = private_key_path {
                let json = serde_json::to_string(&ExtendedPrivKey::serialize(&master_private_key))
                    .expect("failed to serialize key to JSON");
                let mut file = File::create(private_key_path.clone())
                    .expect("failed to open file for writing");
                file.write_all(json.as_bytes())
                    .expect("failed to write key to file");
                println!("Successfully wrote private key to {:?}", private_key_path);
            }
            let private_key = master_private_key.private_key.as_ref();
            println!(
                "Keep your private key securely {:?}",
                hex::encode(private_key)
            );

            // generate public key
            print_public_key(private_key, &key_type);

            // generate keystore json file
            let result =
                Keystore::create_keystore_json(private_key, password.as_str(), &keystore_path);
            if result.is_ok() {
                println!("Successfully generated keystore file.");
            } else {
                println!("Keystore file generation failed, try again.");
            }
        }
        "recover" => {
            println!("Recovering private and public key from keystore file...");
            let password: String = if args.password.is_some() {
                args.password.unwrap()
            } else {
                Input::with_theme(&ColorfulTheme::default())
                    .with_prompt("Provide a password to decrypt generated key.")
                    .allow_empty(true)
                    .interact_text()
                    .unwrap()
            };

            // recover private key
            let result = Keystore::load_key(&keystore_path, &password);
            let private_key = match result {
                Ok(private_key) => private_key,
                Err(err) => {
                    println!("Unable to recover private key");
                    match err {
                        keystore::KeystoreError::InvalidJSONFormat => {
                            println!("Invalid JSON format")
                        }
                        keystore::KeystoreError::KDFError(kdf_err) => {
                            println!("KDFError {:?}", kdf_err)
                        }
                        keystore::KeystoreError::ChecksumError(chksum_err) => {
                            println!("ChecksumError {:?}", chksum_err)
                        }
                        keystore::KeystoreError::FileIOError(io_err) => {
                            println!("IO Error {:?}", io_err)
                        }
                    }
                    return;
                }
            };
            println!(
                "Keep your private key securely {:?}",
                hex::encode(&private_key)
            );

            print_public_key(&private_key, &key_type);
        }
        "import" => {
            let Some(private_key_path) = private_key_path else {
                println!("error: a value is required for '--private-key-path <PRIVATE_KEY_PATH>' but none was supplied");
                return;
            };
            let Some(password) = args.password else {
                println!(
                    "error: a value is required for '--password <PASSWORD>' but none was supplied"
                );
                return;
            };
            let private_key = ExtendedPrivKey::deserialize(
                &serde_json::from_str::<Vec<u8>>(
                    &std::fs::read_to_string(private_key_path)
                        .expect("failed to read private key JSON file"),
                )
                .expect("failed to parse private key file as JSON"),
            )
            .expect("failed to deserialize Vec<u8> into ExtendedPrivKey");

            // generate public key
            print_public_key(private_key.private_key.as_ref(), &key_type);

            // generate keystore json file
            let result = Keystore::create_keystore_json(
                private_key.private_key.as_ref(),
                password.as_str(),
                &keystore_path,
            );
            if result.is_ok() {
                println!("Successfully generated keystore file.");
            } else {
                println!("Keystore file generation failed, try again.");
            }
        }
        _ => {
            println!("Unknown mode.");
        }
    }
}

pub fn print_public_key(private_key: &[u8], key_type: &str) {
    match key_type {
        "bls" => {
            let bls_keypair = BlsKeyPair::from_bytes(private_key.to_vec());
            let pubkey = bls_keypair.unwrap().pubkey();
            println!("BLS public key: {:?}", pubkey);
        }
        "secp" => {
            let secp_keypair = KeyPair::from_bytes(&mut private_key.to_vec());
            let pubkey = secp_keypair.unwrap().pubkey();
            println!("Secp public key: {:?}", pubkey);
        }
        _ => {
            println!("Unsupported key type, try again.")
        }
    }
}
