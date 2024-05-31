/// A placeholder CLI tool to generate the keystore json file
/// The key generation tool is unaudited
/// DO NOT USE IN PRODUCTION YET
/// `cargo run -- --mode create --keystore-path <path_for_file_to_be_created>`

use std::path::PathBuf;

use bip39::{Language, Mnemonic, MnemonicType, Seed};
use clap::Parser;
use dialoguer::{theme::ColorfulTheme, Input};
use hdwallet::ExtendedPrivKey;

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

    /// Path to read/write keystore file
    #[arg(long)]
    keystore_path: PathBuf,
}

fn main() {
    let args = Args::parse();
    let mode = args.mode;
    let keystore_path = args.keystore_path;

    match mode.as_str() {
        "create" => {
            println!("It is recommended to generate key in air-gapped machine to be secure.");
            println!("This tool is currently not fit for production use.");
            let password: String = Input::with_theme(&ColorfulTheme::default())
                .with_prompt("Provide a password to encrypt generated key.")
                .allow_empty(true)
                .interact_text()
                .unwrap();

            // create a new randomly generated mnemonic phrase
            let mnemonic = Mnemonic::new(MnemonicType::Words12, Language::English);

            // get the HD wallet seed and the correspondin private key
            let seed = Seed::new(&mnemonic, "");
            let master_private_key = ExtendedPrivKey::with_seed(seed.as_bytes())
                .expect("Failed to create master private key");
            let private_key = master_private_key.private_key.as_ref();
            println!(
                "Keep your private key securely {:?}",
                hex::encode(private_key)
            );

            // generate keystore json file
            let result =
                Keystore::create_keystore_json(private_key, password.as_str(), &keystore_path);
            if result.is_ok() {
                println!("Successfully generated keystore file.");
            } else {
                println!("Keystore file generation failed, try again.");
            }
        }
        // TODO: add functionality such as recovering key from keystore
        _ => {
            println!("Unknown mode.");
        }
    }
}
