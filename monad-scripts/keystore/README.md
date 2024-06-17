## Keystore

Utility functions to generate the keystore json files for a validator. Run `bash generate_keystores.sh install` in the `monad-scripts/keystore` directory to generate both a BLS keystore and a SECP keystore file.

Pass in optional command line arguments to set custom values:
1. `--password` for encryption on the keystore
2. `--bls-keystore-path` for output file path of generated bls keystore
3. `--secp-keystore-path` for output file path of generated secp keystore