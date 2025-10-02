# monad-keystore

The keystore CLI tool can be used to work with keystore json files for the BLS key and SECP key that are needed to run a validator.

## Supported commands

### Create:

```sh
cargo run --release -- create --keystore-path <path_to_keystore_file> --password "<password>" --key-type [bls|secp]
```

- The `create` command will generate a new random 32 byte IKM that will be encrypted with the password and stored in the keystore file.
- The IKM is used to generate either BLS or SECP key depending on the provided key type. The private key and the public key will be displayed once the key is generated.
- **NOTE: The keystore does not store the type of key that was generated with the IKM. That responsibility is left to the user**

### Recover:

```sh
cargo run --release -- recover --keystore-path <path_to_keystore_file> --password "<password>" [--key-type [bls|secp]]
```

- The `recover` command will display the IKM stored in the keystore file (if the password is correct)
- Since the keystore does not store the key type, providing the key type as a parameter will also display the corresponding private and public key.

### Import:

```sh
cargo run --release -- import --ikm <ikm> --keystore-path <path_to_keystore_file> --password "<password>" [--key-type [bls|secp]]
```

- The `import` command will generate a new keystore file with the provided IKM (in hex)
- Since the keystore does not store the key type, providing the key type as a parameter will also display the corresponding private and public key.

### Disclaimer

This tool is currently unaudited, do not use in production.