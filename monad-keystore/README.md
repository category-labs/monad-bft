# monad-keystore

The keystore CLI tool can be used to generate keystore json files for the BLS key and SECP key that are needed to run a validator.

### Getting Started

```sh
cargo run --release --bin monad-keystore -- [create|recover|import] --keystore-path <path> --password <password> --key-type [bls|secp]
```

### Disclaimer

This tool is currently unaudited, do not use in production.