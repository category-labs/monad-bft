# monad-keystore

The keystore CLI tool can be used to generate keystore json files for the BLS key and SECP key that are needed to run a validator.

### Getting Started

```sh
cargo run --release -- --mode [create|recover] --key-type [bls|secp] --keystore-path <path_to_output_file>
```

### Disclaimer

This tool is currently unaudited, do not use in production.