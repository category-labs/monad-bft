pub mod ipc;

#[cfg(test)]
mod tests {
    use monad_bls::BlsSignatureCollection;
    use monad_consensus_types::validator_data::ParsedValidatorData;
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_secp::SecpSignature;

    type SignatureType = SecpSignature;
    type SignatureCollectionType =
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

    #[test]
    fn test_json() {
        let input = r#"
epoch = 10
[[validators]]
node_id = "0x02093852f554d6196025b0bebe21ea646f73e34f1fd6f154e6ae77c890363ea5be"
stake = 1
cert_pubkey = "0xab1011a17be0921c122fc7362f6e0401f80623c644bb9d9e9eedff0410a4de2259f0d3a36414d4dd9dd748ace56ffb5e"
        "#;
        let parsed_validators: ParsedValidatorData<SignatureCollectionType> =
            toml::from_str(input).unwrap();
        println!(
            "{}",
            serde_json::to_string_pretty(&parsed_validators).unwrap()
        );
    }
}
