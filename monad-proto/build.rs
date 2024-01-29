fn main() {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(
            &[
                "proto/basic.proto",
                "proto/block.proto",
                "proto/event.proto",
                "proto/ledger.proto",
                "proto/message.proto",
                "proto/quorum_certificate.proto",
                "proto/signing.proto",
                "proto/state_root_hash.proto",
                "proto/timeout.proto",
                "proto/validator_data.proto",
                "proto/voting.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
