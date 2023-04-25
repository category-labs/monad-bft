extern crate prost_build;
extern crate protobuf_src;
fn main() {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    prost_build::Config::new()
        .compile_protos(
            &[
                "proto/basic.proto",
                "proto/block.proto",
                "proto/ledger.proto",
                "proto/message.proto",
                "proto/quorum_certificate.proto",
                "proto/signing.proto",
                "proto/timeout.proto",
                "proto/voting.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
