extern crate prost_build;
extern crate protobuf_src;
fn main() {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    prost_build::compile_protos(
        &[
            "src/proto/basic.proto",
            "src/proto/block.proto",
            "src/proto/ledger.proto",
            "src/proto/message.proto",
            "src/proto/quorum_certificate.proto",
            "src/proto/signing.proto",
            "src/proto/timeout.proto",
            "src/proto/voting.proto",
        ],
        &["src/proto/"],
    )
    .unwrap();
}
