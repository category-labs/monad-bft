use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    std::env::set_var("PROTOC", protobuf_src::protoc());

    let descriptor_path = PathBuf::from(std::env::var("OUT_DIR")?).join("proto_descriptor.bin");
    prost_build::Config::new()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .bytes(["."])
        .compile_protos(
            &[
                "proto/basic.proto",
                "proto/discovery.proto",
                "proto/block.proto",
                "proto/blocksync.proto",
                "proto/event.proto",
                "proto/message.proto",
                "proto/quorum_certificate.proto",
                "proto/signing.proto",
                "proto/state_root_hash.proto",
                "proto/timeout.proto",
                "proto/validator_data.proto",
                "proto/voting.proto",
            ],
            &["proto/"],
        )?;

    let descriptor_set = std::fs::read(descriptor_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)?
        .build(&[".monad_proto"])?;

    Ok(())
}
