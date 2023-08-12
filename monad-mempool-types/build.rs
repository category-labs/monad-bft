fn main() {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    prost_build::Config::new()
        .compile_protos(&["proto/tx.proto"], &["proto/"])
        .unwrap();
}
