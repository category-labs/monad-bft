fn main() {
    prost_build::compile_protos(&["src/tx.proto"], &["src/"]).unwrap();
}
