fn main() {
    let openrpc = monad_rpc::docs::as_openrpc();
    println!(
        "{}",
        serde_json::to_string(&openrpc).expect("failed to serialize OpenRPC document")
    );
}
