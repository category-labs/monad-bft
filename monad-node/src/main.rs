use clap::CommandFactory;

mod cli;
use cli::Cli;

mod config;

mod error;
use error::NodeSetupError;

mod state;
use state::NodeState;

fn main() {
    let mut cmd = Cli::command();

    let state = NodeState::setup(&mut cmd).unwrap_or_else(|e| cmd.error(e.kind(), e).exit());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.into())
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    drop(cmd);

    if let Err(e) = runtime.block_on(run(state)) {
        log::error!("monad consensus node crashed: {:?}", e);
    }
}

async fn run(_node_state: NodeState) -> Result<(), ()> {
    Ok(())
}
