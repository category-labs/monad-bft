use futures::{Stream, StreamExt};

mod state;
pub use state::*;

pub mod executor;
pub mod mock_swarm;
pub mod replay_nodes;
pub mod timed_event;
pub mod transformer;

pub mod convert;

// driver loop
async fn run<S: State>(
    mut executor: impl Executor<Command = Command<S::Message, S::OutboundMessage, S::Block, S::Checkpoint>>
        + Stream<Item = S::Event>
        + Unpin,
    config: S::Config,
    init_events: Vec<S::Event>,
) {
    let (mut state, mut init_commands) = S::init(config);
    for event in init_events {
        let cmds = state.update(event);
        init_commands.extend(cmds);
    }
    executor.exec(init_commands);

    while let Some(event) = executor.next().await {
        let commands = state.update(event);

        // TODO persist event (append)
        // observation: only need to flush if commands is nonempty AND commands contains
        //              something important (must be committed - eg RouterCommand::Publish)
        //
        // other: does sending an ack require state change?

        executor.exec(commands);
    }
}
