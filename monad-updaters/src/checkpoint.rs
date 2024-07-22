use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::CheckpointCommand;

pub struct MockCheckpoint<C> {
    pub checkpoint: Option<C>,
    metrics: ExecutorMetrics,
}

impl<C> Default for MockCheckpoint<C> {
    fn default() -> Self {
        Self {
            checkpoint: None,
            metrics: Default::default(),
        }
    }
}

impl<C> Executor for MockCheckpoint<C> {
    type Command = CheckpointCommand<C>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                CheckpointCommand::Save(c) => {
                    self.checkpoint = Some(c);
                    println!("checkpoint saved");
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
