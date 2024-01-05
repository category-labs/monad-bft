use monad_executor::Executor;
use monad_executor_glue::CheckpointCommand;

pub struct MockCheckpoint<C> {
    pub checkpoint: Option<C>,
}

impl<C> Default for MockCheckpoint<C> {
    fn default() -> Self {
        Self { checkpoint: None }
    }
}

impl<C> Executor for MockCheckpoint<C> {
    type Command = CheckpointCommand<C>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            CheckpointCommand::Save(..) => true,
        });
        self.exec(commands)
    }

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
}
