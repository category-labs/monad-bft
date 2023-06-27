use crate::{CheckpointCommand, Executor};

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
