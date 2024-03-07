use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum RunModeCommand {
    ProdMode,
    TestMode {
        #[arg(long, default_value_t = false)]
        byzantine_execution: bool,
    },
}

impl Default for RunModeCommand {
    fn default() -> Self {
        RunModeCommand::ProdMode
    }
}
