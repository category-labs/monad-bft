use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub ipc_path: PathBuf,

    #[arg(long)]
    pub blockdb_path: Option<PathBuf>,

    #[arg(long)]
    pub triedb_path: Option<PathBuf>,

    #[arg(long, default_value_t = String::from("0.0.0.0"))]
    pub rpc_addr: String,

    #[arg(long, default_value_t = 8080)]
    pub rpc_port: u16,
}
