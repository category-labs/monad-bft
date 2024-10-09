use std::path::PathBuf;

use clap::Parser;
use url::Url;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct EthTxGenCli {
    #[arg(long, default_value = "http://localhost:8080")]
    pub rpc_url: Url,

    #[arg(long)]
    pub config: PathBuf,
}
