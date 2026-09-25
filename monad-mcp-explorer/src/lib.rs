// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Block explorer for the MCP ledger: indexes `<ledger_dir>/blocks` in memory and serves a
//! JSON API plus a static web page.

pub mod api;
pub mod assets;
pub mod index;
pub mod loader;
pub mod stats;

use std::{io, path::PathBuf, sync::Arc};

use actix_web::{App, HttpServer, middleware::Compress, web};
use monad_mcp_chorus::ledger::LedgerReader;

use crate::{
    api::AppState,
    index::{Index, IndexConfig},
    loader::{Loader, LoaderConfig, Progress, SharedIndex},
};

#[derive(Clone, Debug)]
pub struct ExplorerConfig {
    pub ledger_dir: PathBuf,
    pub http_addr: String,
    // where the page's send panel posts txs.
    pub rpc_url: String,
    pub index: IndexConfig,
    pub loader: LoaderConfig,
}

pub fn app_state(
    reader: LedgerReader,
    index: SharedIndex,
    progress: Arc<Progress>,
    rpc_url: String,
) -> web::Data<AppState> {
    web::Data::new(AppState {
        index,
        reader,
        progress,
        rpc_url,
    })
}

// indexes the ledger in the background and serves until the server stops.
pub async fn run(config: ExplorerConfig) -> io::Result<()> {
    let reader = LedgerReader::new(&config.ledger_dir);
    let index = SharedIndex::new(Index::new(config.index));
    let progress = Arc::new(Progress::default());
    let loader = Loader::spawn(
        reader.clone(),
        index.clone(),
        progress.clone(),
        config.loader,
    );
    let state = app_state(reader, index, progress, config.rpc_url);
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Compress::default())
            .app_data(state.clone())
            .configure(api::configure)
    })
    .bind(&config.http_addr)?;
    tracing::info!(addr = %config.http_addr, ledger = %config.ledger_dir.display(), "explorer listening");
    let result = server.run().await;
    loader.stop();
    result
}
