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

//! The tx rpc: takes txs over http and, beside one validator, sends each over
//! udp to the leader its schedule names for a slot a lead ahead, resending
//! until that validator's ledger shows it. Every validator must run
//! `proposal.source = "mempool"`: a random-source leader drops the tx unseen.

pub mod api;
pub mod cli;
pub mod config;
pub mod pending;
pub mod schedule;
pub mod service;
pub mod udp;
pub mod watch;

use std::{io, net::SocketAddr, sync::Arc};

use actix_web::{HttpServer, dev::Server, web};

pub use self::{
    config::{RpcConfig, RpcSection},
    service::RpcState,
};

pub struct RpcServer {
    pub server: Server,
    pub addrs: Vec<SocketAddr>,
    pub state: Arc<RpcState>,
}

// binds and starts serving; must be called inside an actix system
pub fn start(config: RpcConfig) -> io::Result<RpcServer> {
    config
        .validate()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let watch = service::watch_from_now(&config)?;
    let http_addr = config.http_addr.clone();
    let state = web::Data::new(RpcState::new(config)?);
    let server = HttpServer::new({
        let state = state.clone();
        move || api::app(state.clone())
    })
    .workers(2)
    .bind(&http_addr)?;
    let addrs = server.addrs();
    let state = state.into_inner();
    actix_web::rt::spawn(service::maintain(state.clone(), watch));
    tracing::info!(?addrs, "rpc listening");
    Ok(RpcServer {
        server: server.run(),
        addrs,
        state,
    })
}

pub async fn run(config: RpcConfig) -> io::Result<()> {
    start(config)?.server.await
}
