use monad_executor::executor::mempool::Mempool;

use axum::{extract::State, http::StatusCode, routing::post, Router};
use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
};

type Sync<M> = Arc<RwLock<M>>;

async fn start<M: Mempool>(bind_address: SocketAddr, mempool: Sync<M>) {
    let app = Router::new()
        .route("/add_tx", post(add_tx::<Sync<M>>))
        .with_state(mempool);

    tracing::debug!("mempool listening on {}", bind_address);
    axum::Server::bind(&bind_address)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn add_tx<SM: Mempool>(
    State(mut mempool): State<SM>,
    request: String,
) -> Result<&'static str, StatusCode> {
    let bytes = hex::decode(request).map_err(|_| StatusCode::PRECONDITION_FAILED)?;
    if !mempool.add_tx(bytes) {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    Ok("ok")
}
