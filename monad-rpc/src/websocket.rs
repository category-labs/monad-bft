use std::time::{Duration, Instant};

use actix::prelude::*;
use actix_http::ws::{Message as WebsocketMessage, ProtocolError};
use actix_web::{web, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use monad_triedb_utils::triedb_env::Triedb;
use tracing::debug;

use crate::MonadRpcResources;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {}

pub async fn handler<T: Triedb + Unpin + Clone>(
    req: HttpRequest,
    stream: web::Payload,
    app_state: web::Data<MonadRpcResources<T>>,
) -> Result<HttpResponse, actix_web::Error> {
    debug!("ws_handler {:?}", &req);
    ws::start(
        WebsocketSession {
            heartbeat: Instant::now(),
            server: app_state.get_ref().clone().start(),
        },
        &req,
        stream,
    )
}

pub struct WebsocketSession<T: Triedb + Unpin + 'static> {
    pub heartbeat: Instant,
    pub server: Addr<MonadRpcResources<T>>,
}

impl<T: Triedb + Unpin + 'static> WebsocketSession<T> {
    fn heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |actor, ctx| {
            debug!("heartbeat");
            if Instant::now().duration_since(actor.heartbeat) > CLIENT_TIMEOUT {
                debug!("client failed heartbeat, disconnecting");

                actor.server.do_send(Disconnect {});

                ctx.stop();
                return;
            }

            ctx.ping(b"");
        });
    }
}

impl<T: Triedb + 'static + Unpin> Actor for WebsocketSession<T> {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.heartbeat(ctx);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        self.server.do_send(Disconnect {});
        Running::Stop
    }
}

impl<T: Triedb + 'static + Unpin> StreamHandler<Result<WebsocketMessage, ProtocolError>>
    for WebsocketSession<T>
{
    fn handle(&mut self, item: Result<WebsocketMessage, ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(message) => {
                debug!("StreamHandler::handle {:?}", message);
                match message {
                    WebsocketMessage::Text(text) => {}
                    WebsocketMessage::Binary(binary) => {}
                    WebsocketMessage::Continuation(continuation) => {}
                    WebsocketMessage::Ping(_) => {
                        debug!("received ping frame from client {:?}", ctx.address());
                    }
                    WebsocketMessage::Pong(_) => {
                        debug!("received pong frame from client {:?}", ctx.address());
                    }
                    WebsocketMessage::Close(close) => {}
                    WebsocketMessage::Nop => {}
                }
            }
            Err(e) => {
                debug!("StreamHandler::handle error {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actix_http::{ws, ws::Frame};
    use alloy_consensus::TxEnvelope;
    use bytes::Bytes;
    use futures_util::{SinkExt as _, StreamExt as _};
    use monad_triedb_utils::triedb_env::TriedbEnv;
    use tokio::sync::Semaphore;

    use crate::{create_app, tests::MonadRpcResourcesState, MonadRpcResources};

    fn create_test_server() -> (MonadRpcResourcesState, actix_test::TestServer) {
        let (ipc_sender, ipc_receiver) = flume::unbounded::<TxEnvelope>();
        let resources = MonadRpcResources::<TriedbEnv> {
            mempool_sender: ipc_sender,
            triedb_reader: None,
            archive_reader: None,
            chain_id: 41454,
            batch_request_limit: 1000,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            rate_limiter: Arc::new(Semaphore::new(1000)),
            metrics: None,
        };
        (
            MonadRpcResourcesState { ipc_receiver },
            actix_test::start(move || create_app(resources.clone())),
        )
    }

    #[actix::test]
    async fn websocket_wait_for_ping() {
        env_logger::try_init().expect("failed to initialize logger");

        let (_, mut server) = create_test_server();

        let mut framed = server.ws_at("/ws/").await.unwrap();
        let frame = framed.next().await.unwrap().unwrap();
        assert_eq!(frame, Frame::Ping(Bytes::from_static(b"")));
        framed
            .send(ws::Message::Pong(Bytes::from_static(b"")))
            .await
            .unwrap();
    }
}
