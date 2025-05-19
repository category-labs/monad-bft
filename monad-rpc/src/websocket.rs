use std::time::{Duration, Instant};

use actix::prelude::*;
use actix_http::ws::{Message as WebsocketMessage, ProtocolError};
use actix_web::{web, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use tracing::debug;

use crate::handlers::resources::MonadRpcResources;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {}

pub async fn handler(
    req: HttpRequest,
    stream: web::Payload,
    app_state: web::Data<MonadRpcResources>,
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

pub struct WebsocketSession {
    pub heartbeat: Instant,
    pub server: Addr<MonadRpcResources>,
}

impl WebsocketSession {
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

impl Actor for WebsocketSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.heartbeat(ctx);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        self.server.do_send(Disconnect {});
        Running::Stop
    }
}

impl StreamHandler<Result<WebsocketMessage, ProtocolError>> for WebsocketSession {
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
    use actix_web::{web, App};
    use bytes::Bytes;
    use futures_util::{SinkExt as _, StreamExt as _};
    use tokio::sync::Semaphore;
    use tracing_actix_web::TracingLogger;

    use crate::{
        fee::FixedFee,
        handlers::{
            eth::call::EthCallStatsTracker,
            resources::{MonadJsonRootSpanBuilder, MonadRpcResources},
        },
        txpool::EthTxPoolBridgeClient,
    };

    fn create_test_server() -> actix_test::TestServer {
        let resources = MonadRpcResources {
            txpool_bridge_client: EthTxPoolBridgeClient::for_testing(),
            triedb_reader: None,
            eth_call_executor: None,
            eth_call_executor_fibers: 64,
            eth_call_stats_tracker: Some(Arc::new(EthCallStatsTracker::default())),
            archive_reader: None,
            base_fee_per_gas: FixedFee::new(2000),
            chain_id: 41454,
            batch_request_limit: 1000,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            rate_limiter: Arc::new(Semaphore::new(1000)),
            total_permits: 1000,
            logs_max_block_range: 1000,
            eth_call_provider_gas_limit: u64::MAX,
            eth_estimate_gas_provider_gas_limit: u64::MAX,
            dry_run_get_logs_index: false,
            use_eth_get_logs_index: false,
            max_finalized_block_cache_len: 200,
            enable_eth_call_statistics: true,
        };

        actix_test::start(move || {
            App::new()
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .app_data(web::PayloadConfig::default().limit(8192))
                .app_data(web::Data::new(resources.clone()))
                .service(web::resource("/").route(web::post().to(crate::handlers::rpc_handler)))
                .service(web::resource("/ws/").route(web::get().to(crate::websocket::handler)))
        })
    }

    #[actix::test]
    async fn websocket_wait_for_ping() {
        env_logger::try_init().expect("failed to initialize logger");

        let mut server = create_test_server();

        let mut framed = server.ws_at("/ws/").await.unwrap();
        let frame = framed.next().await.unwrap().unwrap();
        assert_eq!(frame, Frame::Ping(Bytes::from_static(b"")));
        framed
            .send(ws::Message::Pong(Bytes::from_static(b"")))
            .await
            .unwrap();
    }
}
