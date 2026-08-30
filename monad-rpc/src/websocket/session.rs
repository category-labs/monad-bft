use std::time::Duration;

use actix_ws::{CloseCode, CloseReason, Closed};
use bytes::Bytes;
use bytestring::ByteString;
use tracing::debug;

const SEND_TIMEOUT: Duration = Duration::from_secs(5);

macro_rules! with_timeout {
    ($self:ident . $session:ident . $($tt:tt)*) => {
        match tokio::time::timeout(SEND_TIMEOUT, $self.$session.$($tt)*).await {
            Ok(result) => result,
            Err(elapsed) => {
                debug!(?elapsed, "ws session timed out");

                let () = $self.$session
                    .clone()
                    .close(Some(CloseReason {
                        code: CloseCode::Normal,
                        description: Some("session timed out".to_string()),
                    }))
                    .await?;

                Err(Closed)
            }
        }
    };
}

pub(super) struct Session {
    session: actix_ws::Session,
}

impl Session {
    pub fn new(session: actix_ws::Session) -> Self {
        Self { session }
    }

    pub async fn text(&mut self, msg: impl Into<ByteString>) -> Result<(), Closed> {
        with_timeout!(self.session.text(msg))
    }

    pub async fn binary(&mut self, msg: impl Into<Bytes>) -> Result<(), Closed> {
        with_timeout!(self.session.binary(msg))
    }

    pub async fn ping(&mut self, msg: &[u8]) -> Result<(), Closed> {
        with_timeout!(self.session.ping(msg))
    }

    pub async fn pong(&mut self, msg: &[u8]) -> Result<(), Closed> {
        with_timeout!(self.session.pong(msg))
    }

    pub async fn close(self, reason: Option<CloseReason>) -> Result<(), Closed> {
        self.session.close(reason).await
    }
}
