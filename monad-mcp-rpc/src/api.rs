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

use std::fmt;

use actix_web::{
    App, Error, HttpRequest, HttpResponse, ResponseError,
    body::MessageBody,
    dev::{ServiceFactory, ServiceRequest, ServiceResponse},
    error::JsonPayloadError,
    guard,
    http::{
        StatusCode,
        header::{self, CacheControl, CacheDirective},
    },
    middleware::DefaultHeaders,
    web,
};
use bytes::Bytes;
use monad_mcp_chorus::ledger::{Address, Hash, MAX_TX_PAYLOAD, Tx};
use serde::{Deserialize, Serialize};

use crate::{
    pending::{Admission, Record, SendOutcome, TxState},
    service::{RpcState, SubmitError, unix_ms},
};

// a 1 KiB payload as hex plus the other fields fits well within this
pub const MAX_BODY: usize = 16 * 1024;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TxRequest {
    pub sender: Option<String>,
    pub nonce: Option<u64>,
    pub payload_hex: Option<String>,
    pub payload_utf8: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RequestError {
    #[error("sender must be 20 bytes of hex")]
    Sender,
    #[error("payload_hex is not valid hex")]
    PayloadHex,
    #[error("give exactly one of payload_hex and payload_utf8")]
    Payload,
    #[error("payload is {len} bytes; the limit is {MAX_TX_PAYLOAD}")]
    TooLarge { len: usize },
}

impl TxRequest {
    // `sender` and `nonce` default to a random address and `nonce()`
    pub fn into_tx(self, nonce: impl FnOnce() -> u64) -> Result<Tx, RequestError> {
        let payload = match (self.payload_hex, self.payload_utf8) {
            (Some(hex), None) => decode_hex(&hex).ok_or(RequestError::PayloadHex)?,
            (None, Some(text)) => text.into_bytes(),
            _ => return Err(RequestError::Payload),
        };
        if payload.len() > MAX_TX_PAYLOAD {
            return Err(RequestError::TooLarge { len: payload.len() });
        }
        let sender = match self.sender {
            Some(sender) => parse_fixed(&sender).ok_or(RequestError::Sender)?,
            None => rand::random::<Address>(),
        };
        Ok(Tx {
            sender,
            nonce: self.nonce.unwrap_or_else(nonce),
            payload: Bytes::from(payload),
        })
    }
}

fn strip_0x(s: &str) -> &str {
    let s = s.trim();
    s.strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
}

pub fn decode_hex(s: &str) -> Option<Vec<u8>> {
    hex::decode(strip_0x(s)).ok()
}

pub fn parse_fixed<const N: usize>(s: &str) -> Option<[u8; N]> {
    let mut out = [0; N];
    hex::decode_to_slice(strip_0x(s), &mut out).ok()?;
    Some(out)
}

pub fn hex0x(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptView {
    pub at_ms: u64,
    pub status: String,
    pub target_slot: Option<u64>,
    pub target_lane: Option<u32>,
    pub leader: Option<u64>,
    pub error: Option<String>,
}

impl AttemptView {
    fn new(at_ms: u64, outcome: &SendOutcome) -> Self {
        let (route, error) = match outcome {
            SendOutcome::Sent(route) => (Some(route), None),
            SendOutcome::Error(error) => (None, Some(error.clone())),
        };
        Self {
            at_ms,
            status: outcome.status_str().to_owned(),
            target_slot: route.map(|route| route.slot.0),
            target_lane: route.map(|route| route.lane as u32),
            leader: route.map(|route| u64::from(route.leader)),
            error,
        }
    }
}

// what `GET /tx/{hash}` and `POST /tx` answer
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxView {
    pub tx_hash: String,
    pub sender: String,
    pub nonce: u64,
    pub payload_len: usize,
    pub payload_hash: String,
    // pending | committed | failed
    pub state: String,
    // the last send: sent | error
    pub status: Option<String>,
    // where the last send went; `slot` and `lane` are where it committed
    pub target_slot: Option<u64>,
    pub target_lane: Option<u32>,
    pub leader: Option<u64>,
    pub attempts: u32,
    pub slot: Option<u64>,
    pub lane: Option<u32>,
    pub path: Option<String>,
    pub finalized_at_ms: Option<u64>,
    pub error: Option<String>,
    pub submitted_at_ms: u64,
    pub history: Vec<AttemptView>,
}

impl TxView {
    pub fn new(record: &Record) -> Self {
        let history: Vec<AttemptView> = record
            .history
            .iter()
            .map(|attempt| AttemptView::new(unix_ms(attempt.at), &attempt.outcome))
            .collect();
        let last = history.last().cloned();
        let commit = match &record.state {
            TxState::Committed(commit) => Some(*commit),
            _ => None,
        };
        let error = match &record.state {
            TxState::Failed(failure) => Some(failure.reason()),
            TxState::Committed(_) => None,
            TxState::Pending => last.as_ref().and_then(|last| last.error.clone()),
        };
        Self {
            tx_hash: hex0x(&record.hash),
            sender: hex0x(&record.tx.sender),
            nonce: record.tx.nonce,
            payload_len: record.tx.payload.len(),
            payload_hash: hex0x(&record.tx.payload_hash()),
            state: record.state.as_str().to_owned(),
            status: last.as_ref().map(|last| last.status.clone()),
            target_slot: last.as_ref().and_then(|last| last.target_slot),
            target_lane: last.as_ref().and_then(|last| last.target_lane),
            leader: last.as_ref().and_then(|last| last.leader),
            attempts: record.attempts(),
            slot: commit.map(|c| c.slot),
            lane: commit.map(|c| c.lane),
            path: commit.map(|c| c.path.as_str().to_owned()),
            finalized_at_ms: commit.map(|c| (c.finalized_at_ns / 1_000_000) as u64),
            error,
            submitted_at_ms: unix_ms(record.submitted_at),
            history,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitView {
    // the rpc already owned this tx; nothing was sent
    pub known: bool,
    #[serde(flatten)]
    pub tx: TxView,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl From<RequestError> for ApiError {
    fn from(error: RequestError) -> Self {
        let status = match error {
            RequestError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::BAD_REQUEST,
        };
        Self::new(status, error.to_string())
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        self.status
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status)
            .insert_header(CacheControl(vec![CacheDirective::NoStore]))
            .json(serde_json::json!({ "error": self.message }))
    }
}

fn json_error(error: JsonPayloadError, _: &HttpRequest) -> Error {
    let status = match error {
        JsonPayloadError::Overflow { .. } | JsonPayloadError::OverflowKnownLength { .. } => {
            StatusCode::PAYLOAD_TOO_LARGE
        }
        _ => StatusCode::BAD_REQUEST,
    };
    ApiError::new(status, error.to_string()).into()
}

fn ok_json(status: StatusCode, value: impl Serialize) -> HttpResponse {
    HttpResponse::build(status)
        .insert_header(CacheControl(vec![CacheDirective::NoStore]))
        .json(value)
}

async fn post_tx(
    state: web::Data<RpcState>,
    request: web::Json<TxRequest>,
) -> Result<HttpResponse, ApiError> {
    let tx = request.into_inner().into_tx(|| state.next_nonce())?;
    let submitted = state.submit(tx).map_err(|error| match error {
        SubmitError::Full(full) => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("{full}; retry later"),
        ),
        SubmitError::Evicted => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{error}; raise retain_finished"),
        ),
    })?;
    let view = TxView::new(&submitted.record);
    let known = submitted.admission == Admission::Known;
    let status = match submitted.record.last_outcome() {
        // owned and resent on the next resend
        Some(SendOutcome::Error(_)) if !known => StatusCode::ACCEPTED,
        _ => StatusCode::OK,
    };
    Ok(ok_json(status, SubmitView { known, tx: view }))
}

async fn get_tx(
    state: web::Data<RpcState>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let hash: Hash = parse_fixed(&path)
        .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "expected a 32-byte hex tx hash"))?;
    let record = state
        .record(&hash)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "unknown tx"))?;
    Ok(ok_json(StatusCode::OK, TxView::new(&record)))
}

async fn health(state: web::Data<RpcState>) -> HttpResponse {
    let (in_flight, tracked) = state.counts();
    let ledger = state.ledger_status();
    let last_send = state.last_send();
    let maintenance_age = state.maintenance_age();
    let maintaining = maintenance_age < state.maintenance_deadline();
    let config = state.config();
    let planner = &config.planner;
    let proposers = planner.schedule().config();
    let clock = planner.clock();
    ok_json(
        StatusCode::OK,
        serde_json::json!({
            "ok": maintaining
                && ledger.error.is_none()
                && !matches!(last_send, Some(SendOutcome::Error(_))),
            "maintenance_age_ms": maintenance_age.as_millis() as u64,
            "in_flight": in_flight,
            "tracked": tracked,
            "max_pending": config.max_pending,
            "node": {
                "id": u64::from(config.node_id),
                "last_status": last_send.as_ref().map(SendOutcome::status_str),
                "last_error": match &last_send {
                    Some(SendOutcome::Error(error)) => Some(error),
                    _ => None,
                },
            },
            "ledger": {
                "highest_slot": ledger.highest_slot,
                "blocks_seen": ledger.blocks_seen,
                "commits_seen": ledger.commits_seen,
                "error": ledger.error,
            },
            "schedule": {
                "concurrent_proposers": proposers.concurrent_proposers,
                "observation_cutoff": proposers.observation_cutoff,
                "rotation_slack": proposers.rotation_slack,
                "slots_per_epoch": proposers.slots_per_epoch,
                "horizon": planner.horizon(),
                "slot_interval_ms": clock.slot_interval.as_millis(),
                "lead_ms": planner.lead().as_millis(),
                "genesis_ms": (clock.genesis_deadline.as_nanos() / 1_000_000) as u64,
            },
        }),
    )
}

async fn preflight(request: HttpRequest) -> HttpResponse {
    let headers = request
        .headers()
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .cloned()
        .unwrap_or_else(|| header::HeaderValue::from_static("content-type"));
    HttpResponse::NoContent()
        .insert_header((header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS"))
        .insert_header((header::ACCESS_CONTROL_ALLOW_HEADERS, headers))
        .insert_header((header::ACCESS_CONTROL_MAX_AGE, "86400"))
        .finish()
}

pub fn routes(cfg: &mut web::ServiceConfig) {
    cfg.app_data(
        web::JsonConfig::default()
            .limit(MAX_BODY)
            .content_type_required(false)
            .error_handler(json_error),
    )
    // any path: a preflight must not hit a resource's 405
    .service(
        web::resource("/{tail:.*}")
            .guard(guard::Options())
            .to(preflight),
    )
    .route("/tx", web::post().to(post_tx))
    .route("/tx/{hash}", web::get().to(get_tx))
    .route("/health", web::get().to(health));
}

// permissive cors, so the explorer page on another origin can post txs
pub fn app(
    state: web::Data<RpcState>,
) -> App<
    impl ServiceFactory<
        ServiceRequest,
        Config = (),
        Response = ServiceResponse<impl MessageBody>,
        Error = Error,
        InitError = (),
    >,
> {
    App::new()
        .wrap(DefaultHeaders::new().add((header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")))
        .app_data(state)
        .configure(routes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(payload_utf8: Option<&str>, payload_hex: Option<&str>) -> TxRequest {
        TxRequest {
            payload_utf8: payload_utf8.map(str::to_owned),
            payload_hex: payload_hex.map(str::to_owned),
            ..TxRequest::default()
        }
    }

    #[test]
    fn a_request_needs_exactly_one_payload_within_the_cap() {
        assert_eq!(
            request(None, None).into_tx(|| 0),
            Err(RequestError::Payload)
        );
        assert_eq!(
            request(Some("a"), Some("00")).into_tx(|| 0),
            Err(RequestError::Payload)
        );
        assert_eq!(
            request(None, Some("0xzz")).into_tx(|| 0),
            Err(RequestError::PayloadHex)
        );
        let max = "a".repeat(MAX_TX_PAYLOAD);
        assert_eq!(
            request(Some(&max), None)
                .into_tx(|| 0)
                .unwrap()
                .payload
                .len(),
            MAX_TX_PAYLOAD
        );
        let over = "ab".repeat(MAX_TX_PAYLOAD + 1);
        assert_eq!(
            request(None, Some(&over)).into_tx(|| 0),
            Err(RequestError::TooLarge {
                len: MAX_TX_PAYLOAD + 1
            })
        );
        // an empty payload is a valid tx
        assert!(
            request(Some(""), None)
                .into_tx(|| 0)
                .unwrap()
                .payload
                .is_empty()
        );
    }

    #[test]
    fn sender_and_nonce_are_parsed_or_defaulted() {
        let tx = TxRequest {
            sender: Some(format!("0x{}", "11".repeat(20))),
            nonce: Some(7),
            payload_hex: Some("0xdead".into()),
            payload_utf8: None,
        }
        .into_tx(|| unreachable!())
        .unwrap();
        assert_eq!(tx.sender, [0x11; 20]);
        assert_eq!(tx.nonce, 7);
        assert_eq!(&tx.payload[..], &[0xde, 0xad]);

        let a = request(Some("x"), None).into_tx(|| 42).unwrap();
        let b = request(Some("x"), None).into_tx(|| 42).unwrap();
        assert_eq!(a.nonce, 42);
        assert_ne!(a.sender, b.sender, "a random sender each time");

        for bad in ["0x11", "zz".repeat(20).as_str(), &"11".repeat(21)] {
            let bad = TxRequest {
                sender: Some(bad.to_owned()),
                ..request(Some("x"), None)
            };
            assert_eq!(bad.into_tx(|| 0), Err(RequestError::Sender));
        }
    }

    #[test]
    fn an_oversized_payload_maps_to_413() {
        let error = ApiError::from(RequestError::TooLarge { len: 2000 });
        assert_eq!(error.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            ApiError::from(RequestError::Sender).status_code(),
            StatusCode::BAD_REQUEST
        );
    }
}
