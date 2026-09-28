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

//! reference: https://www.jsonrpc.org/specification

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use tracing::error;

use crate::data::ChainStateError;

pub const JSONRPC_VERSION: &str = "2.0";

#[derive(Debug, Serialize)]
pub struct Request<'p> {
    pub jsonrpc: String,
    pub method: String,
    #[serde(borrow)]
    pub params: RequestParams<'p>,
    pub id: RequestId,
}

impl<'p> Request<'p> {
    pub fn from_raw_value(raw_value: &'p RawValue) -> serde_json::Result<Self> {
        #[derive(Deserialize)]
        struct RequestRaw<'p> {
            #[serde(deserialize_with = "deserialize_jsonrpc")]
            pub jsonrpc: String,
            pub method: String,
            #[serde(borrow, default)]
            pub params: RequestParams<'p>,
            #[serde(borrow)]
            pub id: &'p RawValue,
        }

        let RequestRaw {
            jsonrpc,
            method,
            params,
            id,
        } = serde_json::from_str(raw_value.get())?;

        if let Ok(number) = serde_json::from_str(id.get()) {
            return Ok(Self {
                jsonrpc,
                method,
                params,
                id: RequestId::Number(number),
            });
        }

        if let Ok(string) = serde_json::from_str(id.get()) {
            return Ok(Self {
                jsonrpc,
                method,
                params,
                id: RequestId::String(string),
            });
        }

        if id.get().trim() == "null" {
            return Ok(Self {
                jsonrpc,
                method,
                params,
                id: RequestId::Null,
            });
        }

        Err(serde::de::Error::custom(
            "id must be an integer, string, or null",
        ))
    }
}

fn deserialize_jsonrpc<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value == "2.0" {
        Ok(value)
    } else {
        Err(serde::de::Error::custom("jsonrpc must be \"2.0\""))
    }
}

#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize)]
pub struct RequestParams<'p>(#[serde(borrow)] Option<&'p RawValue>);

impl<'p> RequestParams<'p> {
    pub fn new(params: &'p RawValue) -> Self {
        Self(Some(params))
    }

    pub fn get(&self) -> &'p str {
        self.0.map_or("", RawValue::get)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
    Null,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Notification<T> {
    pub jsonrpc: String,
    pub method: String,
    pub params: T,
}

impl<T> Notification<T> {
    pub fn new(method: String, params: T) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.into(),
            method,
            params,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RequestWrapper<'p> {
    Single(&'p RawValue),
    Batch(Vec<&'p RawValue>),
}

impl<'p> RequestWrapper<'p> {
    pub fn from_body_bytes(body: &'p bytes::Bytes) -> serde_json::Result<Self> {
        if let Ok(batch) = serde_json::from_slice(body.as_ref()) {
            return Ok(Self::Batch(batch));
        }

        serde_json::from_slice(body.as_ref()).map(Self::Single)
    }
}

impl<'p> Request<'p> {
    pub fn new(method: String, params: &'p RawValue, id: i64) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.into(),
            method,
            params: RequestParams::new(params),
            id: RequestId::Number(id),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Response {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Box<RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    #[schemars(with = "Option<i64>")]
    pub id: RequestId,
}

impl PartialEq for Response {
    fn eq(&self, other: &Self) -> bool {
        self.jsonrpc == other.jsonrpc
            && self.result.as_ref().map(|result| result.get())
                == other.result.as_ref().map(|result| result.get())
            && self.error == other.error
            && self.id == other.id
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ResponseWrapper<T> {
    Single(T),
    Batch(Vec<T>),
}

impl<T> ResponseWrapper<T>
where
    T: for<'de> Deserialize<'de>,
{
    pub fn from_body_bytes(body: bytes::Bytes) -> serde_json::Result<Self> {
        if let Ok(batch) = serde_json::from_slice(body.as_ref()) {
            return Ok(Self::Batch(batch));
        }

        serde_json::from_slice(body.as_ref()).map(Self::Single)
    }
}

impl Response {
    pub fn new(result: Option<Box<RawValue>>, error: Option<JsonRpcError>, id: RequestId) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.into(),
            result,
            error,
            id,
        }
    }

    pub fn from_result(request_id: RequestId, result: Result<Box<RawValue>, JsonRpcError>) -> Self {
        match result {
            Ok(v) => Self::new(Some(v), None, request_id),
            Err(e) => Self::new(None, Some(e), request_id),
        }
    }

    pub fn from_error(error: JsonRpcError) -> Self {
        Self::new(None, Some(error), RequestId::Null)
    }
}

pub fn serialize_with_size_limit<T: Serialize>(
    value: &T,
    max_size: usize,
) -> Result<Box<RawValue>, JsonRpcError> {
    let raw = serde_json::value::to_raw_value(value)
        .map_err(|e| JsonRpcError::internal_error(format!("serialization error: {}", e)))?;

    if raw.get().as_bytes().len() > max_size {
        return Err(JsonRpcError::max_response_size_exceeded());
    }

    Ok(raw)
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Box<RawValue>>,
}

impl PartialEq for JsonRpcError {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
            && self.message == other.message
            && self.data.as_ref().map(|data| data.get())
                == other.data.as_ref().map(|data| data.get())
    }
}

pub type JsonRpcResult<T> = Result<T, JsonRpcError>;

pub trait JsonRpcResultExt: Sized {
    type Result;
    fn invalid_params(self) -> Self::Result;
    fn method_not_found(self) -> Self::Result;
    fn block_not_found(self) -> Self::Result;
}

impl<T, E> JsonRpcResultExt for Result<T, E>
where
    serde_json::Error: From<E>,
{
    type Result = JsonRpcResult<T>;

    fn invalid_params(self) -> JsonRpcResult<T> {
        self.map_err(|_| JsonRpcError::invalid_params())
    }

    fn method_not_found(self) -> JsonRpcResult<T> {
        self.map_err(|_| JsonRpcError::new(ErrorCode::MethodNotFound))
    }

    fn block_not_found(self) -> JsonRpcResult<T> {
        self.map_err(|_| JsonRpcError::block_not_found())
    }
}

impl<T> JsonRpcResultExt for Option<T> {
    type Result = JsonRpcResult<T>;

    fn invalid_params(self) -> JsonRpcResult<T> {
        self.ok_or(JsonRpcError::invalid_params())
    }

    fn method_not_found(self) -> JsonRpcResult<T> {
        self.ok_or(JsonRpcError::new(ErrorCode::MethodNotFound))
    }

    fn block_not_found(self) -> JsonRpcResult<T> {
        self.ok_or(JsonRpcError::block_not_found())
    }
}

pub trait ChainStateResultMap<T> {
    /// Map a ChainStateResult to an JsonRpcResult with Option<U>
    /// If the result is present, and no error is returned, then the function is applied to the result.
    /// If the result is an not found error, then None is returned.
    /// If the result is an error other than not found, then the error is returned.
    fn map_present_and_no_err<F, U>(self, f: F) -> Result<Option<U>, JsonRpcError>
    where
        F: FnOnce(T) -> U;
}

impl<T> ChainStateResultMap<T> for Result<T, ChainStateError> {
    fn map_present_and_no_err<F, U>(self, f: F) -> Result<Option<U>, JsonRpcError>
    where
        F: FnOnce(T) -> U,
    {
        self.to_jsonrpc_result().map(|x| x.map(f))
    }
}

pub trait ChainStateResultExt {
    type Result;
    fn to_jsonrpc_result(self) -> Self::Result;
}

impl<T> ChainStateResultExt for Result<T, ChainStateError> {
    type Result = JsonRpcResult<Option<T>>;

    fn to_jsonrpc_result(self) -> JsonRpcResult<Option<T>> {
        match self {
            Ok(x) => Ok(Some(x)),
            Err(ChainStateError::ResourceNotFound) => Ok(None),
            Err(ChainStateError::Triedb(e)) => {
                Err(JsonRpcError::internal_error(format!("Triedb error: {e}")))
            }
            Err(ChainStateError::Archive(e)) => {
                Err(JsonRpcError::internal_error(format!("Archive error: {e}")))
            }
            Err(ChainStateError::DataSource(e)) => Err(JsonRpcError::internal_error(format!(
                "DataSource error: {e}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorCode {
    ExecutionReverted = 3,
    TransactionTimeout = 4,
    TransactionUnready = 5,
    ServerError = -32000,
    TransactionRejected = -32003,
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
    MethodNotSupported = -32004,
}

impl ErrorCode {
    pub const fn code(self) -> i32 {
        self as i32
    }

    pub const fn default_message(self) -> &'static str {
        match self {
            Self::ExecutionReverted => "execution reverted",
            Self::TransactionTimeout => "Transaction confirmation timed out",
            Self::TransactionUnready => "The transaction is not ready to be processed",
            Self::ServerError => "Server error",
            Self::TransactionRejected => "Transaction rejected",
            Self::ParseError => "Parse error",
            Self::InvalidRequest => "Invalid request",
            Self::MethodNotFound => "Method not found",
            Self::InvalidParams => "Invalid params",
            Self::InternalError => "Internal error",
            Self::MethodNotSupported => "Method not supported",
        }
    }
}

pub mod msg {
    pub const INSUFFICIENT_FUNDS: &str = "Insufficient funds for gas * price + value";
    pub const OUT_OF_GAS: &str = "out of gas";
    pub const OUT_OF_GAS_ALLOWANCE: &str = "out of gas: gas required exceeds allowance";
    pub const GAS_LIMIT_TOO_HIGH: &str = "gas limit too high";
    pub const INTERNAL_ETH_CALL_ERROR: &str = "internal eth_call error";
    pub const RESERVE_BALANCE_VIOLATION: &str = "reserve balance violation";
    pub const RESPONSE_SIZE_EXCEEDED: &str = "response exceeds size limit";
    pub const TXN_DECODE_ERROR: &str = "Transaction decoding error";
}

impl JsonRpcError {
    pub fn new(code: ErrorCode) -> Self {
        Self {
            code: code.code(),
            message: code.default_message().into(),
            data: None,
        }
    }

    pub fn with_message(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code: code.code(),
            message: message.into(),
            data: None,
        }
    }

    pub fn with_message_and_data(
        code: ErrorCode,
        message: impl Into<String>,
        data: Option<String>,
    ) -> Self {
        Self {
            code: code.code(),
            message: message.into(),
            data: data.map(|data| {
                serde_json::value::to_raw_value(&data).expect("RawValue can be built from String")
            }),
        }
    }

    pub fn invalid_params() -> Self {
        Self::new(ErrorCode::InvalidParams)
    }

    pub fn invalid_chain_id(expected: u64, got: u64) -> Self {
        Self::with_message(
            ErrorCode::TransactionRejected,
            format!("Invalid chain ID: expected {}, got {}", expected, got),
        )
    }

    pub fn block_not_found() -> Self {
        Self::with_message(
            ErrorCode::InvalidParams,
            "Block requested not found. Request might be querying \
             historical state that is not available. If possible, \
             reformulate query to point to more recent blocks",
        )
    }

    pub fn internal_error(message: String) -> Self {
        Self::with_message(
            ErrorCode::InternalError,
            format!("Internal error: {}", message),
        )
    }

    pub fn txn_decode_error() -> Self {
        Self::with_message(ErrorCode::InvalidParams, msg::TXN_DECODE_ERROR)
    }

    /// EIP-7966 errors
    pub fn tx_sync_timeout(tx_hash: String, timeout_ms: u64) -> Self {
        Self {
            code: ErrorCode::TransactionTimeout.code(),
            message: format!(
                "Transaction receipt not available within {}ms timeout",
                timeout_ms
            ),
            data: Some(
                serde_json::value::to_raw_value(&serde_json::json!({
                    "hash": tx_hash
                }))
                .expect("RawValue can be built from Value"),
            ),
        }
    }

    pub fn eth_call_error(message: String, data: Option<String>) -> Self {
        Self {
            code: ErrorCode::ServerError.code(),
            message,
            data: data.map(|data| {
                serde_json::value::to_raw_value(&data).expect("RawValue can be built from String")
            }),
        }
    }

    pub fn eth_call_execution_revert(message: String, data: Option<String>) -> Self {
        Self {
            code: ErrorCode::ExecutionReverted.code(),
            message,
            data: data.map(|data| {
                serde_json::value::to_raw_value(&data).expect("RawValue can be built from String")
            }),
        }
    }

    pub fn insufficient_funds() -> Self {
        Self::with_message(ErrorCode::TransactionRejected, msg::INSUFFICIENT_FUNDS)
    }

    pub fn code_size_too_large(size: usize) -> Self {
        Self::with_message(
            ErrorCode::TransactionRejected,
            format!(
                "Contract code size is {} bytes and exceeds code size limit",
                size
            ),
        )
    }

    pub fn overloaded() -> Self {
        Self::with_message(ErrorCode::ServerError, "overloaded, try again later")
    }

    pub fn max_response_size_exceeded() -> Self {
        Self::with_message(ErrorCode::ServerError, msg::RESPONSE_SIZE_EXCEEDED)
    }
}

pub fn archive_to_jsonrpc_error<'a, A: Into<std::borrow::Cow<'a, str>>>(
    message: A,
) -> impl FnOnce(monad_archive::prelude::Report) -> JsonRpcError {
    // Log with debug to get more details, but return a generic error for response
    move |e: monad_archive::prelude::Report| {
        let message = message.into();
        error!("Archive error: {message}. {e:?}");
        JsonRpcError::internal_error(format!("Archive error: {message}"))
    }
}

pub trait ArchiveErrorExt<T> {
    fn to_jsonrpc_error<'a, A: Into<std::borrow::Cow<'a, str>>>(
        self,
        message: A,
    ) -> JsonRpcResult<T>;
}

impl<T> ArchiveErrorExt<T> for monad_archive::prelude::Result<T> {
    fn to_jsonrpc_error<'a, A: Into<std::borrow::Cow<'a, str>>>(
        self,
        message: A,
    ) -> JsonRpcResult<T> {
        self.map_err(archive_to_jsonrpc_error(message))
    }
}

impl From<monad_archive::prelude::Report> for JsonRpcError {
    fn from(e: monad_archive::prelude::Report) -> Self {
        // Log with debug to get more details, but return a generic error for response
        error!("Archive error: {e:?}");
        Self::internal_error(format!("Archive error: {}", e))
    }
}

impl From<monad_ethcall::EthTraceError> for JsonRpcError {
    fn from(error: monad_ethcall::EthTraceError) -> Self {
        use monad_ethcall::EthTraceError::*;
        let message = match error {
            InternalError => String::from("internal error"),
            Other(message) => message,
        };
        Self::eth_call_error(message, None)
    }
}

impl From<monad_ethcall::EthSimulateError> for JsonRpcError {
    fn from(error: monad_ethcall::EthSimulateError) -> Self {
        use monad_ethcall::EthSimulateError::*;
        let message = match error {
            BlockOverrideFailure => String::from("block override failure"),
            InternalError => String::from("internal error"),
            InputSizeMismatch => String::from("input size mismatch"),
            StateOverrideFailure => String::from("state override failure"),
            Other(message) => message,
        };
        Self::eth_call_error(message, None)
    }
}

#[cfg(test)]
mod test {
    use super::{Request, RequestId};

    #[test]
    fn test_request() {
        for (s, expected_request_id) in [
            (
                r#"
                {
                    "jsonrpc": "2.0",
                    "method": "foobar",
                    "params": [42, 43],
                    "id": 1
                }
                "#,
                RequestId::Number(1),
            ),
            (
                r#"
                {
                    "jsonrpc": "2.0",
                    "method": "foobar",
                    "params": [42, 43],
                    "id": "string-id"
                }
                "#,
                RequestId::String("string-id".to_string()),
            ),
            (
                r#"
                {
                    "jsonrpc": "2.0",
                    "method": "foobar",
                    "params": [42, 43],
                    "id": null
                }
                "#,
                RequestId::Null,
            ),
        ] {
            for raw_value in [
                serde_json::from_str(s).unwrap(),
                serde_json::from_slice(s.as_bytes()).unwrap(),
            ] {
                let Request {
                    jsonrpc,
                    method,
                    id,
                    params,
                } = Request::from_raw_value(raw_value).unwrap();

                assert_eq!(jsonrpc, "2.0");
                assert_eq!(method, "foobar");
                assert_eq!(params.get(), "[42, 43]");
                assert_eq!(id, expected_request_id);
            }
        }
    }

    #[test]
    fn test_missing_params() {
        let s = r#"
                {
                    "jsonrpc": "2.0",
                    "method": "foobar",
                    "id": 1
                }
                "#;

        for raw_value in [
            serde_json::from_str(s).unwrap(),
            serde_json::from_slice(s.as_bytes()).unwrap(),
        ] {
            let Request {
                jsonrpc,
                method,
                id,
                params,
            } = Request::from_raw_value(raw_value).unwrap();

            assert_eq!(jsonrpc, "2.0");
            assert_eq!(method, "foobar");
            assert_eq!(params.get(), "");
            assert_eq!(id, RequestId::Number(1));
        }
    }

    #[test]
    fn test_request_without_id() {
        let s = r#"
                {
                    "jsonrpc": "2.0",
                    "method": "foobar",
                    "params": [42, 43]
                }
                "#;

        assert_eq!(
            Request::from_raw_value(serde_json::from_str(s).unwrap())
                .err()
                .unwrap()
                .to_string(),
            "missing field `id` at line 5 column 17"
        );
    }

    #[test]
    fn test_request_with_invalid_id() {
        // ID is one smaller than i64::MIN
        let s = r#"
                {
                    "jsonrpc": "2.0",
                    "method": "foobar",
                    "params": [42, 43],
                    "id": -9223372036854775809
                }
                "#;

        assert_eq!(
            Request::from_raw_value(serde_json::from_str(s).unwrap())
                .err()
                .unwrap()
                .to_string(),
            "id must be an integer, string, or null"
        );

        // ID is float
        let s = r#"
                {
                    "jsonrpc": "2.0",
                    "method": "foobar",
                    "params": [42, 43],
                    "id": 1.0
                }
                "#;

        assert_eq!(
            Request::from_raw_value(serde_json::from_str(s).unwrap())
                .err()
                .unwrap()
                .to_string(),
            "id must be an integer, string, or null"
        );
    }

    #[test]
    fn test_serialize_with_size_limit() {
        use super::serialize_with_size_limit;

        // Small value should succeed with sufficient limit
        let small_value = serde_json::json!({"key": "value"});
        let result = serialize_with_size_limit(&small_value, 1000);
        assert!(result.is_ok());

        // Value should fail when limit is smaller than serialized size
        let result = serialize_with_size_limit(&small_value, 5);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.message, "response exceeds size limit");
        assert_eq!(err.code, -32000);

        // Exact boundary: serialized form is `{"key":"value"}` = 15 bytes
        let result = serialize_with_size_limit(&small_value, 15);
        assert!(result.is_ok());

        let result = serialize_with_size_limit(&small_value, 14);
        assert!(result.is_err());
    }
}
