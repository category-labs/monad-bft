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
use serde_json::{value::RawValue, Value};
use tracing::error;

use crate::chainstate::ChainStateError;

pub const JSONRPC_VERSION: &str = "2.0";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Request {
    #[serde(deserialize_with = "deserialize_jsonrpc")]
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    #[serde(deserialize_with = "deserialize_id")]
    pub id: RequestId,
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

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
    Null,
}

impl<'de> Deserialize<'de> for RequestId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(RequestId::Number(i))
                } else {
                    Err(serde::de::Error::custom("number must be a valid integer"))
                }
            }
            Value::String(s) => Ok(RequestId::String(s)),
            Value::Null => Ok(RequestId::Null),
            _ => Err(serde::de::Error::custom(
                "id must be a integer, string, or null",
            )),
        }
    }
}

fn deserialize_id<'de, D>(deserializer: D) -> Result<RequestId, D::Error>
where
    D: Deserializer<'de>,
{
    RequestId::deserialize(deserializer)
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

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum RequestWrapper<T> {
    /// To be JSON-RPC spec-compliant, `Batch(Vec<T>)` needs to be the first variant in this enum.
    /// To see why, refer to these examples from https://www.jsonrpc.org/specification
    ///
    /// ```text
    /// rpc call with an invalid Batch (but not empty):
    /// --> [1]
    /// <-- [
    ///   {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null}
    /// ]
    /// rpc call with invalid Batch:
    ///
    /// --> [1,2,3]
    /// <-- [
    ///   {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null},
    ///   {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null},
    ///   {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null}
    /// ]
    /// ```
    ///
    /// If `Batch(Vec<T>)` is not the first variant, we will fail to return a batched JSON array of
    /// the individual failure responses and instead return a single JSON object as a failure response.
    Batch(Vec<T>),
    Single(T),
}

impl Request {
    #[allow(dead_code)]
    pub fn new(method: String, params: Value, id: i64) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.into(),
            method,
            params,
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ResponseWrapper<T> {
    Single(T),
    Batch(Vec<T>),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

pub type JsonRpcResult<T> = Result<T, JsonRpcError>;

pub trait JsonRpcResultExt: Sized {
    type Result;
    fn invalid_params(self) -> Self::Result;
    fn method_not_supported(self) -> Self::Result;
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

    fn method_not_supported(self) -> JsonRpcResult<T> {
        self.map_err(|_| JsonRpcError::method_not_supported())
    }

    fn block_not_found(self) -> JsonRpcResult<T> {
        self.map_err(|_| JsonRpcError::internal_error("block not found".into()))
    }
}

impl<T> JsonRpcResultExt for Option<T> {
    type Result = JsonRpcResult<T>;

    fn invalid_params(self) -> JsonRpcResult<T> {
        self.ok_or(JsonRpcError::invalid_params())
    }

    fn method_not_supported(self) -> JsonRpcResult<T> {
        self.ok_or(JsonRpcError::method_not_supported())
    }

    fn block_not_found(self) -> JsonRpcResult<T> {
        self.ok_or(JsonRpcError::internal_error("block not found".into()))
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
            Err(ChainStateError::Archive(e)) => {
                Err(JsonRpcError::internal_error(format!("Archive error: {e}")))
            }
            Err(ChainStateError::Triedb(e)) => {
                Err(JsonRpcError::internal_error(format!("Triedb error: {e}")))
            }
        }
    }
}

impl JsonRpcError {
    // reserved pre-defined errors
    //
    pub fn parse_error() -> Self {
        Self {
            code: -32601,
            message: "Parse error".into(),
            data: None,
        }
    }

    pub fn invalid_request() -> Self {
        Self {
            code: -32601,
            message: "Invalid request".into(),
            data: None,
        }
    }

    pub fn method_not_found() -> Self {
        Self {
            code: -32601,
            message: "Method not found".into(),
            data: None,
        }
    }

    pub fn method_not_supported() -> Self {
        Self {
            code: -32601,
            message: "Method not supported".into(),
            data: None,
        }
    }

    pub fn filter_error(message: String) -> Self {
        Self {
            code: -32602,
            message,
            data: None,
        }
    }

    pub fn invalid_params() -> Self {
        Self {
            code: -32602,
            message: "Invalid params".into(),
            data: None,
        }
    }

    pub fn invalid_chain_id(expected: u64, got: u64) -> Self {
        Self {
            code: -32000,
            message: format!("Invalid chain ID: expected {}, got {}", expected, got),
            data: None,
        }
    }

    // application errors
    pub fn custom(message: String) -> Self {
        Self {
            code: -32603,
            message,
            data: None,
        }
    }

    pub fn block_not_found() -> Self {
        Self {
            code: -32602,
            message: "Block requested not found. Request might be querying \
                      historical state that is not available. If possible, \
                      reformulate query to point to more recent blocks"
                .into(),
            data: None,
        }
    }

    pub fn internal_error(message: String) -> Self {
        Self {
            code: -32603,
            message: format!("Internal error: {}", message),
            data: None,
        }
    }

    pub fn txn_decode_error() -> Self {
        Self {
            code: -32603,
            message: "Transaction decoding error".into(),
            data: None,
        }
    }

    pub fn eth_call_error(message: String, data: Option<String>) -> Self {
        Self {
            code: -32603,
            message,
            data: data.map(Value::String),
        }
    }

    pub fn insufficient_funds() -> Self {
        Self {
            code: -32003,
            message: "Insufficient funds for gas * price + value".into(),
            data: None,
        }
    }

    pub fn code_size_too_large(size: usize) -> Self {
        Self {
            code: -32603,
            message: format!(
                "Contract code size is {} bytes and exceeds code size limit",
                size
            ),
            data: None,
        }
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
        Self::internal_error(format!("Archive error: {}", e.to_string()))
    }
}

#[cfg(test)]
mod test {
    use serde_json::Value;

    use super::Request;
    use crate::jsonrpc::RequestId;

    #[test]
    fn test_request() {
        let s = r#"
                {
                    "jsonrpc": "2.0",
                    "method": "foobar",
                    "params": [42, 43],
                    "id": 1
                }
                "#;
        let req: Result<Request, serde_json::Error> = serde_json::from_str(s);
        assert_eq!(
            Request {
                jsonrpc: "2.0".into(),
                method: "foobar".into(),
                params: Value::Array(vec![Value::Number(42.into()), Value::Number(43.into())]),
                id: RequestId::Number(1),
            },
            req.unwrap()
        );

        let req: Result<Request, serde_json::Error> = serde_json::from_slice(s.as_bytes());
        assert_eq!(
            Request {
                jsonrpc: "2.0".into(),
                method: "foobar".into(),
                params: Value::Array(vec![Value::Number(42.into()), Value::Number(43.into())]),
                id: RequestId::Number(1),
            },
            req.unwrap()
        );
    }

    #[test]
    fn test_str_request() {
        let s = r#"
                {
                    "jsonrpc": "2.0",
                    "method": "foobar",
                    "params": [42, 43],
                    "id": "string-id"
                }
                "#;
        let req: Result<Request, serde_json::Error> = serde_json::from_str(s);
        assert_eq!(
            Request {
                jsonrpc: "2.0".into(),
                method: "foobar".into(),
                params: Value::Array(vec![Value::Number(42.into()), Value::Number(43.into())]),
                id: RequestId::String("string-id".into()),
            },
            req.unwrap()
        );

        let req: Result<Request, serde_json::Error> = serde_json::from_slice(s.as_bytes());
        assert_eq!(
            Request {
                jsonrpc: "2.0".into(),
                method: "foobar".into(),
                params: Value::Array(vec![Value::Number(42.into()), Value::Number(43.into())]),
                id: RequestId::String("string-id".into()),
            },
            req.unwrap()
        );
    }
}
