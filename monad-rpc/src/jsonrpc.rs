//! reference: https://www.jsonrpc.org/specification

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const JSONRPC_VERSION: &str = "2.0";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Request {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Value,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum RequestWrapper<T> {
    /// To be JSON-RPC spec-compliant, `Batch(Vec<T>)` needs to be the first variant in this enum.
    /// To see why, refer to these examples from https://www.jsonrpc.org/specification
    ///
    /// ```
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
    pub fn new(method: String, params: Value, id: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.into(),
            method,
            params,
            id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Response {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ResponseWrapper<T> {
    Single(T),
    Batch(Vec<T>),
}

impl Response {
    pub fn new(result: Option<Value>, error: Option<JsonRpcError>, id: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.into(),
            result,
            error,
            id,
        }
    }

    pub fn from_result(request_id: Value, result: Result<Value, JsonRpcError>) -> Self {
        match result {
            Ok(v) => Self::new(Some(v), None, request_id),
            Err(e) => Self::new(None, Some(e), request_id),
        }
    }

    pub fn from_error(error: JsonRpcError) -> Self {
        Self::new(None, Some(error), Value::Null)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcError {
    // reserved pre-defined errors
    //
    pub fn parse_error() -> Self {
        Self {
            code: -32700,
            message: "Parse error".into(),
            data: None,
        }
    }

    pub fn invalid_request() -> Self {
        Self {
            code: -32600,
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

    pub fn invalid_params() -> Self {
        Self {
            code: -32602,
            message: "Invalid params".into(),
            data: None,
        }
    }

    pub fn internal_error() -> Self {
        Self {
            code: -32603,
            message: "Internal error".into(),
            data: None,
        }
    }

    // application errors

    pub fn txn_decode_error() -> Self {
        Self {
            code: -32000,
            message: "Transaction decoding error".into(),
            data: None,
        }
    }

    pub fn eth_call_error(message: String, data: Option<String>) -> Self {
        Self {
            code: -32000,
            message,
            data: data.map(Value::String),
        }
    }

    pub fn eth_filter_error(message: String) -> Self {
        Self {
            code: -32000,
            message,
            data: None,
        }
    }

    pub fn method_not_supported() -> Self {
        Self {
            code: -32001,
            message: "Method not supported".into(),
            data: None,
        }
    }
}

#[cfg(test)]
mod test {
    use serde_json::Value;

    use super::Request;

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
                id: Value::Number(1.into()),
            },
            req.unwrap()
        );

        let req: Result<Request, serde_json::Error> = serde_json::from_slice(s.as_bytes());
        assert_eq!(
            Request {
                jsonrpc: "2.0".into(),
                method: "foobar".into(),
                params: Value::Array(vec![Value::Number(42.into()), Value::Number(43.into())]),
                id: Value::Number(1.into()),
            },
            req.unwrap()
        );
    }
}
