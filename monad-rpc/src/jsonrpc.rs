//! reference: https://www.jsonrpc.org/specification

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const JSONRPC_VERSION: &str = "2.0";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Request {
    pub jsonrpc: String,
    pub method: String,
    pub params: Value,
    pub id: Value,
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

impl Response {
    pub fn new(result: Option<Value>, error: Option<JsonRpcError>, id: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.into(),
            result,
            error,
            id,
        }
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

    #[allow(dead_code)]
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
