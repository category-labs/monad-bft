use crate::{
    eth_json_types::{
        deserialize_fixed_data, serialize_result, EthAddress, EthHash, UnformattedData,
    },
    jsonrpc::JsonRpcError,
};
use alloy_primitives::{
    aliases::{B256, U256, U64, U8},
    Address, Bytes,
};
use alloy_rlp::Decodable;
use alloy_rlp::RlpDecodable;
use monad_blockdb::EthTxKey;
use monad_blockdb_utils::BlockDbEnv;
use monad_triedb_utils::{TriedbEnv, TriedbResult};
use reth_rpc_types::Transaction;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{cell::RefCell, rc::Rc};
use tracing::{debug, trace};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallFrame {
    #[serde(rename = "type")]
    pub typ: U8,
    pub flags: U64,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: U64,
    pub gas_used: U64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: U8,
    pub depth: U64,
}

impl Decodable for CallFrame {
    fn decode(rlp_buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let typ: U8 = U8::decode(rlp_buf)?;
        let flags: U64 = U64::decode(rlp_buf)?;
        let from: Address = Address::decode(rlp_buf)?;

        // Decode the `to` field, handling the case where it's `None`.
        let to: Option<Address> = {
            let first_byte = rlp_buf.get(0).ok_or(alloy_rlp::Error::InputTooShort)?;
            if *first_byte == 0x80 {
                // If the first byte is 0x80, it represents an empty value (None for the Address).
                *rlp_buf = &rlp_buf[1..]; // Advance the buffer
                None
            } else {
                // Otherwise, decode it as a normal Address.
                Some(Address::decode(rlp_buf)?)
            }
        };

        let value: U256 = U256::decode(rlp_buf)?;
        let gas: U64 = U64::decode(rlp_buf)?;
        let gas_used: U64 = U64::decode(rlp_buf)?;
        let input = Bytes::decode(rlp_buf)?;
        let output = Bytes::decode(rlp_buf)?;
        let status: U8 = U8::decode(rlp_buf)?;
        let depth: U64 = U64::decode(rlp_buf)?;

        Ok(Self {
            typ,
            flags,
            from,
            to,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            depth,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, RlpDecodable)]
pub struct CallFrames(Vec<CallFrame>);

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TracerCfg {
    pub only_top_call: bool,
}

#[derive(Deserialize, Debug, Default)]
pub struct TracerObject {
    #[serde(default)]
    tracer: Tracer,
    tracerCfg: Option<TracerCfg>,
    timeout: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub enum Tracer {
    #[default]
    #[serde(rename = "callTracer")]
    CallTracer,
    #[serde(rename = "prestateTracer")]
    PreStateTracer,
}

#[derive(Deserialize, Debug)]
struct MonadDebugTraceTransactionParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
    #[serde(default)]
    tracer: TracerObject,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct MonadCallFrame {
    #[serde(rename = "type")]
    typ: String,
    from: EthAddress,
    to: Option<EthAddress>,
    value: U256,
    gas: U64,
    gas_used: U64,
    input: UnformattedData,
    output: UnformattedData,
    #[serde(skip)]
    depth: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revert_reason: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    calls: Vec<std::rc::Rc<std::cell::RefCell<MonadCallFrame>>>,
}

impl From<CallFrame> for MonadCallFrame {
    fn from(value: CallFrame) -> Self {
        Self {
            typ: value.typ.to_string(),
            from: value.from.into(),
            to: value.to.map(Into::into),
            value: value.value.into(),
            gas: value.gas.into(),
            gas_used: value.gas_used.into(),
            input: value.input.into(),
            output: value.output.into(),
            depth: value.depth.to::<usize>(),
            error: None, //TODO
            revert_reason: None,
            calls: Vec::new(),
        }
    }
}

#[allow(non_snake_case)]
pub async fn monad_debugTraceTransaction(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_debugTraceTransaction: {params:?}");

    let p: MonadDebugTraceTransactionParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let tx_hash = p.tx_hash.0;
    let key = EthTxKey(B256::new(tx_hash));
    let Some(result) = blockdb_env.get_txn(key).await else {
        debug!("transaction not found");
        return serialize_result(None::<Transaction>);
    };
    let block_key = result.block_hash;
    let block = blockdb_env
        .get_block_by_hash(block_key)
        .await
        .expect("txn was found so its block should exist");
    let block_num = block.block.number;

    match triedb_env.get_call_frame(tx_hash, block_num).await {
        TriedbResult::Null => return serialize_result(None::<CallFrame>),
        TriedbResult::CallFrame(rlp_call_frames) => {
            let call_frames = Vec::<Vec<CallFrame>>::decode(&mut rlp_call_frames.as_slice())
                .map_err(|e| JsonRpcError::custom(format!("Rlp Decode error: {e}")))?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            return serialize_result(build_call_tree(call_frames));
        }
        _ => return Err(JsonRpcError::custom("Not matched".to_string())),
    };
}

fn build_call_tree(
    nodes: Vec<CallFrame>,
) -> Option<std::rc::Rc<std::cell::RefCell<MonadCallFrame>>> {
    if nodes.is_empty() {
        return None;
    }

    let root = Rc::new(RefCell::new(MonadCallFrame::from(nodes[0].clone())));
    let mut stack = vec![Rc::clone(&root)];

    for value in nodes.into_iter().skip(1) {
        let depth = value.depth.to::<usize>();
        let new_node = Rc::new(RefCell::new(MonadCallFrame::from(value)));

        while let Some(last) = stack.last() {
            if last.borrow().depth < depth {
                last.borrow_mut().calls.push(Rc::clone(&new_node));
                break;
            }
            stack.pop();
        }

        stack.push(new_node);
    }

    Some(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hex;

    #[test]
    fn test_build_call_tree() {
        // depth of each call is the following [1, 2, 3, 3]
        let frames = hex::decode("0xf90aa0f9019f808094f39fd6e51aad88f6f4ce6ab8827279cfffb92266949fe46736679d2d9a65f0992f2272de9f3c7fa6e080831e84808307a930b90144f4a6659c000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb922660000000000000000000000005fbdb2315678afecb367f032d93f642f64180aa3000000000000000000000000e7f1725e7734ce288f8367e1bb143e90bb3f0512000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005f5e100000000000000000000000000000000000000000000000000000000000000271000000000000000000000000000000000000000000000000000005af3107a40000000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000e451980132e65465d0a498c53f0b5227326dd73f8080f906bf0380949fe46736679d2d9a65f0992f2272de9f3c7fa6e094e451980132e65465d0a498c53f0b5227326dd73f80831d2263830608c3b9068460806040526040516104c43803806104c4833981016040819052610022916102d2565b61002d82825f610034565b50506103e7565b61003d8361005f565b5f825111806100495750805b1561005a57610058838361009e565b505b505050565b610068816100ca565b6040516001600160a01b038216907fbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b905f90a250565b60606100c3838360405180606001604052806027815260200161049d6027913961017d565b9392505050565b6001600160a01b0381163b61013c5760405162461bcd60e51b815260206004820152602d60248201527f455243313936373a206e657720696d706c656d656e746174696f6e206973206e60448201526c1bdd08184818dbdb9d1c9858dd609a1b60648201526084015b60405180910390fd5b7f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc80546001600160a01b0319166001600160a01b0392909216919091179055565b60605f80856001600160a01b031685604051610199919061039a565b5f60405180830381855af49150503d805f81146101d1576040519150601f19603f3d011682016040523d82523d5f602084013e6101d6565b606091505b5090925090506101e8868383876101f2565b9695505050505050565b606083156102605782515f03610259576001600160a01b0385163b6102595760405162461bcd60e51b815260206004820152601d60248201527f416464726573733a2063616c6c20746f206e6f6e2d636f6e74726163740000006044820152606401610133565b508161026a565b61026a8383610272565b949350505050565b8151156102825781518083602001fd5b8060405162461bcd60e51b815260040161013391906103b5565b634e487b7160e01b5f52604160045260245ffd5b5f5b838110156102ca5781810151838201526020016102b2565b50505f910152565b5f80604083850312156102e3575f80fd5b82516001600160a01b03811681146102f9575f80fd5b60208401519092506001600160401b0380821115610315575f80fd5b818501915085601f830112610328575f80fd5b81518181111561033a5761033a61029c565b604051601f8201601f19908116603f011681019083821181831017156103625761036261029c565b8160405282815288602084870101111561037a575f80fd5b61038b8360208301602088016102b0565b80955050505050509250929050565b5f82516103ab8184602087016102b0565b9190910192915050565b602081525f82518060208401526103d38160408501602087016102b0565b601f01601f19169190910160400192915050565b60aa806103f35f395ff3fe608060405236601057600e6013565b005b600e5b601f601b6021565b6057565b565b5f60527f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc546001600160a01b031690565b905090565b365f80375f80365f845af43d5f803e8080156070573d5ff35b3d5ffdfea2646970667358221220dc385d1a646905a2bf7c2558648b32507745ba71a9f460aa1dc57cc1bf40e8ce64736f6c63430008140033416464726573733a206c6f772d6c6576656c2064656c65676174652063616c6c206661696c656400000000000000000000000075537828f2ce51be7289709686a69cbfdbb714f10000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000014415fcc826000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb922660000000000000000000000005fbdb2315678afecb367f032d93f642f64180aa3000000000000000000000000e7f1725e7734ce288f8367e1bb143e90bb3f0512000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005f5e100000000000000000000000000000000000000000000000000000000000000271000000000000000000000000000000000000000000000000000005af3107a4000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000808001f9017f018094e451980132e65465d0a498c53f0b5227326dd73f9475537828f2ce51be7289709686a69cbfdbb714f180831c3f6e83051220b9014415fcc826000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb922660000000000000000000000005fbdb2315678afecb367f032d93f642f64180aa3000000000000000000000000e7f1725e7734ce288f8367e1bb143e90bb3f0512000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005f5e100000000000000000000000000000000000000000000000000000000000000271000000000000000000000000000000000000000000000000000005af3107a40000000000000000000000000000000000000000000000000000000000000000000808002f85b800194e451980132e65465d0a498c53f0b5227326dd73f94e7f1725e7734ce288f8367e1bb143e90bb3f0512808318fc7881f884313ce567a000000000000000000000000000000000000000000000000000000000000000128003f85b800194e451980132e65465d0a498c53f0b5227326dd73f945fbdb2315678afecb367f032d93f642f64180aa3808318998f81f884313ce567a000000000000000000000000000000000000000000000000000000000000000068003").expect("decode call frame");
        let frames = Vec::<Vec<CallFrame>>::decode(&mut frames.as_slice())
            .expect("decode call frame")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let result = build_call_tree(frames);
        assert!(result.is_some());
        let result: Rc<RefCell<MonadCallFrame>> = result.unwrap();
        assert_eq!(result.borrow().calls.len(), 1);
        result
            .borrow()
            .calls
            .iter()
            .enumerate()
            .for_each(|(idx, frame)| match idx {
                0 => assert_eq!(frame.borrow().calls.len(), 1),
                1 => assert_eq!(frame.borrow().calls.len(), 1),
                2 => assert_eq!(frame.borrow().calls.len(), 2),
                _ => panic!("unexpected index"),
            });
    }
}
