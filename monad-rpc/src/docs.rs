use std::{borrow::BorrowMut, collections::HashMap};

use monad_rpc_docs::OpenRpc;

use crate::{
    account_handlers::StorageProof,
    call::CallRequest,
    eth_json_types::{
        BlockTags, EthAddress, EthHash, MonadBlock, MonadLog, MonadTransaction,
        MonadTransactionReceipt, MonadU256, Quantity, UnformattedData,
    },
    eth_txn_handlers::{AddressValueOrArray, FilterParams, LogFilter},
    trace::{TraceCallObject, Tracer, TracerObject},
};

pub fn as_openrpc() -> OpenRpc {
    let method_map = &monad_rpc_docs::FUNCTION_MAP;

    let mut components = monad_rpc_docs::Components {
        schemas: HashMap::new(),
    };

    let quantity_schema = schemars::schema_for!(Quantity);
    components
        .schemas
        .insert("Quantity".to_string(), quantity_schema.schema.into());
    let hash_schema = schemars::schema_for!(EthHash);
    components
        .schemas
        .insert("EthHash".to_string(), hash_schema.schema.into());
    let address_schema = schemars::schema_for!(EthAddress);
    components
        .schemas
        .insert("EthAddress".to_string(), address_schema.schema.into());
    let monad_u256 = schemars::schema_for!(MonadU256);
    components
        .schemas
        .insert("MonadU256".to_string(), monad_u256.schema.into());
    let monad_block = schemars::schema_for!(MonadBlock);
    components
        .schemas
        .insert("MonadBlock".to_string(), monad_block.schema.into());
    let monad_receipt = schemars::schema_for!(MonadTransactionReceipt);
    components.schemas.insert(
        "MonadTransactionReceipt".to_string(),
        monad_receipt.schema.into(),
    );
    let monad_transaction = schemars::schema_for!(MonadTransaction);
    components.schemas.insert(
        "MonadTransaction".to_string(),
        monad_transaction.schema.into(),
    );
    let unformatted_data = schemars::schema_for!(UnformattedData);
    components.schemas.insert(
        "UnformattedData".to_string(),
        unformatted_data.schema.into(),
    );
    let mut blocktags = schemars::schema_for!(BlockTags).schema.into();
    clean_schema_refs(&mut blocktags);
    components
        .schemas
        .insert("BlockTags".to_string(), blocktags);
    let call_request = schemars::schema_for!(CallRequest);
    components
        .schemas
        .insert("CallRequest".to_string(), call_request.schema.into());
    let storage_proof = schemars::schema_for!(StorageProof);
    let mut storage_proof = storage_proof.schema.into();
    clean_schema_refs(&mut storage_proof);
    components
        .schemas
        .insert("StorageProof".to_string(), storage_proof);
    components.schemas.insert(
        "MonadLog".to_string(),
        schemars::schema_for!(MonadLog).schema.into(),
    );
    let mut tracer = schemars::schema_for!(Tracer).schema.into();
    clean_schema_refs(&mut tracer);
    components.schemas.insert("Tracer".to_string(), tracer);
    let mut tracer_obj = schemars::schema_for!(TracerObject).schema.into();
    clean_schema_refs(&mut tracer_obj);
    components
        .schemas
        .insert("TracerObject".to_string(), tracer_obj);
    let mut tracer_call_obj = schemars::schema_for!(TraceCallObject).schema.into();
    clean_schema_refs(&mut tracer_call_obj);
    components
        .schemas
        .insert("TraceCallObject".to_string(), tracer_call_obj);
    let mut filter_params = schemars::schema_for!(FilterParams).schema.into();
    clean_schema_refs(&mut filter_params);
    components
        .schemas
        .insert("FilterParams".to_string(), filter_params);
    let mut address_value_or_array = schemars::schema_for!(AddressValueOrArray).schema.into();
    clean_schema_refs(&mut address_value_or_array);
    components
        .schemas
        .insert("AddressValueOrArray".to_string(), address_value_or_array);
    let mut log_filter = schemars::schema_for!(LogFilter).schema.into();
    clean_schema_refs(&mut log_filter);
    components
        .schemas
        .insert("LogFilter".to_string(), log_filter);

    let mut methods = Vec::new();
    for (name, info) in method_map.iter() {
        println!("{name}");

        let mut params: Vec<monad_rpc_docs::Params> = Vec::new();
        if let Some(in_schema) = (info.input_schema)() {
            let in_schema_title = in_schema.schema.clone().metadata().clone().title.unwrap();
            let mut in_sub_schema: schemars::schema::Schema = in_schema.schema.into();
            clean_schema_refs(&mut in_sub_schema);

            let inputs = in_sub_schema
                .clone()
                .into_object()
                .object()
                .clone()
                .properties;

            for (k, v) in &inputs {
                components
                    .schemas
                    .insert(k.clone(), v.clone().into_object().into());
            }

            for (k, v) in inputs {
                let param = monad_rpc_docs::Params {
                    name: k.clone(),
                    description: "".to_string(),
                    required: true,
                    schema: v.clone(),
                };
                params.push(param);
            }

            components
                .schemas
                .insert(in_schema_title.clone(), in_sub_schema.clone());
        }

        let out_schema = ((info.output_schema)()).unwrap();
        let out_schema_title = out_schema.schema.clone().metadata().clone().title.unwrap();
        let mut out_sub_schema: schemars::schema::Schema = out_schema.schema.into();
        clean_schema_refs(&mut out_sub_schema);

        components
            .schemas
            .insert(out_schema_title.clone(), out_sub_schema.clone());

        methods.push(monad_rpc_docs::Method {
            name: name.to_string(),
            summary: Some(info.docs.to_string()),
            description: None,
            params,
            result: monad_rpc_docs::MethodResult {
                name: out_schema_title.to_string(),
                description: "".to_string(),
                schema: out_sub_schema.clone(),
            },
        })
    }

    monad_rpc_docs::OpenRpc {
        openrpc: "1.0.0".to_string(),
        info: monad_rpc_docs::Info {
            version: "1.0.0".to_string(),
            description: "Monad RPC".to_string(),
            title: "Monad RPC".to_string(),
        },
        methods,
        components,
    }
}

fn clean_schema_refs(schema: &mut schemars::schema::Schema) {
    if let schemars::schema::Schema::Object(ref mut o) = schema {
        o.array().items.iter_mut().for_each(|item| match item {
            schemars::schema::SingleOrVec::Single(s) => {
                if let schemars::schema::Schema::Object(ref mut o) = s.as_mut() {
                    if let Some(reference) = &o.reference {
                        let reference = reference.split("/").last().unwrap().to_string();
                        o.reference = Some(format!("#/components/schemas/{reference}"));

                        o.object().properties.iter_mut().for_each(|(_, v)| {
                            if v.is_ref() {
                                if let schemars::schema::Schema::Object(ref mut o) = v.borrow_mut()
                                {
                                    let reference = o
                                        .reference
                                        .as_ref()
                                        .unwrap()
                                        .split("/")
                                        .last()
                                        .unwrap()
                                        .to_string();
                                    o.reference = Some(format!("#/components/schemas/{reference}"));
                                }
                            }
                        });
                    }

                    o.object().properties.iter_mut().for_each(|(_, v)| {
                        clean_schema_refs(v);
                    });
                }
            }
            schemars::schema::SingleOrVec::Vec(v) => v.iter_mut().for_each(|item| {
                clean_schema_refs(item);
            }),
        });

        o.object().properties.iter_mut().for_each(|(_, v)| {
            if let schemars::schema::Schema::Object(ref mut o) = v {
                if let Some(reference) = &o.reference {
                    let reference = reference.split("/").last().unwrap().to_string();
                    o.reference = Some(format!("#/components/schemas/{reference}"));
                }
            }
        });

        o.subschemas().any_of.iter_mut().for_each(|v| {
            for item in v.iter_mut() {
                clean_schema_refs(item);
            }
        });

        o.object().properties.iter_mut().for_each(|(_, v)| {
            clean_schema_refs(v);
        });
    }

    if schema.is_ref() {
        if let schemars::schema::Schema::Object(ref mut o) = schema {
            let reference = o
                .reference
                .as_ref()
                .unwrap()
                .split("/")
                .last()
                .unwrap()
                .to_string();
            o.reference = Some(format!("#/components/schemas/{reference}"));
        }
    }
}
