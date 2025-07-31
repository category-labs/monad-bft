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

use std::{borrow::BorrowMut, collections::HashMap};

pub use inventory;
pub use monad_rpc_docs_derive::rpc;
pub use once_cell::sync::Lazy;
use serde::Serialize;

inventory::collect!(RpcMethodInfo);

pub static FUNCTION_MAP: Lazy<HashMap<String, &RpcMethodInfo>> = Lazy::new(|| {
    let mut map = HashMap::new();
    for info in inventory::iter::<RpcMethodInfo> {
        map.insert(info.name.to_string(), info);
    }
    map
});

pub trait RpcMethod {
    type Input;
    type Output;

    const NAME: &'static str;

    fn input_schema() -> Option<schemars::schema::RootSchema>;
    fn output_schema() -> Option<schemars::schema::RootSchema>;
    fn register_components(components: &mut Components);
}

#[derive(Clone, Copy)]
pub struct RpcMethodInfo {
    pub name: &'static str,
    pub docs: &'static str,
    pub input_schema: fn() -> Option<schemars::schema::RootSchema>,
    pub output_schema: fn() -> Option<schemars::schema::RootSchema>,
    pub register_components: fn(&mut Components),
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenRpc {
    pub openrpc: String,
    pub info: Info,
    pub components: Components,
    pub methods: Vec<Method>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Info {
    pub version: String,
    pub title: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Method {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub params: Vec<Params>,
    pub result: MethodResult,
}

#[derive(Debug, Clone, Serialize)]
pub struct MethodResult {
    pub name: String,
    pub description: String,
    pub schema: schemars::schema::Schema,
}

#[derive(Debug, Clone, Serialize)]
pub struct Components {
    pub schemas: HashMap<String, schemars::schema::Schema>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Params {
    pub name: String,
    pub description: String,
    pub required: bool,
    pub schema: schemars::schema::Schema,
}

pub fn clean_schema_refs(schema: &mut schemars::schema::Schema) {
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

        o.object().additional_properties.iter_mut().for_each(|v| {
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
