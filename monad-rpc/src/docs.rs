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

use std::collections::HashMap;

use monad_rpc_docs::OpenRpc;

pub fn as_openrpc() -> OpenRpc {
    let method_map = &monad_rpc_docs::FUNCTION_MAP;

    let mut components = monad_rpc_docs::Components {
        schemas: HashMap::new(),
    };

    // Register components from all RPC methods
    for (_, info) in method_map.iter() {
        (info.register_components)(&mut components);
    }

    let mut methods = Vec::new();
    for (name, info) in method_map.iter() {
        let mut params: Vec<monad_rpc_docs::Params> = Vec::new();
        if let Some(in_schema) = (info.input_schema)() {
            let in_schema_title = in_schema.schema.clone().metadata().clone().title.unwrap();
            let mut in_sub_schema: schemars::schema::Schema = in_schema.schema.into();
            monad_rpc_docs::clean_schema_refs(&mut in_sub_schema);

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
        monad_rpc_docs::clean_schema_refs(&mut out_sub_schema);

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

    // Sort methods alphabetically
    methods.sort_by(|a, b| a.name.cmp(&b.name));

    monad_rpc_docs::OpenRpc {
        openrpc: "1.0.0".to_string(),
        info: monad_rpc_docs::Info {
            version: "1.0.0".to_string(),
            description: "This section provides an interactive reference for the Monad's JSON-RPC API.\n\nView the JSON-RPC API methods by selecting a method in the left sidebar. You can test the methods directly in the page using the API playground, with pre-configured examples or custom parameters. You can also save URLs with custom parameters using your browser's bookmarks.".to_string(),
            title: "Monad RPC".to_string(),
        },
        methods,
        components,
    }
}
