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

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{
    parse_macro_input, parse_quote, Attribute, AttributeArgs, FnArg, GenericArgument, Ident,
    ItemFn, Lit, Meta, NestedMeta, PatType, PathArguments, ReturnType, Type,
};

/// Decorates an rpc function with a helper function to generate open-rpc documentation.
/// It is expected that schemars::JsonSchema is derived for inputs and outputs passed to the function.
/// `rpc(method_name = "my_method")` can be used to override the default method name.
/// `rpc(ignore = variable)` can be used to ignore a variable from the documentation.
/// Reference types are ignored by default.
#[proc_macro_attribute]
pub fn rpc(attr_args: TokenStream, decorated: TokenStream) -> TokenStream {
    let input = parse_macro_input!(decorated as ItemFn);

    let fn_name = &input.sig.ident;
    let inputs = &input.sig.inputs;

    let attr_args: AttributeArgs = parse_macro_input!(attr_args);
    let attr_args = extract_attrs(&attr_args);
    let method_name = attr_args.method_name.unwrap_or(fn_name.to_string());
    let input_info = extract_input_info(inputs, &attr_args.ignore_inputs);
    let output = &input.sig.output;
    let output_info = extract_output_info(output.clone());

    let docs = extract_docs(&input.attrs);

    let name_ident: Ident = Ident::new(
        &to_upper_camel_case(format!("{}_NAME", fn_name).as_str()),
        fn_name.span(),
    );
    let info_ident = Ident::new(
        &to_upper_camel_case(format!("{}_INFO", fn_name).as_str()),
        fn_name.span(),
    );

    let input_type: Type = input_info
        .as_ref()
        .map(|(_, typ, _)| typ.clone())
        .unwrap_or(parse_quote!([u8; 0]));
    let input_schema: TokenStream2 = input_info
        .as_ref()
        .map(|(_, _, schema)| schema.clone())
        .unwrap_or(parse_quote!(None));

    let output_type: Type = output_info
        .as_ref()
        .map(|(_, typ, _)| typ.clone())
        .unwrap_or(parse_quote!(()));
    let output_schema: TokenStream2 = output_info
        .as_ref()
        .map(|(_, _, schema)| schema.clone())
        .unwrap_or(parse_quote!(None));

    TokenStream::from(quote! {
        #input

        #[doc(hidden)]
        struct #name_ident;

        #[doc(hidden)]
        impl monad_rpc_docs::RpcMethod for #name_ident {
            type Input = #input_type;
            type Output = #output_type;
            const NAME: &'static str = #method_name;

            fn input_schema() -> Option<schemars::schema::RootSchema> {
                #input_schema
            }

            fn output_schema() -> Option<schemars::schema::RootSchema> {
                #output_schema
            }

            fn register_components(components: &mut monad_rpc_docs::Components) {
                // Register input schema components
                if let Some(schema) = Self::input_schema() {
                    let definitions = schema.definitions;
                    let schemas: std::collections::HashMap<_, _> = definitions.into_iter().map(|(k,v)| {
                        let mut v = v.clone();
                        monad_rpc_docs::clean_schema_refs(&mut v);
                        (k, v)
                     }).collect();
                    components.schemas.extend(schemas);
                };

                // Register output schema components
                if let Some(schema) = Self::output_schema() {
                    let definitions = schema.definitions;
                    let schemas: std::collections::HashMap<_, _> = definitions.into_iter().map(|(k,v)| {
                        let mut v = v.clone();
                        monad_rpc_docs::clean_schema_refs(&mut v);
                        (k, v)
                     }).collect();
                    components.schemas.extend(schemas);
                };
            }
        }

        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        pub static #info_ident: monad_rpc_docs::RpcMethodInfo = ::monad_rpc_docs::RpcMethodInfo {
            name: #method_name,
            docs: #docs,
            input_schema: <#name_ident as monad_rpc_docs::RpcMethod>::input_schema,
            output_schema: <#name_ident as monad_rpc_docs::RpcMethod>::output_schema,
            register_components: <#name_ident as monad_rpc_docs::RpcMethod>::register_components,
        };

        monad_rpc_docs::inventory::submit! {
            #info_ident
        }
    })
}

fn extract_docs(attrs: &[Attribute]) -> String {
    attrs
        .iter()
        .filter(|attr| attr.path.is_ident("doc"))
        .filter_map(|attr| {
            if let Ok(syn::Meta::NameValue(nv)) = attr.parse_meta() {
                if let syn::Lit::Str(lit) = nv.lit {
                    Some(lit.value())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Debug, Default)]
struct Args {
    method_name: Option<String>,
    ignore_inputs: Vec<String>,
}

fn extract_attrs(attrs: &[NestedMeta]) -> Args {
    let mut args = Args::default();

    for attr in attrs {
        if let NestedMeta::Meta(Meta::NameValue(nv)) = attr {
            if nv.path.is_ident("method") {
                if let Lit::Str(lit) = &nv.lit {
                    args.method_name = Some(lit.value());
                }
            }

            if nv.path.is_ident("ignore") {
                if let Lit::Str(lit) = &nv.lit {
                    let value = lit.value();
                    for item in value.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                        args.ignore_inputs.push(item.to_string());
                    }
                }
            }
        }
    }

    args
}

fn extract_input_info(
    inputs: &syn::punctuated::Punctuated<FnArg, syn::token::Comma>,
    ignore_inputs: &[String],
) -> Option<(String, Type, TokenStream2)> {
    let collected: Vec<(String, Type, TokenStream2)> = inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(PatType { pat, ty, .. }) = arg {
                let name = pat.to_token_stream().to_string();

                if ignore_inputs.contains(&name) {
                    return None;
                }

                if !matches!(&**ty, Type::Path(_)) {
                    return None;
                }

                let schema_expr: TokenStream2 = generate_schema_expr(ty);
                Some((name, (**ty).clone(), schema_expr))
            } else {
                None
            }
        })
        .collect();

    if collected.len() > 1 {
        panic!("Expected at most one input, got {}", collected.len());
    }

    collected.into_iter().next()
}

fn extract_output_info(output: ReturnType) -> Option<(String, Type, TokenStream2)> {
    match output {
        ReturnType::Default => None,
        ReturnType::Type(_, ty) => match *ty {
            Type::Path(type_path) => {
                let mut segments = type_path.path.segments.iter();
                let result = segments.next()?;
                let mut args = match &result.arguments {
                    PathArguments::AngleBracketed(args) => args.args.iter(),
                    _ => return None,
                };

                match args.next() {
                    Some(GenericArgument::Type(inner_ty)) => {
                        let type_ident = match &type_path.path.segments.last() {
                            Some(segment) => &segment.ident,
                            None => return None,
                        };

                        let name = type_ident.to_string();

                        // Check if inner_ty is Option<T>
                        if let Type::Path(inner_path) = inner_ty {
                            if let Some(last_segment) = inner_path.path.segments.last() {
                                if last_segment.ident == "Option" || last_segment.ident == "Vec" {
                                    if let PathArguments::AngleBracketed(inner_args) =
                                        &last_segment.arguments
                                    {
                                        if let Some(GenericArgument::Type(inner_ty)) =
                                            inner_args.args.first()
                                        {
                                            let schema_expr = generate_schema_expr(inner_ty);
                                            return Some((name, (*inner_ty).clone(), schema_expr));
                                        }
                                    }
                                }
                            }
                        }

                        let schema_expr = generate_schema_expr(inner_ty);
                        Some((name, (*inner_ty).clone(), schema_expr))
                    }
                    _ => None,
                }
            }
            _ => None,
        },
    }
}

fn generate_schema_expr(ty: &Type) -> TokenStream2 {
    match ty {
        Type::Path(type_path) => {
            quote! {
                Some(schemars::schema_for!(#type_path))
            }
        }
        Type::Reference(type_reference) => {
            if let Type::Path(type_path) = &*type_reference.elem {
                let type_ident = match &type_path.path.segments.last() {
                    Some(segment) => &segment.ident,
                    None => return quote! { None },
                };
                quote! {
                    Some(schemars::schema_for!(#type_ident))
                }
            } else {
                quote! { None }
            }
        }
        _ => quote! { None },
    }
}

fn to_upper_camel_case(s: &str) -> String {
    s.split("_")
        .flat_map(|word| {
            word.chars().enumerate().map(|(i, c)| {
                if i == 0 {
                    c.to_uppercase().next().unwrap()
                } else {
                    c.to_lowercase().next().unwrap()
                }
            })
        })
        .collect()
}
