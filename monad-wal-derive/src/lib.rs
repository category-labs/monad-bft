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

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Meta, NestedMeta};

enum WALLogMode {
    Disabled,
    Enabled,
    Nested,
}

#[proc_macro_derive(WALLog, attributes(wal))]
pub fn derive_wal_log(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let Data::Enum(data) = input.data else {
        return syn::Error::new_spanned(ident, "WALLog can only be derived on enums")
            .to_compile_error()
            .into();
    };

    let mut arms = Vec::new();
    for variant in data.variants {
        let ident = variant.ident;
        let mode = match wal_log_mode(&variant.attrs) {
            Ok(mode) => mode,
            Err(err) => return err.to_compile_error().into(),
        };
        let fields = variant.fields;
        let arm = match mode {
            WALLogMode::Enabled => match fields {
                Fields::Named(_) => quote!(Self::#ident { .. } => true),
                Fields::Unnamed(_) => quote!(Self::#ident(..) => true),
                Fields::Unit => quote!(Self::#ident => true),
            },
            WALLogMode::Nested => match fields {
                Fields::Named(fields) if fields.named.len() == 1 => {
                    let field_ident = fields.named.into_iter().next().unwrap().ident.unwrap();
                    quote!(
                        Self::#ident { #field_ident: wal_field } =>
                            ::monad_wal::wal::WALLog::is_wal_logged(wal_field)
                    )
                }
                Fields::Unnamed(fields) if fields.unnamed.len() == 1 => quote!(
                    Self::#ident(wal_field) => ::monad_wal::wal::WALLog::is_wal_logged(wal_field)
                ),
                _ => {
                    return syn::Error::new_spanned(
                        ident,
                        "#[wal(enable(nested))] requires exactly one field",
                    )
                    .to_compile_error()
                    .into()
                }
            },
            WALLogMode::Disabled => match fields {
                Fields::Named(_) => quote!(Self::#ident { .. } => false),
                Fields::Unnamed(_) => quote!(Self::#ident(..) => false),
                Fields::Unit => quote!(Self::#ident => false),
            },
        };

        arms.push(arm);
    }

    let is_wal_logged = quote! {
        match self {
            #( #arms, )*
        }
    };

    quote! {
        impl #impl_generics ::monad_wal::wal::WALLog for #ident #ty_generics #where_clause {
            fn is_wal_logged(&self) -> bool {
                #is_wal_logged
            }
        }
    }
    .into()
}

fn wal_log_mode(attrs: &[syn::Attribute]) -> syn::Result<WALLogMode> {
    let mut mode = WALLogMode::Disabled;
    for attr in attrs {
        let Some(parsed_mode) = parse_wal_log_mode(attr)? else {
            continue;
        };
        mode = parsed_mode;
    }
    Ok(mode)
}

fn parse_wal_log_mode(attr: &syn::Attribute) -> syn::Result<Option<WALLogMode>> {
    if !attr.path.is_ident("wal") {
        return Ok(None);
    }

    match attr.parse_meta()? {
        Meta::List(list) if list.nested.len() == 1 => match list.nested.first().unwrap() {
            NestedMeta::Meta(Meta::Path(path)) if path.is_ident("enable") => {
                Ok(Some(WALLogMode::Enabled))
            }
            NestedMeta::Meta(Meta::List(list))
                if list.path.is_ident("enable")
                    && list.nested.len() == 1
                    && matches!(
                        list.nested.first(),
                        Some(NestedMeta::Meta(Meta::Path(path))) if path.is_ident("nested")
                    ) =>
            {
                Ok(Some(WALLogMode::Nested))
            }
            _ => Err(invalid_wal_attribute(attr)),
        },
        _ => Err(invalid_wal_attribute(attr)),
    }
}

fn invalid_wal_attribute(attr: &syn::Attribute) -> syn::Error {
    syn::Error::new_spanned(
        attr,
        "unrecognized #[wal(...)] attribute; expected #[wal(enable)] or #[wal(enable(nested))]",
    )
}
