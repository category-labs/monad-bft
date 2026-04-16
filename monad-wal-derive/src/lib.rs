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
use syn::{parse_macro_input, Data, DeriveInput, Fields, Ident};

#[proc_macro_derive(WALLogged, attributes(wal_logged))]
pub fn derive_wal_logged(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let is_wal_logged = match input.data {
        Data::Enum(data) => {
            let arms = data.variants.into_iter().map(|variant| {
                let ident = variant.ident;
                let is_logged = variant
                    .attrs
                    .iter()
                    .any(|attr| attr.path.is_ident("wal_logged"));
                let pattern = match variant.fields {
                    Fields::Named(_) => quote!(Self::#ident { .. }),
                    Fields::Unnamed(_) => quote!(Self::#ident ( .. )),
                    Fields::Unit => quote!(Self::#ident),
                };

                quote!(#pattern => #is_logged)
            });

            quote! {
                match self {
                    #( #arms, )*
                }
            }
        }
        _ => quote!(true),
    };

    quote! {
        impl #impl_generics ::monad_wal::wal::WALLoggable for #ident #ty_generics #where_clause {
            fn is_wal_logged(&self) -> bool {
                #is_wal_logged
            }
        }

        impl #impl_generics ::monad_wal::wal::WALLogged for #ident #ty_generics #where_clause {}
    }
    .into()
}

#[proc_macro_derive(NotLogged)]
pub fn derive_not_logged(input: TokenStream) -> TokenStream {
    derive_marker(input, "NotLogged", false)
}

fn derive_marker(input: TokenStream, marker: &str, is_wal_logged: bool) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let marker = Ident::new(marker, proc_macro2::Span::call_site());
    let is_wal_logged = if is_wal_logged {
        quote!(true)
    } else {
        quote!(false)
    };

    quote! {
        impl #impl_generics ::monad_wal::wal::WALLoggable for #ident #ty_generics #where_clause {
            fn is_wal_logged(&self) -> bool {
                #is_wal_logged
            }
        }

        impl #impl_generics ::monad_wal::wal::#marker for #ident #ty_generics #where_clause {}
    }
    .into()
}
