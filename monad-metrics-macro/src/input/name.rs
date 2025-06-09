use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::{
    parse::{Parse, ParseStream},
    PathSegment,
};

use super::attrs::Attrs;

pub struct MetricName<T> {
    pub path: PathSegment,
    pub data: T,

    pub ts_field: TokenStream,
    pub ts_error: TokenStream,
}

impl<T> Parse for MetricName<Attrs<T>>
where
    T: Parse,
{
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let data = input.parse()?;

        let path: PathSegment = input.parse()?;

        let ts_field = path.to_token_stream();

        Ok(Self {
            path,
            data,

            ts_field,
            ts_error: TokenStream::new(),
        })
    }
}
