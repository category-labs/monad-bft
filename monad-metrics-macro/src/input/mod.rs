use convert_case::{Case, Casing};
use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::TokenStreamExt;
use syn::{
    braced,
    parse::{discouraged::Speculative, Parse, ParseStream},
    spanned::Spanned,
    Ident, Token,
};

pub use self::{
    attrs::{Attrs, ScalarAttrs, ScalarLabelAttrs},
    name::MetricName,
};

mod attrs;
mod name;

pub struct MetricsInput {
    pub namespace: Ident,
    pub struct_name: Ident,
    pub ts_error: TokenStream,
    pub scalars: Vec<MetricName<Attrs<ScalarAttrs>>>,
    pub components: MetricsInputList,
}

impl Parse for MetricsInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let namespace_attrs: Option<Attrs<Ident>> = input.parse().ok();

        let struct_name = input.parse::<Ident>()?;

        let namespace = namespace_attrs
            .map(|Attrs { data }| data)
            .unwrap_or_else(|| {
                Ident::new(
                    &struct_name.to_string().to_case(Case::Snake),
                    struct_name.span(),
                )
            });

        let mut ts_error = TokenStream::new();

        if ["counters", "gauges"].contains(&namespace.to_string().as_str()) {
            ts_error.append_all([quote::quote_spanned! {
                namespace.span() => compile_error!("Metric namespace cannot be instrument name!");
            }]);
        }

        let content;
        braced!(content in input);

        let mut scalars: Vec<MetricName<Attrs<ScalarAttrs>>> = Vec::default();

        loop {
            let fork = content.fork();

            let Ok(scalar) = fork.parse() else {
                break;
            };

            scalars.push(scalar);
            content.advance_to(&fork);

            if content.is_empty() {
                break;
            }

            content.parse::<Token![,]>()?;
        }

        for metric in scalars.iter_mut() {
            if metric.path.ident.to_string().ends_with("_total") {
                metric.ts_error.append_all([quote::quote_spanned! {
                    metric.path.span() => compile_error!("Metric should not end in _total");
                }]);
            }
        }

        for (ident, ts_error) in scalars
            .iter_mut()
            .map(|metric| (metric.path.ident.to_owned(), &mut metric.ts_error))
            .duplicates_by(|(ident, _)| ident.to_string())
        {
            ts_error.append_all([quote::quote_spanned! {
                ident.span() => compile_error!("Duplicate metric name!");
            }]);
        }

        let components = content.parse().unwrap();

        if !content.is_empty() {
            return Err(syn::Error::new(content.span(), "Unexpected token stream!"));
        }

        Ok(Self {
            struct_name,
            namespace,
            ts_error,
            scalars,
            components,
        })
    }
}

#[derive(Default)]
pub struct MetricsInputList(pub Vec<MetricsInput>);

impl Parse for MetricsInputList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut components = Vec::default();

        while !input.is_empty() {
            components.push(input.parse()?);

            if input.is_empty() {
                break;
            }

            input.parse::<Token![,]>()?;
        }

        Ok(Self(components))
    }
}
