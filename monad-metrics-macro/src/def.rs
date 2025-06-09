use convert_case::{Case, Casing};
use itertools::Itertools;
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use syn::{spanned::Spanned, Ident};

use crate::input::{
    Attrs, MetricName, MetricsInput, MetricsInputList, ScalarAttrs, ScalarLabelAttrs,
};

pub struct MetricsDef {
    namespace: Ident,
    struct_name: Ident,
    ts: TokenStream,
}

impl MetricsDef {
    pub fn generate(
        parent_struct_name: String,
        parent_reported_name: String,
        input: MetricsInput,
    ) -> Self {
        let struct_name = Ident::new(
            &format!("{}{parent_struct_name}", input.struct_name),
            input.struct_name.span(),
        );
        let struct_name_str = struct_name.to_string();

        let reported_name = format!("{parent_reported_name}.{}", input.namespace.to_string());

        let (scalar_fields, scalar_calls, scalar_ts) =
            Self::generate_scalar_metrics(&struct_name_str, &reported_name, input.scalars);

        let (component_fields, component_calls, component_ts): (Vec<_>, Vec<_>, Vec<_>) =
            Self::generate_components(struct_name_str, reported_name, input.components);

        let ts_error = input.ts_error;

        let ts = quote::quote! {
            #( #component_ts )*
            #( #scalar_ts )*

            #[derive(Clone, Debug, Default)]
            pub struct #struct_name {
                #( pub #component_fields, )*
                #( pub #scalar_fields, )*
            }

            impl #struct_name {
                pub fn for_each(
                    &self,
                    on_counter: &mut impl FnMut(&'static str, &monad_metrics::Counter),
                    on_counter_labeled: &mut impl FnMut(&'static str, &'static str, &[(&'static str, &monad_metrics::Counter)]),
                    on_gauge: &mut impl FnMut(&'static str, &monad_metrics::Gauge),
                    on_gauge_labeled: &mut impl FnMut(&'static str, &'static str, &[(&'static str, &monad_metrics::Gauge)]),
                ) {
                    #ts_error

                    #( #component_calls )*
                    #( #scalar_calls )*
                }
            }
        };

        Self {
            namespace: input.namespace,
            struct_name,
            ts,
        }
    }

    fn generate_scalar_metrics(
        parent_struct_name: &String,
        parent_reported_name: &String,
        scalar_metrics: Vec<MetricName<Attrs<ScalarAttrs>>>,
    ) -> (Vec<TokenStream>, Vec<TokenStream>, Vec<TokenStream>) {
        let (fields, calls, struct_defs): (Vec<_>, Vec<_>, Vec<_>) = scalar_metrics
            .into_iter()
            .map(|scalar_metric| {
                let scalar_instrument = Ident::new(
                    &scalar_metric
                        .data
                        .data
                        .scalar_type
                        .to_string()
                        .to_case(Case::Pascal),
                    Span::call_site(),
                );

                let fn_call = Ident::new(
                    &format!("on_{}", scalar_metric.data.data.scalar_type.to_string()),
                    Span::call_site(),
                );

                let fn_call_labeled = Ident::new(&format!("{fn_call}_labeled"), fn_call.span());

                let reported_name = format!(
                    "{}.{}",
                    parent_reported_name,
                    scalar_metric.path.ident.to_string()
                );
                let ts_field = scalar_metric.ts_field;
                let ts_error = scalar_metric.ts_error;

                if let Some(ScalarLabelAttrs { label, variants }) = scalar_metric.data.data.labels {
                    let struct_name = Ident::new(
                        &format!(
                            "{}{parent_struct_name}",
                            scalar_metric.path.ident.to_string().to_case(Case::Pascal)
                        ),
                        scalar_metric.path.span(),
                    );

                    let reported_variants = variants.iter().map(|variant| {
                        let variant_str = variant.to_string();

                        quote::quote! { ( #variant_str, &self.#ts_field.#variant ) }
                    });

                    (
                        quote::quote! {
                            #ts_field: #struct_name
                        },
                        quote::quote! {
                            #ts_error

                            #fn_call_labeled(
                                #reported_name,
                                #label,
                                &[ #( #reported_variants ),* ]
                            );
                        },
                        Some(quote::quote! {
                            #[derive(Clone, Debug, Default)]
                            pub struct #struct_name {
                                #(
                                    pub #variants: monad_metrics::#scalar_instrument,
                                )*
                            }
                        }),
                    )
                } else {
                    (
                        quote::quote! {
                            #ts_field: monad_metrics::#scalar_instrument
                        },
                        quote::quote! {
                            #ts_error

                            #fn_call(#reported_name, &self.#ts_field);
                        },
                        None,
                    )
                }
            })
            .multiunzip();

        (fields, calls, struct_defs.into_iter().flatten().collect())
    }

    fn generate_components(
        struct_name_str: String,
        reported_name: String,
        components: MetricsInputList,
    ) -> (Vec<TokenStream>, Vec<TokenStream>, Vec<TokenStream>) {
        components
            .0
            .into_iter()
            .map(|input| {
                let Self {
                    namespace,
                    struct_name,
                    ts,
                } = Self::generate(struct_name_str.clone(), reported_name.clone(), input);

                (
                    quote::quote! {
                        #namespace: #struct_name
                    },
                    quote::quote! {
                        self.#namespace.for_each(on_counter, on_counter_labeled, on_gauge, on_gauge_labeled);
                    },
                    ts,
                )
            })
            .multiunzip()
    }
}

impl ToTokens for MetricsDef {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend([self.ts.clone()]);
    }
}
