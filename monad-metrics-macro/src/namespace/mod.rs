use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    braced,
    parse::{Parse, ParseStream},
    Ident, PathSegment, Token,
};

pub use self::list::NamespaceList;
use self::util::{parse_metric_field, try_parse_optional_metric_field};
use crate::{names::MetricGroup, result::ComponentResult};

mod list;
mod util;

pub struct Namespace {
    struct_name: Ident,
    name: PathSegment,
    counters: Option<MetricGroup>,
    gauges: Option<MetricGroup>,
    components: Option<NamespaceList>,
}

impl Parse for Namespace {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let struct_name = input.parse::<Ident>()?;

        let content;
        braced!(content in input);

        let namespace = parse_metric_field::<PathSegment>(&content, "namespace")?;

        if !content.is_empty() {
            content.parse::<Token![,]>()?;
        }

        let counters = try_parse_optional_metric_field(&content, "counters")?;

        if counters.is_some() && !content.is_empty() {
            content.parse::<Token![,]>()?;
        }

        let gauges = try_parse_optional_metric_field(&content, "gauges")?;

        if gauges.is_some() && !content.is_empty() {
            content.parse::<Token![,]>()?;
        }

        let components = try_parse_optional_metric_field(&content, "components")?;

        if components.is_some() && !content.is_empty() {
            content.parse::<Token![,]>()?;
        }

        if !content.is_empty() {
            return Err(syn::Error::new(content.span(), "Unexpected token stream!"));
        }

        Ok(Self {
            struct_name,
            name: namespace,
            counters,
            gauges,
            components,
        })
    }
}

impl Namespace {
    pub fn augment(self, augment_name: &str) -> Self {
        let Self {
            struct_name: name,
            name: namespace,
            counters,
            gauges,
            components,
        } = self;

        let name = Ident::new(&format!("{name}{augment_name}"), name.span());
        let components = components.map(|component| component.augment(name.to_string().as_str()));

        Self {
            struct_name: name,
            name: namespace,
            counters,
            gauges,
            components,
        }
    }

    pub fn generate(self, namespace_prefix: String) -> ComponentResult {
        let Self {
            struct_name,
            name,
            counters,
            gauges,
            components,
        } = self;

        let components_results = {
            let namespace_prefix = format!("{namespace_prefix}{}.", name.ident.to_string());

            components
                .map(|components| components.generate(namespace_prefix))
                .unwrap_or_default()
        };

        let generate_tuple_thing = |path: &PathSegment| {
            let counter_path_str = format!(
                "{}{}.{}",
                namespace_prefix,
                name.ident,
                path.ident.to_string()
            );

            quote! {
                #counter_path_str,
                &self.#path
            }
        };

        let counters = counters
            .map(|counters| counters.as_vec())
            .unwrap_or_default();
        let counter_calls = counters.iter().map(generate_tuple_thing);

        let gauges = gauges.map(|gauges| gauges.as_vec()).unwrap_or_default();
        let gauge_calls = gauges.iter().map(generate_tuple_thing);

        let component_field_definitions = components_results
            .iter()
            .map(ComponentResult::name_field)
            .map(|(component_name, component_field)| {
                quote! {
                    #component_field: #component_name
                }
            })
            .collect::<Vec<_>>();

        let component_fields = components_results
            .iter()
            .map(ComponentResult::field)
            .collect::<Vec<_>>();

        let components_ts = components_results
            .iter()
            .map(|component_result| component_result.as_token_stream())
            .collect::<TokenStream>();

        let tokens = quote! {
            #components_ts

            #[derive(Debug)]
            pub struct #struct_name<MP>
            where
                MP: MetricsPolicy
            {
                #(
                    pub #component_field_definitions<MP>,
                )*
                #(
                    pub #counters: MP::Counter,
                )*
                #(
                    pub #gauges: MP::Gauge,
                )*
            }

            impl<MP> Clone for #struct_name<MP>
            where
                MP: MetricsPolicy,
            {
                fn clone(&self) -> Self {
                    Self {
                        #(
                            #component_fields: self.#component_fields.clone(),
                        )*
                        #(
                            #counters: self.#counters.clone(),
                        )*
                        #(
                            #gauges: self.#gauges.clone(),
                        )*
                    }
                }
            }

            impl<MP> Default for #struct_name<MP>
            where
                MP: MetricsPolicy,
                MP::Counter: Default,
                MP::Gauge: Default,
            {
                fn default() -> Self {
                    Self {
                        #(
                            #component_fields: Default::default(),
                        )*
                        #(
                            #counters: Default::default(),
                        )*
                        #(
                            #gauges: Default::default(),
                        )*
                    }
                }
            }

            impl<MP> #struct_name<MP>
            where
                MP: MetricsPolicy,
            {
                pub fn traverse(
                    &self,
                    on_counter: &mut impl FnMut(&'static str, &MP::Counter) -> (),
                    on_gauge: &mut impl FnMut(&'static str, &MP::Gauge) -> (),
                ) {
                    #(
                        on_counter(#counter_calls);
                    )*
                    #(
                        on_gauge(#gauge_calls);
                    )*
                    #(
                        self.#component_fields.traverse(on_counter, on_gauge);
                    )*
                }
            }
        };

        ComponentResult::new(
            struct_name,
            name,
            counters,
            gauges,
            components_results,
            tokens,
        )
    }
}
