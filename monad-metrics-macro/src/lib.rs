use proc_macro::TokenStream;
use syn::{parse::Parse, parse_macro_input};

use self::namespace::Namespace;

mod names;
mod namespace;
mod result;

fn generate_monad_metrics(
    input: TokenStream,
    augment_name: &'static str,
    namespace_prefix: &'static str,
) -> TokenStream {
    let component = parse_macro_input!(input with Namespace::parse);

    let result = component
        .augment(augment_name)
        .generate(namespace_prefix.to_string());

    let result_name = result.name_field().0;

    let result_ts = result.as_token_stream();

    let (static_metric_statics, static_metric_init) = result.gen_static();

    quote::quote! {
        #result_ts

        mod r#static {
            use std::sync::atomic::AtomicU64;

            use monad_metrics::{StaticMetricsPolicy, StaticCounter, StaticGauge};

            #(
                #static_metric_statics
            )*

            impl super::#result_name<StaticMetricsPolicy> {
                pub fn build() -> Self
                {
                    #static_metric_init
                }
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn state_metrics(input: TokenStream) -> TokenStream {
    return generate_monad_metrics(input, "Metrics", "monad_bft.");
}

#[proc_macro]
pub fn executor_metrics(input: TokenStream) -> TokenStream {
    return generate_monad_metrics(input, "ExecutorMetrics", "monad_bft.");
}
