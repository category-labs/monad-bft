use proc_macro::TokenStream;
use quote::ToTokens;
use syn::{parse::Parse, parse_macro_input};

use self::{def::MetricsDef, input::MetricsInput};

mod def;
mod input;

fn generate_monad_metrics(
    input: TokenStream,
    parent_name: &'static str,
    parent_reported_name: &'static str,
) -> TokenStream {
    let input = parse_macro_input!(input with MetricsInput::parse);

    MetricsDef::generate(
        parent_name.to_string(),
        parent_reported_name.to_string(),
        input,
    )
    .to_token_stream()
    .into()
}

#[proc_macro]
pub fn metrics_bft(input: TokenStream) -> TokenStream {
    generate_monad_metrics(input, "Metrics", "monad_bft")
}
