use proc_macro2::TokenStream;
use syn::{Ident, PathSegment};

pub struct ComponentResult {
    name: Ident,
    path: PathSegment,
    counters: Vec<PathSegment>,
    gauges: Vec<PathSegment>,
    components: Vec<ComponentResult>,
    tokens: TokenStream,
}

impl ComponentResult {
    pub fn new(
        name: Ident,
        path: PathSegment,
        counters: Vec<PathSegment>,
        gauges: Vec<PathSegment>,
        components: Vec<ComponentResult>,
        tokens: TokenStream,
    ) -> Self {
        Self {
            name,
            path,
            counters,
            gauges,
            components,
            tokens,
        }
    }

    pub fn name_field(&self) -> (Ident, PathSegment) {
        (self.name.clone(), self.path.clone())
    }

    pub fn field(&self) -> PathSegment {
        self.path.clone()
    }

    pub fn as_token_stream(&self) -> TokenStream {
        self.tokens.clone()
    }

    pub fn gen_static(&self) -> (Vec<TokenStream>, TokenStream) {
        let mut static_defs = Vec::default();

        let static_init = self.gen_static_rec(&mut static_defs, Vec::default());

        (static_defs, static_init)
    }

    fn gen_static_rec(
        &self,
        static_defs: &mut Vec<TokenStream>,
        mut path_prefix: Vec<PathSegment>,
    ) -> TokenStream {
        let name = self.name.clone();
        path_prefix.push(self.path.clone());

        let mut gen_static_ident = |name: &Ident| {
            let mut path = path_prefix
                .iter()
                .map(|segment| segment.ident.to_string().to_uppercase())
                .collect::<Vec<_>>();
            path.push(name.to_string().to_uppercase());

            let ident = Ident::new(&format!("STATIC_METRICS_{}", path.join("_")), name.span());

            static_defs.push(quote::quote! {
                static #ident: AtomicU64 = AtomicU64::new(0);
            });

            ident
        };

        let counter_inits = self
            .counters
            .iter()
            .map(|path| {
                let counter_name = path.ident.clone();
                let static_name = gen_static_ident(&counter_name);

                quote::quote! {
                    #counter_name: StaticCounter::from_static(&#static_name)
                }
            })
            .collect::<Vec<_>>();

        let gauge_inits = self
            .gauges
            .iter()
            .map(|path| {
                let counter_name = path.ident.clone();
                let static_name = gen_static_ident(&counter_name);

                quote::quote! {
                    #counter_name: StaticGauge::from_static(&#static_name)
                }
            })
            .collect::<Vec<_>>();

        let components = self
            .components
            .iter()
            .map(|component| {
                let name = component.path.ident.clone();
                let ts = component.gen_static_rec(static_defs, path_prefix.clone());

                quote::quote! {
                    #name: #ts
                }
            })
            .collect::<Vec<_>>();

        quote::quote! {
            super::#name {
                #(
                    #counter_inits,
                )*
                #(
                    #gauge_inits,
                )*
                #(
                    #components,
                )*
            }
        }
    }
}
