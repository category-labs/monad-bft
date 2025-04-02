use syn::{
    bracketed,
    parse::{Parse, ParseStream},
    Token,
};

use super::Namespace;
use crate::result::ComponentResult;

pub struct NamespaceList(Vec<Namespace>);

impl Parse for NamespaceList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content;
        bracketed!(content in input);

        let mut components = Vec::default();

        loop {
            components.push(content.parse()?);

            if content.is_empty() {
                break;
            }

            content.parse::<Token![,]>()?;

            if content.is_empty() {
                break;
            }
        }

        Ok(Self(components))
    }
}

impl NamespaceList {
    pub fn augment(self, augment_name: &str) -> Self {
        let Self(components) = self;

        Self(
            components
                .into_iter()
                .map(|component| component.augment(augment_name))
                .collect(),
        )
    }

    pub fn generate(self, path_prefix: String) -> Vec<ComponentResult> {
        self.0
            .into_iter()
            .map(move |component| component.generate(path_prefix.clone()))
            .collect()
    }
}
