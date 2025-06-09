use std::str::FromStr;

use syn::{
    bracketed, parenthesized,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Ident, LitStr, Path, Result, Token,
};

pub struct Attrs<T>
where
    T: Parse,
{
    pub data: T,
}

impl<T> Parse for Attrs<T>
where
    T: Parse,
{
    fn parse(input: ParseStream) -> Result<Self> {
        input.parse::<Token![#]>()?;

        let content;
        bracketed!(content in input);

        let data = content.parse()?;

        Ok(Self { data })
    }
}

pub struct ScalarAttrs {
    pub scalar_type: ScalarType,
    pub labels: Option<ScalarLabelAttrs>,
}

impl Parse for ScalarAttrs {
    fn parse(input: ParseStream) -> Result<Self> {
        let scalar_type: Ident = input.parse()?;

        let scalar_type = ScalarType::from_str(&scalar_type.to_string())
            .map_err(|err| syn::Error::new(scalar_type.span(), err))?;

        let mut labels = None;

        if !input.is_empty() {
            let inner;
            parenthesized!(inner in input);

            labels = Some(inner.parse()?);
        }

        Ok(Self {
            scalar_type,
            labels,
        })
    }
}

#[derive(strum::EnumString, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum ScalarType {
    Counter,
    Gauge,
}

pub struct ScalarLabelAttrs {
    pub label: LitStr,
    pub variants: Vec<Ident>,
}

impl Parse for ScalarLabelAttrs {
    fn parse(input: ParseStream) -> Result<Self> {
        let path: Path = input.parse()?;

        if !path.is_ident("label") {
            return Err(syn::Error::new_spanned(path, "expected `label` attribute"));
        }

        input.parse::<Token![=]>()?;

        let label: LitStr = input.parse()?;

        input.parse::<Token![,]>()?;

        let path: Path = input.parse()?;

        if !path.is_ident("variants") {
            return Err(syn::Error::new_spanned(
                path,
                "expected `variants` attribute",
            ));
        }

        input.parse::<Token![=]>()?;

        let inner;
        bracketed!(inner in input);

        let variants = Punctuated::<Ident, Token![,]>::parse_terminated(&inner)?;

        Ok(Self {
            label,
            variants: variants.into_iter().collect(),
        })
    }
}
