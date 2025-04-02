use syn::{
    parse::{Parse, ParseBuffer},
    Token,
};

pub fn try_parse_metric_field_name(content: &ParseBuffer, name: &'static str) -> syn::Result<()> {
    content.step(|cursor| {
        let (ident, cursor) = cursor.ident().ok_or(syn::Error::new(
            cursor.span(),
            "Expected metric field, found non-ident!",
        ))?;

        if ident.to_string() != name {
            return Err(syn::Error::new(
                ident.span(),
                format!("Expected metric field `{name}`"),
            ));
        }

        Ok(((), cursor))
    })
}

pub fn parse_metric_field<T>(content: &ParseBuffer, name: &'static str) -> syn::Result<T>
where
    T: Parse,
{
    try_parse_metric_field_name(content, name)?;

    content.parse::<Token![:]>()?;

    content.parse()
}

pub fn try_parse_optional_metric_field<T>(
    content: &ParseBuffer,
    name: &'static str,
) -> syn::Result<Option<T>>
where
    T: Parse,
{
    match try_parse_metric_field_name(content, name) {
        Ok(()) => {
            content.parse::<Token![:]>()?;
            content.parse().map(Option::Some)
        }
        Err(_) => Ok(None),
    }
}
