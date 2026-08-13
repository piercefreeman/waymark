use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

#[proc_macro_attribute]
pub fn obs(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[obs] does not take arguments",
        )
        .to_compile_error()
        .into();
    }
    let mut item = parse_macro_input!(input as ItemFn);
    handle_item(&mut item);
    TokenStream::from(quote!(#item))
}

// The span is generated inline through the `waymark_observability::__inner`
// re-export instead of delegating to `#[tracing::instrument]`: the code
// that attribute generates refers to the absolute `::tracing` path, which
// would force a direct `tracing` dependency onto every `#[obs]` user, and
// the attribute has no crate-path override argument to point it at the
// re-export instead (checked against tracing-attributes 0.1.31, the
// latest release).
#[cfg(waymark_observability_chrome_trace)]
fn handle_item(item: &mut ItemFn) {
    let name = item.sig.ident.to_string();
    let body = &item.block;
    let block: syn::Block = if item.sig.asyncness.is_some() {
        syn::parse_quote! {{
            let __obs_span = ::waymark_observability::__inner::tracing::info_span!(#name);
            ::waymark_observability::__inner::tracing::Instrument::instrument(
                async move #body,
                __obs_span,
            )
            .await
        }}
    } else {
        syn::parse_quote! {{
            let __obs_span = ::waymark_observability::__inner::tracing::info_span!(#name);
            let _obs_entered = __obs_span.entered();
            #body
        }}
    };
    *item.block = block;
}

#[cfg(not(waymark_observability_chrome_trace))]
fn handle_item(_item: &mut ItemFn) {}
