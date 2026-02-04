use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

#[proc_macro_attribute]
pub fn obs(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as ItemFn);
    let attr = if args.is_empty() {
        syn::parse_quote!(#[cfg_attr(feature = "observability", tracing::instrument(skip_all))])
    } else {
        let args = proc_macro2::TokenStream::from(args);
        syn::parse_quote!(#[cfg_attr(feature = "observability", tracing::instrument(#args))])
    };
    item.attrs.push(attr);
    TokenStream::from(quote!(#item))
}
