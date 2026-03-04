use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

#[proc_macro_attribute]
pub fn obs(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as ItemFn);
    handle_item(args, &mut item);
    TokenStream::from(quote!(#item))
}

#[cfg(waymark_observability_trace)]
fn handle_item(args: TokenStream, item: &mut ItemFn) {
    let attr = if args.is_empty() {
        syn::parse_quote!(#[::waymark_observability::__inner::tracing::instrument(skip_all)])
    } else {
        let args = proc_macro2::TokenStream::from(args);
        syn::parse_quote!(#[::waymark_observability::__inner::tracing::instrument(#args)])
    };
    item.attrs.push(attr);
}

#[cfg(not(waymark_observability_trace))]
fn handle_item(_args: TokenStream, _item: &mut ItemFn) {}
