//! The [`macro@blanket_impl`] attribute macro.
//!
//! Generates the blanket implementation for a unifying trait: a trait with
//! an empty body whose only purpose is to bundle its supertrait bounds under
//! one name. The blanket implementation is a verbatim restatement of those
//! bounds, and this macro keeps them written once.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use quote::quote;

/// Generate a blanket implementation for the annotated unifying trait.
///
/// The trait body must be empty: a blanket implementation cannot provide
/// associated items. The generated implementation covers every type that
/// satisfies the supertrait bounds (and the trait's own `where` clause):
///
/// ```ignore
/// #[waymark_blanket_impl_macros::blanket_impl]
/// pub trait Spec: FirstSpec + SecondSpec<Item = <Self as FirstSpec>::Item> {}
/// ```
///
/// expands to the trait itself plus:
///
/// ```ignore
/// impl<BlanketImplementor> Spec for BlanketImplementor
/// where
///     BlanketImplementor: FirstSpec + SecondSpec<Item = <Self as FirstSpec>::Item>,
/// {
/// }
/// ```
///
/// `Self` in the supertrait bounds refers to the implementing type, matching
/// its meaning in the trait definition.
#[proc_macro_attribute]
pub fn blanket_impl(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item = syn::parse_macro_input!(input as syn::ItemTrait);

    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[blanket_impl] takes no arguments",
        )
        .to_compile_error()
        .into();
    }

    match expand(item) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Expand the trait item into the trait itself plus its blanket
/// implementation.
fn expand(item: syn::ItemTrait) -> syn::Result<proc_macro2::TokenStream> {
    if let Some(first_item) = item.items.first() {
        return Err(syn::Error::new_spanned(
            first_item,
            "#[blanket_impl] requires an empty trait body; \
             a blanket implementation cannot provide associated items",
        ));
    }

    if item.supertraits.is_empty() {
        return Err(syn::Error::new_spanned(
            &item.ident,
            "#[blanket_impl] requires supertrait bounds to implement the trait from",
        ));
    }

    let trait_ident = &item.ident;
    let unsafety = &item.unsafety;
    let supertraits = &item.supertraits;

    let implementor = syn::Ident::new("BlanketImplementor", proc_macro2::Span::call_site());

    // The trait's own generic parameters, with defaults stripped since
    // defaults are not permitted on `impl` generics.
    let generic_params: Vec<syn::GenericParam> = item
        .generics
        .params
        .iter()
        .cloned()
        .map(|mut param| {
            match &mut param {
                syn::GenericParam::Type(type_param) => {
                    type_param.eq_token = None;
                    type_param.default = None;
                }
                syn::GenericParam::Const(const_param) => {
                    const_param.eq_token = None;
                    const_param.default = None;
                }
                syn::GenericParam::Lifetime(_) => {}
            }
            param
        })
        .collect();

    let (_, type_generics, where_clause) = item.generics.split_for_impl();
    let where_predicates = where_clause.map(|where_clause| &where_clause.predicates);

    let expanded = quote! {
        #item

        #unsafety impl<#(#generic_params,)* #implementor> #trait_ident #type_generics
            for #implementor
        where
            #implementor: #supertraits,
            #where_predicates
        {
        }
    };

    Ok(expanded)
}
