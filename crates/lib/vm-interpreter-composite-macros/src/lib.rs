//! The derive macro for composite interpreters.
//!
//! See the `waymark-vm-interpreter-composite` crate for the user-facing
//! documentation; the derive is re-exported from there.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use quote::{format_ident, quote};

/// Derive `waymark_vm_interpreter::Interpreter` for a composite interpreter.
///
/// The annotated struct must have named fields, each holding a
/// sub-interpreter. The derive generates:
///
/// - payload-generic `Error` and `Effect` sum enums, one variant per field,
///   emitted under exactly those names into the deriving module's scope —
///   so one composite per module; put a second composite in its own `mod`;
/// - the `Interpreter` implementation: instruction dispatch delegating each
///   instruction-set variant to its field, and the three hooks chaining the
///   sub-interpreters in field declaration order (the next sub-interpreter
///   runs only while the frame stays in the same state).
///
/// Runtime view capture is the views' own business: each sub-interpreter's
/// view — and the composite's, for the composite to be drivable itself —
/// implements `waymark_vm_runtime_view_capture::CaptureRuntimeView` for
/// the composite's view type.
///
/// # Attributes
///
/// Struct level:
///
/// ```ignore
/// #[interpreter(
///     instruction = path::to::CombinedInstructionSet<Spec>,
///     frame = path::to::Frame<FunctionId, StateId, Value>,
///     view = path::to::FullRuntimeView<'r, Executable, FunctionId, StateId, Value>,
///     // the composite's runtime view is this type, held by value
///     bound(Value: Clone, /* the where-clause of the generated impls */),
/// )]
/// ```
///
/// An optional `crate = path::to::composite` entry overrides the path the
/// generated code uses to reach `waymark-vm-interpreter-composite`
/// (default: `waymark_vm_interpreter_composite`), for crates that rename
/// the dependency.
///
/// The `view` type must use the literal lifetime `'r`; the generated impls
/// introduce that binder. `bound(…)` supplies the where-clause predicates
/// under which the sub-interpreter implementations resolve; the struct's own
/// where-clause is included automatically.
///
/// Field level:
///
/// ```ignore
/// #[interpreter(variant = CoreSet, instruction = path::to::CoreSet<Spec>)]
/// ```
///
/// `variant` names the arm in the combined instruction enum and in the
/// generated `Error`/`Effect` sums; `instruction` is that sub-interpreter's
/// instruction type.
#[proc_macro_derive(Interpreter, attributes(interpreter))]
pub fn derive_interpreter(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    match expand(input) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// The struct-level configuration collected from `#[interpreter(…)]`.
struct CompositeConfig {
    /// The combined instruction enum type.
    instruction: syn::Type,
    /// The frame type.
    frame: syn::Type,
    /// The composite's runtime view type, spelled with the `'r` lifetime.
    view: syn::Type,
    /// Where-clause predicates for the generated impls.
    bounds: Vec<syn::WherePredicate>,
    /// The path to the `waymark-vm-interpreter-composite` crate that the
    /// generated code resolves through.
    crate_path: syn::Path,
}

/// One sub-interpreter field and its per-field configuration.
struct CompositeField {
    /// The field name.
    ident: syn::Ident,
    /// The field (sub-interpreter) type.
    ty: syn::Type,
    /// The variant name shared by the instruction, error, and effect enums.
    variant: syn::Ident,
    /// The sub-interpreter's instruction type.
    instruction: syn::Type,
}

/// Expand the derive input into the generated items.
fn expand(input: syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let config = parse_struct_config(&input)?;
    let fields = parse_fields(&input)?;

    let struct_ident = &input.ident;

    let variant_idents: Vec<&syn::Ident> = fields.iter().map(|field| &field.variant).collect();
    let field_idents: Vec<&syn::Ident> = fields.iter().map(|field| &field.ident).collect();
    let field_types: Vec<&syn::Type> = fields.iter().map(|field| &field.ty).collect();

    let instruction_pattern_path = type_to_pattern_path(&config.instruction)?;

    let sum_enums = expand_sum_enums(struct_ident, &fields);
    let interpreter_impl = expand_interpreter_impl(
        &input,
        &config,
        &fields,
        &variant_idents,
        &field_idents,
        &field_types,
        &instruction_pattern_path,
    )?;
    Ok(quote! {
        #sum_enums
        #interpreter_impl
    })
}

/// Parse and merge the struct-level `#[interpreter(…)]` attributes.
fn parse_struct_config(input: &syn::DeriveInput) -> syn::Result<CompositeConfig> {
    let mut instruction = None;
    let mut frame = None;
    let mut view = None;
    let mut bounds = Vec::new();
    let mut crate_path = None;

    for attribute in &input.attrs {
        if !attribute.path().is_ident("interpreter") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("instruction") {
                instruction = Some(meta.value()?.parse::<syn::Type>()?);
                return Ok(());
            }
            if meta.path.is_ident("frame") {
                frame = Some(meta.value()?.parse::<syn::Type>()?);
                return Ok(());
            }
            if meta.path.is_ident("view") {
                view = Some(meta.value()?.parse::<syn::Type>()?);
                return Ok(());
            }
            if meta.path.is_ident("bound") {
                let content;
                syn::parenthesized!(content in meta.input);
                let predicates = content.parse_terminated(
                    <syn::WherePredicate as syn::parse::Parse>::parse,
                    syn::Token![,],
                )?;
                bounds.extend(predicates);
                return Ok(());
            }
            if meta.path.is_ident("crate") {
                crate_path = Some(meta.value()?.parse::<syn::Path>()?);
                return Ok(());
            }
            Err(meta.error("expected `instruction`, `frame`, `view`, `bound`, or `crate`"))
        })?;
    }

    let missing = |key: &str| {
        syn::Error::new_spanned(
            &input.ident,
            format!("#[interpreter(…)] is missing the `{key}` entry"),
        )
    };

    Ok(CompositeConfig {
        instruction: instruction.ok_or_else(|| missing("instruction"))?,
        frame: frame.ok_or_else(|| missing("frame"))?,
        view: view.ok_or_else(|| missing("view"))?,
        bounds,
        crate_path: crate_path
            .unwrap_or_else(|| syn::parse_quote!(waymark_vm_interpreter_composite)),
    })
}

/// Parse the sub-interpreter fields and their `#[interpreter(…)]` attributes.
fn parse_fields(input: &syn::DeriveInput) -> syn::Result<Vec<CompositeField>> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Interpreter)] requires a struct",
        ));
    };
    let syn::Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Interpreter)] requires named fields",
        ));
    };

    let mut composite_fields = Vec::new();
    for field in &fields.named {
        let ident = field
            .ident
            .clone()
            .expect("named fields always have identifiers");

        let mut variant = None;
        let mut instruction = None;
        for attribute in &field.attrs {
            if !attribute.path().is_ident("interpreter") {
                continue;
            }
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("variant") {
                    variant = Some(meta.value()?.parse::<syn::Ident>()?);
                    return Ok(());
                }
                if meta.path.is_ident("instruction") {
                    instruction = Some(meta.value()?.parse::<syn::Type>()?);
                    return Ok(());
                }
                Err(meta.error("expected `variant` or `instruction`"))
            })?;
        }

        let missing = |key: &str| {
            syn::Error::new_spanned(
                &ident,
                format!("field `{ident}` is missing the #[interpreter({key} = …)] entry"),
            )
        };

        composite_fields.push(CompositeField {
            ty: field.ty.clone(),
            variant: variant.ok_or_else(|| missing("variant"))?,
            instruction: instruction.ok_or_else(|| missing("instruction"))?,
            ident,
        });
    }

    if composite_fields.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Interpreter)] requires at least one sub-interpreter field",
        ));
    }

    Ok(composite_fields)
}

/// Turn the combined instruction type into a path usable in match patterns
/// by dropping the generic arguments from every segment.
fn type_to_pattern_path(instruction: &syn::Type) -> syn::Result<syn::Path> {
    let syn::Type::Path(type_path) = instruction else {
        return Err(syn::Error::new_spanned(
            instruction,
            "the `instruction` type must be a path to an enum",
        ));
    };
    let mut path = type_path.path.clone();
    for segment in &mut path.segments {
        segment.arguments = syn::PathArguments::None;
    }
    Ok(path)
}

/// Generate the payload-generic `Error` and `Effect` sum enums.
fn expand_sum_enums(
    struct_ident: &syn::Ident,
    fields: &[CompositeField],
) -> proc_macro2::TokenStream {
    let variant_idents: Vec<&syn::Ident> = fields.iter().map(|field| &field.variant).collect();

    let error_doc = format!("The error for [`{struct_ident}`].");
    let effect_doc = format!("The effect for [`{struct_ident}`].");
    let error_variant_docs: Vec<String> = fields
        .iter()
        .map(|field| format!("The `{}` interpreter failed.", field.ident))
        .collect();
    let effect_variant_docs: Vec<String> = fields
        .iter()
        .map(|field| format!("An effect from the `{}` interpreter.", field.ident))
        .collect();

    quote! {
        #[doc = #error_doc]
        #[derive(Debug)]
        pub enum Error<#(#variant_idents),*> {
            #(
                #[doc = #error_variant_docs]
                #variant_idents(#variant_idents),
            )*
        }

        // Transparent delegation: the composite error *is* the failed
        // sub-interpreter's error for display and source purposes.
        impl<#(#variant_idents),*> ::core::fmt::Display for Error<#(#variant_idents),*>
        where
            #(#variant_idents: ::core::fmt::Display,)*
        {
            fn fmt(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                match self {
                    #(Self::#variant_idents(inner) => ::core::fmt::Display::fmt(inner, formatter),)*
                }
            }
        }

        impl<#(#variant_idents),*> ::core::error::Error for Error<#(#variant_idents),*>
        where
            #(#variant_idents: ::core::error::Error,)*
        {
            fn source(&self) -> Option<&(dyn ::core::error::Error + 'static)> {
                match self {
                    #(Self::#variant_idents(inner) => ::core::error::Error::source(inner),)*
                }
            }
        }

        #[doc = #effect_doc]
        #[derive(Debug)]
        pub enum Effect<#(#variant_idents),*> {
            #(
                #[doc = #effect_variant_docs]
                #variant_idents(#variant_idents),
            )*
        }
    }
}

/// Generate the `waymark_vm_interpreter::Interpreter` implementation.
fn expand_interpreter_impl(
    input: &syn::DeriveInput,
    config: &CompositeConfig,
    fields: &[CompositeField],
    variant_idents: &[&syn::Ident],
    field_idents: &[&syn::Ident],
    field_types: &[&syn::Type],
    instruction_pattern_path: &syn::Path,
) -> syn::Result<proc_macro2::TokenStream> {
    let struct_ident = &input.ident;
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();
    let where_clause = combined_where_clause(input, config);

    let instruction_ty = &config.instruction;
    let frame_ty = &config.frame;
    let view_ty = &config.view;

    let crate_path = &config.crate_path;
    let vm_interpreter_path = quote! { #crate_path::__hidden::waymark_vm_interpreter };
    let view_capture_path = quote! { #crate_path::__hidden::waymark_vm_runtime_view_capture };
    let composite_core_path =
        quote! { #crate_path::__hidden::waymark_vm_interpreter_composite_core };

    let sub_instructions: Vec<&syn::Type> = fields.iter().map(|field| &field.instruction).collect();

    let hooks = [
        format_ident!("enter_state"),
        format_ident!("before_execute"),
        format_ident!("after_execute"),
    ]
    .map(|method| {
        quote! {
            fn #method<'r>(
                &self,
                mut runtime_view: Self::RuntimeView<'r>,
                mut frame: Self::Frame,
            ) -> Result<
                #vm_interpreter_path::ExecutionOutcome<Self::Frame, Self::Effect>,
                Self::Error,
            > {
                #(
                    let state_token =
                        #composite_core_path::DetectStateSwitch::capture_state_token(
                            &frame,
                        );
                    let captured =
                        <<#field_types as #vm_interpreter_path::Interpreter>::RuntimeView<
                            '_,
                        > as #view_capture_path::CaptureRuntimeView<
                            '_,
                            _,
                        >>::capture_runtime_view(&mut runtime_view);
                    let outcome = #vm_interpreter_path::Interpreter::#method(
                        &self.#field_idents,
                        captured,
                        frame,
                    )
                    .map_err(Error::#variant_idents)?
                    .map_effect(Effect::#variant_idents);
                    frame = match #crate_path::chain(state_token, outcome) {
                        ::core::ops::ControlFlow::Continue(frame) => frame,
                        ::core::ops::ControlFlow::Break(outcome) => return Ok(outcome),
                    };
                )*
                Ok(#vm_interpreter_path::ExecutionOutcome::Continue(frame))
            }
        }
    });

    Ok(quote! {
        #[allow(clippy::let_unit_value)]
        impl #impl_generics #vm_interpreter_path::Interpreter for #struct_ident #ty_generics
        #where_clause
        {
            type RuntimeView<'r> = #view_ty;
            type Frame = #frame_ty;
            type Instruction = #instruction_ty;
            type Error = Error<
                #(<#field_types as #vm_interpreter_path::Interpreter>::Error,)*
            >;
            type Effect = Effect<
                #(<#field_types as #vm_interpreter_path::Interpreter>::Effect,)*
            >;

            #(#hooks)*

            fn execute<'r>(
                &self,
                mut runtime_view: Self::RuntimeView<'r>,
                frame: Self::Frame,
                instruction: &Self::Instruction,
            ) -> Result<
                #vm_interpreter_path::ExecutionOutcome<Self::Frame, Self::Effect>,
                Self::Error,
            > {
                Ok(match instruction {
                    #(
                        #instruction_pattern_path::#variant_idents(instruction) => {
                            let _: &#sub_instructions = instruction;
                            let captured =
                                <<#field_types as #vm_interpreter_path::Interpreter>::RuntimeView<
                                    '_,
                                > as #view_capture_path::CaptureRuntimeView<
                                    '_,
                                    _,
                                >>::capture_runtime_view(&mut runtime_view);
                            #vm_interpreter_path::Interpreter::execute(
                                &self.#field_idents,
                                captured,
                                frame,
                                instruction,
                            )
                            .map_err(Error::#variant_idents)?
                            .map_effect(Effect::#variant_idents)
                        }
                    )*
                })
            }
        }
    })
}

/// Build the where-clause for the generated impls: the struct's own
/// where-clause predicates followed by the `bound(…)` predicates.
fn combined_where_clause(input: &syn::DeriveInput, config: &CompositeConfig) -> syn::WhereClause {
    let mut where_clause =
        input
            .generics
            .where_clause
            .clone()
            .unwrap_or_else(|| syn::WhereClause {
                where_token: syn::Token![where](proc_macro2::Span::call_site()),
                predicates: syn::punctuated::Punctuated::new(),
            });
    where_clause
        .predicates
        .extend(config.bounds.iter().cloned());
    where_clause
}
