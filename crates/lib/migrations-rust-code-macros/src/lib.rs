//! The [`migration!`] and [`migration_metadata!`] macros.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use quote::quote;
use waymark_migrations_rust_code_core::render_crate_capture;
use waymark_migrations_source_checksums_core::source_digest;
use waymark_migrations_source_checksums_proc_macro_helpers::{
    mismatch_help, mismatch_revision, read_build_flags, read_update_at_run_time, release_guards,
    update_command, watched_variables,
};

mod crate_name;
mod source_checksums;

/// Declare the Rust-code migration a crate is.
///
/// ```ignore
/// waymark_migrations_rust_code::migration! {
///     revision: "id-map-tables-1",
///     connection_type: sqlx::PgConnection,
///     transaction_support: waymark_migrations_engine_migration::MigrationTransactionSupport::Supported,
/// }
/// ```
///
/// The macro defines the unit struct `Migration`, private to the crate,
/// and implements on it the connection type, the transaction support and
/// the metadata, the last as [`migration_metadata!`] does with `revision`.
/// The crate
/// implements the three operations on the struct. `migration_type: <ident>`
/// gives the struct another identifier.
///
/// The macro also defines `pub fn migration()`, the migration as a
/// `waymark_migrations_rust_code::Migration` of the connection type.
///
/// The crate it is used in depends on `waymark-migrations-rust-code` and
/// on `waymark-migrations-engine-migration`.
#[proc_macro]
pub fn migration(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as MigrationInput);

    expand_migration(input).into()
}

/// Implement the metadata of the Rust-code migration a crate is, on the
/// type `migration_type`.
///
/// ```ignore
/// waymark_migrations_rust_code::migration_metadata! {
///     migration_type: IdMapTablesMigration,
///     revision: "id-map-tables-1",
/// }
/// ```
///
/// The version and the description come from the crate's name,
/// `<prefix>-migration-<version>-<description>`, the description with its
/// dashes as spaces. The revision is `revision`: any text that is not
/// empty.
///
/// The crate's files are checked against
/// `migrations-engine-crate-capture.json`, the crate capture beside its
/// manifest, which has the SHA-256 digest of each of them. A crate that
/// does not match its capture does not compile, unless
/// `WAYMARK_MIGRATIONS_ALLOW_SOURCE_CHECKSUMS_MISMATCH` or
/// `WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS` is `true` when it is
/// compiled: then the migration's revision is `revision` followed by
/// ` (source checksums mismatch)`. Neither variable can be `true` in a
/// build without debug assertions.
///
/// The migration's source digest is the SHA-256 digest of the capture as
/// the files it has make it: the digest of the capture's file when the
/// crate matches its capture.
///
/// The macro adds a test to the crate that checks the capture against the
/// files cargo counts as the crate's package and against the packages the
/// crate depends on. When the check fails, the test says how to write the
/// capture anew. The test writes it anew itself when, as it runs,
/// `WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS` is `true`, or
/// `WAYMARK_MIGRATIONS_TESTS_WRITE_SOURCE_CHECKSUMS` is. The second is
/// read by the tests alone: turning it on or off compiles nothing again.
/// The capture has each
/// dependency as one entry: a package from crates.io with its version, a
/// package from a git repository with its commit as well. The only path
/// dependencies a migration can have are the crates of the migrations
/// engine it is built with; any other one fails the test, and so does a
/// package from anywhere else.
#[proc_macro]
pub fn migration_metadata(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as MetadataInput);

    match expand_metadata(input) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// The arguments of [`migration!`].
struct MigrationInput {
    /// The migration's revision.
    revision: syn::LitStr,

    /// The connection type the migration runs through.
    connection_type: syn::Type,

    /// Whether the migration can run inside a transaction.
    transaction_support: syn::Expr,

    /// The identifier of the struct the macro defines, when it is not
    /// `Migration`.
    migration_type: Option<syn::Ident>,
}

impl syn::parse::Parse for MigrationInput {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let mut revision: Option<syn::LitStr> = None;
        let mut connection_type: Option<syn::Type> = None;
        let mut transaction_support: Option<syn::Expr> = None;
        let mut migration_type: Option<syn::Ident> = None;

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            let _: syn::Token![:] = input.parse()?;

            if key == "revision" && revision.is_none() {
                revision = Some(input.parse()?);
            } else if key == "connection_type" && connection_type.is_none() {
                connection_type = Some(input.parse()?);
            } else if key == "transaction_support" && transaction_support.is_none() {
                transaction_support = Some(input.parse()?);
            } else if key == "migration_type" && migration_type.is_none() {
                migration_type = Some(input.parse()?);
            } else {
                return Err(syn::Error::new_spanned(
                    &key,
                    "expected `revision`, `connection_type`, `transaction_support` or \
                     `migration_type`, each once",
                ));
            }

            if input.is_empty() {
                break;
            }
            let _: syn::Token![,] = input.parse()?;
        }

        let Some(revision) = revision else {
            return Err(input.error("`revision` is missing"));
        };
        let Some(connection_type) = connection_type else {
            return Err(input.error("`connection_type` is missing"));
        };
        let Some(transaction_support) = transaction_support else {
            return Err(input.error("`transaction_support` is missing"));
        };

        Ok(Self {
            revision,
            connection_type,
            transaction_support,
            migration_type,
        })
    }
}

/// The arguments of [`migration_metadata!`].
struct MetadataInput {
    /// The type the metadata is implemented on.
    migration_type: syn::Type,

    /// The migration's revision.
    revision: syn::LitStr,
}

impl syn::parse::Parse for MetadataInput {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let mut migration_type: Option<syn::Type> = None;
        let mut revision: Option<syn::LitStr> = None;

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            let _: syn::Token![:] = input.parse()?;

            if key == "migration_type" && migration_type.is_none() {
                migration_type = Some(input.parse()?);
            } else if key == "revision" && revision.is_none() {
                revision = Some(input.parse()?);
            } else {
                return Err(syn::Error::new_spanned(
                    &key,
                    "expected `migration_type` or `revision`, each once",
                ));
            }

            if input.is_empty() {
                break;
            }
            let _: syn::Token![,] = input.parse()?;
        }

        let Some(migration_type) = migration_type else {
            return Err(input.error("`migration_type` is missing"));
        };
        let Some(revision) = revision else {
            return Err(input.error("`revision` is missing"));
        };

        Ok(Self {
            migration_type,
            revision,
        })
    }
}

/// Expand the arguments of [`migration!`].
fn expand_migration(input: MigrationInput) -> proc_macro2::TokenStream {
    let MigrationInput {
        revision,
        connection_type,
        transaction_support,
        migration_type,
    } = input;

    let migration_type = migration_type
        .unwrap_or_else(|| syn::Ident::new("Migration", proc_macro2::Span::call_site()));

    quote! {
        /// The migration this crate is: its operations are implemented on
        /// it.
        #[derive(Debug)]
        struct #migration_type;

        impl ::waymark_migrations_engine_migration::HasConnection for #migration_type {
            type Connection = #connection_type;
        }

        impl ::waymark_migrations_engine_migration::HasMigrationTransactionSupport
            for #migration_type
        {
            fn migration_transaction_support(
                &self,
            ) -> ::waymark_migrations_engine_migration::MigrationTransactionSupport {
                #transaction_support
            }
        }

        ::waymark_migrations_rust_code::migration_metadata! {
            migration_type: #migration_type,
            revision: #revision,
        }

        /// The migration this crate is.
        pub fn migration() -> ::waymark_migrations_rust_code::Migration<#connection_type> {
            ::waymark_migrations_rust_code::Migration::new(#migration_type)
        }
    }
}

/// A variable of the crate being compiled, which cargo sets.
fn crate_variable(variable: &str) -> syn::Result<String> {
    std::env::var(variable).map_err(|error| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("reading {variable}, which cargo sets for the crate: {error}"),
        )
    })
}

/// What the compiler says when a migration's source does not match its
/// crate capture and nothing lets it build.
fn mismatch_message(
    crate_name: &str,
    version: u32,
    differences: &[source_checksums::Difference],
) -> String {
    let mut message =
        format!("the source of migration {version} does not match its crate capture\n");

    for difference in differences {
        message.push_str(&format!("    {difference}\n"));
    }

    message.push('\n');
    message.push_str(&mismatch_help(crate_name));

    message
}

/// Expand the arguments of [`migration_metadata!`].
fn expand_metadata(input: MetadataInput) -> syn::Result<proc_macro2::TokenStream> {
    let MetadataInput {
        migration_type,
        revision,
    } = input;

    if revision.value().is_empty() {
        return Err(syn::Error::new_spanned(&revision, "the revision is empty"));
    }

    let crate_name = crate_variable("CARGO_PKG_NAME")?;
    let manifest_dir = crate_variable("CARGO_MANIFEST_DIR")?;
    let manifest_dir = std::path::Path::new(&manifest_dir);

    let crate_name::Parsed {
        version,
        description,
    } = crate_name::parse(&crate_name).map_err(|error| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("crate {crate_name}: {error}"),
        )
    })?;

    let build_flags = read_build_flags()
        .map_err(|error| syn::Error::new(proc_macro2::Span::call_site(), error.to_string()))?;

    let source_checksums::Compared {
        differences,
        as_the_files_make_it,
    } = source_checksums::compare(manifest_dir);

    let revision = if differences.is_empty() {
        revision.value()
    } else {
        if !build_flags.mismatch_builds() {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                mismatch_message(&crate_name, version, &differences),
            ));
        }

        mismatch_revision(&revision.value())
    };

    let as_the_files_make_it = render_crate_capture(&as_the_files_make_it).map_err(|error| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("rendering the crate capture as the files make it: {error}"),
        )
    })?;
    let source_digest = source_digest(as_the_files_make_it.as_bytes());

    let release_guards = release_guards(&build_flags);
    let watched_variables = watched_variables();

    // The command that writes the capture anew, for the test's message:
    // the crate is known here already.
    let update_command = update_command(&crate_name);

    let update =
        read_update_at_run_time(&quote! { ::waymark_migrations_rust_code::macro_support::envfury });

    // The files come through `include_bytes!` so the compiler knows the
    // crate depends on them, and compiles it again when one changes.
    let mut watched_files = Vec::new();
    for path in source_checksums::watched_files(manifest_dir) {
        let Some(path) = path.to_str() else {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!(
                    "the path of a file of the crate is not valid UTF-8: {}",
                    path.display()
                ),
            ));
        };

        watched_files.push(path.to_owned());
    }

    Ok(quote! {
        #release_guards

        const _: () = {
            #(const _: &[u8] = ::core::include_bytes!(#watched_files);)*
            #watched_variables
        };

        impl ::waymark_migrations_engine_migration::HasMigrationMetadata for #migration_type {
            fn migration_metadata(
                &self,
            ) -> ::waymark_migrations_engine_migration::MigrationMetadata<'_> {
                static REVISION: ::waymark_migrations_engine_migration::MigrationRevision =
                    ::waymark_migrations_engine_migration::MigrationRevision::from_static(
                        #revision
                    );

                ::waymark_migrations_engine_migration::MigrationMetadata {
                    version: ::waymark_migrations_engine_migration::MigrationVersion::from_raw(
                        #version
                    ),
                    description: #description,
                    revision: &REVISION,
                    source_digest: #source_digest,
                }
            }
        }

        #[cfg(test)]
        mod migrations_engine_source_checksums {
            #[test]
            fn crate_capture_is_up_to_date() {
                let update: bool = #update;

                let checked = ::waymark_migrations_rust_code::source_checksums::check_or_update(
                    ::std::path::Path::new(::core::env!("CARGO_MANIFEST_DIR")),
                    ::core::env!("CARGO_PKG_NAME"),
                    update,
                );

                if let ::core::result::Result::Err(error) = checked {
                    ::core::panic!(
                        "{error}\n\nafter verifying the migration, write the capture anew \
                         with:\n    {}",
                        #update_command,
                    );
                }
            }
        }
    })
}
