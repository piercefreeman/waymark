//! The proc macros behind `waymark_migrations_sql_file::migration!` and
//! `waymark_migrations_sql_file::source_checksums_test!`.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use quote::quote;
use waymark_migrations_source_checksums_core::source_digest;
use waymark_migrations_source_checksums_proc_macro_helpers::{
    mismatch_help, mismatch_revision, read_build_flags, read_update_at_run_time, release_guards,
    update_command, watched_variables,
};

mod file_name;
mod metadata;
mod source_checksums;

/// Make an SQL-file migration out of an SQL file, read when the crate is
/// compiled: what `waymark_migrations_sql_file::migration!` expands
/// through, where the macro is documented.
///
/// ```ignore
/// waymark_migrations_sql_file::macro_support::migration_at!(
///     $crate,
///     "migrations/0007_add_the_widgets_table.sql"
/// )
/// ```
///
/// The first argument is the path of the `waymark-migrations-sql-file`
/// crate where the macro is used: the expansion refers to that crate
/// through it and through nothing else. The second is the file's path.
#[proc_macro]
pub fn migration(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as Input);

    match expand(input) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Add to a crate with SQL-file migrations the test of its SQL files
/// checksums: what `waymark_migrations_sql_file::source_checksums_test!`
/// expands through, where the macro is documented.
///
/// ```ignore
/// waymark_migrations_sql_file::macro_support::source_checksums_test_at!($crate, "migrations");
/// ```
///
/// The first argument is the path of the `waymark-migrations-sql-file`
/// crate where the macro is used: the expansion refers to that crate
/// through it and through nothing else. The second is the directory of
/// the crate's SQL-file migrations.
#[proc_macro]
pub fn source_checksums_test(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as SourceChecksumsTestInput);

    match expand_source_checksums_test(input) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// The arguments of [`source_checksums_test!`].
struct SourceChecksumsTestInput {
    /// The path of the `waymark-migrations-sql-file` crate where the
    /// macro is used.
    sql_file_crate: syn::Path,

    /// The directory of the crate's SQL-file migrations.
    migrations_dir: syn::LitStr,
}

impl syn::parse::Parse for SourceChecksumsTestInput {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let sql_file_crate: syn::Path = input.parse()?;
        let _: syn::Token![,] = input.parse()?;
        let migrations_dir: syn::LitStr = input.parse()?;
        let _: Option<syn::Token![,]> = input.parse()?;

        Ok(Self {
            sql_file_crate,
            migrations_dir,
        })
    }
}

/// Expand the arguments into the test of the crate's SQL files checksums.
fn expand_source_checksums_test(
    input: SourceChecksumsTestInput,
) -> syn::Result<proc_macro2::TokenStream> {
    let SourceChecksumsTestInput {
        sql_file_crate,
        migrations_dir,
    } = input;

    let crate_name = crate_variable(&migrations_dir, "CARGO_PKG_NAME")?;

    // The command that writes the checksums anew, for the test's message:
    // the crate is known here already.
    let update_command = update_command(&crate_name);

    let update = read_update_at_run_time(&quote! { #sql_file_crate::macro_support::envfury });

    Ok(quote! {
        #[cfg(test)]
        mod migrations_engine_source_checksums {
            #[test]
            fn sql_files_checksums_are_up_to_date() {
                let update: bool = #update;

                let checked = #sql_file_crate::source_checksums::check_or_update(
                    ::std::path::Path::new(::core::env!("CARGO_MANIFEST_DIR")),
                    #migrations_dir,
                    update,
                );

                if let ::core::result::Result::Err(error) = checked {
                    ::core::panic!(
                        "{error}\n\nafter verifying the migrations, write the checksums anew \
                         with:\n    {}",
                        #update_command,
                    );
                }
            }
        }
    })
}

/// The arguments of [`migration!`].
struct Input {
    /// The path of the `waymark-migrations-sql-file` crate where the
    /// macro is used.
    sql_file_crate: syn::Path,

    /// The path of the SQL file.
    path: syn::LitStr,
}

impl syn::parse::Parse for Input {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let sql_file_crate: syn::Path = input.parse()?;
        let _: syn::Token![,] = input.parse()?;
        let path: syn::LitStr = input.parse()?;
        let _: Option<syn::Token![,]> = input.parse()?;

        Ok(Self {
            sql_file_crate,
            path,
        })
    }
}

/// A variable of the crate being compiled, which cargo sets.
fn crate_variable(path: &syn::LitStr, variable: &str) -> syn::Result<String> {
    std::env::var(variable).map_err(|error| {
        syn::Error::new_spanned(
            path,
            format!("reading {variable}, which cargo sets for the crate: {error}"),
        )
    })
}

/// What the compiler says when an SQL file does not match the SQL files
/// checksums of its crate and nothing lets it build.
fn mismatch_message(
    crate_name: &str,
    path: &str,
    difference: &source_checksums::Difference,
) -> String {
    format!(
        "SQL file {path} does not match the SQL files checksums of its crate\n    \
         {difference}\n\n{}",
        mismatch_help(crate_name)
    )
}

/// Expand the arguments into the SQL-file migration of the file.
fn expand(input: Input) -> syn::Result<proc_macro2::TokenStream> {
    let Input {
        sql_file_crate,
        path,
    } = input;

    let crate_name = crate_variable(&path, "CARGO_PKG_NAME")?;
    let manifest_dir = crate_variable(&path, "CARGO_MANIFEST_DIR")?;
    let manifest_dir = std::path::Path::new(&manifest_dir);

    let listed_path = path.value();
    let full_path = manifest_dir.join(&listed_path);

    let Some(full_path_str) = full_path.to_str() else {
        return Err(syn::Error::new_spanned(
            &path,
            format!(
                "the path of the SQL file is not valid UTF-8: {}",
                full_path.display()
            ),
        ));
    };

    let Some(file_name) = full_path
        .file_name()
        .and_then(|file_name| file_name.to_str())
    else {
        return Err(syn::Error::new_spanned(
            &path,
            "the path of the SQL file has no file name",
        ));
    };

    let file_name::Parsed {
        version,
        description,
    } = file_name::parse(file_name).map_err(|error| {
        syn::Error::new_spanned(&path, format!("SQL file {file_name}: {error}"))
    })?;

    let sql = std::fs::read_to_string(&full_path).map_err(|error| {
        syn::Error::new_spanned(&path, format!("reading SQL file {full_path_str}: {error}"))
    })?;

    let metadata::Metadata {
        revision,
        runs_inside_a_transaction,
    } = metadata::parse(&sql).map_err(|error| {
        syn::Error::new_spanned(&path, format!("SQL file {file_name}: {error}"))
    })?;

    let build_flags =
        read_build_flags().map_err(|error| syn::Error::new_spanned(&path, error.to_string()))?;

    let source_digest = source_digest(sql.as_bytes());

    let revision = match source_checksums::compare(manifest_dir, &listed_path, &source_digest) {
        None => revision,
        Some(difference) => {
            if !build_flags.mismatch_builds() {
                return Err(syn::Error::new_spanned(
                    &path,
                    mismatch_message(&crate_name, &listed_path, &difference),
                ));
            }

            mismatch_revision(&revision)
        }
    };

    let transaction_support = if runs_inside_a_transaction {
        quote! { Supported }
    } else {
        quote! { Unsupported }
    };

    let release_guards = release_guards(&build_flags);
    let watched_variables = watched_variables();

    // The checksums come through `include_bytes!` and the content through
    // `include_str!`, so the compiler knows the crate depends on them, and
    // compiles it again when one changes.
    let mut watched_checksums = Vec::new();
    if let Some(checksums_path) = source_checksums::watched_checksums(manifest_dir) {
        let Some(checksums_path) = checksums_path.to_str() else {
            return Err(syn::Error::new_spanned(
                &path,
                format!(
                    "the path of the SQL files checksums is not valid UTF-8: {}",
                    checksums_path.display()
                ),
            ));
        };

        watched_checksums.push(checksums_path.to_owned());
    }

    Ok(quote! {
        {
            #release_guards

            #(const _: &[u8] = ::core::include_bytes!(#watched_checksums);)*
            #watched_variables

            #sql_file_crate::Migration {
                version: #sql_file_crate::macro_support::MigrationVersion::from_raw(#version),
                description: #description,
                sql: ::core::include_str!(#full_path_str),
                revision: #sql_file_crate::macro_support::MigrationRevision::from_static(
                    #revision
                ),
                source_digest: #source_digest,
                transaction_support:
                    #sql_file_crate::macro_support::MigrationTransactionSupport::#transaction_support,
                connection_type: ::core::marker::PhantomData,
            }
        }
    })
}
