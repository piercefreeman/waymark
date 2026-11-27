//! What the proc macros of the migrations share about a source and its
//! checksums: the variables that switch the checks, what a build with one
//! of them on is refused and watched for, what the migration and the
//! compiler's message say of a source that does not match, and what the
//! tests the macros write read and print.
//!
//! Only proc-macro crates use this crate, and the names of the variables
//! are nowhere else: what else needs one gets it as a literal a proc
//! macro writes.

#![warn(missing_docs)]

use quote::quote;

/// The variable that allows a migration whose source does not match its
/// checksums to build.
const ALLOW_MISMATCH_VARIABLE: &str = "WAYMARK_MIGRATIONS_ALLOW_SOURCE_CHECKSUMS_MISMATCH";

/// The variable that asks for the checksums to be written anew: a source
/// that does not match them builds, and the tests write them.
const UPDATE_VARIABLE: &str = "WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS";

/// The variable that lets the tests write the checksums and does nothing
/// else: no build reads it.
const TESTS_WRITE_VARIABLE: &str = "WAYMARK_MIGRATIONS_TESTS_WRITE_SOURCE_CHECKSUMS";

/// What comes after the revision of a migration built from a source that
/// does not match its checksums.
const MISMATCH_REVISION_SUFFIX: &str = " (source checksums mismatch)";

/// The build flags, as the variables of the build have them.
#[derive(Debug)]
pub struct BuildFlags {
    /// Whether a source that does not match its checksums can build.
    pub allow_mismatch: bool,

    /// Whether the checksums are being written anew: a source that does
    /// not match them can build then too, for the tests that write them.
    pub update: bool,
}

impl BuildFlags {
    /// Whether a source that does not match its checksums builds.
    pub fn mismatch_builds(&self) -> bool {
        self.allow_mismatch || self.update
    }
}

/// Error returned by [`read_build_flags`].
#[derive(Debug, thiserror::Error)]
pub enum ReadBuildFlagsError {
    /// The variable that allows a mismatch could not be read.
    #[error("reading the variable that allows a mismatch: {0}")]
    AllowMismatch(#[source] envfury::Error<envfury::OrParseError<core::str::ParseBoolError>>),

    /// The update variable could not be read.
    #[error("reading the update variable: {0}")]
    Update(#[source] envfury::Error<envfury::OrParseError<core::str::ParseBoolError>>),
}

/// Read the build flags from the variables of the build: each is off
/// unless its variable is `true`.
pub fn read_build_flags() -> Result<BuildFlags, ReadBuildFlagsError> {
    let allow_mismatch: bool = envfury::or_parse(ALLOW_MISMATCH_VARIABLE, "false")
        .map_err(ReadBuildFlagsError::AllowMismatch)?;
    let update: bool =
        envfury::or_parse(UPDATE_VARIABLE, "false").map_err(ReadBuildFlagsError::Update)?;

    Ok(BuildFlags {
        allow_mismatch,
        update,
    })
}

/// The statements that refuse a build without debug assertions for each
/// flag of `flags` that is on.
///
/// The profile is the compiled crate's, which only its own `cfg` tells:
/// the statements go into the macro's expansion.
pub fn release_guards(flags: &BuildFlags) -> proc_macro2::TokenStream {
    let mut guards = Vec::new();

    for (variable, flag) in [
        (ALLOW_MISMATCH_VARIABLE, flags.allow_mismatch),
        (UPDATE_VARIABLE, flags.update),
    ] {
        if !flag {
            continue;
        }

        let message =
            format!("{variable} is `true`: it cannot be in a build without debug assertions");

        guards.push(quote! {
            #[cfg(not(debug_assertions))]
            ::core::compile_error!(#message);
        });
    }

    quote! { #(#guards)* }
}

/// The items that make the compiler watch the variables of the build
/// flags, and compile the crate again when one changes.
pub fn watched_variables() -> proc_macro2::TokenStream {
    quote! {
        const _: ::core::option::Option<&str> = ::core::option_env!(#ALLOW_MISMATCH_VARIABLE);
        const _: ::core::option::Option<&str> = ::core::option_env!(#UPDATE_VARIABLE);
    }
}

/// The revision of a migration built from a source that does not match
/// its checksums, out of `revision`, the one it declares.
pub fn mismatch_revision(revision: &str) -> String {
    format!("{revision}{MISMATCH_REVISION_SUFFIX}")
}

/// The command that writes the checksums of the crate `package` anew: the
/// crate's tests, with the update variable on.
pub fn update_command(package: &str) -> String {
    format!("{UPDATE_VARIABLE}=true cargo test -p {package}")
}

/// The part of the compiler's message about a source that does not match
/// its checksums that says what can be done about it, for the crate
/// `package`.
pub fn mismatch_help(package: &str) -> String {
    format!(
        "to build it anyway while developing:\n    {ALLOW_MISMATCH_VARIABLE}=true\n\nto approve \
         the source as it is, after verifying the migration:\n    {}\n",
        update_command(package)
    )
}

/// The expression, for a test a proc macro writes, of whether the test
/// may write the checksums: read when the test runs, it is `true` when
/// the update variable or the variable that lets the tests write is.
///
/// `envfury` is the path the test reaches the `envfury` crate through.
/// The test panics when a variable cannot be read.
pub fn read_update_at_run_time(envfury: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    quote! {
        {
            let update: bool = #envfury::or_parse(#UPDATE_VARIABLE, "false")
                .expect("read the update variable");
            let tests_write: bool = #envfury::or_parse(#TESTS_WRITE_VARIABLE, "false")
                .expect("read the variable that lets the tests write");

            update || tests_write
        }
    }
}

#[cfg(test)]
mod tests;
