//! The compiled webapp, embedded at build time.
//!
//! `WAYMARK_BUILD_WEBAPP_ENABLE` selects how the build includes it: `required`
//! (the default when `CI=true`) fails the build when the webapp does not build,
//! `best-effort` (the default otherwise) embeds a placeholder page instead, and
//! `disabled` always embeds the placeholder.

/// Whether this build embedded the placeholder page instead of the webapp.
pub const EMBEDS_PLACEHOLDER: bool = cfg!(waymark_webapp_placeholder);

/// The embedded webapp files.
#[derive(rust_embed::Embed)]
#[folder = "$WAYMARK_WEBAPP_ASSETS_DIR"]
pub struct Assets;
