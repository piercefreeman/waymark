//! The closed classification of events.

/// Which event, across every source.
///
/// Closed: the readers match on it, the API document lists its values,
/// and a producer cannot invent one the readers don't know. Empty until
/// the first source lands; each source slice adds its variants.
///
/// Its string form is the stable tag — the store's `kind` column and the
/// wire value — spelled out per variant (`#[strum(serialize = "…")]`),
/// as `&'static str: From<Kind>`. The way back (`strum::EnumString`)
/// comes with the first variant: on an empty enum it is unreachable
/// code, and nothing parses a kind until there is a kind filter.
#[derive(Debug, strum::IntoStaticStr)]
pub enum Kind {}
