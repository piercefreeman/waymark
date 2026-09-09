//! The page: one slice of a read, with where to resume from.

/// One page of a read: its items, in the read's order, and where to
/// resume from for what follows them.
//
// The schema is named after the item — `EventPage` for a page of
// `Event`s — and has to be: a generic is named by its bare ident
// otherwise, so every instantiation would fight over `Page` and the
// losers would be numbered.
//
// Neither serde nor schemars can infer the bounds: the cursor is on the
// wire through its proxy's codec, not through impls of its own.
//
// The cursor must also stay out of the schema's identity: schemars folds
// every type parameter it meets in a field into the schema id, and a page
// of one item type is read with as many cursor types as there are reads.
// A `schema_with` field is not visited, so `next` takes its schema from
// the cursor module by hand, and `EventPage` stays one component.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[schemars(rename = "{Item}Page", bound = "Item: schemars::JsonSchema")]
#[serde(bound(serialize = "Item: serde::Serialize, Cursor: waymark_cursor_core::EncodeCursor"))]
pub struct Page<Item, Cursor> {
    /// The items of this page, in the read's order; empty when the read
    /// had nothing (more) to give.
    pub items: Vec<Item>,

    /// Where to resume from for what follows this page, as an opaque
    /// cursor for the read's `after`; absent when the page is empty.
    #[schemars(schema_with = "crate::cursor::optional_schema::<Cursor>")]
    pub next: Option<crate::cursor::Cursor<Cursor>>,
}

#[cfg(test)]
mod tests;
