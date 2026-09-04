use std::collections::HashSet;

use waymark_observability_events_core::kind::{FromTag as _, Tagged as _};

use super::*;

/// The lookup is built from the table, so every kind reads back from
/// its own tag — and no two kinds share one, or the lookup would have
/// dropped one of them.
#[test]
fn every_kind_reads_back_from_its_tag() {
    let mut tags = HashSet::new();

    for kind in Kind::all() {
        let tag = kind.tag();
        assert_eq!(Kind::from_tag(tag), Some(kind), "{tag}");
        assert!(tags.insert(tag), "tag {tag} names more than one kind");
    }

    assert_eq!(Kind::from_tag("not a kind"), None);
}
