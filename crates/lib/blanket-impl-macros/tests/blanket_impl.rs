//! Behavioral tests for the `#[blanket_impl]` expansion: each case defines
//! a unifying trait, and the assertions prove the generated blanket
//! implementation covers exactly the types satisfying the supertrait
//! bounds.

trait First {}
trait Second {}

struct Both;
impl First for Both {}
impl Second for Both {}

#[waymark_blanket_impl_macros::blanket_impl]
trait Unified: First + Second {}

#[test]
fn plain_supertraits_are_blanket_covered() {
    fn assert_unified<Type: Unified>() {}
    assert_unified::<Both>();
}

trait WithItem {
    type Item;
}

trait AlsoWithItem {
    type Item;
}

struct SameItems;
impl WithItem for SameItems {
    type Item = u8;
}
impl AlsoWithItem for SameItems {
    type Item = u8;
}

#[waymark_blanket_impl_macros::blanket_impl]
trait MatchingItems: WithItem + AlsoWithItem<Item = <Self as WithItem>::Item> {}

#[test]
fn self_referential_associated_type_equalities_are_preserved() {
    fn assert_matching<Type: MatchingItems>() {}
    assert_matching::<SameItems>();
}

trait Container<Element> {}

struct Holder;
impl Container<u8> for Holder {}
impl First for Holder {}

#[waymark_blanket_impl_macros::blanket_impl]
trait UnifiedContainer<Element>: Container<Element> + First
where
    Element: Copy,
{
}

#[test]
fn generic_trait_with_where_clause_is_blanket_covered() {
    fn assert_container<Type: UnifiedContainer<u8>>() {}
    assert_container::<Holder>();
}

/// # Safety
///
/// A test fixture; carries no actual safety obligations.
#[waymark_blanket_impl_macros::blanket_impl]
unsafe trait UnsafeUnified: First {}

#[test]
fn unsafe_trait_gets_an_unsafe_blanket_implementation() {
    fn assert_unsafe_unified<Type: UnsafeUnified>() {}
    assert_unsafe_unified::<Both>();
}
