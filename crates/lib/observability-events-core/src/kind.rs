//! The kind: what it is to the readers — text, both ways — the family it
//! belongs to, and the payload that carries it.

/// A kind with a tag: the stable text the store column and the wire
/// carry.
pub trait Tagged {
    /// This kind's tag.
    fn tag(&self) -> &'static str;
}

/// The kind some text names, if any.
///
/// The way back from text that came from outside — a filter, a stored
/// column — which may name no kind at all.
pub trait FromTag: Sized {
    /// The kind `tag` names.
    fn from_tag(tag: &str) -> Option<Self>;
}

/// A subset of a kind family: a type whose values are some of a root
/// kind type's kinds, each convertible to the root kind it is, and back.
///
/// A root kind type is a subset of itself — the whole of it.
pub trait Subset: Sized {
    /// The root kind type the subset's values are kinds of.
    type RootKind: Kind;

    /// Every kind in the subset, once, as itself.
    fn subset() -> impl Iterator<Item = Self>;

    /// The root kind this is.
    fn root_kind(&self) -> Self::RootKind;

    /// The subset's kind a root kind is, if it is one of them.
    fn from_root_kind(root: Self::RootKind) -> Option<Self>;
}

/// A kind family, seen from any of its subsets: the subset's kinds as the
/// root kinds they are.
pub trait SubsetExt: Subset {
    /// Every kind in the subset, once, as the root kind it is.
    fn root_subset() -> impl Iterator<Item = Self::RootKind>;

    /// The tag of the root kind this is.
    fn root_tag(&self) -> &'static str;

    /// The subset's kind a root kind's tag names, if it names one of them.
    fn try_from_root_tag(tag: &str) -> Option<Self>;
}

impl<Member> SubsetExt for Member
where
    Member: Subset,
{
    fn root_subset() -> impl Iterator<Item = Self::RootKind> {
        Self::subset().map(|kind| kind.root_kind())
    }

    fn root_tag(&self) -> &'static str {
        Tagged::tag(&self.root_kind())
    }

    fn try_from_root_tag(tag: &str) -> Option<Self> {
        let root = <Self::RootKind as FromTag>::from_tag(tag)?;
        Self::from_root_kind(root)
    }
}

/// A kind: tagged text both ways, and the root of its own family.
///
/// Held by whatever has the three; nothing implements it by hand.
pub trait Kind: Tagged + FromTag + Subset<RootKind = Self> {
    /// Every kind there is, once.
    fn all() -> impl Iterator<Item = Self>;
}

impl<Root> Kind for Root
where
    Root: Tagged + FromTag + Subset<RootKind = Self>,
{
    fn all() -> impl Iterator<Item = Self> {
        Self::subset()
    }
}

/// Implemented by every payload type: a payload with a kind — what the
/// readers may know about an event without knowing its type.
///
/// The kind is the payload's own closed set — the readers match
/// on it, the API document lists its values, and a producer cannot
/// invent one the readers don't know. Its tag is the stable text the
/// store and the wire carry, both ways, and it is the root of its own
/// family — a [`Kind`], so no reader has to ask for any of it.
pub trait Kinded {
    /// The closed set of kinds.
    type Kind: Kind;

    /// The event's kind.
    fn kind(&self) -> Self::Kind;
}
