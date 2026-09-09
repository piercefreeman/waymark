use super::*;

/// A kind family for the tests: three kinds, one of them a stop.
#[derive(Debug, PartialEq)]
enum TestKind {
    Started,
    Noted,
    Stopped,
}

impl waymark_observability_events_core::kind::Tagged for TestKind {
    fn tag(&self) -> &'static str {
        match self {
            TestKind::Started => "test.started",
            TestKind::Noted => "test.noted",
            TestKind::Stopped => "test.stopped",
        }
    }
}

impl waymark_observability_events_core::kind::FromTag for TestKind {
    fn from_tag(tag: &str) -> Option<Self> {
        <TestKind as waymark_observability_events_core::kind::Subset>::subset()
            .find(|kind| waymark_observability_events_core::kind::Tagged::tag(kind) == tag)
    }
}

impl waymark_observability_events_core::kind::Subset for TestKind {
    type RootKind = TestKind;

    fn subset() -> impl Iterator<Item = Self> {
        [TestKind::Started, TestKind::Noted, TestKind::Stopped].into_iter()
    }

    fn root_kind(&self) -> TestKind {
        match self {
            TestKind::Started => TestKind::Started,
            TestKind::Noted => TestKind::Noted,
            TestKind::Stopped => TestKind::Stopped,
        }
    }

    fn from_root_kind(root: TestKind) -> Option<Self> {
        Some(root)
    }
}

/// The stops: a subset of the test kinds with a value type of its own.
#[derive(Debug, PartialEq)]
enum TestStop {
    Stopped,
}

impl waymark_observability_events_core::kind::Subset for TestStop {
    type RootKind = TestKind;

    fn subset() -> impl Iterator<Item = Self> {
        [TestStop::Stopped].into_iter()
    }

    fn root_kind(&self) -> TestKind {
        match self {
            TestStop::Stopped => TestKind::Stopped,
        }
    }

    fn from_root_kind(root: TestKind) -> Option<Self> {
        match root {
            TestKind::Stopped => Some(TestStop::Stopped),
            _ => None,
        }
    }
}

/// The name of the whole family's component.
#[derive(Debug)]
struct Name;

impl waymark_http_api_types::TypeName for Name {
    const NAME: &'static str = "TestKind";
}

/// The name of the stops' component.
#[derive(Debug)]
struct Stops;

impl waymark_http_api_types::TypeName for Stops {
    const NAME: &'static str = "StopKind";
}

/// The schema of `T`, as JSON, under the OpenAPI 3 settings the
/// document is generated with.
fn schema_of<T: schemars::JsonSchema>() -> serde_json::Value {
    let mut generator = schemars::generate::SchemaSettings::openapi3().into_generator();
    let schema = generator.root_schema_for::<T>();
    serde_json::to_value(schema).expect("schema json")
}

#[test]
fn serializes_as_the_tag() {
    let tag: KindTag<TestKind, Name> = TestKind::Stopped.into();

    assert_eq!(
        serde_json::to_value(tag).expect("json"),
        serde_json::json!("test.stopped")
    );
}

#[test]
fn deserializes_through_the_lookup() {
    let tag: KindTag<TestKind, Name> =
        serde_json::from_value(serde_json::json!("test.started")).expect("tag");

    assert_eq!(tag.kind, TestKind::Started);
}

#[test]
fn rejects_an_unknown_tag() {
    let error = serde_json::from_value::<KindTag<TestKind, Name>>(serde_json::json!("nope"))
        .expect_err("unknown");

    assert!(
        error.to_string().contains("not a kind of this set"),
        "{error}"
    );
}

#[test]
fn schema_is_a_component_named_by_the_marker_listing_every_tag() {
    assert_eq!(
        <KindTag<TestKind, Name> as schemars::JsonSchema>::schema_name(),
        "TestKind"
    );

    let schema = schema_of::<KindTag<TestKind, Name>>();

    assert_eq!(schema["type"], "string", "{schema}");
    assert_eq!(
        schema["enum"],
        serde_json::json!(["test.started", "test.noted", "test.stopped"]),
        "{schema}"
    );
}

#[test]
fn a_subset_holds_its_own_values_and_admits_only_its_members() {
    let schema = schema_of::<KindTag<TestStop, Stops>>();
    assert_eq!(schema["title"], "StopKind", "{schema}");
    assert_eq!(
        schema["enum"],
        serde_json::json!(["test.stopped"]),
        "{schema}"
    );

    let tag: KindTag<TestStop, Stops> = TestStop::Stopped.into();
    assert_eq!(
        serde_json::to_value(tag).expect("json"),
        serde_json::json!("test.stopped")
    );

    let tag: KindTag<TestStop, Stops> =
        serde_json::from_value(serde_json::json!("test.stopped")).expect("a stop");
    assert_eq!(tag.kind, TestStop::Stopped);

    let error = serde_json::from_value::<KindTag<TestStop, Stops>>(serde_json::json!("test.noted"))
        .expect_err("not a stop");
    assert!(
        error.to_string().contains("not a kind of this set"),
        "{error}"
    );
}
