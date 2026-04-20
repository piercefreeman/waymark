use waymark_core_backend_metadata::MetadataValues;

struct Field1;
struct Field2;

impl waymark_core_backend_metadata::MetadataItem for Field1 {
    const KEY: &str = "field1";
    type Value = &'static str;
}

impl waymark_core_backend_metadata::MetadataItem for Field2 {
    const KEY: &str = "field2";
    type Value = &'static str;
}

#[test]
fn no_fields() {
    type Metadata = ();
    let values: MetadataValues<Metadata> = ().into();

    assert_eq!(format!("{values:?}"), "MetadataValues");
}

#[test]
fn one_field() {
    type Metadata = (Field1,);
    let values: MetadataValues<Metadata> = ("1",).into();

    assert_eq!(format!("{values:?}"), "MetadataValues { field1: \"1\" }");
}

#[test]
fn two_fields() {
    type Metadata = (Field1, Field2);
    let values: MetadataValues<Metadata> = ("1", "2").into();

    assert_eq!(
        format!("{values:?}"),
        "MetadataValues { field1: \"1\", field2: \"2\" }"
    );
}
