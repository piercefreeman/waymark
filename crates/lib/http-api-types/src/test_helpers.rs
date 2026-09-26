//! Fixtures shared by the modules' tests.

/// The schema of `T`, as JSON, under the settings the document is
/// generated with: `aide` builds its generator from schemars' draft-07
/// settings, with the subschemas kept as references under
/// `#/components/schemas/`. The document declares OpenAPI 3.1, whose
/// schema dialect renders an absent value as the `null` type, never as
/// `nullable`.
pub fn schema_of<T: schemars::JsonSchema>() -> serde_json::Value {
    let mut generator = schemars::generate::SchemaSettings::draft07()
        .with(|settings| {
            settings.inline_subschemas = false;
            settings.definitions_path = "#/components/schemas/".into();
        })
        .into_generator();
    let schema = generator.root_schema_for::<T>();
    serde_json::to_value(schema).expect("schema json")
}
