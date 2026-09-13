//! Fixtures shared by the modules' tests.

/// The schema of `T`, as JSON, under the OpenAPI 3 settings the
/// document is generated with.
pub(crate) fn schema_of<T: schemars::JsonSchema>() -> serde_json::Value {
    let mut generator = schemars::generate::SchemaSettings::openapi3().into_generator();
    let schema = generator.root_schema_for::<T>();
    serde_json::to_value(schema).expect("schema json")
}
