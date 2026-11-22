//! The vendored snapshots the tests run over.

/// The bytes of a fixture file: its hex digits, with the comments and the
/// layout dropped.
fn fixture_bytes(text: &str) -> Vec<u8> {
    text.lines()
        .flat_map(|line| {
            let (digits, _comment) = line.split_once('#').unwrap_or((line, ""));

            digits.split_whitespace()
        })
        .map(|byte| u8::from_str_radix(byte, 16).expect("a fixture byte is two hex digits"))
        .collect()
}

/// A populated runtime state in the shape before the migration.
pub fn populated_before() -> Vec<u8> {
    fixture_bytes(include_str!("../fixtures/populated_before.hex"))
}

/// The runtime state of [`populated_before`] in the shape after the
/// migration, as the runtime at the migration's commit wrote it.
pub fn populated_after() -> Vec<u8> {
    fixture_bytes(include_str!("../fixtures/populated_after.hex"))
}

/// A runtime state with nothing in it, in the shape before the migration.
pub fn empty_before() -> Vec<u8> {
    fixture_bytes(include_str!("../fixtures/empty_before.hex"))
}

/// The runtime state of [`empty_before`] in the shape after the migration.
pub fn empty_after() -> Vec<u8> {
    fixture_bytes(include_str!("../fixtures/empty_after.hex"))
}
