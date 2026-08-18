//! Structural comparison of a case's outcome against its ground truth.
//!
//! Both sides are VM values, so the comparison is a walk over the values
//! themselves rather than over renderings of them.  Rendering happens
//! only to name where a walk found a difference.

use crate::outcome::CaseOutcome;

/// Compare a case's expected outcome against what it produced.
///
/// The comparison is structural and ORDER-SENSITIVE: a dict's entries
/// must appear in the same order on both sides, since insertion order is
/// part of the value in both languages.  Value equality alone would not
/// see that — an [`IndexMap`](indexmap::IndexMap) compares as a map,
/// ignoring order.
pub fn compare_outcomes(expected: &CaseOutcome, actual: &CaseOutcome) -> Result<(), String> {
    match (expected, actual) {
        (CaseOutcome::Completion(expected), CaseOutcome::Completion(actual)) => {
            compare_values(Path::root(), expected, actual)
        }
        (CaseOutcome::Exception(expected), CaseOutcome::Exception(actual)) => {
            compare_exceptions(Path::root(), expected, actual)
        }
        (expected, actual) => Err(format!(
            "expected a {}, got a {}\nexpected={expected:?}\nactual={actual:?}",
            expected.kind(),
            actual.kind(),
        )),
    }
}

/// Where in a value a comparison currently is, for naming the spot a
/// mismatch was found.
#[derive(Clone, Copy)]
struct Path<'p>(Option<&'p PathSegment<'p>>);

struct PathSegment<'p> {
    parent: Path<'p>,
    step: Step<'p>,
}

enum Step<'p> {
    Key(&'p str),
    Index(usize),
    Details,
}

impl<'p> Path<'p> {
    fn root() -> Self {
        Self(None)
    }

    fn step(self, step: Step<'p>) -> PathSegment<'p> {
        PathSegment { parent: self, step }
    }

    fn render(&self) -> String {
        let Some(segment) = self.0 else {
            return "the value".to_owned();
        };

        let mut rendered = segment.parent.render();
        match segment.step {
            Step::Key(key) => rendered.push_str(&format!("[{key:?}]")),
            Step::Index(index) => rendered.push_str(&format!("[{index}]")),
            Step::Details => rendered.push_str(".details"),
        }
        rendered
    }
}

impl<'p> PathSegment<'p> {
    fn as_path(&'p self) -> Path<'p> {
        Path(Some(self))
    }
}

fn mismatch(
    path: Path<'_>,
    what: &str,
    expected: impl core::fmt::Debug,
    actual: impl core::fmt::Debug,
) -> String {
    format!(
        "{} differs in {what}\n  expected: {expected:?}\n  actual:   {actual:?}",
        path.render(),
    )
}

fn compare_values(
    path: Path<'_>,
    expected: &waymark_system_vm::ReadyValue,
    actual: &waymark_system_vm::ReadyValue,
) -> Result<(), String> {
    use waymark_system_vm::ReadyValue;

    match (expected, actual) {
        (ReadyValue::List(expected_items), ReadyValue::List(actual_items)) => {
            if expected_items.len() != actual_items.len() {
                return Err(mismatch(
                    path,
                    "list length",
                    expected_items.len(),
                    actual_items.len(),
                ));
            }
            for (index, (expected, actual)) in
                expected_items.iter().zip(actual_items.iter()).enumerate()
            {
                let segment = path.step(Step::Index(index));
                compare_promise_values(segment.as_path(), expected, actual)?;
            }
            Ok(())
        }
        (ReadyValue::Dict(expected_entries), ReadyValue::Dict(actual_entries)) => {
            let expected_keys: Vec<&str> = expected_entries.keys().map(String::as_str).collect();
            let actual_keys: Vec<&str> = actual_entries.keys().map(String::as_str).collect();
            // Insertion order is part of a dict's value, so the keys are
            // compared as a sequence rather than as a set.
            if expected_keys != actual_keys {
                return Err(mismatch(path, "dict keys", expected_keys, actual_keys));
            }
            for (key, expected) in expected_entries {
                let actual = actual_entries
                    .get(key)
                    .expect("the key sequences are equal");
                let segment = path.step(Step::Key(key));
                compare_promise_values(segment.as_path(), expected, actual)?;
            }
            Ok(())
        }
        (ReadyValue::Exception(expected), ReadyValue::Exception(actual)) => {
            compare_exception_values(path, expected, actual)
        }
        (expected, actual) if expected == actual => Ok(()),
        (expected, actual) => Err(mismatch(path, "value", expected, actual)),
    }
}

fn compare_promise_values(
    path: Path<'_>,
    expected: &waymark_system_vm::Value,
    actual: &waymark_system_vm::Value,
) -> Result<(), String> {
    use waymark_system_vm::Value;

    match (expected, actual) {
        (Value::Ready(expected), Value::Ready(actual)) => compare_values(path, expected, actual),
        // A settled outcome holds no pending promise; if one shows up,
        // say where rather than comparing past it.
        (expected, actual) => Err(mismatch(path, "readiness", expected, actual)),
    }
}

/// Compare two exceptions.
///
/// Only the type id is compared.  The details are each producer's own
/// account of what happened — Python's carry the traceback, module and
/// class hierarchy, the VM's carry whatever it built — so requiring them
/// to agree would test the accounts against each other rather than the
/// exception.  Both are reported when the ids differ.
fn compare_exceptions(
    path: Path<'_>,
    expected: &waymark_vm_runtime_exception::Exception<waymark_system_vm::ReadyValue>,
    actual: &waymark_vm_runtime_exception::Exception<waymark_system_vm::ReadyValue>,
) -> Result<(), String> {
    if expected.type_id != actual.type_id {
        return Err(format!(
            "{} differs in exception type\n  expected: {:?}\n  actual:   {:?}\n  expected details: {:?}\n  actual details:   {:?}",
            path.render(),
            expected.type_id,
            actual.type_id,
            expected.details,
            actual.details,
        ));
    }
    Ok(())
}

fn compare_exception_values(
    path: Path<'_>,
    expected: &waymark_vm_runtime_exception::Exception<waymark_system_vm::Value>,
    actual: &waymark_vm_runtime_exception::Exception<waymark_system_vm::Value>,
) -> Result<(), String> {
    if expected.type_id != actual.type_id {
        return Err(mismatch(
            path,
            "exception type",
            &expected.type_id,
            &actual.type_id,
        ));
    }
    let segment = path.step(Step::Details);
    compare_promise_values(segment.as_path(), &expected.details, &actual.details)
}
