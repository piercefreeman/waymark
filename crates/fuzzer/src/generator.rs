//! Random IR program generator used by the fuzz harness.

use proptest::collection;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

#[derive(Debug, Clone)]
enum Step {
    Add(u8),
    ListLen(Vec<u8>),
    BranchAdd { even_add: u8, odd_add: u8 },
    NegateCancel,
    DotAccess,
}

/// Generated workflow case source and input.
#[derive(Debug, Clone)]
pub struct GeneratedCase {
    pub source: String,
    pub base_input: i64,
}

/// Generate a deterministic case from a proptest runner state.
pub fn generate_case(
    runner: &mut TestRunner,
    max_steps: usize,
    case_index: usize,
) -> Result<GeneratedCase, String> {
    let tree = case_strategy(max_steps, case_index)
        .new_tree(runner)
        .map_err(|err| err.to_string())?;
    Ok(tree.current())
}

fn step_strategy() -> impl Strategy<Value = Step> {
    prop_oneof![
        (0u8..=5).prop_map(Step::Add),
        collection::vec(0u8..=6, 1..=4).prop_map(Step::ListLen),
        (0u8..=3, 0u8..=3).prop_map(|(even_add, odd_add)| Step::BranchAdd { even_add, odd_add }),
        Just(Step::NegateCancel),
        Just(Step::DotAccess),
    ]
}

fn case_strategy(max_steps: usize, case_index: usize) -> impl Strategy<Value = GeneratedCase> {
    (
        0i64..=10,
        collection::vec(step_strategy(), 1..=max_steps.max(1)),
    )
        .prop_map(move |(base_input, steps)| {
            render_case(steps, base_input + (case_index as i64 % 3))
        })
}

fn render_case(steps: Vec<Step>, base_input: i64) -> GeneratedCase {
    let mut lines = vec![
        "fn main(input: [base], output: [result]):".to_string(),
        "    acc = base".to_string(),
    ];

    let mut temp_counter: usize = 0;

    for step in steps {
        match step {
            // Simple arithmetic assignment.
            Step::Add(addend) => {
                lines.push(format!("    acc = acc + {addend}"));
            }
            // List + length expression.
            Step::ListLen(values) => {
                let values_name = format!("values_{temp_counter}");
                let items: Vec<String> =
                    values.into_iter().map(|value| value.to_string()).collect();
                lines.push(format!("    {values_name} = [{}]", items.join(", ")));
                lines.push(format!("    acc = acc + len({values_name})"));
                temp_counter += 1;
            }
            // Modulo branch with arithmetic updates.
            Step::BranchAdd { even_add, odd_add } => {
                lines.push("    if acc % 2 == 0:".to_string());
                lines.push(format!("        acc = acc + {even_add}"));
                lines.push("    else:".to_string());
                lines.push(format!("        acc = acc + {odd_add}"));
            }
            // Unary negation cancellation.
            Step::NegateCancel => {
                lines.push("    acc = -(-acc)".to_string());
            }
            // Dict/index access path.
            Step::DotAccess => {
                lines.push("    payload = {\"acc\": acc}".to_string());
                lines.push("    acc = payload.acc".to_string());
            }
        }
    }

    lines.push("    result = @double(value=acc)".to_string());
    lines.push("    return result".to_string());

    GeneratedCase {
        source: lines.join("\n"),
        base_input,
    }
}
