//! The curated fixture cases.

use color_eyre::eyre::bail;

/// A fixture's keyword argument, as authored.
///
/// The literals a fixture needs, kept small enough that the case table
/// stays `const` — and kept in the VM's own value terms, so the kwargs
/// are authored once and encoded for the Python helper rather than
/// written out twice.
#[derive(Clone, Debug)]
pub enum Kwarg {
    /// An integer.
    Int(i64),

    /// A string.
    Str(&'static str),

    /// A list of integers.
    Ints(&'static [i64]),

    /// A list of strings.
    Strs(&'static [&'static str]),
}

impl Kwarg {
    /// The VM value this argument denotes.
    pub fn value(&self) -> waymark_system_vm::ReadyValue {
        use waymark_system_vm::{ReadyValue, Value};

        match self {
            Self::Int(value) => ReadyValue::Int(*value),
            Self::Str(value) => ReadyValue::String((*value).to_owned()),
            Self::Ints(values) => ReadyValue::List(
                values
                    .iter()
                    .map(|value| Value::Ready(ReadyValue::Int(*value)))
                    .collect(),
            ),
            Self::Strs(values) => ReadyValue::List(
                values
                    .iter()
                    .map(|value| Value::Ready(ReadyValue::String((*value).to_owned())))
                    .collect(),
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FixtureCase {
    pub id: &'static str,
    pub module_name: &'static str,
    pub workflow_class: &'static str,
    pub kwargs: &'static [(&'static str, Kwarg)],
}

pub const CASES: &[FixtureCase] = &[
    FixtureCase {
        id: "simple",
        module_name: "simple_workflow",
        workflow_class: "SimpleWorkflow",
        kwargs: &[("name", Kwarg::Str("world"))],
    },
    FixtureCase {
        id: "sequential",
        module_name: "sequential_workflow",
        workflow_class: "SequentialWorkflow",
        kwargs: &[],
    },
    FixtureCase {
        id: "conditional",
        module_name: "conditional_workflow",
        workflow_class: "ConditionalWorkflow",
        kwargs: &[("tier", Kwarg::Str("high"))],
    },
    FixtureCase {
        id: "immediate-conditional",
        module_name: "immediate_conditional_workflow",
        workflow_class: "ImmediateConditionalWorkflow",
        kwargs: &[("value", Kwarg::Int(17))],
    },
    FixtureCase {
        id: "chain",
        module_name: "chain_workflow",
        workflow_class: "ChainWorkflow",
        kwargs: &[("text", Kwarg::Str("hello"))],
    },
    FixtureCase {
        id: "for-loop",
        module_name: "for_loop_workflow",
        workflow_class: "ForLoopWorkflow",
        kwargs: &[("items", Kwarg::Strs(&["alpha", "beta", "gamma"]))],
    },
    FixtureCase {
        id: "parallel",
        module_name: "parallel_workflow",
        workflow_class: "ParallelWorkflow",
        kwargs: &[("value", Kwarg::Int(7))],
    },
    FixtureCase {
        id: "gather-listcomp",
        module_name: "integration_gather_listcomp",
        workflow_class: "GatherListCompWorkflow",
        kwargs: &[("items", Kwarg::Ints(&[1, 2, 3]))],
    },
    FixtureCase {
        id: "tuple-unpack-fn-call",
        module_name: "integration_tuple_unpack_fn_call",
        workflow_class: "TupleUnpackFnCallWorkflow",
        kwargs: &[("user_id", Kwarg::Str("user_42"))],
    },
    FixtureCase {
        id: "nested-conditionals",
        module_name: "integration_nested_conditionals",
        workflow_class: "NestedConditionalsWorkflow",
        kwargs: &[("user_id", Kwarg::Str("user_c"))],
    },
    FixtureCase {
        id: "data-pipeline",
        module_name: "integration_data_pipeline",
        workflow_class: "DataPipelineWorkflow",
        kwargs: &[
            ("source", Kwarg::Str("sales")),
            ("threshold", Kwarg::Int(100)),
        ],
    },
    FixtureCase {
        id: "string-processing",
        module_name: "integration_string_processing",
        workflow_class: "StringProcessingWorkflow",
        kwargs: &[("text", Kwarg::Str("Alpha123"))],
    },
    FixtureCase {
        id: "timeout",
        module_name: "integration_timeout_workflow",
        workflow_class: "TimeoutWorkflow",
        kwargs: &[],
    },
];

pub fn select_cases(filters: &[String]) -> Result<Vec<FixtureCase>, color_eyre::eyre::Report> {
    if filters.is_empty() {
        return Ok(CASES.to_vec());
    }

    let mut selected = Vec::new();
    for filter in filters {
        let Some(case) = CASES.iter().find(|candidate| candidate.id == filter) else {
            bail!("unknown fixture case '{filter}'")
        };
        selected.push(case.clone());
    }
    Ok(selected)
}
