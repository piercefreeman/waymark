//! The curated fixture cases.

use color_eyre::eyre::bail;

#[derive(Clone, Debug)]
pub struct FixtureCase {
    pub id: &'static str,
    pub module_name: &'static str,
    pub workflow_class: &'static str,
    pub kwargs_json: &'static str,
}

pub const CASES: &[FixtureCase] = &[
    FixtureCase {
        id: "simple",
        module_name: "simple_workflow",
        workflow_class: "SimpleWorkflow",
        kwargs_json: r#"{"name":"world"}"#,
    },
    FixtureCase {
        id: "sequential",
        module_name: "sequential_workflow",
        workflow_class: "SequentialWorkflow",
        kwargs_json: r#"{}"#,
    },
    FixtureCase {
        id: "conditional",
        module_name: "conditional_workflow",
        workflow_class: "ConditionalWorkflow",
        kwargs_json: r#"{"tier":"high"}"#,
    },
    FixtureCase {
        id: "immediate-conditional",
        module_name: "immediate_conditional_workflow",
        workflow_class: "ImmediateConditionalWorkflow",
        kwargs_json: r#"{"value":17}"#,
    },
    FixtureCase {
        id: "chain",
        module_name: "chain_workflow",
        workflow_class: "ChainWorkflow",
        kwargs_json: r#"{"text":"hello"}"#,
    },
    FixtureCase {
        id: "for-loop",
        module_name: "for_loop_workflow",
        workflow_class: "ForLoopWorkflow",
        kwargs_json: r#"{"items":["alpha","beta","gamma"]}"#,
    },
    FixtureCase {
        id: "parallel",
        module_name: "parallel_workflow",
        workflow_class: "ParallelWorkflow",
        kwargs_json: r#"{"value":7}"#,
    },
    FixtureCase {
        id: "gather-listcomp",
        module_name: "integration_gather_listcomp",
        workflow_class: "GatherListCompWorkflow",
        kwargs_json: r#"{"items":[1,2,3]}"#,
    },
    FixtureCase {
        id: "tuple-unpack-fn-call",
        module_name: "integration_tuple_unpack_fn_call",
        workflow_class: "TupleUnpackFnCallWorkflow",
        kwargs_json: r#"{"user_id":"user_42"}"#,
    },
    FixtureCase {
        id: "nested-conditionals",
        module_name: "integration_nested_conditionals",
        workflow_class: "NestedConditionalsWorkflow",
        kwargs_json: r#"{"user_id":"user_c"}"#,
    },
    FixtureCase {
        id: "data-pipeline",
        module_name: "integration_data_pipeline",
        workflow_class: "DataPipelineWorkflow",
        kwargs_json: r#"{"source":"sales","threshold":100}"#,
    },
    FixtureCase {
        id: "string-processing",
        module_name: "integration_string_processing",
        workflow_class: "StringProcessingWorkflow",
        kwargs_json: r#"{"text":"Alpha123"}"#,
    },
    FixtureCase {
        id: "timeout",
        module_name: "integration_timeout_workflow",
        workflow_class: "TimeoutWorkflow",
        kwargs_json: r#"{}"#,
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
