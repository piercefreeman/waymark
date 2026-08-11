//! The benchmark workload cases, built from the smoke IR sources.

use std::collections::HashMap;

use prost::Message;
use sha2::{Digest, Sha256};
use waymark_convert_core::Convert;
use waymark_proto::ast as ir;
use waymark_smoke_sources::{
    build_control_flow_program, build_parallel_spread_program, build_program,
    build_try_except_program, build_while_loop_program,
};

pub struct BenchmarkCase {
    pub program: waymark_vm_ast_old::Program,
    pub inputs: HashMap<String, waymark_system_vm::Value>,
    pub ir_hash: String,
}

pub fn build_cases(base: i64) -> HashMap<String, BenchmarkCase> {
    let mut cases = HashMap::new();
    let entries: Vec<(&str, ir::Program, HashMap<String, serde_json::Value>)> = vec![
        (
            "smoke",
            build_program(),
            HashMap::from([("base".to_string(), serde_json::Value::Number(base.into()))]),
        ),
        (
            "control_flow",
            build_control_flow_program().expect("control_flow program"),
            HashMap::from([("base".to_string(), serde_json::Value::Number(2.into()))]),
        ),
        (
            "parallel_spread",
            build_parallel_spread_program().expect("parallel_spread program"),
            HashMap::from([("base".to_string(), serde_json::Value::Number(3.into()))]),
        ),
        (
            "try_except",
            build_try_except_program().expect("try_except program"),
            // No value may equal 2: the program divides by `item - 2`, and a
            // division-by-zero fault is not yet catchable by `except` — it
            // kills the VM, and the released workload crash-loops forever
            // (see "Missing feature: catchable runtime exceptions" in
            // notes/postponed.md).
            HashMap::from([(
                "values".to_string(),
                serde_json::Value::Array(vec![1.into(), 3.into(), 4.into()]),
            )]),
        ),
        (
            "while_loop",
            build_while_loop_program().expect("while_loop program"),
            HashMap::from([("limit".to_string(), serde_json::Value::Number(6.into()))]),
        ),
    ];

    for (name, program, inputs) in entries {
        let program_proto = program.encode_to_vec();
        let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
        let program = waymark_vm_ast_old_proto::convert(program).expect("convert IR to VM AST");
        let inputs = inputs
            .into_iter()
            .map(|(name, value)| {
                let value: waymark_system_vm::Value =
                    waymark_vm_value_convert_json::Converter::convert(value);
                (name, value)
            })
            .collect();
        cases.insert(
            name.to_string(),
            BenchmarkCase {
                program,
                inputs,
                ir_hash,
            },
        );
    }
    cases
}
