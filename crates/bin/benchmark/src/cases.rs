//! The benchmark workload cases, built from the smoke IR sources.

use std::collections::HashMap;

use color_eyre::eyre::{WrapErr as _, eyre};
use prost::Message;
use sha2::{Digest, Sha256};
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

/// The VM value for an integer input.
fn int(value: i64) -> waymark_system_vm::Value {
    waymark_system_vm::Value::Ready(waymark_system_vm::ReadyValue::Int(value))
}

/// The VM value for a list of integer inputs.
fn int_list(values: impl IntoIterator<Item = i64>) -> waymark_system_vm::Value {
    waymark_system_vm::Value::Ready(waymark_system_vm::ReadyValue::List(
        values.into_iter().map(int).collect(),
    ))
}

pub fn build_cases(base: i64) -> Result<HashMap<String, BenchmarkCase>, color_eyre::eyre::Report> {
    let mut cases = HashMap::new();
    let entries: Vec<(&str, ir::Program, HashMap<String, waymark_system_vm::Value>)> = vec![
        (
            "smoke",
            build_program(),
            HashMap::from([("base".to_string(), int(base))]),
        ),
        (
            "control_flow",
            build_control_flow_program()
                .map_err(|err| eyre!(err))
                .wrap_err("build control_flow program")?,
            HashMap::from([("base".to_string(), int(2))]),
        ),
        (
            "parallel_spread",
            build_parallel_spread_program()
                .map_err(|err| eyre!(err))
                .wrap_err("build parallel_spread program")?,
            HashMap::from([("base".to_string(), int(3))]),
        ),
        (
            "try_except",
            build_try_except_program()
                .map_err(|err| eyre!(err))
                .wrap_err("build try_except program")?,
            HashMap::from([("values".to_string(), int_list([1, 2, 3]))]),
        ),
        (
            "while_loop",
            build_while_loop_program()
                .map_err(|err| eyre!(err))
                .wrap_err("build while_loop program")?,
            HashMap::from([("limit".to_string(), int(6))]),
        ),
    ];

    for (name, program, inputs) in entries {
        let program_proto = program.encode_to_vec();
        let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
        let program = waymark_vm_ast_old_proto::convert(program)
            .wrap_err_with(|| format!("convert IR to VM AST for case '{name}'"))?;
        cases.insert(
            name.to_string(),
            BenchmarkCase {
                program,
                inputs,
                ir_hash,
            },
        );
    }
    Ok(cases)
}
