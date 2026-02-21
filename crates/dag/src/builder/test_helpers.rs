use crate::{DAG, DAGConverter, convert_to_dag};
use waymark::waymark_core::ir_parser::parse_program;
use waymark_proto::ast as ir;

pub(super) fn dedent(source: &str) -> String {
    let lines: Vec<&str> = source.lines().collect();
    let mut min_indent = usize::MAX;
    for line in &lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let indent = line.chars().take_while(|ch| ch.is_whitespace()).count();
        min_indent = min_indent.min(indent);
    }
    if min_indent == usize::MAX {
        return String::new();
    }

    lines
        .into_iter()
        .map(|line| {
            if line.len() >= min_indent {
                line[min_indent..].to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn parse_program_source(source: &str) -> ir::Program {
    let source = dedent(source);
    parse_program(source.trim()).expect("parse program")
}

pub(super) fn build_dag_with_pointers(source: &str) -> DAG {
    let program = parse_program_source(source);
    let mut converter = DAGConverter::new();
    converter
        .convert_with_pointers(&program)
        .expect("convert program with pointers")
}

pub(super) fn build_dag(source: &str) -> DAG {
    let program = parse_program_source(source);
    convert_to_dag(&program).expect("convert program to dag")
}
