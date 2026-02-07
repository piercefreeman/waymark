//! IR -> DAG conversion entrypoints.

mod assignments;
mod conditionals;
mod converter;
mod data_flow;
mod exceptions;
mod expansion;
mod loops;
mod spreads;
mod utils;

pub use converter::{DAGConverter, convert_to_dag};

#[cfg(test)]
mod test_helpers;
