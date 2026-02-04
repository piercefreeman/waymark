//! Example IR programs for exercising the compiler and runtime.

mod examples;
mod helpers;

pub use examples::{
    build_control_flow_program, build_parallel_spread_program, build_try_except_program,
    build_while_loop_program, get_example, list_examples,
};
pub use helpers::*;
