//! Protocol buffer message types.

/// Re-export [`prost_types`] for easier consumption.
pub use prost_types;

/// Re-export generated protobuf types
pub mod messages {
    // Messages for worker bridge communication
    tonic::include_proto!("waymark.messages");
}

/// AST types from ast.proto for IR representation
pub mod ast {
    // IR AST types
    tonic::include_proto!("waymark.ast");
}

/// Execution graph types from execution.proto
pub mod execution {
    // Execution state types
    tonic::include_proto!("waymark.execution");
}
