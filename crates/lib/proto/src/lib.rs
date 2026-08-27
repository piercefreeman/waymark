//! Protocol buffer message types.

/// Maximum gRPC message payload size for Waymark services.
pub const GRPC_MAX_MESSAGE_SIZE_BYTES: usize = 25 * 1024 * 1024;

/// Re-export [`prost_wkt_types`] for easier consumption.
pub use prost_wkt_types;

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
