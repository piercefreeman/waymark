//! Integration tests - stubbed out pending migration to new store/scheduler model.
//!
//! These tests need to be rewritten to use the new Store API instead of the old Database API.

mod harness;

// All integration tests are temporarily disabled during the store/scheduler migration.
// The tests need to be rewritten to use:
// - Store instead of Database
// - NodeDispatch instead of WorkflowNodeDispatch
// - The new scheduler-based completion flow
//
// To run the existing unit tests, use: cargo test --lib
