//! Benchmark for get_state() / to_bytes_fully_stripped() performance.
//!
//! This measures the bottleneck in finalize_completed_instances when there are
//! many active instances.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use prost::Message;
use rappel::messages::execution::{ExecutionGraph, ExecutionNode, NodeKind, NodeStatus};
use uuid::Uuid;

const EXECUTION_GRAPH_ZSTD_LEVEL: i32 = 3;

/// Create a synthetic ExecutionGraph with the specified number of nodes
fn create_synthetic_execution_graph(num_nodes: usize) -> ExecutionGraph {
    let mut nodes = HashMap::new();

    // Input node
    nodes.insert(
        "input".to_string(),
        ExecutionNode {
            template_id: "input".to_string(),
            spread_index: None,
            status: NodeStatus::Completed.into(),
            worker_id: None,
            started_at_ms: Some(1000),
            execution_id: Some(Uuid::new_v4().to_string()),
            completed_at_ms: Some(1100),
            duration_ms: Some(100),
            inputs: Some(vec![0u8; 256]), // Realistic input size
            result: Some(vec![0u8; 512]), // Realistic result size
            error: None,
            error_type: None,
            waiting_for: vec![],
            completed_count: 0,
            attempt_number: 1,
            max_retries: 3,
            attempts: vec![],
            timeout_seconds: 30,
            timeout_retry_limit: 3,
            backoff: None,
            loop_index: None,
            loop_accumulators: None,
            targets: vec![],
            node_kind: NodeKind::Input.into(),
            parent_execution_id: None,
            iteration_index: None,
        },
    );

    // Action nodes
    for i in 0..num_nodes {
        let node_id = format!("action_{}", i);
        let status = if i % 3 == 0 {
            NodeStatus::Completed
        } else if i % 3 == 1 {
            NodeStatus::Running
        } else {
            NodeStatus::Pending
        };

        nodes.insert(
            node_id.clone(),
            ExecutionNode {
                template_id: node_id.clone(),
                spread_index: None,
                status: status.into(),
                worker_id: if status == NodeStatus::Running {
                    Some(format!("worker_{}", i % 4))
                } else {
                    None
                },
                started_at_ms: if status != NodeStatus::Pending {
                    Some(1000 + i as i64 * 100)
                } else {
                    None
                },
                execution_id: Some(Uuid::new_v4().to_string()),
                completed_at_ms: if status == NodeStatus::Completed {
                    Some(1100 + i as i64 * 100)
                } else {
                    None
                },
                duration_ms: if status == NodeStatus::Completed {
                    Some(100)
                } else {
                    None
                },
                inputs: Some(vec![0u8; 256]),
                result: if status == NodeStatus::Completed {
                    Some(vec![0u8; 512])
                } else {
                    None
                },
                error: None,
                error_type: None,
                waiting_for: vec![],
                completed_count: 0,
                attempt_number: 1,
                max_retries: 3,
                attempts: vec![],
                timeout_seconds: 30,
                timeout_retry_limit: 3,
                backoff: None,
                loop_index: None,
                loop_accumulators: None,
                targets: vec![format!("result_{}", i)],
                node_kind: NodeKind::Action.into(),
                parent_execution_id: None,
                iteration_index: None,
            },
        );
    }

    // Output node
    nodes.insert(
        "output".to_string(),
        ExecutionNode {
            template_id: "output".to_string(),
            spread_index: None,
            status: NodeStatus::Blocked.into(),
            worker_id: None,
            started_at_ms: None,
            execution_id: None,
            completed_at_ms: None,
            duration_ms: None,
            inputs: None,
            result: None,
            error: None,
            error_type: None,
            waiting_for: (0..num_nodes).map(|i| format!("action_{}", i)).collect(),
            completed_count: 0,
            attempt_number: 0,
            max_retries: 0,
            attempts: vec![],
            timeout_seconds: 0,
            timeout_retry_limit: 0,
            backoff: None,
            loop_index: None,
            loop_accumulators: None,
            targets: vec![],
            node_kind: NodeKind::Output.into(),
            parent_execution_id: None,
            iteration_index: None,
        },
    );

    // Add some variables
    let mut variables = HashMap::new();
    for i in 0..num_nodes {
        variables.insert(format!("result_{}", i), vec![0u8; 128]);
    }

    ExecutionGraph {
        nodes,
        variables,
        ready_queue: (0..num_nodes / 3)
            .map(|i| format!("action_{}", i * 3 + 2))
            .collect(),
        exceptions: HashMap::new(),
        next_wakeup_time: None,
    }
}

/// Clone and strip the graph (simulating to_bytes_fully_stripped)
fn clone_and_strip(graph: &ExecutionGraph) -> Vec<u8> {
    let mut stripped = graph.clone();

    stripped.variables.clear();
    stripped.exceptions.clear();

    for node in stripped.nodes.values_mut() {
        node.inputs = None;
        node.result = None;
        node.loop_accumulators = None;
    }

    stripped.encode_to_vec()
}

fn benchmark_clone_and_strip(num_nodes: usize, iterations: usize) -> (Duration, usize) {
    let graph = create_synthetic_execution_graph(num_nodes);

    let start = Instant::now();
    let mut total_bytes = 0;
    for _ in 0..iterations {
        let bytes = clone_and_strip(&graph);
        total_bytes += bytes.len();
    }
    (start.elapsed(), total_bytes / iterations)
}

fn benchmark_just_clone(num_nodes: usize, iterations: usize) -> Duration {
    let graph = create_synthetic_execution_graph(num_nodes);

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = graph.clone();
    }
    start.elapsed()
}

fn benchmark_just_encode(num_nodes: usize, iterations: usize) -> Duration {
    let graph = create_synthetic_execution_graph(num_nodes);

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = graph.encode_to_vec();
    }
    start.elapsed()
}

fn benchmark_with_compression(num_nodes: usize, iterations: usize) -> (Duration, usize, usize) {
    let graph = create_synthetic_execution_graph(num_nodes);

    let start = Instant::now();
    let mut total_raw = 0;
    let mut total_compressed = 0;
    for _ in 0..iterations {
        // Clone, strip, encode (same as clone_and_strip)
        let mut stripped = graph.clone();
        stripped.variables.clear();
        stripped.exceptions.clear();
        for node in stripped.nodes.values_mut() {
            node.inputs = None;
            node.result = None;
            node.loop_accumulators = None;
        }
        let raw = stripped.encode_to_vec();
        total_raw += raw.len();

        // Now compress with zstd level 3 (same as production)
        let compressed = zstd::bulk::compress(&raw, EXECUTION_GRAPH_ZSTD_LEVEL).unwrap();
        total_compressed += compressed.len();
    }
    (
        start.elapsed(),
        total_raw / iterations,
        total_compressed / iterations,
    )
}

fn main() {
    println!("=== Finalize Performance Benchmark ===\n");
    println!("This simulates the get_state() bottleneck in finalize_completed_instances.\n");

    // Test different node counts
    let node_counts = [10, 50, 100, 200, 500, 1000];
    let iterations = 100;

    println!("Benchmark 1: Clone graph only");
    println!(
        "{:>8} {:>12} {:>12} {:>16}",
        "Nodes", "Total (ms)", "Per call", "x2400 instances"
    );
    println!("{:-<56}", "");

    for &num_nodes in &node_counts {
        let elapsed = benchmark_just_clone(num_nodes, iterations);
        let per_call = elapsed / iterations as u32;
        let projected_2400 = per_call * 2400;

        println!(
            "{:>8} {:>12.2} {:>12.3}ms {:>12.2}s",
            num_nodes,
            elapsed.as_millis(),
            per_call.as_secs_f64() * 1000.0,
            projected_2400.as_secs_f64()
        );
    }

    println!("\n\nBenchmark 2: Encode to bytes only (no clone)");
    println!(
        "{:>8} {:>12} {:>12} {:>16}",
        "Nodes", "Total (ms)", "Per call", "x2400 instances"
    );
    println!("{:-<56}", "");

    for &num_nodes in &node_counts {
        let elapsed = benchmark_just_encode(num_nodes, iterations);
        let per_call = elapsed / iterations as u32;
        let projected_2400 = per_call * 2400;

        println!(
            "{:>8} {:>12.2} {:>12.3}ms {:>12.2}s",
            num_nodes,
            elapsed.as_millis(),
            per_call.as_secs_f64() * 1000.0,
            projected_2400.as_secs_f64()
        );
    }

    println!("\n\nBenchmark 3: Clone + strip + encode (full to_bytes_fully_stripped simulation)");
    println!(
        "{:>8} {:>12} {:>12} {:>16} {:>12}",
        "Nodes", "Total (ms)", "Per call", "x2400 instances", "Bytes/graph"
    );
    println!("{:-<72}", "");

    for &num_nodes in &node_counts {
        let (elapsed, bytes) = benchmark_clone_and_strip(num_nodes, iterations);
        let per_call = elapsed / iterations as u32;
        let projected_2400 = per_call * 2400;

        println!(
            "{:>8} {:>12.2} {:>12.3}ms {:>12.2}s {:>12}",
            num_nodes,
            elapsed.as_millis(),
            per_call.as_secs_f64() * 1000.0,
            projected_2400.as_secs_f64(),
            bytes
        );
    }

    println!("\n\nBenchmark 4: Clone + strip + encode + ZSTD compress (actual production path)");
    println!(
        "{:>8} {:>12} {:>12} {:>16} {:>12} {:>12}",
        "Nodes", "Total (ms)", "Per call", "x2400 instances", "Raw bytes", "Compressed"
    );
    println!("{:-<88}", "");

    for &num_nodes in &node_counts {
        let (elapsed, raw_bytes, compressed_bytes) =
            benchmark_with_compression(num_nodes, iterations);
        let per_call = elapsed / iterations as u32;
        let projected_2400 = per_call * 2400;

        println!(
            "{:>8} {:>12.2} {:>12.3}ms {:>12.2}s {:>12} {:>12}",
            num_nodes,
            elapsed.as_millis(),
            per_call.as_secs_f64() * 1000.0,
            projected_2400.as_secs_f64(),
            raw_bytes,
            compressed_bytes
        );
    }

    // Benchmark 5: Simulate actual loop with many instances
    println!("\n\nBenchmark 5: Simulate finalize loop with N instances (100 nodes each)");
    println!(
        "{:>12} {:>12} {:>12}",
        "Instances", "Total (ms)", "Per instance"
    );
    println!("{:-<40}", "");

    for &instance_count in &[100, 500, 1000, 2000, 2400, 3000] {
        // Create all graphs upfront
        let graphs: Vec<_> = (0..instance_count)
            .map(|_| create_synthetic_execution_graph(100))
            .collect();

        let start = Instant::now();
        for graph in &graphs {
            // Simulate what happens in finalize loop for each instance
            let mut stripped = graph.clone();
            stripped.variables.clear();
            stripped.exceptions.clear();
            for node in stripped.nodes.values_mut() {
                node.inputs = None;
                node.result = None;
                node.loop_accumulators = None;
            }
            let raw = stripped.encode_to_vec();
            let _ = zstd::bulk::compress(&raw, EXECUTION_GRAPH_ZSTD_LEVEL).unwrap();
        }
        let elapsed = start.elapsed();
        let per_instance = elapsed / instance_count as u32;

        println!(
            "{:>12} {:>12.2} {:>12.3}ms",
            instance_count,
            elapsed.as_millis(),
            per_instance.as_secs_f64() * 1000.0
        );
    }

    // Benchmark 6: Test with larger graph sizes to find what matches 20-25s
    println!("\n\nBenchmark 6: Finding graph size that matches observed 20-25s for 2400 instances");
    println!(
        "{:>12} {:>15} {:>15}",
        "Nodes/graph", "Time for 2400", "Match?"
    );
    println!("{:-<48}", "");

    for &nodes in &[500, 1000, 2000, 3000, 5000, 8000, 10000] {
        // Create 2400 graphs with this many nodes
        let graphs: Vec<_> = (0..2400)
            .map(|_| create_synthetic_execution_graph(nodes))
            .collect();

        let start = Instant::now();
        for graph in &graphs {
            let mut stripped = graph.clone();
            stripped.variables.clear();
            stripped.exceptions.clear();
            for node in stripped.nodes.values_mut() {
                node.inputs = None;
                node.result = None;
                node.loop_accumulators = None;
            }
            let raw = stripped.encode_to_vec();
            let _ = zstd::bulk::compress(&raw, EXECUTION_GRAPH_ZSTD_LEVEL).unwrap();
        }
        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        let matches = if (18.0..=28.0).contains(&secs) {
            "<-- MATCH"
        } else {
            ""
        };

        println!("{:>12} {:>12.2}s {:>15}", nodes, secs, matches);
    }

    println!("\n\n=== Analysis ===");
    println!("Your logs show fin_apply_ms of ~20-25 seconds with ~2400 active instances.");
    println!("This benchmark shows the per-instance serialization cost scales with node count.");
    println!("\nWith compression (actual production path), for 100 nodes x 2400 instances:");
    let (elapsed_100, _, _) = benchmark_with_compression(100, iterations);
    let per_call_100 = elapsed_100 / iterations as u32;
    let projected = per_call_100 * 2400;
    println!("  Projected: {:.2}s", projected.as_secs_f64());
    println!("  Your observed: ~20-25s");
    println!("\nPossible explanations for the 100x gap:");
    println!("  1. Actual workflow graphs are MUCH larger (1000+ nodes each)");
    println!("  2. The execution_metadata() loop adds significant overhead");
    println!("  3. Lock contention on active_instances RwLock");
    println!("  4. Async runtime context switching");
    println!("  5. Memory pressure / allocation overhead under load");
}
