-- Rename workload_pinnings to runnable_workloads.
--
-- The table states two orthogonal facts and the old name only covered
-- one of them: row presence means the workload is runnable (in the set
-- of things that should run), while the nullable node_id/expires_at
-- columns hold its current pinning to a node.  The workload vocabulary
-- also replaces the ambiguous "instance" in the key column.

ALTER TABLE workload_pinnings RENAME TO runnable_workloads;
ALTER TABLE runnable_workloads RENAME COLUMN instance_id TO workload_id;
ALTER TABLE runnable_workloads RENAME CONSTRAINT workload_pinnings_node_expiry_paired
    TO runnable_workloads_node_expiry_paired;
ALTER TABLE runnable_workloads RENAME CONSTRAINT workload_pinnings_instance_id_fkey
    TO runnable_workloads_workload_id_fkey;
ALTER INDEX workload_pinnings_pkey RENAME TO runnable_workloads_pkey;
