-- Migration: Add execution graph and instance ownership
--
-- This migration adds support for the new instance-local execution model:
-- - execution_graph: Protobuf-encoded execution state (replaces action_queue, node_inputs, etc.)
-- - owner_id: Which runner currently owns this instance
-- - lease_expires_at: When the ownership lease expires (for failover)

-- Add new columns to workflow_instances
ALTER TABLE workflow_instances
    ADD COLUMN execution_graph BYTEA,
    ADD COLUMN owner_id TEXT,
    ADD COLUMN lease_expires_at TIMESTAMPTZ;

-- Index for finding orphaned instances (lease expired)
CREATE INDEX idx_workflow_instances_orphaned
    ON workflow_instances (lease_expires_at)
    WHERE status = 'running' AND lease_expires_at IS NOT NULL;

-- Index for finding instances owned by a specific runner
CREATE INDEX idx_workflow_instances_owner
    ON workflow_instances (owner_id)
    WHERE status = 'running' AND owner_id IS NOT NULL;

-- Index for finding unowned running instances (available for claiming)
CREATE INDEX idx_workflow_instances_unowned
    ON workflow_instances (created_at)
    WHERE status = 'running' AND owner_id IS NULL;
