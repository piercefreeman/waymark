-- Add instance throughput columns to worker_status table
ALTER TABLE worker_status ADD COLUMN IF NOT EXISTS instances_per_sec DOUBLE PRECISION NOT NULL DEFAULT 0.0;
ALTER TABLE worker_status ADD COLUMN IF NOT EXISTS instances_per_min DOUBLE PRECISION NOT NULL DEFAULT 0.0;
