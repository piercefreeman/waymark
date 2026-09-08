CREATE INDEX observability_events_vm_timeline_idx
    ON observability_events (((payload->>'vm_id')::uuid), at, node_id, node_sequence)
    WHERE payload ? 'vm_id';
