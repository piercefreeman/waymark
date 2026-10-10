ALTER TABLE observability_vm_instances ADD COLUMN workflow_name text;

-- Names travel with every event so a dropped start does not lose identity.
-- Once known, the name survives late batches and runtime snapshot cleanup.
CREATE FUNCTION observability_vm_instances_record_names() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE observability_vm_instances AS instance
    SET workflow_name = named.workflow_name
    FROM (
        SELECT DISTINCT ON ((payload->>'vm_id')::uuid)
            (payload->>'vm_id')::uuid AS vm_id,
            payload->>'workflow_name' AS workflow_name
        FROM inserted
        WHERE payload ? 'vm_id'
          AND starts_with(kind, 'vm_driver.')
          AND NULLIF(payload->>'workflow_name', '') IS NOT NULL
        ORDER BY (payload->>'vm_id')::uuid, at DESC, node_id DESC, node_sequence DESC
    ) AS named
    WHERE instance.vm_id = named.vm_id
      AND instance.workflow_name IS NULL;
    RETURN NULL;
END
$$;

-- PostgreSQL fires same-event triggers alphabetically: record names after
-- observability_events_absorb_into_vm_instances has created the rows.
CREATE TRIGGER observability_events_record_workflow_names
    AFTER INSERT ON observability_events
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT
    EXECUTE FUNCTION observability_vm_instances_record_names();
