-- 005_status_check_atomicity.sql
-- Plan 1+2 review carry-forward: tighten ingest_jobs.status to a known
-- enumeration (Plan 3 bg worker will write statuses; aggregate must be
-- predictable), and harden _maybe_apply_parity_indexes against partial
-- DROP-then-failed-CREATE.

ALTER TABLE pgrg.ingest_jobs
    ADD CONSTRAINT ingest_jobs_status_check
    CHECK (status IN ('queued', 'running', 'completed', 'failed'));

CREATE OR REPLACE FUNCTION pgrg._maybe_apply_parity_indexes()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    parity bool := current_setting('pg_raggraph.parity_mode', true)::bool;
    has_chunks bool;
    has_entities bool;
BEGIN
    IF parity IS DISTINCT FROM true THEN
        RETURN;
    END IF;

    SELECT EXISTS(SELECT 1 FROM pgrg.chunks LIMIT 1) INTO has_chunks;
    SELECT EXISTS(SELECT 1 FROM pgrg.entities LIMIT 1) INTO has_entities;

    IF has_chunks OR has_entities THEN
        RETURN;
    END IF;

    BEGIN
        DROP INDEX IF EXISTS pgrg.chunks_embedding_hnsw;
        DROP INDEX IF EXISTS pgrg.entities_name_emb_hnsw;
        CREATE INDEX chunks_embedding_hnsw
            ON pgrg.chunks USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 10);
        CREATE INDEX entities_name_emb_hnsw
            ON pgrg.entities USING ivfflat (name_emb vector_cosine_ops)
            WITH (lists = 10);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'parity index swap failed mid-way; database may have inconsistent indexes: %', SQLERRM;
    END;
END;
$$;
