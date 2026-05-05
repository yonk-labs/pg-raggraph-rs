-- 004_retrieval_indexes.sql — IVFFlat alternates wired through pgrg.parity_mode.
-- Per spec §10: parity benchmarks must use IVFFlat (deterministic) instead
-- of HNSW (build-time randomness). The swap is gated by pgrg.parity_mode at
-- the moment pgrg.namespace_create runs in a fresh DB.
--
-- DC-004 contract: parity_mode is read once at namespace_create. Existing
-- namespaces are never re-indexed by toggling the GUC.

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
        -- Data already present; do not disturb existing indexes.
        RETURN;
    END IF;

    DROP INDEX IF EXISTS pgrg.chunks_embedding_hnsw;
    DROP INDEX IF EXISTS pgrg.entities_name_emb_hnsw;

    -- IVFFlat with conservative lists count for empty tables.
    CREATE INDEX chunks_embedding_hnsw
        ON pgrg.chunks USING ivfflat (embedding vector_cosine_ops)
        WITH (lists = 10);

    CREATE INDEX entities_name_emb_hnsw
        ON pgrg.entities USING ivfflat (name_emb vector_cosine_ops)
        WITH (lists = 10);
END;
$$;
