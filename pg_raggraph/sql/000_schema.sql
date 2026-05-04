-- 000_schema.sql — bootstrap assertions before pgrx-generated functions.
--
-- Note: the `pgrg` schema is created automatically by PostgreSQL because the
-- .control file sets `schema = 'pgrg'`. Manually CREATEing it here produces a
-- non-extension-member schema that PG18 rejects with "schema pgrg is not a
-- member of extension". So we don't create it — we just assert the
-- prerequisite extensions are loaded.

-- Required extensions (declared via .control `requires`, but harmless to assert):
DO $$
BEGIN
    PERFORM 1 FROM pg_extension WHERE extname = 'vector';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_raggraph requires the vector extension; CREATE EXTENSION vector first';
    END IF;
    PERFORM 1 FROM pg_extension WHERE extname = 'pg_trgm';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_raggraph requires the pg_trgm extension; CREATE EXTENSION pg_trgm first';
    END IF;
END;
$$;
