-- 000_schema.sql — bootstrap schema before pgrx-generated functions.
CREATE SCHEMA IF NOT EXISTS pgrg;

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
