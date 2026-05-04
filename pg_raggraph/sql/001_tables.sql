-- 001_tables.sql — full schema per design-spec Section 5.
-- Vector dimension is read from the GUC pg_raggraph.embed_dim (default 384).
-- We use a SQL-level GUC lookup to template the dimension.

DO $$
DECLARE
    embed_dim int := current_setting('pg_raggraph.embed_dim', true)::int;
BEGIN
    IF embed_dim IS NULL THEN
        embed_dim := 384;
    END IF;

    EXECUTE format($f$
        CREATE TABLE pgrg.namespaces (
            name text PRIMARY KEY,
            embedding_model text NOT NULL DEFAULT 'bge-small-en-v1.5',
            llm_provider text,
            settings jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE pgrg.documents (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            namespace text NOT NULL REFERENCES pgrg.namespaces(name) ON DELETE CASCADE,
            source text NOT NULL,
            content_hash text NOT NULL UNIQUE,
            title text,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            ingested_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE pgrg.chunks (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            namespace text NOT NULL,
            document_id uuid NOT NULL REFERENCES pgrg.documents(id) ON DELETE CASCADE,
            ord int NOT NULL,
            text text NOT NULL,
            token_count int NOT NULL,
            embedding vector(%1$s),
            text_search tsvector GENERATED ALWAYS AS
                (to_tsvector('english', coalesce(text, ''))) STORED,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE(document_id, ord)
        );

        CREATE TABLE pgrg.entities (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            namespace text NOT NULL,
            name text NOT NULL,
            kind text,
            name_emb vector(%1$s),
            description text,
            properties jsonb NOT NULL DEFAULT '{}'::jsonb,
            degree int NOT NULL DEFAULT 0,
            UNIQUE(namespace, name, kind)
        );

        CREATE TABLE pgrg.relationships (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            namespace text NOT NULL,
            src_id uuid NOT NULL REFERENCES pgrg.entities(id) ON DELETE CASCADE,
            dst_id uuid NOT NULL REFERENCES pgrg.entities(id) ON DELETE CASCADE,
            kind text NOT NULL,
            description text,
            weight float NOT NULL DEFAULT 1.0,
            provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE(namespace, src_id, dst_id, kind)
        );

        CREATE TABLE pgrg.chunk_entities (
            chunk_id uuid NOT NULL REFERENCES pgrg.chunks(id) ON DELETE CASCADE,
            entity_id uuid NOT NULL REFERENCES pgrg.entities(id) ON DELETE CASCADE,
            confidence float NOT NULL DEFAULT 1.0,
            classification text NOT NULL DEFAULT 'extracted',
            PRIMARY KEY(chunk_id, entity_id)
        );

        CREATE TABLE pgrg.ingest_jobs (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            status text NOT NULL DEFAULT 'queued',
            source text NOT NULL,
            namespace text NOT NULL,
            chunk_strategy text NOT NULL DEFAULT 'auto',
            error text,
            attempt_count int NOT NULL DEFAULT 0,
            payload bytea,
            enqueued_at timestamptz NOT NULL DEFAULT now(),
            started_at timestamptz,
            finished_at timestamptz,
            updated_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE pgrg.providers (
            name text PRIMARY KEY,
            kind text NOT NULL CHECK (kind IN ('llm', 'embedding')),
            provider text NOT NULL,
            base_url text,
            model text,
            credential text,
            config jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        );
    $f$, embed_dim);
END;
$$;

-- Default namespace (referenced by ingest defaults).
INSERT INTO pgrg.namespaces (name) VALUES ('default') ON CONFLICT DO NOTHING;
