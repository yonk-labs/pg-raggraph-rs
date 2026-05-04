-- 002_indexes.sql — indexes per design-spec Section 5.

CREATE INDEX chunks_ns_doc_idx        ON pgrg.chunks (namespace, document_id);
CREATE INDEX chunks_text_search_idx   ON pgrg.chunks USING gin(text_search);
CREATE INDEX chunks_metadata_idx      ON pgrg.chunks USING gin(metadata jsonb_path_ops);
CREATE INDEX chunks_embedding_hnsw    ON pgrg.chunks USING hnsw(embedding vector_cosine_ops);

CREATE INDEX entities_ns_name_idx     ON pgrg.entities (namespace, name);
CREATE INDEX entities_name_trgm_idx   ON pgrg.entities USING gin(name gin_trgm_ops);
CREATE INDEX entities_name_emb_hnsw   ON pgrg.entities USING hnsw(name_emb vector_cosine_ops);

CREATE INDEX relationships_src_idx    ON pgrg.relationships (src_id);
CREATE INDEX relationships_dst_idx    ON pgrg.relationships (dst_id);
CREATE INDEX relationships_ns_kind    ON pgrg.relationships (namespace, kind);

CREATE INDEX chunk_entities_eid_idx   ON pgrg.chunk_entities (entity_id);

CREATE INDEX ingest_jobs_active_idx
    ON pgrg.ingest_jobs (status, enqueued_at)
    WHERE status IN ('queued', 'running');
