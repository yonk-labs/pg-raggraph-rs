-- The ONLY privileged setup a managed-PG user can do. NO pg_raggraph.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
