-- 003_migrations_table.sql — track schema migrations applied so far.

CREATE TABLE pgrg.migrations (
    version int PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO pgrg.migrations (version) VALUES (1);
