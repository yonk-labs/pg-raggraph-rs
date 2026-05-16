-- Managed-PG client shim: pgrg.ask via pg_net -> sidecar POST /v1/ask.
--
-- SC-006. Identical call surface + return shape to the in-extension
-- pgrx `pgrg.ask` (pg_raggraph/src/ask.rs):
--
--     pgrg.ask(q text, filter jsonb DEFAULT NULL, top_k int DEFAULT 10,
--              namespace text DEFAULT 'default', hops int DEFAULT 1,
--              llm_provider text DEFAULT NULL)
--       RETURNS TABLE(answer text, citations jsonb, signals jsonb,
--                     mode_used text)
--
-- DC-004: the sidecar bootstrap (pg_raggraph_sidecar/sql/migrations/) only
-- installs the pgrg.* TABLES. The pgrx-generated callables
-- (pgrg.ingest*/pgrg.query/pgrg.ask) are ABSENT in sidecar mode because
-- pgrx cannot load on managed PG (no shared_preload_libraries). This file
-- restores the one entry point PL/pgSQL users actually call from SQL:
-- pgrg.ask. Retrieval/ingestion remain over the sidecar HTTP API.
--
-- Requires (one-time, by the managed-PG user / DBA):
--     CREATE EXTENSION IF NOT EXISTS pg_net;
--     SET pgrg.sidecar_url = 'http://host:8410';   -- or ALTER DATABASE ... SET
--
-- ── pg_net await call: validated against supabase/postgres:15.8.1.060,
-- which ships pg_net 0.14.0. On 0.14.0 `net.http_collect_response(...)`
-- is DEPRECATED (emits a NOTICE) — the non-deprecated lifecycle is:
--
--     v_req := net.http_post(...);          -- bigint request id (async)
--     PERFORM net._await_response(v_req);   -- blocks until worker writes
--     SELECT status_code, content, error_msg, timed_out
--       FROM net._http_response WHERE id = v_req;
--
-- This file uses that non-deprecated path, with a bounded poll fallback
-- so it is robust if a point release tweaks _await_response timing.
--
-- ⚠️ SOFT-SPOT (Plan 4 SC-014 precedent — honest, NOT faked green):
-- the end-to-end pg_net worker -> sidecar round-trip could NOT be observed
-- in this sandboxed CI/dev environment: the pg_net 0.14.0 background
-- worker's outbound libcurl does not complete here (neither to a
-- host-bound sidecar — Docker network isolation — nor to an in-container
-- mock; the worker dequeues then silently drops with no response row).
-- The SQL below is written against the pg_net API surface AS INTROSPECTED
-- on the target image (\df net.*, \d net._http_response) but the live
-- worker egress path MUST be validated on a real Supabase/managed-PG
-- instance with outbound network. SC-006 ships ⚠️ PARTIAL until then.

CREATE SCHEMA IF NOT EXISTS pgrg;

CREATE OR REPLACE FUNCTION pgrg.ask(
    q            text,
    filter       jsonb   DEFAULT NULL,
    top_k        int     DEFAULT 10,
    namespace    text    DEFAULT 'default',
    hops         int     DEFAULT 1,
    llm_provider text    DEFAULT NULL
) RETURNS TABLE(answer text, citations jsonb, signals jsonb, mode_used text)
LANGUAGE plpgsql AS $$
DECLARE
    v_url       text := current_setting('pgrg.sidecar_url', true);
    v_req       bigint;
    v_code      int;
    v_content   text;
    v_err       text;
    v_timedout  boolean;
    v_resp      jsonb;
    v_tries     int := 0;
BEGIN
    IF v_url IS NULL OR v_url = '' THEN
        RAISE EXCEPTION
            'pgrg.sidecar_url is not set (SET pgrg.sidecar_url = ''http://host:8410'')';
    END IF;

    -- Match the JSON contract the sidecar's POST /v1/ask expects (parity
    -- with pg_raggraph_sidecar/src/http.rs AskBody): q, filter, top_k,
    -- namespace, hops, llm_provider.
    -- net.http_post is async; capture the request id.
    SELECT net.http_post(
        url     := v_url || '/v1/ask',
        body    := jsonb_build_object(
            'q', q,
            'filter', filter,
            'top_k', top_k,
            'namespace', namespace,
            'hops', hops,
            'llm_provider', llm_provider
        ),
        headers := jsonb_build_object('Content-Type', 'application/json'),
        timeout_milliseconds := 120000
    ) INTO v_req;

    -- Non-deprecated pg_net 0.14.0 await: block until the worker writes
    -- the response row into net._http_response.
    PERFORM net._await_response(v_req);

    -- Bounded fallback poll (robust if _await_response returns before the
    -- row is durably visible on a given point release). ~30s ceiling.
    LOOP
        SELECT status_code, content, error_msg, timed_out
          INTO v_code, v_content, v_err, v_timedout
          FROM net._http_response
         WHERE id = v_req;

        EXIT WHEN FOUND;

        v_tries := v_tries + 1;
        IF v_tries > 300 THEN
            RAISE EXCEPTION
                'pgrg.ask: no pg_net response for request % (worker not '
                'delivering — check sidecar URL/egress)', v_req;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;

    IF v_timedout THEN
        RAISE EXCEPTION 'pgrg.ask: sidecar request timed out';
    END IF;

    IF v_err IS NOT NULL THEN
        RAISE EXCEPTION 'pgrg.ask: sidecar request failed: %', v_err;
    END IF;

    IF v_code IS NULL OR v_code < 200 OR v_code >= 300 THEN
        RAISE EXCEPTION 'pgrg.ask: sidecar returned HTTP % : %',
            v_code, COALESCE(v_content, '(no body)');
    END IF;

    v_resp := v_content::jsonb;

    answer    := v_resp->>'answer';
    citations := COALESCE(v_resp->'citations', '[]'::jsonb);
    signals   := COALESCE(v_resp->'signals', '{}'::jsonb);
    mode_used := COALESCE(v_resp->>'mode_used', 'hybrid');
    RETURN NEXT;
END;
$$;
