# Testing

How to run the pg_raggraph test suites.

## Unit + integration (CI default)

```
cargo test -p pg_raggraph_core            # _core, no DB
RUST_TEST_THREADS=1 cargo pgrx test pg18 --package pg_raggraph   # pgrx, real PG18
```

The suite uses HTTP cassettes (`mockito`) and `MockProvider` so CI runs with NO live LLM credentials.

## Live-LLM smoke test (manual, opt-in)

The automated suite never makes real LLM calls. To exercise a real provider
end-to-end against `pgrg.ask`:

1. Configure a real provider (example: OpenAI):

```sql
SELECT pgrg.provider_create(
  'openai-live', 'llm', 'openai',
  NULL, 'gpt-4o-mini', 'YOUR_OPENAI_API_KEY_HERE', '{}'
);
```

   If `pg_raggraph.master_key_path` is set, the credential is stored
   AES-256-GCM encrypted (`enc:v1:...`) and decrypted only at call time.

2. Ingest a few documents into a namespace whose `llm_provider` is `openai-live`
   (or pass `llm_provider := 'openai-live'` explicitly to `pgrg.ask`).

3. Ask:

```sql
SELECT * FROM pgrg.ask(
  'what changed in the auth module?',
  NULL, 10, 'default', 1, 'openai-live'
);
```

   Expect a non-empty `answer`, a `citations` JSONB array whose `chunk_id`s
   all exist in `pgrg.chunks`, and a `signals.llm` block with
   `provider` / `model` / `latency_ms` / `prompt_tokens` / `completion_tokens`.

This path is intentionally NOT automated — it requires real credentials and
makes billable network calls. Run it by hand before a release if you want
real-provider confidence beyond the cassette tests.

### SC-014 note

Entity-resolution (`pgrg.entities` dedup across documents) is validated in CI
for the decision logic and the real-`pg_trgm` half. The cosine-with-real-
semantic-embeddings half requires the ONNX embedder (deferred carry-forward);
until then, the punctuation-variant merge ("Acme Corp" / "Acme Corp.") is best
verified manually with a real embedding model configured.
