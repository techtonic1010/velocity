# Progress Tracker

Update this file at the end of every milestone, before running `/compact` or ending the session. This is what tells a fresh Claude Code session (or a fresh you) exactly where things stand — don't rely on chat history to remember.

## Status

| Milestone | Status | Notes |
|---|---|---|
| 1. Entity Upload + Embedding pipeline | Done — verified | A/B/C/D all built and verified end-to-end with real MIND-small data (20-row ingest, not yet full ~51K-row run) |
| 2. Vector hashing (LSH) + shard assignment | Done — verified | vector-hasher built and verified end-to-end with real embeddings + real ingested Postgres data |
| 3. Neighbor Index write-side + Kafka resync | Done — verified | Built inside vector-hasher (no new service); real bootstrap + eviction + Kafka round-trip + cache invalidation all verified against real data |
| 4. Entity Interaction ingestion | Not started | |
| 5. Recommendation Service (serving path) | Not started | |

Status values: `Not started` / `In progress` / `Done — verified` / `Blocked`.

## Session log

Add one entry per session, most recent first.

```
Date: 2026-07-21
Milestone worked on: 3. Neighbor Index write-side + Kafka resync

What was completed:
- Built inside the existing vector-hasher service, per the milestone plan's
  resolved scope (no new service — docker-compose.yml has no second stub,
  and PROJECT_SPEC.md §9 already labels the directory "LSH + neighbor
  computation").
- lsh.py: added cosine_distance(v1, v2) and resolve_candidate_shard_ids(...)
  (the same-bucket-then-adjacent-fallback logic, pulled out of main.py's
  /candidates handler into a reusable, pure function).
- heap.py: new pure max-heap module — push_with_eviction (O(log k) insert/
  evict via negated distances in a min-heap) and flatten_sorted. Covered by
  6 unit tests in test_heap.py (up from an initial 4 — 2 more added after
  the duplicate-entry bug below).
- cache.py: new manual OrderedDict-based LRU (get/set/invalidate) — not
  functools.lru_cache, since single-key invalidation is required.
- database/init/002_neighbor_index_schema.sql: new neighbor_index_write /
  neighbor_index_read tables, applied to the live Postgres (container had
  already run once from M1/M2, so this was applied manually via psql rather
  than relying on docker-entrypoint-initdb.d's fresh-volume-only behavior).
- db.py: added fetch_candidates_with_vectors, fetch_vector, fetch_shard_id
  (already existed), fetch_neighbors_write, save_neighbors,
  save_neighbors_read, fetch_neighbors_read.
- kafka_client.py: new — create_producer/send_neighbor_update (synchronous,
  keyed by entityId), run_consumer (blocking loop, meant to run on a
  background thread).
- neighbor_index.py: new orchestrator — index_entity(...) builds the target
  entity's own list, then does the reverse eviction check against every
  candidate, saving + publishing only whatever actually changed.
- main.py: added POST /neighbors/{entityId}/index, POST /neighbors/index-all,
  GET /neighbors/{entityId}; started the Kafka consumer on a background
  thread from the existing lifespan handler; refactored /candidates to call
  the new resolve_candidate_shard_ids instead of its own inline copy of the
  same logic.
- requirements.txt: added kafka-python==2.0.2.
- docker-compose.yml: added NEIGHBOR_INDEX_K, KAFKA_NEIGHBOR_TOPIC,
  NEIGHBOR_CACHE_SIZE env vars to vector-hasher (additive only).

What was verified (and how):
- Unit tests: all 11 (5 lsh.py + 6 heap.py) passed locally, no DB/Docker
  needed.
- Real bootstrap: POST /neighbors/index-all against the real 20-row dataset
  — {"total":20,"changed":82} (after the bug fix below; was 100 before it,
  reflecting wasted duplicate-churn).
- Literal verify criterion, with real data: confirmed via direct SQL that
  shard 7's 4 real entities (N29120, N39237, N55528, N55610) each ended up
  with exactly 3 distinct neighbors — no duplicates, genuine eviction.
- Kafka round-trip: neighbor_index_write and neighbor_index_read compared
  directly via SQL for all 20 entities — zero mismatches.
- Cache correctness, end to end with one real new data point: added one
  more real article via a genuine embedding-creator call ("Kate Middleton
  Royal Style Through the Decades", inserted as N90001, real shard = 7 via
  /assign-shards). GET /neighbors/N55610 twice returned identical results
  (cache hit). POST /neighbors/N90001/index then genuinely evicted N29120
  (distance 0.922) in favor of N90001 (distance 0.336) from N55610's list —
  confirmed via the "changed" response and direct SQL. The very next GET
  immediately reflected the new neighbor, not the stale cached one —
  invalidation confirmed working, not just assumed. N90001 was left in
  place afterward (not cleaned up) per instruction.
- Real bug found and fixed during this verification (not anticipated by the
  original plan): push_with_eviction had no check for whether an entity_id
  was already present in the heap, so an entity touched by two separate
  index_entity() calls (once as itself, once as another entity's candidate)
  could get inserted twice, wasting a heap slot and evicting a genuinely
  different neighbor to make room for a duplicate. Fixed by checking heap
  membership by entity_id before inserting/evicting; covered by 2 new
  regression tests; re-verified live against a truncated-and-rebootstrapped
  neighbor_index_write/read (confirmed zero duplicates afterward).
- Hit an unrelated environment issue while starting Postgres: a native
  (non-Docker) Postgres service was already bound to host port 5432 from
  before this session. Resolved by stopping that native service (user's own
  action, since sudo needed an interactive password this environment
  couldn't provide) rather than changing docker-compose.yml's port mapping.

What's left / known issues:
- Milestones 4 and 5 (Entity Interaction ingestion, Recommendation Service)
  not started.
- The Kafka consumer thread has no supervision/graceful shutdown — logged
  as a Defensibility Tracker entry, not a bug.
- N90001 (the synthetic verification article) remains in the entities
  table and its neighbor-index rows — left in place per instruction, not
  cleaned up. Worth remembering this makes the dataset 21 rows, not 20,
  for anyone relying on that exact count later.

Any new entries added to PROJECT_SPEC.md §10 (Defensibility Tracker)? Yes — 4
entries (NEIGHBOR_INDEX_K=3 rationale, in-process-cache-not-Redis rationale,
Kafka consumer thread has no supervision, and the push_with_eviction
duplicate-entry bug found + fixed during verification).
```

```
Date: 2026-07-19
Milestone worked on: 2. Vector hashing (LSH) + shard assignment

What was completed:
- New vector-hasher service (Python/FastAPI): lsh.py (pure hashing math —
  generate_hyperplanes, compute_hash, adjacent_shard_ids), db.py (Postgres
  access — fetch_all_vectors, update_shard_ids, fetch_shard_id,
  fetch_candidates), main.py (GET /health, POST /hash, POST /assign-shards,
  GET /candidates/{entityId}), test_lsh.py (5 unit tests, all pure/no I/O),
  requirements.txt, Dockerfile, .dockerignore.
- Added LSH_SEED/LSH_NUM_HYPERPLANES/NUM_SHARDS/EMBEDDING_DIM env vars to
  vector-hasher's already-present docker-compose.yml stub (additive only).
- Cleaned up unrelated stray Docker state from a prior project (ContentPulse):
  removed its stopped kafka/redis/postgres containers and their 3 data
  volumes, which were colliding with this project's container names.

What was verified (and how):
- Unit tests: all 5 passed locally (no DB/Docker needed) — determinism, fixed-
  seed hyperplane reproducibility, near-duplicate vectors landing in the same
  bucket, opposite vectors producing fully complementary hash_bits, and
  adjacent_shard_ids wraparound. Results + descriptions written to
  vector-hasher/tests/README.md.
- Built and ran vector-hasher as a real container; GET /health returned 200.
- Real end-to-end LSH check reusing Milestone 1's exact 3 test texts (2
  similar sports headlines + 1 unrelated cooking headline) through real
  embedding-creator + vector-hasher calls. Honest result, not the clean outcome
  hoped for: the two similar articles differed in only 3 of 20 hash bits (85%
  agreement) but landed in different, non-adjacent shards (0 and 2), because
  one of those 3 differing bits happened to be one of the 3 low-order bits
  that decide shard_id (= hash_value % 8). Logged as a Defensibility Tracker
  entry with the concrete numbers rather than re-picking test text to hide it.
- Ran POST /assign-shards against the real 20 rows already in Postgres from
  Milestone 1: {"total":20,"updated":20}. Verified directly via SQL that
  shard_id is no longer uniformly 0 and is spread across shards 0-5 and 7
  (shard 6 empty, expected with only 20 sparse rows).
- Ran GET /candidates/N55528 (a real lifestyleroyals article) and confirmed
  N55610 (same subcategory, same shard) appears in the result. Also confirmed,
  honestly, that the same-bucket count (3) was below MIN_CANDIDATES=5, so the
  adjacent-bucket fallback fired and pulled in 5 more, less-related articles
  from shard 0 alongside it — expected given only 20 total rows exist, and a
  real demonstration of why bucket retrieval alone isn't a final ranking.

What's left / known issues:
- No cosine-similarity re-ranking within a candidate set yet — /candidates
  returns everyone in the searched bucket(s), unordered. That's explicitly
  Milestone 3's job (Neighbor Index), not this milestone's.
- The mod-8 shard compression (3 of 20 bits actually used) is a known,
  documented tradeoff, not a bug — see PROJECT_SPEC.md §10 for the concrete
  real-data example and reasoning.
- Full-scale /assign-shards (~51K rows, once Milestone 1's full ingest is
  eventually run) has not been exercised — only the 20-row dataset.

Any new entries added to PROJECT_SPEC.md §10 (Defensibility Tracker)? Yes — 1 entry
(mod-8 bucket compression, with the real sports_1/sports_2 bit-level example).
```

```
Date: 2026-07-19
Milestone worked on: 1. Entity Upload + Embedding pipeline (Deliverable D — closing it out)

What was completed:
- Sourced the actual MIND-small dataset (train + dev). The official msnews.github.io
  links now point to a Hugging Face mirror (huggingface.co/datasets/yjw1029/MIND);
  the original Microsoft blob-storage links are confirmed dead (409 - public access
  no longer permitted), matching PROJECT_SPEC.md's own warning that those expire.
- Verified news.tsv's real structure against MindNewsTsvParser's assumptions: all
  51,282 train rows and 42,416 dev rows have exactly 8 tab-separated columns (no
  short/truncated rows at all); 2,666 rows (~5%) have genuinely blank abstracts,
  which the parser already converts to null correctly. Risk #1 from the original
  plan is now resolved with real data, not just defensive code.
- Moved the dataset into data/mind-small/{train,dev}/ (was briefly loose at the
  repo root). Recreated .gitignore (deleted during the earlier clean-slate reset,
  never rebuilt since).
- Added the planned docker-compose.yml change for entity-upload-service:
  MIND_NEWS_TSV_PATH env var + a read-only volume mount of ./data/mind-small.
  Additive only — no existing lines touched.

What was verified (and how):
- Brought up postgres + embedding-creator + entity-upload-service together;
  confirmed the volume mount actually worked via `docker exec ... ls` showing
  the real news.tsv/behaviors.tsv inside the container.
- Ran a real POST /ingest?limit=20 against actual MIND-small data end-to-end:
  {"total":20,"succeeded":20,"failed":0,"failedIds":[]}.
- Verified directly in Postgres, not just the HTTP response: 20 rows present,
  every vector exactly 1536 bytes, titles/categories matching the real news.tsv
  content exactly (e.g. N19639 "50 Worst Habits For Belly Fat", N55528 "The
  Brands Queen Elizabeth...").
- Idempotency was not re-verified through the live system this session — relying
  on the manual SQL-level upsert proof from the previous session.

What's left / known issues:
- Full-scale ingest (~51,282 rows, no limit) has not been run — only 20 rows
  have gone through the real system, so actual throughput/timing and true
  multi-batch behavior (batch size 100) remain unobserved in practice.
- MINDlarge_train.zip was briefly downloaded by mistake before switching to
  MIND-small to keep this project's stated scope.

Any new entries added to PROJECT_SPEC.md §10 (Defensibility Tracker)? Yes — 1 entry
(verified correctness at small scale via ?limit=20, deferred the full 51K-row run).
```

```
Date: 2026-07-19
Milestone worked on: 1. Entity Upload + Embedding pipeline

What was completed:
- Deliverable A (Embedding Creator): app/main.py (FastAPI + sentence-transformers
  all-MiniLM-L6-v2, lifespan-loaded model), requirements.txt, tests/test_main.py,
  Dockerfile, .dockerignore.
- Deliverable B (Entity Upload Service): full Maven project — NewsArticle,
  MindNewsTsvParser, EmbedRequest/EmbedResponse, EmbeddingCreatorClient,
  EntityRepository, IngestionService, HealthController, IngestController,
  EntityUploadServiceApplication, pom.xml, application.yml, RestClientConfig,
  Dockerfile.
- Deliverable C: database/init/001_schema.sql — entities table, vector as BYTEA,
  shard_id default 0.
- Deliverable D: deliberately deferred — no MIND dataset downloaded yet, no
  docker-compose.yml volume mount added (on hold per instruction).

What was verified (and how):
- Embedding Creator: pytest (3 tests) passed locally; real cosine similarity
  check gave 0.679 for two similar sports headlines vs -0.04 for an unrelated
  cooking headline. Also built + ran as a live Docker container; hit real
  GET /health and POST /embed over the network (not just the in-process
  TestClient).
- Entity Upload Service: Docker build succeeded (11 source files, BUILD SUCCESS)
  after fixing a missing EntityUploadServiceApplication.java (main class was
  never recreated after an earlier full reset). Ran as a live container against
  a live Postgres container; GET /health returned 200. Confirmed genuine DB
  connectivity via Postgres's own pg_stat_activity — matched the app's HikariCP
  connection backend_start timestamp against its own application log's startup
  timestamp, rather than just trusting "no startup errors."
- DB schema: verified against a live Postgres — \d entities matches the design
  exactly; proved the INSERT ... ON CONFLICT (entity_id) DO UPDATE upsert
  EntityRepository uses actually works (re-inserting the same entity_id updated
  the row in place, count stayed at 1).

What's left / known issues:
- Deliverable D: download MIND-small, place under data/mind-small/, add the
  still-pending docker-compose.yml volume mount + MIND_NEWS_TSV_PATH env var
  (additive only).
- Run a real POST /ingest against the actual dataset — this is also the first
  time EmbeddingCreatorClient's real HTTP call from entity-upload-service to
  embedding-creator will be exercised end-to-end (both containers share a
  network already, but this path itself is untested).
- MindNewsTsvParser's column-order assumptions have never been checked against
  a real downloaded news.tsv file — still an open risk (see PROJECT_SPEC.md §0).

Any new entries added to PROJECT_SPEC.md §10 (Defensibility Tracker)? Yes — 4 entries
(vector as BYTEA, batch/concurrency + retry-skip strategy, /ingest as a blocking
endpoint, JdbcTemplate over JPA).
```
