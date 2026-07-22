# Milestone 3 — Neighbor Index Write-Side + Kafka Resync: Plan

## Context

Milestones 1 and 2 are done and verified: real MIND-small articles are embedded, stored, and now carry real `shard_id` LSH bucket assignments. `vector-hasher`'s `/candidates/{entityId}` can already narrow "who's near this entity" down to a small candidate set — but it does no ranking and computes no real distance; per `PROJECT_SPEC.md`'s own architecture diagram, that's deliberately deferred to this milestone: *"Real cosine similarity is then computed only within that small candidate set."*

Milestone 3's job, per `PROJECT_SPEC.md` §7: maintain a real per-entity top-K nearest-neighbor list (max-heap, so eviction is O(log K) and "who's currently farthest" is O(1) to check), propagate every change through Kafka, and keep an entity-hash-sharded **read-side** copy plus a small cache in sync. The stated verify criterion is explicit and bidirectional: *"insert a new article similar to an existing one; confirm the **existing** entity's neighbor list updates (eviction happens correctly), and the read-side copy reflects it after Kafka consumption."* That means indexing a new entity must also reach back and update already-indexed entities' own heaps when the new entity is now one of their top-K — not just build the new entity's own list.

**Resolved before this plan:** this lives inside the existing `vector-hasher` service, not a new one. Two independent signals in the existing docs point the same direction: `docker-compose.yml`'s service list has no second stub for a "neighbor-index" service (only `vector-hasher`'s), and `PROJECT_SPEC.md` §9 explicitly labels the directory `# Python — LSH + neighbor computation` — bundling both jobs in the spec's own words. The architecture diagram's separate "Neighbor Index (write)" box is read as a logical separation of responsibility for explanation purposes, not a literal claim of a separate deployable service — reinforced by the fact the write-side's own shard key (`simhash_bucket % N`) *is* `vector-hasher`'s own `shard_id` output, not an independently-computed value.

---

## 1. File tree (additions to the existing `vector-hasher/` service)

```
vector-hasher/
├── app/
│   ├── lsh.py            # existing (M2) — ADD: cosine_distance(v1, v2) -> float
│   ├── heap.py            # NEW — pure max-heap logic, no I/O (mirrors lsh.py's separation)
│   ├── cache.py            # NEW — small manually-invalidatable LRU cache, no I/O
│   ├── db.py              # existing (M2) — ADD: read/write functions for the two new tables
│   ├── kafka_client.py     # NEW — thin producer.send() + consumer-loop wrappers
│   ├── neighbor_index.py   # NEW — orchestrator: candidates -> distances -> heap updates -> persist -> produce
│   └── main.py             # existing (M2) — ADD: 3 new endpoints + background Kafka consumer in lifespan
├── tests/
│   ├── test_lsh.py         # existing (M2)
│   └── test_heap.py        # NEW — pure heap eviction/ordering tests, no I/O
database/init/
└── 002_neighbor_index_schema.sql   # NEW — two tables, auto-run alongside 001_schema.sql
```

Same separation-of-concerns habit as every prior file: pure logic (`heap.py`, `cache.py`, `lsh.py`) stays testable without I/O; `db.py`/`kafka_client.py` isolate the two external systems; `neighbor_index.py` is the only file that touches both, mirroring how `IngestionService.java` was the only class touching both `EmbeddingCreatorClient` and `EntityRepository`.

---

## 2. New Postgres tables (`002_neighbor_index_schema.sql`)

```sql
CREATE TABLE neighbor_index_write (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    shard_id    INTEGER      NOT NULL,
    neighbors   JSONB        NOT NULL,   -- [{"entityId": "...", "distance": 0.42}, ...] sorted ascending
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE neighbor_index_read (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    neighbors   JSONB        NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

Two tables, not one, deliberately — mirrors the spec's own write/read split (`shard: vector-hash` vs `shard: entity-hash`). In this single-Postgres-instance demo they're both just columns/tables, not physically separate stores (same logical-sharding scope decision as everywhere else in this project) — but keeping them as two tables still correctly models "the write-side heap-maintenance path never gets read by the serving path directly; only Kafka-consumed data does," which is the actual architectural point being demonstrated.

---

## 3. The algorithm (`heap.py` + `neighbor_index.py`)

`heap.py` — pure, no I/O:
- `push_with_eviction(heap, entry, k)` — if the heap has fewer than `k` entries, insert unconditionally. Otherwise peek the root (farthest); if the new entry is closer, pop-and-push (evict); otherwise leave the heap untouched. Returns whether a change happened, since that's exactly what decides whether to persist/produce to Kafka.
- `flatten_sorted(heap)` — turns the heap into a plain list sorted ascending by distance (closest first), matching `PROJECT_SPEC.md`'s `neighbors: [{id, distance}, ...]` shape and the "flat sorted array" read-side structure from §6.
- Python's built-in `heapq` is a **min-heap**; to get max-heap-by-distance behavior (farthest at the root, so eviction is a cheap peek), distances get negated on the way in and back out — a well-known, standard trick, not a deviation from the design.

`neighbor_index.py` — orchestrates indexing one entity, and this is the piece that satisfies the bidirectional verify requirement:
1. Look up the entity's `shard_id` (already computed by M2) and its vector.
2. Fetch its candidate set (reuses `db.fetch_candidates`, same same-bucket-then-adjacent-fallback logic from M2 — no new candidate-retrieval code needed).
3. Compute real `cosine_distance` (new `lsh.py` function) between the entity and every candidate.
4. Build the entity's **own** heap from those distances (bounded to `NEIGHBOR_INDEX_K`).
5. **For every candidate**, also check the *reverse* direction: fetch that candidate's existing heap from `neighbor_index_write`, rebuild it in memory, and call `push_with_eviction` with the new entity. If it changed, persist that candidate's updated heap too and produce a Kafka message for it.
6. Every heap that changed (the new entity's own, plus any existing entity whose heap was mutated) gets flattened, written to `neighbor_index_write`, and produced to the `neighbor-updates` Kafka topic (key = entityId, matching `CLAUDE.md`'s stated Kafka key convention).

**One deliberately small, config-driven parameter worth flagging now:** `NEIGHBOR_INDEX_K` defaults to **3**, not a large number. With only 20 real rows spread across 7 populated shards (2-5 entities each), a large K would mean every entity's candidate set never actually fills up — eviction would never be exercised by real data, and the milestone's own stated verify criterion ("confirm eviction happens correctly") would be untestable at this data scale. K=3 is small enough that indexing even our current 20-row dataset will genuinely exercise eviction, and it's a plain env var, trivial to raise later once more data exists.

---

## 4. Kafka (`kafka_client.py`) and the read-side consumer

- **Producer**: synchronous `kafka-python` `KafkaProducer`, one `send()` per changed heap, keyed by `entityId` (guarantees per-entity ordering, per `CLAUDE.md`).
- **Consumer**: a background thread, started from `main.py`'s existing `lifespan` handler (alongside hyperplane generation), running a blocking consume loop for the service's lifetime. On every message: overwrite the matching row in `neighbor_index_read` wholesale (not a diff — matches `PROJECT_SPEC.md` §5.3's explicit "full list, not a diff... idempotent, safe to replay"), and invalidate that `entityId` in the LRU cache.
- Runs as a thread, not a second process/service — consistent with the "no new service" resolution; FastAPI's own event loop keeps serving HTTP requests independently of it.

## 5. The cache (`cache.py`)

A small manually-managed LRU (`OrderedDict`-based, not `functools.lru_cache`) sitting in front of `GET /neighbors/{entityId}` reads from `neighbor_index_read`. Explicitly **not** `functools.lru_cache`: that decorator has no way to invalidate a single key, and an invalidation hook is not optional here — without it, the cache would keep serving stale neighbor lists forever after a Kafka-driven update, which would demonstrate the *wrong* thing rather than the caching concept. The consumer thread invalidates on every write it makes.

**Simplification worth naming honestly, matching the project's habit:** this cache is in-process only, not Redis-backed. At real scale, with multiple horizontally-scaled instances, an in-process cache wouldn't be shared and you'd want Redis; here, `vector-hasher` isn't wired to Redis at all yet, and adding that wiring just for this one milestone's cache isn't justified by what this milestone needs to demonstrate.

---

## 6. New endpoints (`main.py`)

- **`POST /neighbors/{entity_id}/index`** — the core incremental trigger described above. Returns which entities' heaps actually changed (itself + any evicted-into existing entities).
- **`POST /neighbors/index-all`** — convenience batch wrapper that calls the same logic for every entity currently in `entities`, in order — needed to bootstrap the whole current 20-row dataset without 20 manual calls, same role `/assign-shards` played in M2.
- **`GET /neighbors/{entity_id}`** — read-side lookup: cache first, `neighbor_index_read` on a miss.

New env vars for `vector-hasher` in `docker-compose.yml` (additive only, same pattern as M2): `NEIGHBOR_INDEX_K` (default `3`), `KAFKA_NEIGHBOR_TOPIC` (default `neighbor-updates`), `NEIGHBOR_CACHE_SIZE` (default `32`).

---

## 7. Verification plan

1. **`test_heap.py`**, no I/O: pushing fewer than K entries always inserts; pushing a closer entry once full evicts the farthest and only the farthest; pushing a farther entry once full is a no-op (heap unchanged); `flatten_sorted` returns ascending-by-distance order.
2. **Bootstrap the real dataset**: `POST /neighbors/index-all` against the 20 real M1/M2 rows.
3. **The literal verify criterion, with real data**: pick an entity whose shard already has several members (e.g. shard 7's `N55528`/`N55610`/`N29120`/`N39237`), re-index one of the already-indexed entities' neighbors again or index one more new one, and confirm via direct SQL on `neighbor_index_write` that an **existing** entity's `neighbors` JSONB actually changed — i.e. eviction genuinely occurred, not just that the new entity got a list.
4. **Kafka round-trip**: confirm `neighbor_index_read` ends up matching `neighbor_index_write` for the entities that changed, proving the consumer thread actually processed real messages, not just that the producer sent them.
5. **Cache correctness**: call `GET /neighbors/{entityId}` twice (should be a cache hit the second time — verify via logging/timing), then trigger a change to that entity's neighbors and confirm the very next `GET` reflects the update rather than serving the stale cached value.
6. Log the `NEIGHBOR_INDEX_K=3` and in-process-cache-not-Redis decisions in `PROJECT_SPEC.md` §10, same habit as M1/M2.

---

## Build order (same file-by-file, explain-then-confirm process as M1/M2)

`heap.py` → `test_heap.py` → `cache.py` → `002_neighbor_index_schema.sql` → `db.py` additions → `kafka_client.py` → `neighbor_index.py` → `main.py` additions → `docker-compose.yml` env vars (additive) → run and verify for real, same rigor as M1/M2.
