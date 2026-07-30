# Milestone 5 — Recommendation Service (ideation / design only)

## Context

Milestones 1-4 are done and live-verified: `entities` (Postgres, Milestone 1), the LSH-sharded `neighbor_index_write`/`neighbor_index_read` tables plus vector-hasher's read API (Milestone 2-3), and `entity_history` + Redis last-N/Bloom filter/signals (Milestone 4) are all populated by real data flowing through Kafka. Nothing yet reads all of this back out for a user. Milestone 5 is that read/serving path: `GET /recommendations?userId=` — per `PROJECT_SPEC.md` §7, "retrieval, merge, ranking scale, Bloom filter, response," verified by confirming a real user's results exclude their click history and that ordering reflects distance + like/dislike.

`recommendation-service/` does not exist on disk at all yet (confirmed by exploration) — only a stub entry already sits in `docker-compose.yml`. This is a from-scratch service, not scaffolding to fill in. This document is design-only, per explicit request — no code, no new files, until this is reviewed.

## Key design decisions

### 1. Neighbor lookup: call vector-hasher's existing HTTP endpoint, don't query `neighbor_index_read` directly

vector-hasher already exposes `GET /neighbors/{entityId}` → `{entityId, neighbors: [{entityId, distance}, ...]}`, backed by its own in-process LRU cache (`NEIGHBOR_CACHE_SIZE`) and a 404 for unknown entities. This is the same *kind* of decision already made once in this project (`Entity Upload Service → Embedding Creator: synchronous REST`, §3.1) — reuse that exact pattern (mirror `entity-upload-service`'s `RestClientConfig`/`EmbeddingCreatorClient` shape) rather than opening a second, competing reader of a table vector-hasher already owns and caches. Consequence: `docker-compose.yml` needs a new `VECTOR_HASHER_URL: http://vector-hasher:8000` env var and `vector-hasher` added to `depends_on` for `recommendation-service` (both currently missing).

**Declined alternative**: reading `neighbor_index_read` directly via JDBC from `recommendation-service`. Rejected because it duplicates a query vector-hasher already makes, bypasses its cache entirely (defeating the cache's whole stated purpose — "absorbs hot-key traffic (viral articles)", §4), and creates two independent readers of a table with one designated owner.

### 2. `entities` (title/category) and `entity_history` (seen-check): read directly via JDBC — no new endpoints on other services

Unlike neighbor data, there's no existing API for either lookup, and both are simple reference/fact-table reads. `entities` is already a shared table written by one service (`entity-upload-service`) — `recommendation-service` reading it directly is the same pattern already established (write-owner / read-elsewhere), not a new one. Building a new endpoint on `entity-interaction-service` just to answer "has user X seen entity Y" would be scope creep into an already-finished, already-verified milestone for something a two-line SQL query answers directly. This will be **the first JdbcTemplate SELECT in this codebase** (confirmed: every existing repository is upsert-only) — nothing to mirror, just plain `JdbcTemplate.query(...)`.

### 3. No Kafka in this service at all

`recommendation-service`'s docker-compose stub currently sets `KAFKA_BOOTSTRAP_SERVERS` and depends on `kafka`, but nothing in this design produces or consumes a Kafka message — Redis (kept fresh by `entity-interaction-service`'s consumer) and `neighbor_index_read` (kept fresh by vector-hasher's consumer) are both already-durable, already-fresh state by the time this service reads them. Recommend **dropping `spring-kafka` from `pom.xml`** and **dropping `KAFKA_BOOTSTRAP_SERVERS`/`kafka` from `depends_on`** in docker-compose — this service is a pure synchronous fetch-and-merge read path, which is exactly what §1's stated north star asks for ("push as much computation as possible to the write path so the serving path is just fetch-and-merge").

### 4. Retrieval → merge → rank → filter → respond, concretely

```
GET /recommendations?userId={userId}
```
1. **Seeds (Redis, with Postgres fallback)**: `LRANGE user:{userId}:lastEntities 0 -1` → ordered recent entityIds (≤ `LAST_N_SIZE`, most-recent-first). **An empty result is ambiguous — it means either a genuine cold-start user, or Redis having lost/never held its state (restart, eviction, cache flush) — and must not be treated as "no history" on its own.** On empty, fall back to the durable source: `SELECT entity_id FROM entity_history WHERE user_id = ? ORDER BY event_timestamp DESC LIMIT {LAST_N_SIZE}` (Postgres). This is exactly why `entity_history` exists as the durable record behind Redis's fast/volatile cache — Milestone 4's Kafka consumer writes both from the same batch (`InteractionEventConsumer.consume`), so Postgres is always at least as complete as Redis, and is the correct fallback rather than a second, independent guess. Only if *that* is also empty is this genuinely a cold-start user → return `{userId, recommendations: []}`.
   - **Declined enhancement**: repopulating Redis's `lastEntities` list from this Postgres fallback (self-healing the cache on read). Reasonable for later, but adds a write responsibility to a service designed as Redis-read-only everywhere else in this plan (§2/§3) — left out of this pass, noted here so it isn't silently forgotten.
2. **Neighbors (HTTP, one call per seed, in parallel)**: call vector-hasher's `GET /neighbors/{entityId}` for every seed concurrently (`CompletableFuture.allOf` or equivalent) rather than sequentially — with ≤5 seeds this is cheap, and it directly serves §1's read-latency north star. A 404 for a given seed (not yet indexed) is skipped, not fatal to the request.
3. **Merge + dedupe**: pool every returned neighbor across all seeds into one map `entityId → (minDistance, sourceSeedId)`, per §4's explicit "dedupe: keep min distance" — plus tracking *which seed* produced that minimum, needed for step 4. If the same neighbor arrives from two seeds, only the closer seed's provenance is kept (simplest reading of "keep min distance"; a candidate referenced by a farther LIKE-seed and a closer neutral-seed will use the closer/neutral one — a deliberate, documented simplification, not an oversight).
4. **Soft like/dislike scaling, with a Postgres fallback for the signal itself**: first, `EXISTS user:{userId}:signals`. If it exists, `HGETALL` it once and, for each candidate, look up its *source seed's* signal (not the candidate's own — a candidate that itself has a signal entry would always also be Bloom/history-"seen" and excluded in step 5 anyway, so checking the candidate's own signal would be dead code). If the key is missing (the same Redis-state-loss case as steps 1 and 5 — every processed entity gets a signals entry unconditionally per `InteractionEventConsumer.consume()`, so a healthy user with real seeds always has one; a missing key here means Redis lost it, not that it never existed), fetch the latest `interaction_type` per seed from `entity_history` instead — one batched query over just the (≤5) seed entityIds: `SELECT DISTINCT ON (entity_id) entity_id, interaction_type FROM entity_history WHERE user_id = ? AND entity_id = ANY(?) ORDER BY entity_id, event_timestamp DESC`. This is the same fallback shape as steps 1 and 5 (Redis-missing → confirm against the durable table `entity_history` already tracks it for free, since `entity_history.interaction_type` is written in the same consumer batch as the Redis signal) — a third instance of one consistent pattern: check existence, fall back to Postgres, let Redis self-heal on the user's next real interaction. `LIKE` → multiply distance by 0.8 (closer/better); `DISLIKE` → ×1.5 (farther/worse); `CLICK`/no signal → ×1.0. `score = 1.0 / (1.0 + adjustedDistance)` (higher is better, no clamping needed). These multipliers are placeholder constants, same spirit as Milestone 4's 10%/5% LIKE/DISLIKE simulation ratio — log them in the Defensibility Tracker as a deliberate, tunable demo choice, not a derived value.
5. **Bloom filter, gated by a key-existence check, then a single batched Postgres confirm**: first, `EXISTS user:{userId}:bloomfilter` — **a missing key must not be read as "every bit is 0, so everything is unseen."** `GETBIT` on a key that doesn't exist returns 0 for every position — bit-for-bit indistinguishable from a genuinely unseen entity. A wiped or never-built filter (the same kind of Redis state loss already handled in step 1) would therefore silently pass already-seen articles straight through, with no exception, no empty result, nothing to catch — worse than step 1's gap, since that one at least announces itself as an empty list. So: if the key exists, use the normal per-candidate `mightContain` check (duplicate `BloomFilterService`'s exact formula — `BIT_SIZE=960`, `HASH_COUNT=7`, same `MurmurHash3`, read-only, no `add`) to split candidates into "0 bit → trust as unseen, free" and "all bits set → needs confirming." If the key does *not* exist, the filter can't be trusted for *anyone* this request — every candidate goes into "needs confirming" (it self-heals automatically on this user's next real interaction, since `entity-interaction-service`'s consumer sets these bits on write; nothing here needs to rebuild it). Either way, resolve the whole "needs confirming" set with **one batched query**, not one round-trip per candidate: `SELECT entity_id FROM entity_history WHERE user_id = ? AND entity_id = ANY(?)`. This is cheap even in the worst case (bloom filter missing, whole pool needs confirming) specifically because vector-hasher's `NEIGHBOR_INDEX_K` is only 3 (`PROJECT_SPEC.md` Milestone 3 entry) — the candidate pool going into step 5 is bounded at ≤5 seeds × 3 neighbors = 15 entries before dedup, never large enough to need pagination or partial batching. Exclude anything the batch query confirms as truly seen; sort what's left by score descending; take the top `RECOMMENDATION_TOP_K`.
6. **Response**: batch-fetch `title`/`category` from `entities` for just the surviving entityIds (`WHERE entity_id = ANY(?)`), assemble `{userId, recommendations: [{entityId, title, category, score}, ...]}` in score order.

### 5. Recommendation-service sharding (§3.2's `hash(userId) % 4`) — nothing to build

Same logical-sharding-only treatment already applied to Redis (`M=8`) and the neighbor index (`N=8`) — one physical container regardless. Not worth even computing/logging a shard_id here (unlike `entity_history.shard_id`, nothing downstream ever reads a shard number for this service) — actively decided against adding a no-op `ShardUtil` duplicate just for symmetry.

## Files to create

New Maven service `recommendation-service/` mirroring `entity-interaction-service`'s layout:
- `pom.xml` — Spring Boot 3.3.4 parent, Java 21; `spring-boot-starter-web`, `spring-boot-starter-jdbc`, `postgresql` (runtime), `spring-boot-starter-data-redis`, `spring-boot-starter-test`. No `spring-kafka`, no actuator (no Kafka health to report).
- `Dockerfile` — same two-stage `maven:3.9-eclipse-temurin-21` → `eclipse-temurin:21-jre-alpine` shape as `entity-interaction-service/Dockerfile`.
- `src/main/java/com/velocity/recommendation/`:
  - `RecommendationServiceApplication.java`
  - `controller/RecommendationController.java` — `GET /recommendations?userId=`, `controller/HealthController.java`
  - `service/RecommendationService.java` — orchestrates steps 1-6
  - `client/VectorHasherClient.java` + `config/RestClientConfig.java` — mirrors `entity-upload-service`'s `EmbeddingCreatorClient`/`RestClientConfig` pattern exactly, pointed at `${vector-hasher.base-url}`
  - `service/BloomFilterReadService.java` — read-only `mightContain(userId, entityId)` **and** `exists(userId)` (the key-existence gate, step 5 — a missing key means "don't trust this filter for anyone this request," not "everything is unseen"), constants and formula duplicated from `entity-interaction-service`'s `BloomFilterService`
  - `util/MurmurHash3.java` — duplicated verbatim (no shared library between services, per existing project convention)
  - `repository/EntityLookupRepository.java` — `findByIds(List<String>)` (first SELECT in the codebase)
  - `repository/EntityHistoryLookupRepository.java` — `findSeenEntityIds(userId, List<String> candidateIds)` → `Set<String>`, one batched query covering both the Bloom-positive-confirm and Bloom-missing cases in step 5 (no per-candidate query — see step 5's sizing note); `findRecentEntityIds(userId, limit)` (Redis-empty fallback, step 1); `findLatestInteractionTypes(userId, List<String> seedEntityIds)` → `Map<String, InteractionType>`, one batched `SELECT DISTINCT ON (entity_id) ...` query (signals-missing fallback, step 4)
  - `dto/` — `NeighborEntry`, `RecommendationItem`, `RecommendationResponse` records
  - No `RedisConfig.java` needed — `StringRedisTemplate` is auto-configured by Spring Boot once `spring.data.redis.host/port` are set; unlike `entity-interaction-service`, nothing here needs a custom Lua script bean.
- `src/main/resources/application.yml` — datasource (Postgres), `spring.data.redis.host/port`, `vector-hasher.base-url`, `redis.last-n-size`, `recommendation.top-k` (default 10).
- `docker-compose.yml` edits: add `VECTOR_HASHER_URL`/equivalent env + `vector-hasher` to `depends_on`; drop `KAFKA_BOOTSTRAP_SERVERS` and `kafka` from `depends_on` (§3 above).

## Execution map — build order

Bottom-up: pure logic first (cheap to unit-test in isolation, no mocks needed), then I/O adapters (each testable alone against a mock), then the orchestrator that wires them together, then infra, then live verification. Mirrors how `entity-interaction-service` was actually built this session. A `mvn test` checkpoint closes out every phase before moving to the next — no phase starts on top of an unverified one.

**Phase 1 — Skeleton, buildable and runnable**
`pom.xml`, `Dockerfile`, `application.yml`, `RecommendationServiceApplication.java`, `controller/HealthController.java`.
Checkpoint: `mvn clean package` succeeds; `docker compose up -d --build recommendation-service`; `curl localhost:8083/health` responds. Nothing else exists yet — confirms the base scaffolding before any real logic goes in.

**Phase 2 — Pure logic (no Redis, no Postgres, no HTTP)**
`util/MurmurHash3.java` (duplicated verbatim, already proven correct in `entity-interaction-service`'s test suite — copy, don't re-derive); `dto/` records (`NeighborEntry`, `RecommendationItem`, `RecommendationResponse`, plus an internal candidate record carrying `entityId`/`distance`/`sourceSeedId`); the merge+dedupe+rank step (§4.3/§4.4) as one pure function taking `Map<seedId, List<NeighborEntry>>` + the signals map and returning a scored, sorted candidate list — no I/O, so it's the cheapest and most important thing to get thoroughly unit-tested before anything depends on it, same reasoning as why `UserTimelineBuilder` was built and tested standalone in Milestone 4.
Checkpoint: `mvn test` — the merge/rank logic is fully covered (min-distance-wins dedup, provenance tracking, LIKE/DISLIKE/neutral multipliers, score formula) before it's wired to anything real.

**Phase 3 — I/O adapters, each independently mockable**
- `service/BloomFilterReadService.java` — `exists`/`mightContain`, mirrors the already-tested `entity-interaction-service` formula.
- `repository/EntityLookupRepository.java` — `findByIds`.
- `repository/EntityHistoryLookupRepository.java` — `findSeenEntityIds` (batched) and `findRecentEntityIds` (Redis-empty fallback).
- `client/VectorHasherClient.java` + `config/RestClientConfig.java` — mirrors `entity-upload-service`'s pattern; handle 404 as "skip this seed," not an error.
- small Redis readers for `lastEntities` (LRANGE) and `signals` (HGETALL) — thin enough they may just live as methods on the orchestrator rather than needing their own classes; decide while writing it, not upfront.
Checkpoint: `mvn test` — each adapter tested alone against a mocked `StringRedisTemplate`/`JdbcTemplate`/`RestClient`, same style as `BloomFilterServiceTest`/`EntityHistoryRepositoryTest`/`InteractionEventProducerTest` from Milestone 4.

**Phase 4 — Orchestration**
`service/RecommendationService.java` wires Phases 2+3 together in the order fixed by §4 (seeds w/ fallback → parallel neighbor fetch → merge/rank (Phase 2's pure function) → Bloom-gated batched filter → top-K → entity lookup → response); `controller/RecommendationController.java` — thin, delegates only.
Checkpoint: `mvn test` — orchestrator tested with every dependency mocked (mirrors `InteractionEventConsumerTest`'s style), covering the cold-start-empty, Redis-fallback-used, and Bloom-missing-fallback-used branches explicitly, not just the happy path.

**Phase 5 — Infra wiring**
`docker-compose.yml`: add `VECTOR_HASHER_URL` + `vector-hasher` to `depends_on`; drop `KAFKA_BOOTSTRAP_SERVERS`/`kafka` (§3).
Checkpoint: full `mvn test` suite green, then `docker compose up -d --build recommendation-service`.

**Phase 6 — Live verification**
Exactly the "Verification" section below, run against the real stack and the real users (`U80234`/`U91836`) already seeded earlier this session.

## Verification (once this design is approved and actually implemented)

1. `mvn test` (Docker Java 21, same as Milestone 4) — unit tests per class, mocking `StringRedisTemplate`/`JdbcTemplate`/the vector-hasher `RestClient`, same style as the 47 already in `entity-interaction-service`.
2. `docker compose up -d --build recommendation-service` alongside the already-running stack.
3. Pick a real user already replayed in this session (e.g. `U80234` or `U91836`, both already have real Redis/Postgres state from Milestone 4's verification) and hit `GET /recommendations?userId=...`; confirm none of the returned `entityId`s appear in that user's `entity_history` rows, and that ordering is consistent with distance + any recorded LIKE/DISLIKE signal.
4. Specifically test the Redis-fallback path (step 1): for a user with real `entity_history` rows, manually clear just their Redis key (`redis-cli DEL user:{userId}:lastEntities`, simulating lost cache state without touching Postgres) and confirm `GET /recommendations` still returns sensible results sourced from Postgres, not an empty list.
5. Specifically test the wiped-Bloom-filter path (step 5) — the more dangerous one, since it fails silently rather than visibly: pick a user whose candidate pool (from their real last-N/neighbors) includes at least one entity already in their `entity_history`, confirm it's correctly excluded with the Bloom filter intact, then `redis-cli DEL user:{userId}:bloomfilter` and repeat the same request — confirm that already-seen entity is *still* excluded (via the batched Postgres fallback), not silently let back through.
6. Specifically test the wiped-signals path (step 4): for a user whose seeds include at least one real LIKE or DISLIKE (not just CLICK), confirm the ranking reflects that scaling with the signals hash intact, then `redis-cli DEL user:{userId}:signals` and repeat the same request — confirm the same LIKE/DISLIKE scaling still applies (sourced from `entity_history.interaction_type` via the batched fallback), not silently degraded to neutral (×1.0) for that seed.
7. Log the placeholder ranking constants (step 4) and the min-distance-provenance simplification (step 4/5 boundary) in `PROJECT_SPEC.md` §10.
