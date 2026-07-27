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
1. **Seeds (Redis)**: `LRANGE user:{userId}:lastEntities 0 -1` → ordered recent entityIds (≤ `LAST_N_SIZE`, most-recent-first). Empty → cold-start user → return `{userId, recommendations: []}` immediately, no error.
2. **Neighbors (HTTP, one call per seed, in parallel)**: call vector-hasher's `GET /neighbors/{entityId}` for every seed concurrently (`CompletableFuture.allOf` or equivalent) rather than sequentially — with ≤5 seeds this is cheap, and it directly serves §1's read-latency north star. A 404 for a given seed (not yet indexed) is skipped, not fatal to the request.
3. **Merge + dedupe**: pool every returned neighbor across all seeds into one map `entityId → (minDistance, sourceSeedId)`, per §4's explicit "dedupe: keep min distance" — plus tracking *which seed* produced that minimum, needed for step 4. If the same neighbor arrives from two seeds, only the closer seed's provenance is kept (simplest reading of "keep min distance"; a candidate referenced by a farther LIKE-seed and a closer neutral-seed will use the closer/neutral one — a deliberate, documented simplification, not an oversight).
4. **Soft like/dislike scaling**: fetch `HGETALL user:{userId}:signals` once. For each candidate, look up its *source seed's* signal (not the candidate's own — a candidate that itself has a signal entry would always also be Bloom/history-"seen" and excluded in step 5 anyway, so checking the candidate's own signal would be dead code). `LIKE` → multiply distance by 0.8 (closer/better); `DISLIKE` → ×1.5 (farther/worse); `CLICK`/no signal → ×1.0. `score = 1.0 / (1.0 + adjustedDistance)` (higher is better, no clamping needed). These multipliers are placeholder constants, same spirit as Milestone 4's 10%/5% LIKE/DISLIKE simulation ratio — log them in the Defensibility Tracker as a deliberate, tunable demo choice, not a derived value.
5. **Sort, then Bloom-filter-exclude, keep first K**: sort candidates by score descending, then walk the sorted list checking each against `user:{userId}:bloomfilter` (duplicate the exact `BloomFilterService` bit-position formula — `BIT_SIZE=960`, `HASH_COUNT=7`, same `MurmurHash3`, read-only `mightContain`, no `add`). Any 0 bit → definitely unseen → keep. All bits set → confirm via `SELECT 1 FROM entity_history WHERE user_id=? AND entity_id=? LIMIT 1`; a real row → exclude; no row (false positive) → keep anyway. Stop once `RECOMMENDATION_TOP_K` (config, default 10) survivors are collected.
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
  - `service/BloomFilterReadService.java` — read-only `mightContain(userId, entityId)`, constants and formula duplicated from `entity-interaction-service`'s `BloomFilterService`
  - `util/MurmurHash3.java` — duplicated verbatim (no shared library between services, per existing project convention)
  - `repository/EntityLookupRepository.java` — `findByIds(List<String>)` (first SELECT in the codebase)
  - `repository/EntityHistoryLookupRepository.java` — `exists(userId, entityId)`
  - `dto/` — `NeighborEntry`, `RecommendationItem`, `RecommendationResponse` records
  - No `RedisConfig.java` needed — `StringRedisTemplate` is auto-configured by Spring Boot once `spring.data.redis.host/port` are set; unlike `entity-interaction-service`, nothing here needs a custom Lua script bean.
- `src/main/resources/application.yml` — datasource (Postgres), `spring.data.redis.host/port`, `vector-hasher.base-url`, `redis.last-n-size`, `recommendation.top-k` (default 10).
- `docker-compose.yml` edits: add `VECTOR_HASHER_URL`/equivalent env + `vector-hasher` to `depends_on`; drop `KAFKA_BOOTSTRAP_SERVERS` and `kafka` from `depends_on` (§3 above).

## Verification (once this design is approved and actually implemented)

1. `mvn test` (Docker Java 21, same as Milestone 4) — unit tests per class, mocking `StringRedisTemplate`/`JdbcTemplate`/the vector-hasher `RestClient`, same style as the 47 already in `entity-interaction-service`.
2. `docker compose up -d --build recommendation-service` alongside the already-running stack.
3. Pick a real user already replayed in this session (e.g. `U80234` or `U91836`, both already have real Redis/Postgres state from Milestone 4's verification) and hit `GET /recommendations?userId=...`; confirm none of the returned `entityId`s appear in that user's `entity_history` rows, and that ordering is consistent with distance + any recorded LIKE/DISLIKE signal.
4. Log the placeholder ranking constants (step 4) and the min-distance-provenance simplification (step 4/5 boundary) in `PROJECT_SPEC.md` §10.
