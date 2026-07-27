# News Recommendation Engine — Master Build Spec

This is the reference document Claude Code should read at the start of any milestone (`read PROJECT_SPEC.md and understand milestone X before proposing a plan`). `CLAUDE.md` holds only the lean, always-loaded rules — everything else, including the "why" behind each decision, lives here.

---

## 0. Prerequisites & dataset schema (verified)

**Download:** MIND-small (train + dev splits) — search "MIND dataset download" for the current mirror link; original Microsoft blob links expire periodically.

**`behaviors.tsv`** — tab-separated, 5 columns, no header row:
```
ImpressionID   UserID   Time                        History              Impressions
1              U131     11/13/2019 8:36:57 AM       N11 N22 N33          N45-1 N46-0 N47-0
```
- `History` = space-separated News IDs the user clicked *before* this impression (this is your Entity History replay source).
- `Impressions` = space-separated `NewsID-label` pairs, label `1` = clicked, `0` = shown-but-not-clicked, for *this* impression.

**`news.tsv`** — tab-separated, fields include: News ID, Category, SubCategory, Title, Abstract, URL, Title Entities, Abstract Entities. **Verify the exact column order against the actual file header during Milestone 1** — treat this as a starting assumption, not gospel, since published mirrors have shown minor structural differences.

## 1. Problem statement

Build a two-stage (retrieval → ranking) recommendation engine for news articles — the same generalizable pattern used by YouTube/TikTok/Amazon-style systems, applied to the **MIND-small** dataset (real articles, real click logs).

Two hard requirements:
- Recommend relevant articles to a user.
- Never recommend an article the user has already seen.

North star: minimize **read latency** — push as much computation as possible to the write path so the serving path is just fetch-and-merge.

---

## 2. Scope decisions — what's real vs. deliberately simplified

State these upfront in any interview — they're deliberate engineering trade-offs, not gaps.

| Decision | What's simplified | Why it's fine to simplify |
|---|---|---|
| **Sharding** | Logical sharding (a `shard_id` column/key, all shards on one Docker-Compose deployment), not physical multi-node clusters | The partitioning *algorithm* is what's being demonstrated — physically distributing across real machines is an ops concern, not a design concern. Easy to say out loud in an interview. |
| **Embedding model** | Pretrained `sentence-transformers` (frozen, no fine-tuning) | Training your own encoder is a different project; this one is about the serving/storage/retrieval pipeline around the embedding, not the embedding model itself. |
| **Vector "hashing"** | Random-hyperplane LSH (SimHash-style) instead of literal quadrant/bounding-box splitting | Real embeddings are 384-dimensional — you can't literally draw bounding boxes. Random-hyperplane LSH is the standard N-dimensional generalization of the exact same idea: similar vectors → similar hash prefix. |
| **Interaction replay** | MIND's real click logs replayed through Kafka as if arriving live, rather than a live production frontend | Gives you 100% authentic interaction data without needing real users — this was the entire point of picking MIND. |

Keep a running note of any other simplification you make during the build — see Section 10 (Defensibility Tracker).

---

## 3. Tech stack

| Component | Stack | Why |
|---|---|---|
| Entity Upload Service | Java / Spring Boot | Matches your primary stack and target SDE/Java roles; same pattern you already used in ContentPulse. |
| Entity Interaction Service | Java / Spring Boot | Same reasoning — this is a plain CRUD/event-producing service, Spring Boot's comfort zone. |
| Recommendation Service | Java / Spring Boot | The most "interview-relevant" service — this is where sharding, caching, merging, and Bloom filter logic all live. |
| Embedding Creator | Python / FastAPI + `sentence-transformers` | Python owns the ML tooling ecosystem; also strengthens your data-adjacent/Python prep story. Mirrors the FastAPI split you already used in ContentPulse. |
| Vector hashing (LSH) + neighbor computation | Python | NumPy makes hyperplane projection and cosine similarity trivial; this logic is compute-heavy, not I/O-heavy, so Python is a fine fit here. |
| Entity DB, Entity History DB, Neighbor Index (both copies) | PostgreSQL | One relational engine, multiple logical schemas/tables — keeps local setup simple. Swap for real distributed stores later if you want to extend the project. |
| Redis | Last-N cache + Bloom filter, sharded via `hash(userId) % N` | Gives you durability/persistence for free (RDB/AOF) instead of hand-rolling S3-snapshot recovery — the trade-off we discussed. |
| Kafka (KRaft mode, no Zookeeper) | Event backbone — interaction events, neighbor-index resync | Ordering-per-key, replay via offsets, decouples ingestion from durable writes. Zookeeper-based Kafka is deprecated — KRaft is the current standard. |
| Docker Compose | Orchestrates Kafka, Postgres, Redis, and all services locally | One-command local spin-up — critical for actually finishing this quickly. |

### 3.1 Inter-service communication — decided explicitly

- **Entity Upload Service → Embedding Creator:** synchronous REST (`POST /embed`). Reasoning: embedding generation for a batch ETL load doesn't need to be decoupled — it's a one-time ingestion job, not a high-throughput live path. Keep it simple.
- **Entity Interaction Service → everything downstream:** asynchronous via Kafka only (per the dual-write discussion earlier) — never a direct synchronous call to the Entity History DB or Redis from the interaction service itself.
- **Neighbor Index write-side → read-side:** asynchronous via Kafka (CDC-style resync), never a direct write to the read copy.

### 3.2 Shard counts — reconciled across layers

Pick these once, put them in one config file, and never hardcode any of them inline in service code:

| Layer | Shard key | Count (suggested) |
|---|---|---|
| Neighbor Index (write) | `simhash_bucket % N` | N = 8 |
| Neighbor Index (read) | `hash(entityId) % N` | N = 8 (can differ from write-side; kept equal here for simplicity) |
| Redis (last-N + Bloom filter) | `hash(userId) % M` | M = 8 |
| Recommendation Service instances | `hash(userId) % P` | P = 4 (fewer than Redis shards is fine — no requirement that these match) |

---

## 4. System architecture diagram

```
 MIND Dataset (title+abstract+category)
              │
              ▼
 ┌─────────────────────────┐
 │ Entity Upload Service     │  Spring Boot · reads MIND articles, registers entity
 │ (Java)                    │  Why: single point of ingestion for new entities
 └────────────┬─────────────┘
              │ POST /embed  (entityId, text)
              ▼
 ┌─────────────────────────┐
 │ Embedding Creator          │  FastAPI · sentence-transformers (384-dim)
 │ (Python)                   │  Why: pretrained model = content vector, not
 └────────────┬─────────────┘  interaction-based — fixes the MovieLens defect
              │ vector: float[384]
    ┌─────────┴─────────┐
    ▼                     ▼
┌───────────────┐  ┌───────────────┐
│ Entity DB       │  │ LSH Hasher      │  Random-hyperplane SimHash
│ entityId→vector │  │ (Python)        │  Why: N-dim generalization of
│ Why: O(1) exact │  │ hash = k bits   │  quadrant-splitting; similar
│ lookup by ID    │  └───────┬───────┘  vectors → similar hash prefix
└───────────────┘            │
                    shard_id = hash % N
                              ▼
                  ┌─────────────────────┐
                  │ Neighbor Index (write) │  Postgres, shard: vector-hash
                  │ per-entity MAX-HEAP    │  Why: O(1) peek-farthest for
                  │ (in-memory)            │  cheap eviction on insert;
                  └──────────┬───────────┘  same-shard = atomic co-update
                              │ on change → produce
                              ▼
                        ┌─────────┐
                        │  Kafka   │  key = entityId → ordering per entity
                        └────┬────┘
                              ▼
                  ┌─────────────────────┐        ┌───────────────────┐
                  │ Neighbor Index (read)  │───────▶│ Neighbor Index      │
                  │ shard: entity-hash     │        │ Cache (LRU)          │
                  │ sorted list (flat)     │        │ Why: absorbs hot-key │
                  │ Why: kills hot         │        │ traffic (viral       │
                  │ partitions from        │        │ articles)            │
                  │ similar-content        │        └───────────────────┘
                  │ clustering             │
                  └─────────────────────┘


 User ──▶ Entity Interaction Service (Java) ──▶ Kafka (key = userId)
                                                        │
                              ┌─────────────────────────┼─────────────────────────┐
                              ▼                                                   ▼
                  ┌─────────────────────┐                             ┌───────────────────┐
                  │ Entity History DB      │  Postgres, shard: userId   │ Redis (per user)     │
                  │ index: entityId,       │  Why: durable source of    │ - last-N entities     │
                  │ 2ndry idx: timestamp   │  truth; batched async      │ - Bloom filter bitmap │
                  └─────────────────────┘  writes, not per-event       │ shard: hash(userId)%N │
                                            (avoids DB as bottleneck)   │ Why: RAM speed +      │
                                                                        │ built-in durability    │
                                                                        └───────────────────┘

 User ──▶ LB ──▶ Recommendation Service (Java, shard: hash(userId)%N)
                        │
                        ├─ 1. Redis: fetch last-N entities + read Bloom filter
                        ├─ 2. Neighbor Cache/Index: fetch precomputed neighbor lists
                        ├─ 3. K-way merge sorted lists (dedupe: keep min distance)
                        ├─ 4. Apply like/dislike scaling factor (soft penalty, not hard filter)
                        ├─ 5. Bloom filter pass per candidate
                        │      → "not seen" = keep, no DB call
                        │      → "possibly seen" = confirm via Entity History DB
                        └─ 6. Return sorted, filtered top-K
```

---

## 5. End-to-end data flow — concrete object shapes

### 5.1 Ingestion: article → embedding → storage

```
Article        { entityId, title, abstract, category, subcategory }
EmbedRequest    { entityId, text: title + " " + abstract }
EmbedResponse   { entityId, vector: float[384] }
```
Stored in Entity DB as `(entityId PK, vector float[384], category, subcategory)`.

### 5.2 Vector hashing & shard assignment

```
hash_bits = [ sign(dot(vector, r_i)) for r_i in random_hyperplanes[0..k-1] ]   # k = 20
hash_value = bits_to_int(hash_bits)          # 0 .. 2^20-1
shard_id   = hash_value % NUM_SHARDS         # NUM_SHARDS = 8 (config-driven)
```
Candidates for nearest-neighbor = all entities in the same (or adjacent, if too few) bucket. Real cosine similarity is then computed only within that small candidate set.

### 5.3 Neighbor index write + Kafka resync

```
NeighborUpdate  { entityId, neighbors: [ {id, distance}, ... ] }   # full list, not a diff
```
Keyed by `entityId` on the Kafka topic — guarantees ordering for updates to the same entity's list. Read-side consumer overwrites the row wholesale on every message (idempotent, safe to replay).

### 5.4 User interaction ingestion

```
InteractionEvent { userId, entityId, interactionType: CLICK|LIKE|DISLIKE, timestamp }
```
Keyed by `userId` → all of one user's events land in order, on the recommendation server instance that owns them.

**MIND data note:** MIND only provides click/no-click signals (label `1`/`0` in `behaviors.tsv`). LIKE and DISLIKE don't exist in the raw data. For the ranking-stage scaling factor demo, simulate a small percentage of clicks as LIKE/DISLIKE (e.g., randomly assign 10% of clicks as LIKE, 5% as DISLIKE) during the replay ETL. **Log this in §10 Defensibility Tracker** — it's a deliberate extension to demonstrate the ranking pipeline, not a claim that the data is organic.

Redis state per user:
```
user:{userId}:lastEntities   → bounded list, max N=5 (ring buffer semantics)
user:{userId}:bloomfilter    → bitmap, sized via m = -(n·ln p)/(ln2)², k ≈ (m/n)·ln2
user:{userId}:signals        → { entityId: LIKE|DISLIKE }
```

### 5.5 Serving path

```
GET /recommendations?userId=55
→ 200 OK
  { userId: 55, recommendations: [ {entityId, title, category, score}, ... ] }
```

---

## 6. Data structures used — and why (kept short on purpose)

| Structure | Used where | Why this one |
|---|---|---|
| Fixed-length float array (384) | Embeddings | Direct numeric ops (cosine/dot product) need fixed dimensionality. |
| Bit string / int (LSH hash) | Vector shard key | Prefix/bit-similarity ≈ spatial similarity → enables range/mod partitioning. |
| Max-heap (in-memory) | Neighbor index, write side | O(log k) insert, O(1) peek-farthest — the exact operation needed to decide evictions on insert. |
| Flat sorted array | Neighbor index, read side | Never mutated in place, so no need to pay heap-maintenance cost on reads. |
| Bit array + k hash functions (Bloom filter) | "Has this user seen X" | O(1) fixed-size check; false positives are safe (fallback to DB), false negatives are impossible by construction. |
| Bounded ring buffer | Last-N entities | O(1) push + evict-oldest, preserves recency order without unbounded growth. |

---

## 7. Milestones — one explore → plan → code → verify cycle each

Do not combine milestones. Finish and verify one before starting the next; run `/compact` in between to keep context clean.

### Milestone 1 — Entity Upload + Embedding pipeline
**Goal:** Ingest MIND articles, generate embeddings, persist to Entity DB.
**Claude Code opening prompt (Plan Mode):**
> Read PROJECT_SPEC.md sections 3-6. I want to build Milestone 1: Entity Upload Service (Spring Boot) that reads MIND-small article data and registers entities, plus an Embedding Creator (FastAPI + sentence-transformers) that returns a 384-dim vector for a given title+abstract. Propose a plan: project structure, the API contract between the two services, the Entity DB schema, and what could go wrong. Don't write code yet.
**Verify:** feed 2 known-similar articles (same category) and 1 unrelated article; confirm embeddings exist, correct dimension, and cosine similarity is visibly higher between the similar pair.

### Milestone 2 — Vector hashing (LSH) + shard assignment
**Goal:** Implement random-hyperplane hashing, bucket assignment, candidate retrieval within a bucket.
**Verify:** the same 2 similar articles from Milestone 1 land in the same or adjacent bucket; the unrelated one does not.

### Milestone 3 — Neighbor Index write-side + Kafka resync to read-side
**Goal:** Max-heap-based neighbor list maintenance on insert, Kafka propagation, entity-hash-sharded read copy + cache.
**Verify:** insert a new article similar to an existing one; confirm the existing entity's neighbor list updates (eviction happens correctly), and the read-side copy reflects it after Kafka consumption.

### Milestone 4 — Entity Interaction ingestion
**Goal:** Replay MIND's real click logs through Kafka; consumers update Entity History DB (batched) and Redis (last-N + Bloom filter).
**Verify:** replay one user's real click history; confirm Redis last-N list and Bloom filter both reflect it correctly, and Entity History DB has the durable record.

### Milestone 5 — Recommendation Service (serving path)
**Goal:** Implement `GET /recommendations` — retrieval, merge, ranking scale, Bloom filter, response.
**Verify:** hit the endpoint for a real MIND user; confirm returned articles exclude everything in their real click history, and ordering reflects distance + any like/dislike signal correctly.

---

## 8. Claude Code operating discipline (recap, applied to this project)

- **Plan Mode first, every milestone.** Toggle with Shift+Tab. No edits until you've read and approved the plan.
- **Demand real verification.** Ask explicitly: "run the tests/build and show me the actual output" — never accept "it works" without evidence.
- **`/compact` after each milestone**, before starting the next — keeps the context budget clean instead of one sprawling session.
- **Use subagents for noisy exploration** (e.g. "explore how MIND's file format is structured" or "run the full test suite and summarize failures") so your main session's context stays focused on decisions, not raw output dumps.
- **One milestone, one PR-sized chunk of work.** Resist the urge to ask for the whole system in one prompt — this defeats the entire point of separating explore/plan/code/verify.

---

## 9. Suggested repo structure

```
news-rec-engine/
├── PROJECT_SPEC.md
├── CLAUDE.md
├── PROGRESS.md
├── docker-compose.yml           # Kafka (KRaft), Postgres, Redis, all services
├── database/
│   └── init/                    # SQL scripts auto-run by Postgres on first boot
├── entity-upload-service/       # Java/Spring Boot
├── entity-interaction-service/  # Java/Spring Boot
├── recommendation-service/      # Java/Spring Boot
├── embedding-creator/           # Python/FastAPI
├── vector-hasher/               # Python — LSH + neighbor computation
└── scripts/
    └── mind-loader/             # one-off ETL: load MIND-small into the pipeline
```

---

## 10. Defensibility tracker (keep this updated as you build)

Log every simplification, workaround, or trade-off made during implementation here — same habit you already used for your Log Anomaly Detection project's interview dossier. Format: decision → what you'd do differently at real scale → why you didn't here.

```
Example entry:
- Decision: single Postgres instance with a shard_id column, not physically separate nodes.
- At real scale: each shard_id would be a separate physical Postgres instance/cluster.
- Why not here: demonstrating the partitioning algorithm doesn't require real hardware;
  this was a deliberate scope cut for a portfolio timeline, not an oversight.
```

### Milestone 1 entries (2026-07-19)

```
- Decision: Entity DB's vector column stored as BYTEA (raw packed little-endian
  float32, 1536 bytes, no header) instead of a native float4[] array or a
  pgvector column.
- At real scale: a dedicated vector type/extension (or a separate vector store
  entirely) would likely back similarity search directly in the database.
- Why not here: this project's whole point is hand-building vector-hash sharding
  and neighbor search in application code (Milestones 2-3) — letting pgvector do
  that job would undercut the demonstration. No SQL-side vector math is ever
  needed since all of it happens in Java/Python; BYTEA avoids float4[]'s clunkier
  JDBC java.sql.Array marshaling for a value that's only ever written once and
  read back in bulk elsewhere.

- Decision: ingestion batch size fixed at 100 rows, ~6 concurrent /embed calls,
  one retry + skip-and-log per failed row (no dead-letter queue, no configurable/
  adaptive concurrency).
- At real scale: you'd want adaptive concurrency, a real dead-letter mechanism
  for failed rows, and probably a job-tracking system instead of an in-memory
  failedIds list returned in the HTTP response.
- Why not here: this is a one-time (or occasionally re-run) ETL job over a
  static ~50k-row dataset, not a high-throughput production pipeline — the
  added infrastructure isn't justified by the actual scale or usage pattern.

- Decision: POST /ingest is a synchronous, blocking HTTP call — no background
  job, job ID, or status-polling endpoint.
- At real scale: a long-running ingestion job would be kicked off asynchronously
  (e.g. @Async, a queue, or a scheduled job) with a separate status-check
  endpoint, so the caller isn't stuck waiting.
- Why not here: MIND-small's news.tsv is a static file ingested once or
  occasionally during development, not a continuously arriving feed — there's
  no operational need for the caller to get an immediate response while the
  job runs in the background.

- Decision: EntityRepository uses plain JdbcTemplate with a hand-written upsert
  SQL statement instead of Spring Data JPA.
- At real scale: a service with many entity types and relationships would
  likely benefit from JPA's abstractions.
- Why not here: this service has exactly one write query, and it specifically
  needs native Postgres upsert (INSERT ... ON CONFLICT) semantics that JPA
  doesn't map to cleanly — raw JDBC is less machinery for one well-understood
  statement, not more.
```

### Milestone 1 — Deliverable D entry (2026-07-19)

```
- Decision: verified the ingestion pipeline end-to-end using POST /ingest?limit=20
  (20 real MIND-small articles) rather than a full run against all ~51,282 rows.
- At real scale: a production rollout would need a full-volume load test to
  characterize actual throughput, memory behavior across many batches, and
  total run time before considering the pipeline production-ready.
- Why not here: 20 rows already exercises every code path that matters for
  correctness — real parsing, the real HTTP call to embedding-creator, the
  real Postgres upsert. A full run mainly adds volume and duration, not new
  correctness risk, and can be run later with zero code changes (it's the
  same endpoint, just without the ?limit param).
```

### Milestone 2 — mod-8 bucket compression, observed with real data (2026-07-19)

```
- Decision: kept shard_id = hash_value % 8 (only 3 of the 20 hash bits actually
  determine the bucket) after observing a concrete real-data case where it
  produced a non-ideal result: real embeddings for two similar sports headlines
  ("Lakers win championship game in overtime thriller" / "NBA finals game ends
  in dramatic overtime victory") differed in only 3 of 20 hash bits (85%
  agreement) — but one of those 3 differing bits happened to be one of the 3
  low-order bits that decide shard_id, so they landed in different, non-adjacent
  shards (0 and 2). An unrelated cooking headline coincidentally landed in the
  same shard as one of the sports articles. adjacent_shard_ids(0, 8) = {7,0,1}
  does not include 2, so the "adjacent bucket" fallback would not have caught
  this pair either.
- At real scale: true multi-probe LSH (querying multiple hash variants formed
  by flipping individual hyperplane bits, not just shard_id +/-1) or a larger
  NUM_SHARDS with a proper re-ranking step over full cosine similarity within
  the candidate set would recover pairs like this.
- Why not here: this is the mod-8 compression tradeoff already anticipated in
  the plan for this milestone, now confirmed against real data rather than
  just argued in the abstract. The full 20-bit hash still carries real
  similarity signal (85% agreement in this case) — production nearest-neighbor
  systems built this way always follow up bucket retrieval with an exact
  distance computation over the (small) candidate set precisely because
  bucket membership alone is an approximation, not a guarantee. That exact
  re-ranking step is explicitly Milestone 3's job (Neighbor Index), not this
  milestone's.
```

### Milestone 3 entries (2026-07-21)

```
- Decision: NEIGHBOR_INDEX_K defaults to 3, not a larger number.
- At real scale: K would typically be larger (10-50+), sized to how many
  recommendations the serving layer actually needs to show.
- Why not here: with only ~20 real rows spread across 7 populated shards
  (2-5 entities each), a larger K would mean most entities' candidate sets
  never fill up — eviction would never be genuinely exercised by real data,
  making this milestone's own stated verify criterion ("confirm eviction
  happens correctly") untestable at this data scale. K=3 is small enough
  that indexing even the current dataset genuinely exercises eviction
  (confirmed live — see PROGRESS.md). Plain env var, trivial to raise later.

- Decision: the read-side cache (cache.py) is in-process (a manual
  OrderedDict-based LRU) rather than Redis-backed.
- At real scale: with multiple horizontally-scaled vector-hasher instances,
  an in-process cache wouldn't be shared across them — you'd want Redis so
  every instance sees the same cached state.
- Why not here: vector-hasher isn't wired to Redis at all yet (Redis is
  currently only used by the Recommendation Service's serving path, per
  the architecture in §3), and adding that wiring just for this one
  milestone's cache isn't justified by what this milestone needs to
  demonstrate — cache invalidation semantics, not distributed caching.

- Decision: the Kafka consumer runs as a fire-and-forget daemon thread
  (started once in main.py's lifespan) with no graceful shutdown, no
  reconnect/retry supervision beyond kafka-python's own defaults, and no
  offset-commit monitoring.
- At real scale: you'd want a supervised consumer (auto-restart on failure,
  health-check integration, graceful shutdown that finishes in-flight
  messages before the process exits).
- Why not here: this is a local demo service with a single instance and a
  short-lived process lifetime — the added supervision machinery isn't
  justified by the actual failure modes at this scale, and a daemon thread
  correctly dies with the process either way.

- Decision (found + fixed during verification, not anticipated in the
  original plan): push_with_eviction had no check for "is this entity_id
  already in the heap" — pushing the same entity twice (which genuinely
  happens across the two different index_entity() calls that can each
  touch the same entity's list) created duplicate neighbor entries instead
  of being a no-op. Confirmed with real data (shard 7's entities each had a
  literal duplicate entry after the first bootstrap run), fixed by checking
  heap membership by entity_id before considering insert/evict, and covered
  with two new regression tests in test_heap.py. This is logged here as a
  correctness bug that was caught by real-data verification, not a
  simplification/tradeoff like the other entries above — included for the
  same reason M2's mod-8 entry was: an honest record of what verification
  actually surfaced, not just what was planned in the abstract.
```

### Milestone 4 entries (2026-07-25)

```
- Decision: entity_history's PRIMARY KEY (source_id, entity_id) does NOT
  collapse genuine repeated History entries (sourceId is "HIST-{userId}-{index}",
  one distinct row per position in the user's History list), while Redis's
  last-N ring buffer (LREM+LPUSH+LTRIM) and Bloom filter DO collapse repeats
  of the same entity for the same user — on purpose, not an oversight.
- At real scale: this asymmetry would still hold — it isn't a scale tradeoff,
  it's a correctness distinction between two different questions.
- Why: Postgres's entity_history is the permanent record of what actually
  happened — every real occurrence (real cross-session duplicate or genuine
  repeated History entry) needs to remain a distinct row. Redis's last-N and
  Bloom filter answer "what's currently recent" and "has this ever been
  seen," respectively — neither of those questions benefits from tracking
  exact repeat counts, so collapsing repeats there is correct, not lossy.
  Noted explicitly here so a future pass doesn't "fix" the two stores to
  match each other without realizing they're intentionally answering
  different questions.

- Decision: entity-interaction-service's JUnit suite (45 tests across
  util/parser/service/repository/controller) is unit-level only — Redis
  (StringRedisTemplate), the JdbcTemplate, and KafkaTemplate are all mocked
  with Mockito; there is no @SpringBootTest, no embedded Kafka, and no
  Testcontainers-backed Postgres/Redis. ReplayService is exercised against
  real temp-file TSVs (it takes plain path strings, so no Spring context is
  needed there either). pom.xml has no spring-kafka-test / testcontainers
  dependency added.
- At real scale: you'd add embedded-Kafka + Testcontainers Postgres/Redis
  integration tests that exercise the actual @KafkaListener wiring, the real
  Lua ring-buffer script, and the real ON CONFLICT upsert — none of which a
  mocked unit test can catch (e.g. a typo in the Lua script or a JDBC type
  mismatch would still pass every test in this suite).
- Why not here: every environment-required property (datasource URL, Kafka
  bootstrap servers, Redis host, MIND file paths) is a raw `${ENV_VAR}` with
  no default in application.yml, and there's no test profile — a
  @SpringBootTest would need all of that wired to even start the context.
  Given this milestone's actual logic (TSV parsing, timeline ordering,
  bit-position math, batch upsert parameter binding, per-event vs.
  per-batch call sequencing) is fully expressible as pure functions plus
  thin adapters around three well-defined client APIs, mocking those
  clients tests the real risk (this service's own logic) without paying for
  a docker-compose-dependent test environment. Kafka/Redis/Postgres wiring
  itself is exercised manually via `docker-compose up` + the milestone's
  stated verify criterion, per CLAUDE.md's build/test commands.

- Decision (found during live verification, not a code change): the local
  `redis` container had a corrupted network attachment (HostConfig said
  `velocity_default`, but `NetworkSettings.Networks` was empty — likely
  stale from being stopped/started many times over the project's life
  without ever being recreated), which made `redis` unresolvable from
  entity-interaction-service. Fixed with
  `docker compose up -d --force-recreate redis`. Not an application bug —
  noted here only because it fully masked the real verification for one
  replay attempt (see next entry) and would silently do so again for
  anyone reusing these long-lived local containers.
- At real scale: a real orchestrator (k8s, ECS) wouldn't let a container's
  network attachment silently drift from its declared config like this.
- Why not here: this is host-local Docker Compose state, not something the
  service or its tests control.

- Decision (found live, FIXED same session): while `redis` was unreachable
  above, `InteractionEventConsumer`'s batch listener didn't retry
  indefinitely — Spring Kafka's default batch error handling
  (`FailedBatchProcessor` -> `FallbackBatchErrorHandler`) retried a few
  times, then logged "Records discarded: interaction-events-4@0..18" and
  moved on. The offsets were committed past those records; the 19 events
  were gone from processing (not just delayed) until userId=U80234 was
  replayed a second time by hand. No dead-letter topic, no alerting, no
  infinite-retry/backoff config existed anywhere in KafkaConsumerConfig.
  Fix: `KafkaConsumerConfig` now builds a `DefaultErrorHandler` from an
  `ExponentialBackOff` (1s initial, x2 multiplier, capped at 30s between
  attempts, `maxElapsedTime`/`maxAttempts` left at their unlimited
  defaults) and wires it onto `batchFactory` via `setCommonErrorHandler`.
  Declined, not implemented: a dead-letter topic/recoverer (no
  poison-message case exists for this listener — every operation is
  idempotent, see the entry above — so a DLQ solves a problem this service
  doesn't have) and wrapping the value deserializer in
  `ErrorHandlingDeserializer` (a real but separate gap: a genuinely
  malformed message would still crash the consumer thread before reaching
  this error handler; different failure mode than the one observed, left
  as future work).
- Bug found while building the fix itself, also fixed: the first attempt
  used `errorHandler.setRetryListeners(record, ex, attempt) -> log.warn(...))`
  as a lambda. `RetryListener` has two `failedDelivery` overloads — a
  single-`ConsumerRecord` one (the only abstract method, so the only one a
  lambda can implement) and a default no-op `ConsumerRecords` (plural) one.
  Since `batchFactory` is a *batch* listener, `ErrorHandlingUtils.retryBatch`
  always calls the plural overload, so the lambda silently never fired —
  confirmed via `kill -3` thread dumps showing the consumer thread legitimately
  parked in retry/backoff (not stuck), while zero log lines appeared. Fixed
  by implementing `RetryListener` as an anonymous class overriding the
  `ConsumerRecords` overload explicitly.
- At real scale: you'd likely still keep the infinite-retry choice (matches
  this project's own "Kafka decouples ingestion from durable writes"
  reasoning in §3) but might add metrics/alerting on retry duration so an
  operator knows when a dependency has been down "too long," rather than
  relying on log lines alone.
- Why an infinite retry (not a DLQ) is fine here: every operation this
  listener performs is idempotent (see the entry above), so there is no
  message that can never succeed — only dependencies that are temporarily
  down. A local demo's failure modes don't include a genuinely poisoned
  `InteractionEvent`.
- Verified live, twice, against the corrected code (`docker network
  disconnect/connect velocity_default redis` to simulate the outage without
  touching host systemd services): a fresh user's batch sat retrying with
  `WARN ... Retrying interaction-events batch of N (attempt K) after: ...`
  logged on each attempt (no `Records discarded`), then — once Redis was
  reconnected, with no manual re-replay — the same batches were consumed
  successfully and `entity_history` + Redis ended up fully and correctly
  populated (96/96 rows for the test user). The dominant delay between
  attempts in this test was Lettuce's default ~60s command timeout (not the
  configured 1s/2s backoff), since the simulated outage was a live
  connection going silent rather than an instant DNS failure — worth knowing
  if this is ever demoed live, but doesn't change correctness.
```
